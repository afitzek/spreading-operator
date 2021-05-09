use std::collections::BTreeMap;

use futures::stream::StreamExt;
use kube::Resource;
use kube::{api::{ListParams, PostParams, DeleteParams, PatchParams, Patch}, client::Client, Api};
use kube_runtime::controller::{Context, ReconcilerAction};
use kube_runtime::Controller;
use tokio::time::Duration;

use k8s_openapi::{Metadata, api::core::v1::{Secret, Namespace}};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta};

use serde_json::{json, Value};

mod finalizer;

const OWNER_ANNOTATION: &str = "eu.fitzek.spread.owner";

#[tokio::main]
async fn main() {
    // First, a Kubernetes client must be obtained using the `kube` crate
    // The client will later be moved to the custom controller
    let kubernetes_client: Client = Client::try_default()
        .await
        .expect("Expected a valid KUBECONFIG environment variable.");

    let secret_api: Api<Secret> = Api::all(kubernetes_client.clone());
    let context: Context<ContextData> = Context::new(ContextData::new(kubernetes_client.clone()));

    Controller::new(secret_api.clone(), ListParams::default())
        .run(reconcile, on_error, context)
        .for_each(|reconciliation_result| async move {
            match reconciliation_result {
                Ok(_echo_resource) => {
                    //println!("Reconciliation successful. Resource: {:?}", echo_resource);
                }
                Err(reconciliation_err) => {
                    eprintln!("Reconciliation error: {:?}", reconciliation_err)
                }
            }
        })
        .await;
}

/// Context injected with each `reconcile` and `on_error` method invocation.
struct ContextData {
    /// Kubernetes client to make Kubernetes API requests with. Required for K8S resource management.
    client: Client,
}

impl ContextData {
    /// Constructs a new instance of ContextData.
    ///
    /// # Arguments:
    /// - `client`: A Kubernetes client to make Kubernetes REST API requests with. Resources
    /// will be created and deleted with this client.
    pub fn new(client: Client) -> Self {
        ContextData { client }
    }
}

/// All errors possible to occur during reconciliation
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Any error originating from the `kube-rs` crate
    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },
    /// Error in user input or Echo resource definition, typically missing fields.
    #[error("Invalid Echo CRD: {0}")]
    UserInputError(String),
    #[error("Missing Object key: {name}")]
    MissingObjectKey {
        name: &'static str
    },
}

async fn get_target_namespace(sec: &Secret) -> Option<String> {
    match &sec.metadata.annotations {
        Some(a) => {
            match a.iter().find(|x| x.0.eq_ignore_ascii_case("eu.fitzek.spread.target-namespace")) {
                Some(x) => {
                    Some(x.1.clone())
                },
                None => None
            }
        },
        None => None
    }
}

async fn reconcile(sec: Secret, context: Context<ContextData>) -> Result<ReconcilerAction, Error> {
    let target_namespace = get_target_namespace(&sec).await;

    if target_namespace.is_none() {
        return Ok(ReconcilerAction {
            // Check every 5 minutes if an annotation was added
            requeue_after: Some(Duration::from_secs(300)),
        })
    }

    let source_namespace: String = match sec.namespace() {
        None => {
            return Err(Error::UserInputError(
                "Expected Secret resource to be namespaced. Can't deploy to an unknown namespace."
                    .to_owned(),
            ));
        }
        // If namespace is known, proceed. In a more advanced version of the operator, perhaps
        // the namespace could be checked for existence first.
        Some(namespace) => namespace,
    };

    let source_uid: String = match &sec.metadata().uid {
        None => {
            return Err(Error::UserInputError(
                "Expected Secret resource to have an uid"
                    .to_owned(),
            ));
        },
        Some(v) => v.clone(),
    };

    let target_namespace_name = target_namespace.unwrap();

    let name = sec.name();

    if sec.metadata.deletion_timestamp.is_some() {
        secret_cleanup(sec, context, source_namespace, name, source_uid).await
    } else {
        sync_secret(sec, context, source_uid, source_namespace, name, target_namespace_name).await
    }
}


async fn sync_secret(sec: Secret, context: Context<ContextData>, source_uid: String, source_namespace: String, name: String, target_namespace_name: String) -> Result<ReconcilerAction, Error> {
    let client: Client = context.get_ref().client.clone();

    finalizer::add(client.clone(), &name, &source_namespace, &sec).await?;

    let namespaces: Vec<String>;
    if target_namespace_name == "*" {
        let namespace_api: Api<Namespace> = Api::all(client.clone());
        let lp = ListParams::default();
        namespaces = (namespace_api.list(&lp).await?).iter().map(|ns| ns.name().clone()).collect();
    } else {
        namespaces = target_namespace_name.split(",").map(|s| s.to_owned()).collect();
    }

    println!("=> Secret in {}.{}", &source_namespace, &name);

    for ns in namespaces {
        if ns == source_namespace {
            println!("   Skipping source ns {}", ns);
            continue;
        }
        let secret_api: Api<Secret> = Api::namespaced(client.clone(), &ns);
        let target_secret = match secret_api.get(&name).await {
            Ok(v) => Ok(Some(v)), // a secret with this name already exists
            Err(kube::Error::Api(kube::error::ErrorResponse{
                code: 404,
                ..
            })) => Ok(None), // the secret does not exist in the target namespace yet
            Err(e) => Err(e)
        }?;

        if target_secret.is_none() {
            println!("   Syncing (create new) {} ({}) to {}", &name, &source_uid, &ns);
            let mut target_labels: BTreeMap<String, String> = match sec.metadata.labels.clone() {
                Some(v) => v,
                None => BTreeMap::new()
            };
            target_labels.insert(OWNER_ANNOTATION.to_string(), source_uid.clone());

            let new_secret = Secret{
                type_: sec.type_.clone(),
                string_data: sec.string_data.clone(),
                data: sec.data.clone(),
                metadata: ObjectMeta{
                    name: Some(name.clone()),
                    namespace: Some(ns.clone()),
                    labels: Some(target_labels),
                    ..Default::default()
                }
            };

            let pp = PostParams{
                dry_run: false,
                field_manager: None
            };
            secret_api.create(&pp, &new_secret).await?;
        } else {
            let existing_secret = target_secret.unwrap();
            let s = match &existing_secret.metadata.labels {
                None => None,
                Some(v) => v.iter().find(|&a| a.0.eq_ignore_ascii_case(OWNER_ANNOTATION)),
            };
            if s.is_some() {
                if existing_secret.data.ne(&sec.data) {
                    // sync data
                    println!("   Updating data");
                    let data: Value = json!({
                        "data": sec.data.clone()
                    });
                    let pp = PatchParams::default();
                    secret_api.patch(&existing_secret.name(), &pp, &Patch::Merge(&data)).await?;
                }
            } else {
                println!("   There is an unmanaged secert with the same name already in {}", ns);
            }
        }
    }

    // Performs action as decided by the `determine_action` function.
    Ok(ReconcilerAction {
        // Finalizer is added, deployment is deployed, re-check in 10 seconds.
        requeue_after: Some(Duration::from_secs(60)),
    })
}

async fn secret_cleanup(sec: Secret, context: Context<ContextData>, source_namespace: String, name: String, source_uid: String) -> Result<ReconcilerAction, Error> {
    let client: Client = context.get_ref().client.clone();

    let secret_api: Api<Secret> = Api::all(client.clone());

    let lp = ListParams::default().labels(format!("{}={}", OWNER_ANNOTATION, source_uid).as_str());

    let secrets = secret_api.list(&lp).await?;

    for secret in secrets {
        println!("=> Cleaning up secert in {}.{}", secret.name(), secret.namespace().unwrap());
        let dp = DeleteParams::default();
        let ns_secret_api: Api<Secret> = Api::namespaced(client.clone(), secret.namespace().unwrap().as_str());
        ns_secret_api.delete(secret.name().as_str(), &dp).await?;
    }

    finalizer::rm(client.clone(), &name, &source_namespace, &sec).await?;

    Ok(ReconcilerAction {
        // Finalizer is added, deployment is deployed, re-check in 10 seconds.
        requeue_after: None,
    })
}


/// Actions to be taken when a reconciliation fails - for whatever reason.
/// Prints out the error to `stderr` and requeues the resource for another reconciliation after
/// five seconds.
///
/// # Arguments
/// - `error`: A reference to the `kube::Error` that occurred during reconciliation.
/// - `_context`: Unused argument. Context Data "injected" automatically by kube-rs.
fn on_error(error: &Error, _context: Context<ContextData>) -> ReconcilerAction {
    eprintln!("Reconciliation error:\n{:?}", error);
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}