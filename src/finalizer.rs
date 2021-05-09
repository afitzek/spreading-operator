use k8s_openapi::api::core::v1::Secret;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Error};
use serde_json::{json, Value};

const FINALIZER_NAME: &str = "secretspreading.fitzek.eu/finalizer";

pub async fn add(client: Client, name: &str, namespace: &str, sec: &Secret) -> Result<(), Error> {
    if sec.metadata.finalizers.is_some() {
        if sec.metadata.finalizers.clone().unwrap().iter().any(|s| s.eq_ignore_ascii_case(FINALIZER_NAME)) {
            return Ok(());
        }
    }

    let api: Api<Secret> = Api::namespaced(client, namespace);
    let finalizers = sec.metadata.finalizers.clone();
    let mut fin: Vec<String>;
    if finalizers.is_none() {
        fin = vec![FINALIZER_NAME.to_string()];
    } else {
        fin = finalizers.unwrap();
        if fin.iter().find(|&f| f.eq_ignore_ascii_case(FINALIZER_NAME)).is_none() {
            fin.push(FINALIZER_NAME.to_string());
        }
    }

    let finalizer: Value = json!({
        "metadata": {
            "finalizers": fin
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch).await?;
    Ok(())
}

pub async fn rm(client: Client, name: &str, namespace: &str, sec: &Secret) -> Result<(), Error> {
    let api: Api<Secret> = Api::namespaced(client, namespace);
    let finalizers = sec.metadata.finalizers.clone();

    if finalizers.is_some() {
        let fin:Vec<String> = finalizers.unwrap().iter().filter(|&f| !f.eq_ignore_ascii_case(FINALIZER_NAME)).cloned().collect();

        let finalizer: Value = json!({
            "metadata": {
                "finalizers": fin
            }
        });

        let patch: Patch<&Value> = Patch::Merge(&finalizer);
        api.patch(name, &PatchParams::default(), &patch).await?;
    }

    Ok(())
}
