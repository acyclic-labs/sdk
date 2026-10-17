//! Independent post-admission checks; never call the manifest parser or validator.
use acyclic_manifest::{
    DeploymentManifest, EntryRuntime, Environment, MAX_JSON_BYTES, ResourcePattern,
};

fn inbox_authorized(
    env: &Environment,
    entry: &str,
    binding: &str,
    inbox: &ResourcePattern,
) -> bool {
    let Some(entry) = env.entries.iter().find(|candidate| candidate.name == entry) else {
        return false;
    };
    let Some(binding) = env.bindings.iter().find(|candidate| {
        candidate.name() == binding && entry.bindings.iter().any(|name| name == binding)
    }) else {
        return false;
    };
    let Some(grant) = env
        .grants
        .iter()
        .find(|grant| grant.name == binding.capability())
    else {
        return false;
    };
    let EntryRuntime::Actor { subscriptions, .. } = &entry.runtime else {
        return false;
    };
    let subscribed = subscriptions.iter().any(|subscription| {
        let encoded = subscription
            .stream_path()
            .split('/')
            .map(|part| acyclic_auth::encode_component(part.as_bytes()))
            .collect::<Vec<_>>()
            .join("/");
        inbox.name == format!("streams:/{encoded}")
    });
    !inbox.prefix
        && subscribed
        && grant
            .grant
            .actions
            .iter()
            .any(|action| action == "streams/append")
        && acyclic_auth::authority::resource_matches(&grant.grant.resource, &inbox.name)
}

/// Detect admission bugs independently of round-trip and code-generation checks.
pub fn check(manifest: &DeploymentManifest) -> Result<(), &'static str> {
    if serde_json::to_vec(manifest).map_err(|_| "encode")?.len() > MAX_JSON_BYTES {
        return Err("json-size");
    }
    for env in &manifest.environments {
        for cron in &env.crons {
            if !inbox_authorized(env, &cron.entry, &cron.inbox_binding, &cron.inbox) {
                return Err("inbox-authority");
            }
        }
        for entry in &env.entries {
            if let EntryRuntime::Actor {
                timer_inbox: Some(inbox),
                ..
            } = &entry.runtime
                && !inbox_authorized(env, &entry.name, &inbox.binding, &inbox.stream)
            {
                return Err("inbox-authority");
            }
        }
        for asset in &env.assets {
            let binding = env
                .bindings
                .iter()
                .find(|binding| binding.name() == asset.binding)
                .ok_or("asset-binding")?;
            let grant = env
                .grants
                .iter()
                .find(|grant| grant.name == binding.capability())
                .ok_or("asset-grant")?;
            let bucket = grant
                .grant
                .resource
                .name
                .strip_prefix("objects:/")
                .and_then(|path| path.split('/').next())
                .ok_or("asset-bucket")?;
            let bucket = acyclic_auth::decode_component(bucket).map_err(|_| "asset-bucket")?;
            // Test real object keys, not an encoded directory marker.
            for suffix in ["index.html", "nested/app.js", "space and %/unicode-λ.css"] {
                let key = format!("assets/{}/{suffix}", asset.bundle_sha256);
                let resource = acyclic_auth::objects_resource(&bucket, key.as_bytes());
                if !grant
                    .grant
                    .actions
                    .iter()
                    .any(|action| action == "objects/get")
                    || !acyclic_auth::authority::resource_matches(&grant.grant.resource, &resource)
                {
                    return Err("asset-object-authority");
                }
            }
        }
    }
    Ok(())
}
