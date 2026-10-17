use super::*;
use std::collections::{HashMap, HashSet};

/// Emit structural schema; deployment admission additionally requires validate.
pub fn json_schema() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schemars::schema_for!(DeploymentManifest)).map(|s| s + "\n")
}
/// Emit manifest declarations and imported descriptor-owned JSON shapes.
pub fn typescript() -> Result<String, String> {
    let mut output = String::from("// Generated from acyclic_manifest. Do not edit.\n");
    // Imported JSON shapes come from their existing descriptors, not replicas.
    for source in [
        wire::typescript::<Binding>()?,
        wire::typescript::<ActorLimits>()?,
        wire::typescript::<SubscriptionSpec>()?,
        wire::typescript::<ResourceGrant>()?,
        wire::typescript::<Limits>()?,
    ] {
        let mut seen = output
            .lines()
            .filter(|line| line.starts_with("export type "))
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        let mut include = true;
        for line in source.lines() {
            if line.starts_with("export type ") {
                include = seen.insert(line.to_owned());
            }
            if include {
                output.push_str(line);
                output.push('\n');
            }
        }
    }
    let config = ts_rs::Config::default();
    macro_rules! declarations { ($($ty:ty),+ $(,)?) => {$(
        output.push_str("export "); output.push_str(&<$ty as TS>::decl(&config)); output.push('\n');
    )+}; }
    declarations!(
        DeploymentManifest,
        Environment,
        Entry,
        EntryRuntime,
        TimerInbox,
        WorkerLimits,
        FetchBinding,
        BindingGrant,
        SecretRef,
        SecretReference,
        EgressRule,
        CronSpec,
        AssetSpec,
        AssetCache,
        RouteSpec
    );
    Ok(output
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n")
}
/// Generate only an entry's selected env handles using hash-indexed names.
pub fn environment_typescript(
    manifest: &DeploymentManifest,
    environment: &str,
    entry: &str,
) -> Result<String, ManifestError> {
    validate(manifest)?;
    let environment = manifest
        .environments
        .iter()
        .find(|e| e.name == environment)
        .ok_or_else(|| ManifestError::invalid("environment", "unknown-environment"))?;
    let entry = environment
        .entries
        .iter()
        .find(|e| e.name == entry)
        .ok_or_else(|| ManifestError::invalid("entry", "unknown-entry"))?;
    let bindings: HashMap<_, _> = environment.bindings.iter().map(|b| (b.name(), b)).collect();
    let grants: HashMap<_, _> = environment
        .grants
        .iter()
        .map(|g| (g.name.as_str(), g))
        .collect();
    let mut output = String::from(
        "// Generated from the admitted deployment manifest. Do not edit.\nexport type Env<Streams, Fs, Objects, Actors, Workers, Machines, Execution, Infer, Billing, Identity, Fetch, Secret> = {\n",
    );
    for name in &entry.bindings {
        let binding = bindings
            .get(name.as_str())
            .ok_or_else(|| ManifestError::invalid("bindings", "unknown-binding"))?;
        let grant = grants
            .get(binding.capability())
            .ok_or_else(|| ManifestError::invalid("grants", "unknown-grant"))?;
        let parameter = match grant.grant.resource.name.split_once(":/").map(|(p, _)| p) {
            Some("streams") => "Streams",
            Some("fs") => "Fs",
            Some("objects") => "Objects",
            Some("actors") => "Actors",
            Some("workers") => "Workers",
            Some("machines") => "Machines",
            Some("execution") => "Execution",
            Some("infer") => "Infer",
            Some("billing") => "Billing",
            Some("identity") => "Identity",
            _ => return Err(ManifestError::invalid("resource", "unknown-product")),
        };
        output.push_str(&format!("  readonly {name}: {parameter};\n"));
    }
    for name in &entry.secrets {
        output.push_str(&format!("  readonly {name}: Secret;\n"));
    }
    if let Some(fetch) = &entry.fetch {
        output.push_str(&format!("  readonly {}: Fetch;\n", fetch.name));
    }
    output.push_str("};\n");
    Ok(output)
}
