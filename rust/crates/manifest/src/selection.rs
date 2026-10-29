//! A selected entry retains only its declared configuration and authority.
use crate::{DeploymentManifest, Environment, ManifestError, validate};
use std::collections::HashSet;

/// Select one entry for immutable publication through the existing manifest
/// format. Unselected entries, bindings, grants and secrets cannot influence
/// its identity or become available when the published entry is restored.
///
/// Admission examines the input once; projection moves selected data.
pub fn select_entry(
    manifest: DeploymentManifest,
    environment: &str,
    entry: &str,
) -> Result<DeploymentManifest, ManifestError> {
    validate(&manifest)?;
    let environment = manifest
        .environments
        .into_iter()
        .find(|candidate| candidate.name == environment)
        .ok_or_else(|| ManifestError::invalid("environment", "unknown-environment"))?;
    let entry = environment
        .entries
        .into_iter()
        .find(|candidate| candidate.name == entry)
        .ok_or_else(|| ManifestError::invalid("entry", "unknown-entry"))?;
    let bindings: HashSet<_> = entry.bindings.iter().map(String::as_str).collect();
    let secrets: HashSet<_> = entry.secrets.iter().map(String::as_str).collect();
    let selected_bindings: Vec<_> = environment
        .bindings
        .into_iter()
        .filter(|binding| bindings.contains(binding.name()))
        .collect();
    let grants: HashSet<_> = selected_bindings
        .iter()
        .map(|binding| binding.capability())
        .collect();
    let selected_grants = environment
        .grants
        .into_iter()
        .filter(|grant| grants.contains(grant.name.as_str()))
        .collect();
    Ok(DeploymentManifest {
        schema_version: manifest.schema_version,
        environments: vec![Environment {
            name: environment.name,
            bindings: selected_bindings,
            grants: selected_grants,
            secrets: environment
                .secrets
                .into_iter()
                .filter(|secret| secrets.contains(secret.name.as_str()))
                .collect(),
            crons: environment
                .crons
                .into_iter()
                .filter(|cron| cron.entry == entry.name)
                .collect(),
            assets: environment
                .assets
                .into_iter()
                .filter(|asset| asset.entry == entry.name)
                .collect(),
            routes: environment
                .routes
                .into_iter()
                .filter(|route| route.entry == entry.name)
                .collect(),
            worker_routes: environment
                .worker_routes
                .into_iter()
                .filter(|route| route.entry.as_ref().is_none_or(|name| *name == entry.name))
                .collect(),
            entries: vec![entry],
        }],
    })
}
