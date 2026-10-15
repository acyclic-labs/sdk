use super::*;
use std::collections::BTreeSet;

/// Manifest admission failure. Errors identify fields without exposing credentials.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// JSON did not match the Rust-owned declaration.
    #[error("invalid manifest JSON: {0}")]
    Json(serde_json::Error),
    /// A field or cross-reference violates deployment rules.
    #[error("invalid manifest: {0}")]
    Invalid(String),
}

fn require(condition: bool, field: &str) -> Result<(), ManifestError> {
    if condition {
        Ok(())
    } else {
        Err(ManifestError::Invalid(field.into()))
    }
}

fn reference(value: &str) -> bool {
    !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control)
}

fn name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn unique<'a>(values: impl IntoIterator<Item = &'a str>, field: &str) -> Result<(), ManifestError> {
    let mut seen = BTreeSet::new();
    for value in values {
        require(seen.insert(value), field)?;
    }
    Ok(())
}

fn hostname(value: &str) -> bool {
    value.len() <= 253
        && value.contains('.')
        && value.parse::<std::net::IpAddr>().is_err()
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
        && value != "localhost"
        && !value.ends_with(".localhost")
        && !value.ends_with(".local")
}

fn cron_field(field: &str, min: u8, max: u8) -> bool {
    if !field
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'*' | b',' | b'-' | b'/'))
    {
        return false;
    }
    field.split(',').all(|part| {
        let (range, step) = part
            .split_once('/')
            .map_or((part, None), |(range, step)| (range, Some(step)));
        if let Some(step) = step {
            let Ok(step) = step.parse::<u8>() else {
                return false;
            };
            if step == 0 || step > max - min + 1 {
                return false;
            }
        }
        if range == "*" {
            return true;
        }
        let (start, end) = range.split_once('-').unwrap_or((range, range));
        let (Ok(start), Ok(end)) = (start.parse::<u8>(), end.parse::<u8>()) else {
            return false;
        };
        min <= start && start <= end && end <= max
    })
}

fn cron(schedule: &str) -> bool {
    if !schedule.is_ascii() {
        return false;
    }
    let mut fields = schedule.split_whitespace();
    [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]
        .into_iter()
        .all(|(min, max)| {
            fields
                .next()
                .is_some_and(|field| cron_field(field, min, max))
        })
        && fields.next().is_none()
}

fn entry<'a>(environment: &'a Environment, target: &str) -> Result<&'a Entry, ManifestError> {
    environment
        .entries
        .iter()
        .find(|entry| entry.name == target)
        .ok_or_else(|| ManifestError::Invalid("unknown target entry".into()))
}

fn selected_binding(
    environment: &Environment,
    target: &str,
    binding: &str,
    kind: BindingKind,
) -> Result<(), ManifestError> {
    let entry = entry(environment, target)?;
    require(
        entry.bindings.iter().any(|name| name == binding)
            && environment
                .bindings
                .iter()
                .any(|value| value.name == binding && value.kind == kind),
        "unselected or incompatible target binding",
    )
}

/// Validate the entire deployment before publication, including selected handles and cron targets.
pub fn validate(manifest: &DeploymentManifest) -> Result<(), ManifestError> {
    require(manifest.schema_version == 1, "schemaVersion")?;
    require(!manifest.environments.is_empty(), "environments")?;
    unique(
        manifest
            .environments
            .iter()
            .map(|value| value.name.as_str()),
        "duplicate environment",
    )?;
    for environment in &manifest.environments {
        require(name(&environment.name), "environment name")?;
        require(!environment.entries.is_empty(), "entries")?;
        unique(
            environment.entries.iter().map(|value| value.name.as_str()),
            "duplicate entry",
        )?;
        unique(
            environment
                .bindings
                .iter()
                .map(|value| value.name.as_str())
                .chain(environment.secrets.iter().map(|value| value.name.as_str())),
            "duplicate env property",
        )?;
        for binding in &environment.bindings {
            require(
                name(&binding.name)
                    && reference(&binding.resource)
                    && reference(&binding.capability),
                "binding",
            )?;
        }
        for secret in &environment.secrets {
            require(
                name(&secret.name) && reference(&secret.reference),
                "secret reference",
            )?;
        }
        for entry in &environment.entries {
            require(
                name(&entry.name) && name(&entry.module),
                "entry name/module",
            )?;
            require(
                entry.artifact_sha256.len() == 64
                    && entry
                        .artifact_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                "artifactSha256",
            )?;
            unique(
                entry.bindings.iter().map(String::as_str),
                "duplicate selected binding",
            )?;
            unique(
                entry.secrets.iter().map(String::as_str),
                "duplicate selected secret",
            )?;
            for binding in &entry.bindings {
                require(
                    environment
                        .bindings
                        .iter()
                        .any(|value| &value.name == binding),
                    "unknown selected binding",
                )?;
            }
            for secret in &entry.secrets {
                require(
                    environment
                        .secrets
                        .iter()
                        .any(|value| &value.name == secret),
                    "unknown selected secret",
                )?;
            }
        }
        let mut destinations = BTreeSet::new();
        for rule in &environment.egress {
            require(hostname(&rule.host) && rule.port != 0, "egress authority")?;
            require(
                destinations.insert((&rule.host, rule.port)),
                "duplicate egress authority",
            )?;
        }
        unique(
            environment.crons.iter().map(|value| value.name.as_str()),
            "duplicate cron",
        )?;
        for schedule in &environment.crons {
            require(
                name(&schedule.name) && cron(&schedule.schedule),
                "cron name/schedule",
            )?;
            require(
                entry(environment, &schedule.entry)?.kind == EntryKind::Actor,
                "cron target must be an actor",
            )?;
            selected_binding(
                environment,
                &schedule.entry,
                &schedule.inbox_binding,
                BindingKind::Streams,
            )?;
            require(
                acyclic_stream::StreamPath::new(&schedule.inbox_stream).is_ok(),
                "cron inbox stream",
            )?;
        }
        for asset in &environment.assets {
            require(reference(&asset.prefix), "asset prefix")?;
            selected_binding(
                environment,
                &asset.entry,
                &asset.binding,
                BindingKind::Objects,
            )?;
        }
        let mut routes = BTreeSet::new();
        for route in &environment.routes {
            entry(environment, &route.entry)?;
            require(
                hostname(&route.host)
                    && route.path_prefix.starts_with('/')
                    && !route.path_prefix.contains(['?', '#', '\\'])
                    && !route.path_prefix.chars().any(char::is_control),
                "route",
            )?;
            require(
                routes.insert((&route.host, &route.path_prefix)),
                "duplicate route",
            )?;
        }
    }
    Ok(())
}
