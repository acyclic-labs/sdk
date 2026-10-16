use super::*;
use acyclic_auth::authority::resource_matches as scope;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("manifest {path}: {rule}")]
/// Structured admission failure that never includes submitted credential values.
pub struct ManifestError {
    /// Location of the failing declaration, or $ for structural JSON errors.
    pub path: String,
    /// Stable rule identifier.
    pub rule: String,
}
impl ManifestError {
    /// Build a diagnostic from trusted schema names and rule identifiers.
    pub fn invalid(path: &str, rule: &str) -> Self {
        Self {
            path: path.into(),
            rule: rule.into(),
        }
    }
    pub(crate) fn json(_: serde_json::Error) -> Self {
        Self::invalid("$", "invalid-json-or-contract")
    }
}
fn require(condition: bool, path: &str, rule: &str) -> Result<(), ManifestError> {
    if condition {
        Ok(())
    } else {
        Err(ManifestError::invalid(path, rule))
    }
}
pub(crate) fn bound_json(value: &serde_json::Value) -> Result<usize, ManifestError> {
    fn visit(
        value: &serde_json::Value,
        work: &mut usize,
        nodes: &mut usize,
    ) -> Result<(), ManifestError> {
        *nodes += 1;
        *work += 1;
        require(*nodes <= MAX_NODES, "$", "node-limit")?;
        match value {
            serde_json::Value::Array(items) => {
                require(items.len() <= MAX_ITEMS, "$", "collection-limit")?;
                for item in items {
                    visit(item, work, nodes)?;
                }
            }
            serde_json::Value::Object(fields) => {
                require(fields.len() <= MAX_ITEMS, "$", "field-limit")?;
                for (key, value) in fields {
                    require(key.len() <= MAX_STRING_BYTES, "$", "string-limit")?;
                    *work += key.len();
                    visit(value, work, nodes)?;
                }
            }
            serde_json::Value::String(text) => {
                require(text.len() <= MAX_STRING_BYTES, "$", "string-limit")?;
                *work += text.len();
            }
            _ => {}
        }
        Ok(())
    }
    let mut work = 0;
    visit(value, &mut work, &mut 0)?;
    Ok(work)
}
fn identifier(value: &str) -> bool {
    value.len() <= 63
        && value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
pub(crate) fn hostname(value: &str) -> bool {
    value.len() <= 253
        && value.contains('.')
        && value.split('.').all(label)
        && value
            .rsplit('.')
            .next()
            .is_some_and(|tail| !tail.bytes().all(|b| b.is_ascii_digit()))
        && !value
            .split('.')
            .any(|part| part.starts_with("0x") && part[2..].bytes().all(|b| b.is_ascii_hexdigit()))
        && !value.ends_with(".localhost")
        && !value.ends_with(".local")
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && value.bytes().any(|b| b != b'0')
}
fn path(value: &str) -> bool {
    value.starts_with('/')
        && (value == "/" || !value.ends_with('/'))
        && !value.contains(['%', '?', '#', '\\'])
        && !value.chars().any(char::is_control)
        && value
            .split('/')
            .skip(1)
            .all(|p| !p.is_empty() && p != "." && p != "..")
        || value == "/"
}
fn index<'a, T>(
    items: &'a [T],
    name: impl Fn(&T) -> &str,
    location: &str,
) -> Result<HashMap<&'a str, &'a T>, ManifestError> {
    require(items.len() <= MAX_ITEMS, location, "collection-limit")?;
    let mut result = HashMap::new();
    for item in items {
        require(
            result.insert(name(item), item).is_none(),
            location,
            "duplicate-name",
        )?;
    }
    Ok(result)
}
fn asset_bucket(pattern: &ResourcePattern) -> Result<String, ManifestError> {
    let bucket = pattern
        .name
        .strip_prefix("objects:/")
        .and_then(|name| name.split('/').next())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| ManifestError::invalid("assets", "objects-bucket"))?;
    let bucket = acyclic_auth::decode_component(bucket)
        .map_err(|_| ManifestError::invalid("assets", "objects-bucket"))?;
    Ok(format!(
        "objects:/{}",
        acyclic_auth::encode_component(&bucket)
    ))
}
fn stream_path(resource: &str) -> Result<String, ManifestError> {
    require(
        acyclic_auth::valid_resource(resource),
        "resource",
        "auth-resource",
    )?;
    let encoded = resource
        .strip_prefix("streams:/")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ManifestError::invalid("resource", "streams-resource"))?;
    let parts = encoded
        .split('/')
        .map(|part| {
            acyclic_auth::decode_component(part)
                .map_err(|_| ManifestError::invalid("resource", "auth-resource"))
                .and_then(|bytes| {
                    String::from_utf8(bytes)
                        .map_err(|_| ManifestError::invalid("resource", "stream-path"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Encoded slashes belong to a component, not the hierarchical path syntax.
    require(
        !parts.iter().any(|part| part.contains('/')),
        "resource",
        "stream-component",
    )?;
    let path = parts.join("/");
    acyclic_stream::StreamPath::new(&path)
        .map_err(|_| ManifestError::invalid("resource", "stream-path"))?;
    Ok(path)
}

/// Admit a complete Rust value using the same rules as JSON and WASM ingress.
pub fn validate(manifest: &DeploymentManifest) -> Result<(), ManifestError> {
    validate_work(manifest).map(|_| ())
}

/// Runs the actual validator and counts visited JSON bytes/nodes and indexed edges.
/// Every lookup is hash-based; count depends linearly on document size and selections.
pub fn validate_work(manifest: &DeploymentManifest) -> Result<usize, ManifestError> {
    require(
        manifest.schema_version == 1,
        "$.schemaVersion",
        "unsupported-version",
    )?;
    require(
        !manifest.environments.is_empty() && manifest.environments.len() <= MAX_ENVIRONMENTS,
        "$.environments",
        "environment-limit",
    )?;
    // Check native collection/string bounds before descriptor adapters can clone values.
    native_bounds(manifest)?;
    encoded_budget(manifest)?;
    let mut work = bound_json(&serde_json::to_value(manifest).map_err(ManifestError::json)?)?;
    let mut environments = HashSet::new();
    let mut routes = HashSet::new();
    for (ei, environment) in manifest.environments.iter().enumerate() {
        let p = format!("$.environments[{ei}]");
        require(
            label(&environment.name) && environments.insert(&environment.name),
            &p,
            "environment-name",
        )?;
        let entries = index(&environment.entries, |e| &e.name, &p)?;
        require(!entries.is_empty(), &p, "entries-required")?;
        let bindings = index(&environment.bindings, |b| b.name(), &p)?;
        let grants = index(&environment.grants, |g| &g.name, &p)?;
        let secrets = index(&environment.secrets, |s| &s.name, &p)?;
        work += entries.len() + bindings.len() + grants.len() + secrets.len();
        let mut actions = HashMap::new();
        for grant in &environment.grants {
            require(identifier(&grant.name), &p, "grant-name")?;
            require(
                matches!(
                    grant
                        .grant
                        .resource
                        .name
                        .split_once(":/")
                        .map(|(product, _)| product),
                    Some(
                        "streams"
                            | "fs"
                            | "objects"
                            | "actors"
                            | "workers"
                            | "machines"
                            | "execution"
                            | "infer"
                            | "billing"
                            | "identity"
                    )
                ),
                &p,
                "unsupported-binding-product",
            )?;
            // Conversion invokes S1's resource/action validation, including product equality.
            <ResourceGrant as wire::ContractType>::from_proto(
                acyclic_auth::contract::ResourceGrantProto::from(grant.grant.0.clone()),
            )
            .map_err(|_| ManifestError::invalid(&p, "auth-grant"))?;
            require(
                acyclic_auth::valid_resource(&grant.grant.resource.name),
                &p,
                "auth-resource",
            )?;
            actions.insert(
                grant.name.as_str(),
                grant
                    .grant
                    .actions
                    .iter()
                    .map(String::as_str)
                    .collect::<HashSet<_>>(),
            );
            work += grant.grant.actions.len();
        }
        for binding in &environment.bindings {
            require(
                identifier(binding.name()) && !secrets.contains_key(binding.name()),
                &p,
                "binding-name",
            )?;
            let grant = grants
                .get(binding.capability())
                .ok_or_else(|| ManifestError::invalid(&p, "unknown-binding-grant"))?;
            require(
                binding.resource() == grant.grant.resource.name,
                &p,
                "binding-resource-grant",
            )?;
            work += 2;
        }
        for secret in &environment.secrets {
            require(identifier(&secret.name), &p, "secret-name")?;
            SecretReference::new(secret.reference.as_str().into())?;
            work += 1;
        }
        let mut selected = HashMap::new();
        let mut subscribed = HashMap::new();
        for (ii, entry) in environment.entries.iter().enumerate() {
            let ep = format!("{p}.entries[{ii}]");
            require(
                identifier(&entry.name)
                    && identifier(&entry.module)
                    && digest(&entry.artifact_sha256),
                &ep,
                "entry-artifact",
            )?;
            let mut selection = HashSet::new();
            for name in &entry.bindings {
                require(
                    bindings.contains_key(name.as_str()) && selection.insert(name.as_str()),
                    &ep,
                    "selected-binding",
                )?;
                work += 2;
            }
            let mut secret_names = HashSet::new();
            for name in &entry.secrets {
                require(
                    secrets.contains_key(name.as_str()) && secret_names.insert(name.as_str()),
                    &ep,
                    "selected-secret",
                )?;
                work += 2;
            }
            if let Some(fetch) = &entry.fetch {
                require(
                    identifier(&fetch.name)
                        && !selection.contains(fetch.name.as_str())
                        && !secret_names.contains(fetch.name.as_str()),
                    &ep,
                    "fetch-binding-name",
                )?;
                let mut destinations = HashSet::new();
                require(!fetch.egress.is_empty(), &ep, "fetch-egress-required")?;
                for rule in &fetch.egress {
                    require(
                        hostname(&rule.host)
                            && rule.port != 0
                            && destinations.insert((&rule.host, rule.port)),
                        &ep,
                        "https-authority",
                    )?;
                    work += 1;
                }
            }
            match &entry.runtime {
                EntryRuntime::Actor { subscriptions, .. } => {
                    let mut streams = HashSet::new();
                    for sub in subscriptions {
                        acyclic_stream::StreamPath::new(sub.stream_path())
                            .map_err(|_| ManifestError::invalid(&ep, "stream-path"))?;
                        require(
                            streams.insert(sub.stream_path()),
                            &ep,
                            "duplicate-subscription-stream",
                        )?;
                        work += 1;
                    }
                    // Shared constructor checks positive ActorLimits, starts, IDs and placement anchors.
                    lower_actor_unchecked(&bindings, entry, "manifest-admission")?;
                    subscribed.insert(entry.name.as_str(), streams);
                }
                EntryRuntime::Worker { limits } => require(
                    limits.cpu_millis > 0 && limits.memory_bytes > 0 && limits.timeout_millis > 0,
                    &ep,
                    "worker-limits",
                )?,
            }
            selected.insert(entry.name.as_str(), selection);
        }
        index(&environment.crons, |c| &c.name, &p)?;
        for (ci, cron) in environment.crons.iter().enumerate() {
            require(
                identifier(&cron.name) && super::cron::valid(&cron.schedule),
                &format!("{p}.crons[{ci}]"),
                "cron-schedule",
            )?;
        }
        let cron_targets = environment.crons.iter().enumerate().map(|(ci, cron)| {
            (
                cron.entry.as_str(),
                cron.inbox_binding.as_str(),
                &cron.inbox.0,
                format!("{p}.crons[{ci}]"),
            )
        });
        let timer_targets = environment
            .entries
            .iter()
            .enumerate()
            .filter_map(|(ii, entry)| match &entry.runtime {
                EntryRuntime::Actor {
                    timer_inbox: Some(inbox),
                    ..
                } => Some((
                    entry.name.as_str(),
                    inbox.binding.as_str(),
                    &inbox.stream.0,
                    format!("{p}.entries[{ii}].runtime.timerInbox"),
                )),
                _ => None,
            });
        for (entry, inbox_binding, inbox, cp) in cron_targets.chain(timer_targets) {
            let path = stream_path(&inbox.name)?;
            require(!inbox.prefix, &cp, "exact-inbox")?;
            require(
                subscribed
                    .get(entry)
                    .is_some_and(|s| s.contains(path.as_str())),
                &cp,
                "inbox-wake-subscription",
            )?;
            require(
                selected
                    .get(entry)
                    .is_some_and(|s| s.contains(inbox_binding)),
                &cp,
                "inbox-selected-binding",
            )?;
            let binding = bindings
                .get(inbox_binding)
                .ok_or_else(|| ManifestError::invalid(&cp, "inbox-binding"))?;
            let grant = grants
                .get(binding.capability())
                .ok_or_else(|| ManifestError::invalid(&cp, "inbox-grant"))?;
            require(
                scope(&grant.grant.resource, &inbox.name)
                    && actions[grant.name.as_str()].contains("streams/append"),
                &cp,
                "inbox-authority",
            )?;
            work += 4;
        }
        for asset in &environment.assets {
            require(digest(&asset.bundle_sha256), &p, "asset-digest")?;
            require(
                selected
                    .get(asset.entry.as_str())
                    .is_some_and(|s| s.contains(asset.binding.as_str())),
                &p,
                "asset-selected-binding",
            )?;
            let binding = bindings
                .get(asset.binding.as_str())
                .ok_or_else(|| ManifestError::invalid(&p, "asset-binding"))?;
            let grant = grants
                .get(binding.capability())
                .ok_or_else(|| ManifestError::invalid(&p, "asset-grant"))?;
            require(
                grant.grant.resource.prefix
                    && scope(&grant.grant.resource, &asset_bucket(&grant.grant.resource)?)
                    && actions[grant.name.as_str()].contains("objects/get"),
                &p,
                "asset-resource-grant",
            )?;
            work += 3;
        }
        for route in &environment.routes {
            require(
                entries.contains_key(route.entry.as_str())
                    && hostname(&route.host)
                    && path(&route.path_prefix)
                    && route.host != "run.acyclic.dev"
                    && !route.host.ends_with(".run.acyclic.dev"),
                &p,
                "customer-route",
            )?;
            require(
                routes.insert((&route.host, &route.path_prefix)),
                &p,
                "duplicate-route-across-environments",
            )?;
            work += 2;
        }
    }
    Ok(work)
}

/// Lower one declared actor instance using canonical Actors admission.
pub fn lower_actor(
    environment: &Environment,
    entry: &Entry,
    idempotency_key: &str,
) -> Result<acyclic_actors::domain::CreateActorRequest, ManifestError> {
    validate(&DeploymentManifest {
        schema_version: 1,
        environments: vec![environment.clone()],
    })?;
    require(
        environment
            .entries
            .iter()
            .any(|candidate| candidate == entry),
        "entry",
        "undeclared-entry",
    )?;
    let bindings = environment.bindings.iter().map(|b| (b.name(), b)).collect();
    lower_actor_unchecked(&bindings, entry, idempotency_key)
}
fn lower_actor_unchecked(
    bindings: &HashMap<&str, &Contract<Binding>>,
    entry: &Entry,
    idempotency_key: &str,
) -> Result<acyclic_actors::domain::CreateActorRequest, ManifestError> {
    use acyclic_actors::domain::{CodeSha256, CreateActorRequest};
    let EntryRuntime::Actor {
        home_region,
        limits,
        subscriptions,
        ..
    } = &entry.runtime
    else {
        return Err(ManifestError::invalid("entry", "actor-required"));
    };
    let bytes = entry
        .artifact_sha256
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)
                .map_err(|_| ManifestError::invalid("artifactSha256", "digest"))?;
            u8::from_str_radix(text, 16)
                .map_err(|_| ManifestError::invalid("artifactSha256", "digest"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let selected = entry
        .bindings
        .iter()
        .map(|name| {
            bindings
                .get(name.as_str())
                .map(|b| b.0.clone())
                .ok_or_else(|| ManifestError::invalid("bindings", "unknown-binding"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    CreateActorRequest::new(
        CodeSha256::new(bytes).map_err(|_| ManifestError::invalid("artifactSha256", "digest"))?,
        home_region.clone(),
        selected,
        limits.0,
        subscriptions.iter().map(|s| s.0.clone()).collect(),
        idempotency_key.into(),
    )
    .map_err(|_| ManifestError::invalid("entry.runtime", "actors-admission"))
}

#[derive(Default)]
struct NativeBudget {
    bytes: usize,
    items: usize,
}
impl NativeBudget {
    fn text(&mut self, text: &str) -> Result<(), ManifestError> {
        require(text.len() <= MAX_STRING_BYTES, "$", "string-limit")?;
        // Exact quoted string cost, before any descriptor adapter clones strings.
        self.bytes += 2 + text
            .bytes()
            .map(|b| match b {
                b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 8 | 12 => 2,
                0..=31 => 6,
                _ => 1,
            })
            .sum::<usize>();
        require(self.bytes <= MAX_JSON_BYTES, "$", "json-size")
    }
    fn list<T>(&mut self, items: &[T]) -> Result<(), ManifestError> {
        require(items.len() <= MAX_ITEMS, "$", "collection-limit")?;
        self.items += items.len();
        require(self.items <= MAX_NODES, "$", "node-limit")
    }
}

// Stream exact JSON into a capped counter, never into a document-sized buffer.
fn encoded_budget(manifest: &DeploymentManifest) -> Result<(), ManifestError> {
    struct Counter {
        bytes: usize,
        exceeded: bool,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_JSON_BYTES - self.bytes {
                self.exceeded = true;
                return Err(std::io::Error::other("json-size"));
            }
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter {
        bytes: 0,
        exceeded: false,
    };
    let result = serde_json::to_writer(&mut counter, manifest);
    if counter.exceeded {
        return Err(ManifestError::invalid("$", "json-size"));
    }
    result.map_err(ManifestError::json)
}

fn native_bounds(manifest: &DeploymentManifest) -> Result<(), ManifestError> {
    let mut budget = NativeBudget::default();
    for env in &manifest.environments {
        budget.text(&env.name)?;
        budget.list(&env.entries)?;
        budget.list(&env.bindings)?;
        budget.list(&env.grants)?;
        budget.list(&env.secrets)?;
        budget.list(&env.crons)?;
        budget.list(&env.assets)?;
        budget.list(&env.routes)?;
        for binding in &env.bindings {
            budget.text(binding.name())?;
            budget.text(binding.capability())?;
            budget.text(binding.resource())?;
        }
        for grant in &env.grants {
            budget.text(&grant.name)?;
            budget.text(&grant.grant.resource.name)?;
            budget.list(&grant.grant.actions)?;
            for action in &grant.grant.actions {
                budget.text(action)?;
            }
        }
        for secret in &env.secrets {
            budget.text(&secret.name)?;
            budget.text(secret.reference.as_str())?;
        }
        for entry in &env.entries {
            if let EntryRuntime::Actor {
                timer_inbox: Some(inbox),
                ..
            } = &entry.runtime
            {
                budget.text(&inbox.binding)?;
                budget.text(&inbox.stream.name)?;
            }
            budget.text(&entry.name)?;
            budget.text(&entry.module)?;
            budget.text(&entry.artifact_sha256)?;
            budget.list(&entry.bindings)?;
            budget.list(&entry.secrets)?;
            for name in entry.bindings.iter().chain(&entry.secrets) {
                budget.text(name)?;
            }
            if let Some(fetch) = &entry.fetch {
                budget.text(&fetch.name)?;
                budget.list(&fetch.egress)?;
                for rule in &fetch.egress {
                    budget.text(&rule.host)?;
                }
            }
            if let EntryRuntime::Actor {
                home_region,
                subscriptions,
                ..
            } = &entry.runtime
            {
                budget.text(home_region)?;
                budget.list(subscriptions)?;
                for sub in subscriptions {
                    budget.text(sub.subscription_id())?;
                    budget.text(sub.stream_path())?;
                }
            }
        }
        for cron in &env.crons {
            budget.text(&cron.name)?;
            budget.text(&cron.schedule)?;
            budget.text(&cron.entry)?;
            budget.text(&cron.inbox_binding)?;
            budget.text(&cron.inbox.name)?;
        }
        for asset in &env.assets {
            budget.text(&asset.entry)?;
            budget.text(&asset.binding)?;
            budget.text(&asset.bundle_sha256)?;
        }
        for route in &env.routes {
            budget.text(&route.host)?;
            budget.text(&route.path_prefix)?;
            budget.text(&route.entry)?;
        }
    }
    Ok(())
}
