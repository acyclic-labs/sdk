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
    acyclic_actors::domain::valid_environment_name(value)
}

fn label(value: &str) -> bool {
    acyclic_auth::addressing::validate_dns_label(value.as_bytes()).is_ok()
}
pub(crate) fn hostname(value: &str) -> bool {
    acyclic_actors::domain::EgressRule::valid_host(value)
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && value.bytes().any(|b| b != b'0')
}
fn file_name(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
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
    let mut patterns = HashSet::new();
    for (ei, environment) in manifest.environments.iter().enumerate() {
        let p = format!("$.environments[{ei}]");
        require(
            label(&environment.name) && environments.insert(&environment.name),
            &p,
            "environment-name",
        )?;
        work += validate_environment(environment, &p, &mut routes, &mut patterns)?;
    }
    Ok(work)
}

type Names<'a, T> = HashMap<&'a str, &'a T>;
type Selections<'a> = HashMap<&'a str, HashSet<&'a str>>;

fn validate_environment<'a>(
    environment: &'a Environment,
    p: &str,
    routes: &mut HashSet<(&'a String, &'a String)>,
    patterns: &mut HashSet<&'a String>,
) -> Result<usize, ManifestError> {
    let mut work = 0;
    let entries = index(&environment.entries, |e| &e.name, p)?;
    require(!entries.is_empty(), p, "entries-required")?;
    let bindings = index(&environment.bindings, |b| b.name(), p)?;
    let grants = index(&environment.grants, |g| &g.name, p)?;
    let secrets = index(&environment.secrets, |s| &s.name, p)?;
    work += entries.len() + bindings.len() + grants.len() + secrets.len();
    let actions = grant_actions(environment, p, &mut work)?;
    for binding in &environment.bindings {
        require(
            identifier(binding.name()) && !secrets.contains_key(binding.name()),
            p,
            "binding-name",
        )?;
        if binding.is_actor_state() || binding.guest_admission_kind().is_some() {
            continue;
        }
        let grant = grants
            .get(binding.capability())
            .ok_or_else(|| ManifestError::invalid(p, "unknown-binding-grant"))?;
        require(
            binding.resource() == grant.grant.resource.name,
            p,
            "binding-resource-grant",
        )?;
        work += 2;
    }
    for secret in &environment.secrets {
        require(identifier(&secret.name), p, "secret-name")?;
        require(
            acyclic_actors::domain::SecretSelection::valid_reference(secret.reference.as_str()),
            p,
            "secret-reference",
        )?;
        work += 1;
    }
    let (selected, subscribed) = validate_entries(environment, p, &bindings, &secrets, &mut work)?;
    work += validate_inboxes(
        environment,
        p,
        &bindings,
        &grants,
        &actions,
        &selected,
        &subscribed,
    )?;
    for asset in &environment.assets {
        require(digest(&asset.bundle_sha256), p, "asset-digest")?;
        require(
            selected
                .get(asset.entry.as_str())
                .is_some_and(|s| s.contains(asset.binding.as_str())),
            p,
            "asset-selected-binding",
        )?;
        let binding = bindings
            .get(asset.binding.as_str())
            .ok_or_else(|| ManifestError::invalid(p, "asset-binding"))?;
        let grant = grants
            .get(binding.capability())
            .ok_or_else(|| ManifestError::invalid(p, "asset-grant"))?;
        require(
            grant.grant.resource.prefix
                && scope(&grant.grant.resource, &asset_bucket(&grant.grant.resource)?)
                && actions
                    .get(grant.name.as_str())
                    .is_some_and(|actions| actions.contains("objects/get")),
            p,
            "asset-resource-grant",
        )?;
        work += 3;
    }
    work += validate_routes(environment, p, &entries, routes, patterns)?;
    Ok(work)
}

fn validate_routes<'a>(
    environment: &'a Environment,
    p: &str,
    entries: &Names<'a, Entry>,
    routes: &mut HashSet<(&'a String, &'a String)>,
    patterns: &mut HashSet<&'a String>,
) -> Result<usize, ManifestError> {
    let mut work = 0;
    for route in &environment.routes {
        require(
            entries
                .get(route.entry.as_str())
                .is_some_and(|entry| matches!(entry.runtime, EntryRuntime::Worker { .. }))
                && hostname(&route.host)
                && path(&route.path_prefix)
                && route.host != "run.acyclic.dev"
                && !route.host.ends_with(".run.acyclic.dev"),
            p,
            "customer-route",
        )?;
        require(
            routes.insert((&route.host, &route.path_prefix)),
            p,
            "duplicate-route-across-environments",
        )?;
        work += 2;
    }
    for route in &environment.worker_routes {
        let parsed = acyclic_auth::addressing::RoutePattern::parse(&route.pattern)
            .map_err(|_| ManifestError::invalid(p, "worker-route-pattern"))?;
        require(
            parsed.as_str() == route.pattern && patterns.insert(&route.pattern),
            p,
            "canonical-unique-worker-route",
        )?;
        if let Some(entry) = &route.entry {
            require(
                entries
                    .get(entry.as_str())
                    .is_some_and(|e| matches!(e.runtime, EntryRuntime::Worker { .. })),
                p,
                "worker-route-target",
            )?;
        }
        work += 2;
    }
    Ok(work)
}

fn grant_actions<'a>(
    environment: &'a Environment,
    p: &str,
    work: &mut usize,
) -> Result<Selections<'a>, ManifestError> {
    let mut actions = HashMap::new();
    for grant in &environment.grants {
        require(identifier(&grant.name), p, "grant-name")?;
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
            p,
            "unsupported-binding-product",
        )?;
        // Conversion invokes S1's resource/action validation, including product equality.
        <ResourceGrant as wire::ContractType>::from_proto(
            acyclic_auth::contract::ResourceGrantProto::from(grant.grant.0.clone()),
        )
        .map_err(|_| ManifestError::invalid(p, "auth-grant"))?;
        require(
            acyclic_auth::valid_resource(&grant.grant.resource.name),
            p,
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
        *work += grant.grant.actions.len();
    }
    Ok(actions)
}

fn validate_entries<'a>(
    environment: &'a Environment,
    p: &str,
    bindings: &Names<'a, Contract<Binding>>,
    secrets: &Names<'a, SecretRef>,
    work: &mut usize,
) -> Result<(Selections<'a>, Selections<'a>), ManifestError> {
    let mut selected = HashMap::new();
    let mut subscribed = HashMap::new();
    for (ii, entry) in environment.entries.iter().enumerate() {
        let ep = format!("{p}.entries[{ii}]");
        require(
            identifier(&entry.name) && identifier(&entry.module) && digest(&entry.artifact_sha256),
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
            *work += 2;
        }
        let mut secret_names = HashSet::new();
        for name in &entry.secrets {
            require(
                secrets.contains_key(name.as_str()) && secret_names.insert(name.as_str()),
                &ep,
                "selected-secret",
            )?;
            *work += 2;
        }
        let mut asset_paths = HashSet::new();
        for asset in &entry.assets {
            require(
                file_name(&asset.path)
                    && asset_paths.insert(asset.path.as_str())
                    && digest(&asset.sha256)
                    && asset.size > 0
                    && match asset.role {
                        AssetRole::Module => asset.media_type == "application/wasm",
                    },
                &ep,
                "entry-asset",
            )?;
            *work += 2;
        }
        if let Some(fetch) = &entry.fetch {
            require(
                identifier(&fetch.name)
                    && !selection.contains(fetch.name.as_str())
                    && !secret_names.contains(fetch.name.as_str()),
                &ep,
                "fetch-binding-name",
            )?;
            deployment_fetch(entry)?;
            *work += fetch.egress.len();
        }
        match &entry.runtime {
            EntryRuntime::ActorNamespace { subscriptions, .. } => {
                let mut streams = HashSet::new();
                for sub in subscriptions {
                    acyclic_stream::StreamPath::new(sub.stream_path())
                        .map_err(|_| ManifestError::invalid(&ep, "stream-path"))?;
                    require(
                        streams.insert(sub.stream_path()),
                        &ep,
                        "duplicate-subscription-stream",
                    )?;
                    *work += 1;
                }
                // Shared constructor checks positive ActorLimits, starts, IDs and placement anchors.
                lower_actor_unchecked(bindings, secrets, entry, "manifest-admission")?;
                subscribed.insert(entry.name.as_str(), streams);
            }
            EntryRuntime::Worker { .. } => {
                lower_worker_unchecked(bindings, secrets, entry, "manifest-admission")?;
            }
        }
        selected.insert(entry.name.as_str(), selection);
    }
    Ok((selected, subscribed))
}

fn validate_inboxes(
    environment: &Environment,
    p: &str,
    bindings: &Names<'_, Contract<Binding>>,
    grants: &Names<'_, BindingGrant>,
    actions: &Selections<'_>,
    selected: &Selections<'_>,
    subscribed: &Selections<'_>,
) -> Result<usize, ManifestError> {
    let mut work = 0;
    index(&environment.crons, |c| &c.name, p)?;
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
    let timer_targets =
        environment
            .entries
            .iter()
            .enumerate()
            .filter_map(|(ii, entry)| match &entry.runtime {
                EntryRuntime::ActorNamespace {
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
                && actions
                    .get(grant.name.as_str())
                    .is_some_and(|actions| actions.contains("streams/append")),
            &cp,
            "inbox-authority",
        )?;
        work += 4;
    }
    Ok(work)
}

/// Lower one declared actor instance using canonical Actors admission.
pub fn lower_actor(
    environment: &Environment,
    entry: &Entry,
    idempotency_key: &str,
) -> Result<acyclic_actors::domain::DeploymentRequest, ManifestError> {
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
    let secrets = environment
        .secrets
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();
    lower_actor_unchecked(&bindings, &secrets, entry, idempotency_key)
}
fn lower_actor_unchecked(
    bindings: &HashMap<&str, &Contract<Binding>>,
    secrets: &HashMap<&str, &SecretRef>,
    entry: &Entry,
    idempotency_key: &str,
) -> Result<acyclic_actors::domain::DeploymentRequest, ManifestError> {
    use acyclic_actors::domain::DeploymentRequest;
    let EntryRuntime::ActorNamespace {
        jurisdiction,
        limits,
        subscriptions,
        ..
    } = &entry.runtime
    else {
        return Err(ManifestError::invalid("entry", "actor-required"));
    };
    let (digest, selected) = deployment_parts(bindings, entry)?;
    let selected_secrets = deployment_secrets(secrets, entry)?;
    let selected_fetch = deployment_fetch(entry)?;
    DeploymentRequest::new(
        acyclic_actors::domain::ActorNamespace::new(entry.name.clone())
            .map_err(|_| ManifestError::invalid("entry.name", "namespace"))?,
        digest,
        selected,
        limits.0,
        subscriptions.iter().map(|s| s.0.clone()).collect(),
        idempotency_key.into(),
    )
    .and_then(|request| request.with_jurisdiction(jurisdiction.clone()))
    .and_then(|request| request.with_secrets(selected_secrets))
    .and_then(|request| request.with_fetch(selected_fetch))
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
        budget.list(&env.worker_routes)?;
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
            if let EntryRuntime::ActorNamespace {
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
            budget.list(&entry.assets)?;
            for asset in &entry.assets {
                budget.text(&asset.path)?;
                budget.text(&asset.sha256)?;
                budget.text(&asset.media_type)?;
            }
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
            if let EntryRuntime::ActorNamespace {
                jurisdiction,
                subscriptions,
                ..
            } = &entry.runtime
            {
                budget.text(jurisdiction)?;
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
        for route in &env.worker_routes {
            budget.text(&route.pattern)?;
            if let Some(entry) = &route.entry {
                budget.text(entry)?;
            }
        }
    }
    Ok(())
}

/// Admit a deployable artifact against its validated manifest: the entry bytes match
/// `artifactSha256`, and the supplied files are exactly the entry's declared assets,
/// each with its declared size and digest.
pub fn verify_artifact(
    manifest: &DeploymentManifest,
    environment: &str,
    entry: &str,
    artifact: &[u8],
    files: &[(String, Vec<u8>)],
) -> Result<(), ManifestError> {
    use sha2::Digest;
    let hex = |bytes: &[u8]| hex::encode(sha2::Sha256::digest(bytes));
    validate(manifest)?;
    let entry = manifest
        .environments
        .iter()
        .find(|candidate| candidate.name == environment)
        .and_then(|candidate| candidate.entries.iter().find(|e| e.name == entry))
        .ok_or_else(|| ManifestError::invalid("entry", "undeclared-entry"))?;
    require(
        hex(artifact) == entry.artifact_sha256,
        "artifact",
        "artifact-digest",
    )?;
    let declared: HashMap<&str, &EntryAsset> = entry
        .assets
        .iter()
        .map(|asset| (asset.path.as_str(), asset))
        .collect();
    let mut supplied = HashSet::new();
    for (path, bytes) in files {
        let asset = declared
            .get(path.as_str())
            .ok_or_else(|| ManifestError::invalid("assets", "undeclared-asset"))?;
        require(supplied.insert(path.as_str()), "assets", "duplicate-asset")?;
        require(
            u64::try_from(bytes.len()) == Ok(asset.size),
            "assets",
            "asset-size",
        )?;
        require(hex(bytes) == asset.sha256, "assets", "asset-digest")?;
    }
    require(supplied.len() == declared.len(), "assets", "missing-asset")
}

/// Lower one named Worker script, preserving its CPU, memory and duration limits.
pub fn lower_worker(
    environment: &Environment,
    entry: &Entry,
    idempotency_key: &str,
) -> Result<acyclic_actors::domain::DeploymentRequest, ManifestError> {
    validate(&DeploymentManifest {
        schema_version: 1,
        environments: vec![environment.clone()],
    })?;
    require(
        environment.entries.contains(entry),
        "entry",
        "undeclared-entry",
    )?;
    let bindings = environment.bindings.iter().map(|b| (b.name(), b)).collect();
    let secrets = environment
        .secrets
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();
    lower_worker_unchecked(&bindings, &secrets, entry, idempotency_key)
}
fn lower_worker_unchecked(
    bindings: &HashMap<&str, &Contract<Binding>>,
    secrets: &HashMap<&str, &SecretRef>,
    entry: &Entry,
    idempotency_key: &str,
) -> Result<acyclic_actors::domain::DeploymentRequest, ManifestError> {
    use acyclic_actors::domain::{ActorNamespace, DeploymentRequest, WorkerLimits};
    let EntryRuntime::Worker { limits } = &entry.runtime else {
        return Err(ManifestError::invalid("entry", "worker-required"));
    };
    acyclic_auth::addressing::validate_dns_label(entry.name.as_bytes())
        .map_err(|_| ManifestError::invalid("entry.name", "worker-label"))?;
    let (digest, selected) = deployment_parts(bindings, entry)?;
    let selected_secrets = deployment_secrets(secrets, entry)?;
    let selected_fetch = deployment_fetch(entry)?;
    let limits = WorkerLimits::new(
        limits.cpu_millis,
        limits.memory_bytes,
        limits.timeout_millis,
    )
    .map_err(|_| ManifestError::invalid("entry.runtime", "worker-limits"))?;
    DeploymentRequest::worker(
        ActorNamespace::new(entry.name.clone())
            .map_err(|_| ManifestError::invalid("entry.name", "worker-label"))?,
        digest,
        selected,
        limits,
        idempotency_key.into(),
    )
    .and_then(|request| request.with_secrets(selected_secrets))
    .and_then(|request| request.with_fetch(selected_fetch))
    .map_err(|_| ManifestError::invalid("entry.runtime", "workers-admission"))
}
fn deployment_fetch(
    entry: &Entry,
) -> Result<Option<acyclic_actors::domain::FetchBinding>, ManifestError> {
    entry
        .fetch
        .as_ref()
        .map(|fetch| {
            let rules = fetch
                .egress
                .iter()
                .map(|rule| {
                    acyclic_actors::domain::EgressRule::new(rule.host.clone(), u32::from(rule.port))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ManifestError::invalid("entry.fetch", "https-authority"))?;
            acyclic_actors::domain::FetchBinding::new(fetch.name.clone(), rules)
                .map_err(|_| ManifestError::invalid("entry.fetch", "https-authority"))
        })
        .transpose()
}

fn deployment_parts(
    bindings: &HashMap<&str, &Contract<Binding>>,
    entry: &Entry,
) -> Result<(acyclic_actors::domain::CodeSha256, Vec<Binding>), ManifestError> {
    let bytes = entry
        .artifact_sha256
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
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
    Ok((
        acyclic_actors::domain::CodeSha256::new(bytes)
            .map_err(|_| ManifestError::invalid("artifactSha256", "digest"))?,
        selected,
    ))
}

fn deployment_secrets(
    secrets: &HashMap<&str, &SecretRef>,
    entry: &Entry,
) -> Result<Vec<acyclic_actors::domain::SecretSelection>, ManifestError> {
    entry
        .secrets
        .iter()
        .map(|name| {
            let secret = secrets
                .get(name.as_str())
                .ok_or_else(|| ManifestError::invalid("secrets", "unknown-secret"))?;
            acyclic_actors::domain::SecretSelection::new(
                secret.name.clone(),
                secret.reference.as_str().to_owned(),
            )
            .map_err(|_| ManifestError::invalid("secrets", "secret-reference"))
        })
        .collect()
}
