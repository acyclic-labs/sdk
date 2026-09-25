#![doc = include_str!("../README.md")]
#![deny(unsafe_code)]

pub mod api;
pub mod map;
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test-only HTTP stand-in; a broken mock should fail the test loudly"
)]
mod mock;
pub mod ops;
pub mod usage;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{Mutex, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use acyclic_machines::{
    Capability, CheckpointId, CheckpointObservation, CompatibilityPolicy, CreateMachine, EventPage,
    ForkFidelity, IdempotencyKey, Image, ImageQualification, MAX_EVENT_PAGE_SIZE,
    MAX_FORK_CHILDREN, MAX_PAGE_SIZE, MachineContract, MachineId, MachineObservation, MachinePage,
    MachineState, MachinesProvider, MutationOutcome, OperationId, OperationObservation,
    OperationPhase, OperationStream, Performance, ProviderAssurance, ProviderError,
    SuspensionPolicy, UsageReceipt,
};
use async_trait::async_trait;
use futures::StreamExt as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use api::{CreateSnapshotRequest, DaytonaApi, ExecuteRequest, ForkRequest, Sandbox, Snapshot};
use ops::{Admission, OperationRegistry};

/// Default Daytona API base URL.
pub const DEFAULT_API_URL: &str = "https://app.daytona.io/api";
/// Default directory a container disk fork copies into its children.
pub const DEFAULT_WORKSPACE_DIR: &str = "/home/daytona/workspace";

/// Provider configuration.
#[derive(Clone)]
pub struct DaytonaConfig {
    /// Daytona API key (bearer token).
    pub api_key: String,
    /// API base URL; defaults to [`DEFAULT_API_URL`].
    pub api_url: String,
    /// Region target passed as Daytona `target` (for example `us` or `eu`); `None` lets
    /// Daytona choose.
    pub region: Option<String>,
    /// Snapshot booted when an image digest is not in `snapshots`; `None` makes such images
    /// unsupported. It must be a VM-class snapshot.
    pub default_snapshot: Option<String>,
    /// Outbound network policies by network-policy digest (lowercase hex). A create or
    /// checkpoint fork whose contract commits to a digest missing here is refused; see
    /// [`map::NetworkPolicy`].
    pub network_policies: BTreeMap<String, map::NetworkPolicy>,
    /// Toolbox proxy hosts, besides the API host and its subdomains, that may receive the API
    /// key; see [`api::DaytonaApi::check_toolbox_proxy`]. Needed only for self-hosted
    /// deployments that serve the proxy from an unrelated domain.
    pub toolbox_proxy_hosts: Vec<String>,
    /// Registered image digests (lowercase hex) mapped to Daytona snapshot names.
    pub snapshots: BTreeMap<String, String>,
    /// Tenant recorded in the `acyclic.tenant` label of every sandbox.
    pub tenant: Option<String>,
    /// Daytona organization to act in when the credential spans several.
    pub organization_id: Option<String>,
    /// Interval between state polls while waiting for a sandbox or snapshot to settle.
    pub poll_interval: Duration,
    /// Maximum time to wait for a sandbox or snapshot to settle before reporting indeterminate.
    pub ready_timeout: Duration,
    /// Per-request HTTP timeout.
    pub request_timeout: Duration,
    /// Absolute directory a container disk fork (`Capability::DiskFork`) copies from the
    /// source into each child; everything outside it comes from the source's snapshot.
    pub workspace_dir: String,
}

impl DaytonaConfig {
    /// Configuration with defaults for everything but the key.
    #[must_use]
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            api_url: DEFAULT_API_URL.to_owned(),
            region: None,
            default_snapshot: None,
            network_policies: BTreeMap::new(),
            toolbox_proxy_hosts: Vec::new(),
            snapshots: BTreeMap::new(),
            tenant: None,
            organization_id: None,
            poll_interval: Duration::from_millis(500),
            ready_timeout: Duration::from_secs(180),
            request_timeout: Duration::from_secs(60),
            workspace_dir: DEFAULT_WORKSPACE_DIR.to_owned(),
        }
    }

    /// Reads `DAYTONA_API_KEY` (required), `DAYTONA_API_URL`, `DAYTONA_REGION`,
    /// `DAYTONA_SNAPSHOT`, `DAYTONA_TENANT`, `DAYTONA_ORGANIZATION_ID`, and
    /// `DAYTONA_WORKSPACE_DIR`. Network policies are registered in code with
    /// [`Self::register_network_policy`].
    ///
    /// # Errors
    /// Returns [`ProviderError::Invalid`] when `DAYTONA_API_KEY` is unset or empty.
    pub fn from_env() -> Result<Self, ProviderError> {
        let read = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let api_key = read("DAYTONA_API_KEY")
            .ok_or_else(|| ProviderError::Invalid("DAYTONA_API_KEY is not set".into()))?;
        let mut config = Self::new(api_key);
        if let Some(url) = read("DAYTONA_API_URL") {
            config.api_url = url;
        }
        config.region = read("DAYTONA_REGION");
        config.default_snapshot = read("DAYTONA_SNAPSHOT");
        config.tenant = read("DAYTONA_TENANT");
        config.organization_id = read("DAYTONA_ORGANIZATION_ID");
        if let Some(dir) = read("DAYTONA_WORKSPACE_DIR") {
            config.workspace_dir = dir;
        }
        Ok(config)
    }

    /// Registers the snapshot that backs one image digest.
    pub fn register_snapshot(&mut self, digest: [u8; 32], snapshot: impl Into<String>) {
        self.snapshots.insert(map::hex(&digest), snapshot.into());
    }

    /// Registers the outbound policy a network-policy digest commits to. Sandboxes whose
    /// contract carries `digest` are created with exactly this policy.
    pub fn register_network_policy(&mut self, digest: [u8; 32], policy: map::NetworkPolicy) {
        self.network_policies.insert(map::hex(&digest), policy);
    }
}

impl std::fmt::Debug for DaytonaConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaytonaConfig")
            .field("api_key", &"<redacted>")
            .field("api_url", &self.api_url)
            .field("region", &self.region)
            .field("default_snapshot", &self.default_snapshot)
            .field("network_policies", &self.network_policies)
            .field("toolbox_proxy_hosts", &self.toolbox_proxy_hosts)
            .field("snapshots", &self.snapshots)
            .field("tenant", &self.tenant)
            .field("organization_id", &self.organization_id)
            .field("workspace_dir", &self.workspace_dir)
            .finish_non_exhaustive()
    }
}

/// Assurance level of a declared feature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Assurance {
    /// Not offered.
    No,
    /// Offered by Daytona's specification for the VM classes this provider admits.
    Yes,
    /// Offered with a caveat; do not rely on it for billing or safety decisions yet.
    Provisional,
}

/// Feature declaration of this provider. VM-class features apply to `linux-vm` and `windows`
/// sandboxes; container-class sandboxes offer only [`Self::container_disk_fork`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DaytonaCapabilities {
    /// Checkpoints capture VM memory (hot snapshot, `includeMemory`).
    pub checkpoint_memory: Assurance,
    /// Native fork copies a running VM's memory and disk (`POST /sandbox/{id}/fork`), at any
    /// time, without an intermediate checkpoint.
    pub live_fork_memory: Assurance,
    /// Container disk fork: fresh sandboxes from the source's snapshot, with the source's
    /// workspace directory copied in through the toolbox. No process or memory state.
    pub container_disk_fork: Assurance,
    /// A suspended machine keeps its memory (pause, not stop) and frees CPU and memory quota.
    pub suspend_keeps_memory: Assurance,
    /// Outbound domain policy is enforced. Only Daytona tiers 3 and 4 honour a per-sandbox
    /// allow list; lower tiers apply the organization policy instead.
    pub network_policy: Assurance,
    /// Usage receipts are allocation estimates, not metered consumption.
    pub usage_receipts: Assurance,
    /// A checkpoint can be restored on a different host.
    pub restore_cross_host: Assurance,
}

/// What this provider declares.
pub const CAPABILITIES: DaytonaCapabilities = DaytonaCapabilities {
    checkpoint_memory: Assurance::Yes,
    live_fork_memory: Assurance::Yes,
    container_disk_fork: Assurance::Yes,
    suspend_keeps_memory: Assurance::Yes,
    network_policy: Assurance::Provisional,
    usage_receipts: Assurance::Provisional,
    restore_cross_host: Assurance::No,
};

/// Canonical intent of one mutation, digested for idempotency-key rebinding checks.
#[derive(Clone, Debug, Serialize)]
enum Intent {
    Create(CreateMachine),
    Checkpoint(MachineId),
    Fork(CheckpointId, u32, Performance),
    ForkMachine(MachineId, u32),
    Suspend(MachineId),
    Wake(MachineId),
    Policy(MachineId, SuspensionPolicy),
    DestroyMachine(MachineId),
    DestroyCheckpoint(CheckpointId),
}

/// Provider-side extension the sdk's `CreateMachine` cannot carry: environment variables a
/// sandbox receives at boot, staged under the create idempotency key before `create` runs.
///
/// A host uses this to hand a fresh worker its boot environment (for example an endpoint and a
/// one-time boot token) so the entrypoint can fetch its sealed inputs. Staging is in-memory and
/// cleared by the matching `create` however it ends; a key that never creates leaks nothing but
/// a map entry. Native fork children inherit their parent's environment and memory, so they
/// share its credentials; container disk-fork children receive the environment Daytona
/// reports for their source.
pub trait BootEnvironment: Send + Sync {
    /// Records `env` for the sandbox that `create` with `key` will make. A second call for
    /// the same key replaces the first.
    fn stage_boot_env(&self, key: IdempotencyKey, env: BTreeMap<String, String>);
}

/// Machines provider over Daytona VM sandboxes.
pub struct DaytonaProvider {
    config: DaytonaConfig,
    api: DaytonaApi,
    ops: OperationRegistry,
    boot_env: Mutex<BTreeMap<IdempotencyKey, BTreeMap<String, String>>>,
    snapshot_classes: Mutex<BTreeMap<String, String>>,
}

/// Staged boot environments hold credentials; only their count and variable names are ever
/// rendered.
impl std::fmt::Debug for DaytonaProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let staged = self.boot_env.lock().unwrap_or_else(PoisonError::into_inner);
        let names: BTreeSet<&str> = staged
            .values()
            .flat_map(|env| env.keys().map(String::as_str))
            .collect();
        formatter
            .debug_struct("DaytonaProvider")
            .field("config", &self.config)
            .field("api", &self.api)
            .field("ops", &self.ops)
            .field("staged_boot_envs", &staged.len())
            .field("staged_boot_env_names", &names)
            .finish_non_exhaustive()
    }
}

impl BootEnvironment for DaytonaProvider {
    fn stage_boot_env(&self, key: IdempotencyKey, env: BTreeMap<String, String>) {
        self.boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, env);
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

impl DaytonaProvider {
    /// Binds a provider to one Daytona organization.
    ///
    /// # Errors
    /// Returns [`ProviderError::Invalid`] when the configuration cannot produce a client.
    pub fn new(config: DaytonaConfig) -> Result<Self, ProviderError> {
        let api = DaytonaApi::new(&config)?;
        Ok(Self {
            config,
            api,
            ops: OperationRegistry::default(),
            boot_env: Mutex::default(),
            snapshot_classes: Mutex::default(),
        })
    }

    /// Boot environment staged for `key`, if any; see [`BootEnvironment`].
    #[must_use]
    pub fn staged_boot_env(&self, key: IdempotencyKey) -> Option<BTreeMap<String, String>> {
        self.boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .cloned()
    }

    /// Configuration in use.
    #[must_use]
    pub fn config(&self) -> &DaytonaConfig {
        &self.config
    }

    /// Raw Daytona client, for operations outside the Machines contract (toolbox execution,
    /// fork-tree queries).
    #[must_use]
    pub fn api(&self) -> &DaytonaApi {
        &self.api
    }

    /// Operation registry in use.
    #[must_use]
    pub fn registry(&self) -> &OperationRegistry {
        &self.ops
    }

    /// Opaque compatibility revision of this provider build.
    #[must_use]
    pub fn revision() -> [u8; 32] {
        Sha256::digest(b"acyclic-machines-daytona-v2").into()
    }

    fn tenant(&self) -> Option<&str> {
        self.config.tenant.as_deref()
    }

    fn resolve_snapshot(&self, image: &Image) -> Result<String, ProviderError> {
        match image {
            Image::Checkpoint(checkpoint) => Ok(checkpoint.to_string()),
            Image::ManagedOci(digest) | Image::Custom(digest) => {
                let key = map::hex(&digest.as_bytes());
                self.config.snapshots.get(&key).or(self.config.default_snapshot.as_ref()).cloned().ok_or_else(|| {
                    ProviderError::Unsupported(format!(
                        "image digest {key} is not registered as a Daytona snapshot and no default snapshot is configured"
                    ))
                })
            }
        }
    }

    /// Capabilities of the sandboxes `snapshot` boots, from its class (see
    /// [`map::capabilities_for_class`]). The class is a property of the snapshot, never of the
    /// create request.
    async fn snapshot_capabilities(
        &self,
        snapshot: &str,
    ) -> Result<BTreeSet<Capability>, ProviderError> {
        let cached = self
            .snapshot_classes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(snapshot)
            .cloned();
        let class = if let Some(class) = cached {
            class
        } else {
            let snapshot_class = self
                .api
                .get_snapshot(snapshot)
                .await?
                .sandbox_class
                .unwrap_or_default();
            self.snapshot_classes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(snapshot.to_owned(), snapshot_class.clone());
            snapshot_class
        };
        map::capabilities_for_class(Some(&class)).ok_or_else(|| {
            ProviderError::Unsupported(format!(
                "Daytona snapshot {snapshot} boots class {class:?}; this provider admits VM and container classes only"
            ))
        })
    }

    /// Request checks that need no Daytona call.
    fn check_request(request: &CreateMachine) -> Result<(), ProviderError> {
        if request.budgets.spend_micros != 0 || request.budgets.concurrency != 0 {
            return Err(ProviderError::Unsupported(
                "Daytona cannot enforce per-machine spend or concurrency budgets; leave budgets zero".into(),
            ));
        }
        map::lifetime_fields(request.expiration).map(|_| ())
    }

    fn contract(
        request: &CreateMachine,
        capabilities: BTreeSet<Capability>,
    ) -> Result<MachineContract, ProviderError> {
        Self::check_request(request)?;
        if let CompatibilityPolicy::Require(required) = &request.compatibility
            && (required.is_empty() || !required.is_subset(&capabilities))
        {
            return Err(ProviderError::Unsupported(format!(
                "this Daytona snapshot class offers only {capabilities:?}"
            )));
        }
        // A class without pause never suspends on its own; say so rather than retain a
        // policy nothing enforces.
        let suspension = if capabilities.contains(&Capability::SuspendResume) {
            request.suspension
        } else {
            SuspensionPolicy::Manual
        };
        Ok(MachineContract {
            image: request.image.clone(),
            capabilities,
            compatibility: request.compatibility.clone(),
            compatibility_revision: Self::revision(),
            performance: request.performance,
            suspension,
            expiration: request.expiration,
            network_policy_digest: request.network_policy_digest,
            budgets: request.budgets,
        })
    }

    /// Records a sandbox observation, filling gaps from what the registry already knows.
    fn observe(
        &self,
        sandbox: &Sandbox,
        fallback: Option<&MachineContract>,
        last_checkpoint: Option<CheckpointId>,
    ) -> Result<MachineObservation, ProviderError> {
        // A sandbox of another tenant (or not managed by this provider at all) is reported as
        // absent, so no read path can expose it.
        if !map::owned_by(sandbox, self.tenant()) {
            return Err(ProviderError::NotFound(sandbox.id.clone()));
        }
        let known = map::machine_id(&sandbox.id)
            .ok()
            .and_then(|id| self.ops.machine(id));
        let fallback = fallback.or(known.as_ref().map(|value| &value.contract));
        let last_checkpoint =
            last_checkpoint.or(known.as_ref().and_then(|value| value.last_checkpoint));
        let now = now_unix_ms();
        let observation = map::sandbox_to_observation(sandbox, fallback, last_checkpoint, now)?;
        self.ops.observe_machine(observation.clone(), now);
        Ok(observation)
    }

    /// Reads a machine from Daytona, falling back to the registry once Daytona forgot it.
    async fn fetch_machine(&self, machine: MachineId) -> Result<MachineObservation, ProviderError> {
        match self.api.get(&machine.to_string()).await {
            Ok(sandbox) => self.observe(&sandbox, None, None),
            Err(ProviderError::NotFound(_)) => self
                .ops
                .machine(machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string())),
            Err(error) => Err(error),
        }
    }

    /// Polls a sandbox until its state settles.
    async fn wait_settled(
        &self,
        sandbox_id: &str,
        key: IdempotencyKey,
    ) -> Result<Sandbox, ProviderError> {
        let deadline = tokio::time::Instant::now() + self.config.ready_timeout;
        loop {
            let sandbox = self.api.get(sandbox_id).await?;
            if map::is_settled(sandbox.state.as_deref()) {
                if map::machine_state(sandbox.state.as_deref()) == MachineState::Failed {
                    tracing::error!(sandbox_id, reason = ?sandbox.error_reason, "Daytona sandbox failed");
                    return Err(ProviderError::Failed);
                }
                return Ok(sandbox);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ProviderError::Indeterminate(key));
            }
            tokio::time::sleep(self.config.poll_interval).await;
        }
    }

    /// Polls a snapshot by id or name until it is active or failed. A snapshot that does not
    /// exist yet counts as pending, since `POST /sandbox/{id}/snapshot` returns before the
    /// snapshot record is readable.
    async fn wait_snapshot(
        &self,
        snapshot: &str,
        key: IdempotencyKey,
    ) -> Result<Snapshot, ProviderError> {
        let deadline = tokio::time::Instant::now() + self.config.ready_timeout;
        loop {
            match self.api.get_snapshot(snapshot).await {
                Ok(value) if map::snapshot_settled(value.state.as_deref()) => {
                    if value
                        .state
                        .as_deref()
                        .is_some_and(|state| state.eq_ignore_ascii_case("active"))
                    {
                        return Ok(value);
                    }
                    tracing::error!(snapshot, reason = ?value.error_reason, "Daytona snapshot failed");
                    return Err(ProviderError::Failed);
                }
                Ok(_) | Err(ProviderError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ProviderError::Indeterminate(key));
            }
            tokio::time::sleep(self.config.poll_interval).await;
        }
    }

    /// Creates a sandbox, or adopts the one an earlier attempt under the same deterministic
    /// name already created.
    ///
    /// The name alone proves nothing: in a shared organization another tenant or an unrelated
    /// workload can hold it. A name holder is adopted only when its provider labels (managed,
    /// key, kind, fork slot, tenant, and the serialized contract with its image) are exactly the
    /// ones this request attaches; otherwise the create fails with a conflict. The flag is
    /// `true` for an adopted sandbox (see [`Self::claim`]).
    async fn create_or_adopt(
        &self,
        body: &api::CreateSandboxRequest,
    ) -> Result<(Sandbox, bool), ProviderError> {
        match self.api.create(body).await {
            Err(ProviderError::Conflict(detail)) => {
                let Some(name) = &body.name else {
                    return Err(ProviderError::Conflict(detail));
                };
                let existing = self
                    .api
                    .get(name)
                    .await
                    .map_err(|_| ProviderError::Conflict(detail.clone()))?;
                if map::provider_labels(&existing.labels) == map::provider_labels(&body.labels) {
                    Ok((existing, true))
                } else {
                    Err(ProviderError::Conflict(format!(
                        "sandbox {name} exists but was not created by this request: {detail}"
                    )))
                }
            }
            other => other.map(|created| (created, false)),
        }
    }

    /// Deletes, best-effort and newest first, the sandboxes a failed operation created. Any
    /// that cannot be deleted stay recorded, so a later `cancel` retries them.
    async fn roll_back(&self, operation: OperationId) {
        let mut remaining = Vec::new();
        for sandbox in self.ops.take_created(operation).into_iter().rev() {
            match self.api.delete(&sandbox).await {
                Ok(()) | Err(ProviderError::NotFound(_)) => {}
                Err(error) => {
                    tracing::warn!(sandbox, %error, "rollback delete of a failed operation's sandbox failed");
                    remaining.push(sandbox);
                }
            }
        }
        self.ops.restore_created(operation, remaining);
    }

    /// Records a sandbox this attempt produced: [`Self::claim_created`] for one it created, and
    /// for one it `adopted` from an earlier attempt under the same key a binding that
    /// cancellation and rollback never delete. An adopted sandbox may already have been
    /// reported to the caller as that key's successful outcome (for example before a provider
    /// restart), so undoing this attempt must not destroy it.
    async fn claim(
        &self,
        operation: OperationId,
        sandbox_id: &str,
        adopted: bool,
    ) -> Result<(), ProviderError> {
        if !adopted {
            return self.claim_created(operation, sandbox_id).await;
        }
        if self.ops.bind_adopted(operation, sandbox_id) {
            Ok(())
        } else {
            Err(ProviderError::Cancelled)
        }
    }

    /// Records a sandbox the operation created. When the operation was cancelled while the
    /// create or fork request was in flight, cancellation could not see the sandbox, so it is
    /// deleted here and the mutation ends as cancelled.
    async fn claim_created(
        &self,
        operation: OperationId,
        sandbox_id: &str,
    ) -> Result<(), ProviderError> {
        if self.ops.bind_created(operation, sandbox_id) {
            return Ok(());
        }
        if let Err(error) = self.api.delete(sandbox_id).await {
            tracing::warn!(sandbox_id, %error, "best-effort delete of a sandbox created after cancellation failed");
        }
        Err(ProviderError::Cancelled)
    }

    /// Accepts the holder of a native fork child's deterministic name only when it really is a
    /// fork child of `parent_id` from this request: Daytona must list it among the parent's
    /// forks, and its provider labels must be either the ones this request attaches (already
    /// relabelled) or still the parent's own (inherited, relabel pending). Anything else, such
    /// as another parent's child under a reused key, is a conflict.
    async fn adopt_fork_child(
        &self,
        parent_id: &str,
        name: &str,
        ours: &BTreeMap<String, String>,
        detail: String,
    ) -> Result<Sandbox, ProviderError> {
        let conflict = |why: &str| {
            ProviderError::Conflict(format!(
                "sandbox {name} is not this request's fork child of {parent_id} ({why}): {detail}"
            ))
        };
        let child = self
            .api
            .get(name)
            .await
            .map_err(|_| conflict("unreadable"))?;
        let parent = self.api.get(parent_id).await?;
        if !self
            .api
            .forks(parent_id)
            .await?
            .iter()
            .any(|fork| fork.id == child.id)
        {
            return Err(conflict("not a fork of this parent"));
        }
        let labels = map::provider_labels(&child.labels);
        if labels == map::provider_labels(ours) || labels == map::provider_labels(&parent.labels) {
            Ok(child)
        } else {
            Err(conflict("labelled for another request"))
        }
    }

    /// Runs one mutation under `key`: replays a prior identical success, rejects a rebound
    /// key, and otherwise records the outcome. Failures that happened before any Daytona side
    /// effect release the key so a corrected request can reuse it.
    async fn apply<F>(
        &self,
        key: IdempotencyKey,
        intent: &Intent,
        action: F,
    ) -> Result<MutationOutcome, ProviderError>
    where
        F: AsyncFnOnce(OperationId) -> Result<MutationOutcome, ProviderError>,
    {
        let operation = match self.ops.admit(key, intent)? {
            Admission::Replay(outcome) => return Ok(*outcome),
            Admission::Fresh(operation) => operation,
        };
        match action(operation).await {
            Ok(outcome) => {
                self.ops.complete(operation, outcome.clone());
                match self.ops.inspect(operation).map(|value| value.phase) {
                    Some(OperationPhase::Cancelled) => Err(ProviderError::Cancelled),
                    _ => Ok(outcome),
                }
            }
            Err(error) => {
                let touched = self
                    .ops
                    .record(operation)
                    .is_some_and(|record| !record.created.is_empty() || !record.targets.is_empty());
                let before_side_effect = matches!(
                    error,
                    ProviderError::Invalid(_)
                        | ProviderError::Unsupported(_)
                        | ProviderError::NotFound(_)
                        | ProviderError::Conflict(_)
                        | ProviderError::Rejected(_)
                );
                if !touched && before_side_effect {
                    self.ops.forget(operation);
                } else {
                    self.ops.fail(operation, &error);
                    // A definite failure leaves nothing the caller can use or replay, so undo
                    // whatever the operation created (for example fork children whose
                    // workspace never unpacked). An indeterminate outcome keeps its sandboxes
                    // for `recover`, and `cancel` undoes it.
                    if !matches!(
                        error,
                        ProviderError::Indeterminate(_)
                            | ProviderError::Unavailable
                            | ProviderError::Cancelled
                    ) {
                        self.roll_back(operation).await;
                    }
                }
                Err(error)
            }
        }
    }

    /// Waits for fork children to start and observes them under `contract`.
    async fn settle_children(
        &self,
        ids: Vec<String>,
        key: IdempotencyKey,
        contract: &MachineContract,
        checkpoint: Option<CheckpointId>,
    ) -> Result<Vec<MachineObservation>, ProviderError> {
        let mut children = Vec::with_capacity(ids.len());
        for id in ids {
            let settled = self.wait_settled(&id, key).await?;
            let observation = self.observe(&settled, Some(contract), checkpoint)?;
            if observation.state != MachineState::Running {
                return Err(ProviderError::Failed);
            }
            children.push(observation);
        }
        Ok(children)
    }

    /// Native VM fork (`POST /sandbox/{id}/fork`) as a doubling tree.
    ///
    /// A Daytona VM runs one fork at a time: it stays `forking` for several seconds and refuses
    /// a second fork with 400 ("must be in started state") until it is `started` again. Forking
    /// every child from the parent would take `count` sequential rounds, so every node that
    /// already exists forks once per round instead: round one forks the parent, round two the
    /// parent and child 0 in parallel, round three the parent and children 0 to 2, and so on
    /// (see [`map::fork_sources`]). `count` children take `ceil(log2(count + 1))` rounds, for
    /// example 15 in four and 16 in five. Each fork waits for its source to be started and
    /// retries the state refusal with backoff until the ready timeout.
    ///
    /// Every child is named deterministically from `(key, index)` and relabelled as a live-fork
    /// child of the *requested* machine (`acyclic.parent`), with the sandbox it was actually
    /// forked from in `acyclic.fork_source`. A child forked from an earlier child inherits what
    /// that child executed since its own fork; see [`MachinesProvider::fork_machine`].
    async fn native_fork(
        &self,
        operation: OperationId,
        parent: &MachineObservation,
        count: u32,
        key: IdempotencyKey,
    ) -> Result<Vec<MachineObservation>, ProviderError> {
        let mut nodes = vec![parent.id.to_string()];
        let mut ids: Vec<String> = Vec::with_capacity(count as usize);
        for round in map::fork_rounds(count) {
            let forks = round.iter().map(|&(index, source)| {
                let source = nodes.get(source).cloned().unwrap_or_default();
                let slot = map::ForkSlot { index, count };
                async move { self.fork_one(operation, parent, &source, slot, key).await }
            });
            // Wait for every fork of the round, even after one fails, so no child is left
            // created but unrecorded; the failure then rolls back the ones that were.
            let mut failure = None;
            for result in futures::future::join_all(forks).await {
                match result {
                    Ok(child) => ids.push(child),
                    Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            nodes.extend(ids.iter().skip(nodes.len() - 1).cloned());
        }
        self.settle_children(ids, key, &parent.contract, None).await
    }

    /// Forks child `slot` of the requested `parent` from `source` (the parent or an earlier
    /// child) and returns the child's sandbox id once it is started and relabelled.
    async fn fork_one(
        &self,
        operation: OperationId,
        parent: &MachineObservation,
        source: &str,
        slot: map::ForkSlot,
        key: IdempotencyKey,
    ) -> Result<String, ProviderError> {
        let name = map::sandbox_name(key, Some(slot.index));
        let mut ours = self.live_fork_labels(key, slot, parent);
        ours.insert(map::LABEL_FORK_SOURCE.to_owned(), source.to_owned());
        // A native child is complete the moment it exists: memory and disk were copied.
        ours.insert(map::LABEL_READY.to_owned(), "true".to_owned());
        let deadline = tokio::time::Instant::now() + self.config.ready_timeout;
        let mut backoff = self.config.poll_interval;
        let (child, adopted) = loop {
            self.wait_running(source, key).await?;
            let request = ForkRequest {
                name: Some(name.clone()),
            };
            match self.api.fork(source, &request).await {
                Ok(child) => break (child, false),
                Err(ProviderError::Conflict(detail)) => {
                    break (
                        self.adopt_fork_child(source, &name, &ours, detail).await?,
                        true,
                    );
                }
                // Another fork of `source` is still in flight (ours from an earlier attempt,
                // or a state change the read above missed): wait and retry.
                Err(ProviderError::Invalid(detail))
                    if map::fork_refused_for_state(&detail)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(MAX_FORK_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        };
        self.claim(operation, &child.id, adopted).await?;
        self.api
            .replace_labels(&child.id, map::relabel(&child.labels, ours))
            .await?;
        self.wait_running(&child.id, key).await?;
        Ok(child.id)
    }

    /// Waits until `sandbox_id` is settled and running (`started`, not `forking`).
    async fn wait_running(
        &self,
        sandbox_id: &str,
        key: IdempotencyKey,
    ) -> Result<(), ProviderError> {
        let settled = self.wait_settled(sandbox_id, key).await?;
        if map::machine_state(settled.state.as_deref()) == MachineState::Running {
            Ok(())
        } else {
            Err(ProviderError::Conflict(format!(
                "fork source {sandbox_id} is {:?}, not running",
                settled.state
            )))
        }
    }

    /// Container disk fork: archives the parent's workspace directory once, at a quiescent
    /// instant (see [`quiescent_archive_script`]), creates each child from the parent's snapshot
    /// with the parent's lifecycle settings, environment, and foreign labels, and unpacks the
    /// archive into it. The workspace is this provider's declared persistent disk; everything
    /// else is the snapshot both boot from. Processes and memory are not inherited. A child is
    /// labelled ready only after its workspace is unpacked.
    async fn disk_fork(
        &self,
        operation: OperationId,
        parent: &MachineObservation,
        count: u32,
        key: IdempotencyKey,
    ) -> Result<Vec<MachineObservation>, ProviderError> {
        let source = self.api.get(&parent.id.to_string()).await?;
        let snapshot = source.snapshot.clone().ok_or_else(|| {
            ProviderError::Rejected(format!("sandbox {} reports no snapshot", source.id))
        })?;
        let tar_path = format!("/tmp/acyclic-fork-{key}.tar");
        let archive_path = format!("{tar_path}.gz");
        let workspace = shell_quote(&self.config.workspace_dir);
        let archive_quoted = shell_quote(&archive_path);
        match run_status(
            &self.api,
            &source.id,
            quiescent_archive_script(&self.config.workspace_dir, &tar_path, ARCHIVE_ATTEMPTS),
        )
        .await?
        {
            Some(0) => {}
            Some(NOT_QUIESCENT) => {
                return Err(ProviderError::Conflict(format!(
                    "the workspace of {} kept changing across {ARCHIVE_ATTEMPTS} archive attempts; a disk fork needs one consistent instant, so quiesce the workload and retry",
                    source.id
                )));
            }
            _ => return Err(ProviderError::Failed),
        }
        let archive = self.api.download_file(&source.id, &archive_path).await;
        if let Err(error) = run(&self.api, &source.id, format!("rm -f {archive_quoted}")).await {
            tracing::warn!(%error, "best-effort removal of the fork archive in the parent failed");
        }
        let archive = archive?;
        let mut ids = Vec::with_capacity(count as usize);
        for index in 0..count {
            let slot = map::ForkSlot { index, count };
            let mut body =
                map::create_request(&self.config, &snapshot, key, Some(slot), &parent.contract)?;
            body.labels = map::relabel(&source.labels, self.live_fork_labels(key, slot, parent));
            if let Some(minutes) = source
                .auto_stop_interval
                .and_then(|value| u32::try_from(value).ok())
            {
                body.auto_stop_interval = Some(minutes);
            }
            if let Some(minutes) = source.auto_delete_interval {
                body.auto_delete_interval = Some(minutes);
            }
            body.env = inherited_env(&source);
            let (child, adopted) = self.create_or_adopt(&body).await?;
            self.claim(operation, &child.id, adopted).await?;
            ids.push((child.id, body.labels));
        }
        let mut children = Vec::with_capacity(ids.len());
        for (id, mut labels) in ids {
            let settled = self.wait_settled(&id, key).await?;
            self.api
                .upload_file(&settled.id, &archive_path, &archive)
                .await?;
            run(
                &self.api,
                &settled.id,
                format!(
                    "mkdir -p {workspace} && tar -C {workspace} -xzf {archive_quoted} && rm -f {archive_quoted}"
                ),
            )
            .await?;
            // Only now is the child a complete copy; label recovery requires this marker.
            labels.insert(map::LABEL_READY.to_owned(), "true".to_owned());
            self.api.replace_labels(&id, labels).await?;
            let observation = self.observe(&settled, Some(&parent.contract), None)?;
            if observation.state != MachineState::Running {
                return Err(ProviderError::Failed);
            }
            children.push(observation);
        }
        Ok(children)
    }

    fn live_fork_labels(
        &self,
        key: IdempotencyKey,
        slot: map::ForkSlot,
        parent: &MachineObservation,
    ) -> BTreeMap<String, String> {
        let mut labels = map::labels(key, Some(slot), self.tenant(), &parent.contract);
        labels.insert(map::LABEL_KIND.to_owned(), map::KIND_LIVE_FORK.to_owned());
        labels.insert(map::LABEL_PARENT.to_owned(), parent.id.to_string());
        labels
    }

    /// Refuses to destroy a VM whose native fork children are still alive: Daytona would
    /// refuse too, but with an unspecified status. Disk-fork children are independent sandboxes
    /// and never block their source.
    async fn require_no_live_fork_children(
        &self,
        machine: &MachineObservation,
    ) -> Result<(), ProviderError> {
        if machine.contract.fork_fidelity() != Some(ForkFidelity::MemoryAndDisk) {
            return Ok(());
        }
        let live = self
            .api
            .forks(&machine.id.to_string())
            .await?
            .into_iter()
            .filter(|child| map::machine_state(child.state.as_deref()) != MachineState::Destroyed)
            .count();
        if live == 0 {
            Ok(())
        } else {
            Err(ProviderError::Conflict(format!(
                "machine has {live} live fork children; destroy them first"
            )))
        }
    }

    async fn recover_by_label(
        &self,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let tenant = self.tenant();
        let sandboxes: Vec<Sandbox> = self
            .api
            .list(Some(&map::key_filter(key, tenant)))
            .await?
            .into_iter()
            .filter(|sandbox| map::owned_by(sandbox, tenant))
            .collect();
        let Some(first) = sandboxes.first() else {
            return Err(ProviderError::NotFound(key.to_string()));
        };
        let kind = first
            .labels
            .get(map::LABEL_KIND)
            .cloned()
            .unwrap_or_default();
        match kind.as_str() {
            map::KIND_CREATE => match sandboxes.as_slice() {
                [only] => Ok(MutationOutcome::Created(self.observe(only, None, None)?)),
                _ => Err(ProviderError::Indeterminate(key)),
            },
            map::KIND_FORK | map::KIND_LIVE_FORK => {
                // A fork interrupted after some children were created is not a success: the
                // caller asked for `count` workers. Report it as still indeterminate so the
                // caller cancels or retries instead of proceeding with fewer.
                let Some(ordered) = map::complete_fork(&sandboxes) else {
                    return Err(ProviderError::Indeterminate(key));
                };
                let source = ordered
                    .first()
                    .and_then(|sandbox| sandbox.labels.get(map::LABEL_PARENT))
                    .map(|parent| map::machine_id(parent))
                    .transpose()?;
                let children = ordered
                    .into_iter()
                    .map(|sandbox| self.observe(sandbox, None, None))
                    .collect::<Result<Vec<_>, _>>()?;
                if kind != map::KIND_LIVE_FORK {
                    return Ok(MutationOutcome::Forked(children));
                }
                let source = source.ok_or(ProviderError::Indeterminate(key))?;
                let fidelity = children
                    .first()
                    .and_then(|child| child.contract.fork_fidelity())
                    .ok_or(ProviderError::Indeterminate(key))?;
                Ok(MutationOutcome::MachineForked {
                    source,
                    fidelity,
                    children,
                })
            }
            other => Err(ProviderError::Rejected(format!(
                "sandbox under key {key} has unknown kind {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl MachinesProvider for DaytonaProvider {
    /// Daytona is an external service operated in the customer's own Daytona organization;
    /// `CustomerHosted` is the closest of the three declared classes, with Daytona's VM as the
    /// documented boundary. It is not an Acyclic-operated managed service.
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::CustomerHosted
    }

    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        let snapshot = self.resolve_snapshot(&image)?;
        let capabilities = self.snapshot_capabilities(&snapshot).await?;
        Ok(ImageQualification {
            image,
            capabilities,
            compatibility_revision: Self::revision(),
        })
    }

    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let intent = Intent::Create(request.clone());
        let outcome = self
            .apply(key, &intent, async |operation| {
                let snapshot = self.resolve_snapshot(&request.image)?;
                // Everything checkable offline fails before the snapshot lookup.
                Self::check_request(&request)?;
                map::network_fields(&self.config, &request.network_policy_digest)?;
                let capabilities = self.snapshot_capabilities(&snapshot).await?;
                let contract = Self::contract(&request, capabilities)?;
                let mut body = map::create_request(&self.config, &snapshot, key, None, &contract)?;
                body.env = self.staged_boot_env(key).unwrap_or_default();
                let (created, adopted) = self.create_or_adopt(&body).await?;
                self.claim(operation, &created.id, adopted).await?;
                let settled = self.wait_settled(&created.id, key).await?;
                let observation = self.observe(&settled, Some(&contract), None)?;
                if observation.state != MachineState::Running {
                    return Err(ProviderError::Failed);
                }
                Ok(MutationOutcome::Created(observation))
            })
            .await;
        // The staged environment is only useful to the create request itself. Whatever the
        // outcome, a later attempt stages afresh; keeping it would retain the token for nothing.
        self.boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&key);
        outcome
    }

    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        self.fetch_machine(machine).await
    }

    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "machine page limit must be 1..=256".into(),
            ));
        }
        let limit = usize::try_from(limit)
            .map_err(|_| ProviderError::Invalid("invalid page limit".into()))?;
        let mut merged: BTreeMap<MachineId, MachineObservation> = self
            .ops
            .machines()
            .into_iter()
            .map(|value| (value.id, value))
            .collect();
        let tenant = self.tenant();
        let listed = self.api.list(Some(&map::managed_filter(tenant))).await?;
        for sandbox in listed
            .iter()
            .filter(|sandbox| map::owned_by(sandbox, tenant))
        {
            match self.observe(sandbox, None, None) {
                Ok(observation) => {
                    merged.insert(observation.id, observation);
                }
                Err(error) => {
                    tracing::warn!(sandbox = sandbox.id, %error, "skipping sandbox that cannot be observed");
                }
            }
        }
        let mut values = merged
            .into_values()
            .filter(|value| after.is_none_or(|cursor| value.id > cursor))
            .take(limit + 1)
            .collect::<Vec<_>>();
        let next = if values.len() > limit {
            values.pop();
            values.last().map(|value| value.id)
        } else {
            None
        };
        Ok(MachinePage {
            machines: values,
            next,
        })
    }

    /// Takes a hot snapshot (filesystem and memory) of a running VM. The snapshot is an
    /// independent Daytona object: it outlives its source and can be restored any number of
    /// times by [`MachinesProvider::fork`].
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::Checkpoint(machine), async |operation| {
            let source = self.fetch_machine(machine).await?;
            require_capability(&source, Capability::LiveCheckpoint)?;
            if source.state != MachineState::Running {
                return Err(ProviderError::Conflict(
                    "Daytona memory snapshots require a started sandbox".into(),
                ));
            }
            let id = machine.to_string();
            let name = map::checkpoint_name(key);
            self.ops.bind_target(operation, &id);
            let request = CreateSnapshotRequest {
                name: name.clone(),
                include_memory: true,
            };
            match self.api.snapshot(&id, &request).await {
                // A replay after a lost response finds the snapshot already named.
                Ok(_) | Err(ProviderError::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
            let settled = self.wait_snapshot(&name, key).await?;
            self.wait_settled(&id, key).await?;
            let now = now_unix_ms();
            let checkpoint = map::snapshot_to_checkpoint(
                &settled,
                Some(machine),
                source.contract.clone(),
                true,
                now,
            )?;
            self.ops.remember_checkpoint(checkpoint.clone());
            let mut updated = source;
            updated.last_checkpoint = Some(checkpoint.id);
            updated.changed_at_unix_ms = now;
            self.ops.observe_machine(updated, now);
            Ok(MutationOutcome::Checkpointed(checkpoint))
        })
        .await
    }

    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        let known = self.ops.checkpoint(checkpoint);
        match self.api.get_snapshot(&checkpoint.to_string()).await {
            Ok(snapshot) => {
                let (source, contract, forkable) = if let Some(known) = &known {
                    (Some(known.source), known.contract.clone(), known.forkable)
                } else {
                    let source_id = snapshot
                        .source_sandbox_id
                        .as_deref()
                        .map(map::machine_id)
                        .transpose()?
                        .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))?;
                    (
                        Some(source_id),
                        self.fetch_machine(source_id).await?.contract,
                        true,
                    )
                };
                let observation = map::snapshot_to_checkpoint(
                    &snapshot,
                    source,
                    contract,
                    forkable,
                    now_unix_ms(),
                )?;
                self.ops.remember_checkpoint(observation.clone());
                Ok(observation)
            }
            Err(ProviderError::NotFound(_)) => known
                .map(|mut value| {
                    value.forkable = false;
                    value
                })
                .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string())),
            Err(error) => Err(error),
        }
    }

    /// Restores a checkpoint into `count` new sandboxes by creating each from the hot
    /// snapshot. To fork a *running* machine without a checkpoint, use
    /// [`MachinesProvider::fork_machine`].
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        performance: Performance,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count = count.get();
        if count > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply(
            key,
            &Intent::Fork(checkpoint, count, performance),
            async |operation| {
                let source = self.inspect_checkpoint(checkpoint).await?;
                if !source.forkable {
                    return Err(ProviderError::Conflict(
                        "checkpoint no longer accepts forks".into(),
                    ));
                }
                let mut contract = source.contract;
                contract.performance = performance;
                contract.image = Image::Checkpoint(checkpoint);
                let snapshot = checkpoint.to_string();
                let mut ids = Vec::with_capacity(count as usize);
                for index in 0..count {
                    let slot = map::ForkSlot { index, count };
                    let body =
                        map::create_request(&self.config, &snapshot, key, Some(slot), &contract)?;
                    let (created, adopted) = self.create_or_adopt(&body).await?;
                    self.claim(operation, &created.id, adopted).await?;
                    ids.push(created.id);
                }
                let children = self
                    .settle_children(ids, key, &contract, Some(checkpoint))
                    .await?;
                Ok(MutationOutcome::Forked(children))
            },
        )
        .await
    }

    /// Native VM fork for machines declaring [`Capability::LiveFork`] (memory and disk), and
    /// a workspace-copying disk fork for containers declaring [`Capability::DiskFork`]; see
    /// the crate README. Children are labelled with their source (`acyclic.parent`).
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count = count.get();
        if count > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply(
            key,
            &Intent::ForkMachine(machine, count),
            async |operation| {
                let parent = self.fetch_machine(machine).await?;
                let fidelity = parent.contract.fork_fidelity().ok_or_else(|| {
                    ProviderError::Unsupported("machine contract does not declare live fork".into())
                })?;
                if parent.state != MachineState::Running {
                    return Err(ProviderError::Conflict(
                        "only a running machine can be forked".into(),
                    ));
                }
                let children = match fidelity {
                    ForkFidelity::MemoryAndDisk => {
                        self.native_fork(operation, &parent, count, key).await?
                    }
                    ForkFidelity::DiskOnly => {
                        self.disk_fork(operation, &parent, count, key).await?
                    }
                };
                Ok(MutationOutcome::MachineForked {
                    source: machine,
                    fidelity,
                    children,
                })
            },
        )
        .await
    }

    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::Suspend(machine), async |operation| {
            let current = self.fetch_machine(machine).await?;
            require_capability(&current, Capability::SuspendResume)?;
            match current.state {
                MachineState::Suspended => return Ok(MutationOutcome::Suspended(machine)),
                MachineState::Running => {}
                _ => {
                    return Err(ProviderError::Conflict(
                        "only a running machine can be suspended".into(),
                    ));
                }
            }
            let id = machine.to_string();
            self.ops.bind_target(operation, &id);
            self.api.pause(&id).await?;
            let settled = self.wait_settled(&id, key).await?;
            if self.observe(&settled, None, None)?.state != MachineState::Suspended {
                return Err(ProviderError::Failed);
            }
            Ok(MutationOutcome::Suspended(machine))
        })
        .await
    }

    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::Wake(machine), async |operation| {
            let current = self.fetch_machine(machine).await?;
            require_capability(&current, Capability::SuspendResume)?;
            match current.state {
                MachineState::Running => return Ok(MutationOutcome::Woken(machine)),
                MachineState::Suspended => {}
                _ => {
                    return Err(ProviderError::Conflict(
                        "only a suspended machine can be woken".into(),
                    ));
                }
            }
            let id = machine.to_string();
            self.ops.bind_target(operation, &id);
            self.api.start(&id).await?;
            let settled = self.wait_settled(&id, key).await?;
            if self.observe(&settled, None, None)?.state != MachineState::Running {
                return Err(ProviderError::Failed);
            }
            Ok(MutationOutcome::Woken(machine))
        })
        .await
    }

    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::Policy(machine, policy), async |operation| {
            let mut current = self.fetch_machine(machine).await?;
            require_capability(&current, Capability::SuspendResume)?;
            if current.state == MachineState::Destroyed {
                return Err(ProviderError::Conflict(
                    "destroyed machine cannot change policy".into(),
                ));
            }
            let id = machine.to_string();
            self.ops.bind_target(operation, &id);
            self.api
                .set_autopause(&id, map::autopause_minutes(policy))
                .await?;
            let now = now_unix_ms();
            current.contract.suspension = policy;
            current.changed_at_unix_ms = now;
            self.ops.observe_machine(current, now);
            Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
        })
        .await
    }

    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::DestroyMachine(machine), async |operation| {
            let current = self.fetch_machine(machine).await?;
            if current.state == MachineState::Destroyed {
                return Ok(MutationOutcome::MachineDestroyed(machine));
            }
            self.require_no_live_fork_children(&current).await?;
            let id = machine.to_string();
            self.ops.bind_target(operation, &id);
            match self.api.delete(&id).await {
                Ok(()) | Err(ProviderError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
            self.ops.mark_destroyed(machine, now_unix_ms());
            Ok(MutationOutcome::MachineDestroyed(machine))
        })
        .await
    }

    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &Intent::DestroyCheckpoint(checkpoint),
            async |_operation| {
                // Retain an observation so `inspect_checkpoint` keeps answering after deletion.
                self.inspect_checkpoint(checkpoint).await?;
                match self.api.delete_snapshot(&checkpoint.to_string()).await {
                    Ok(()) | Err(ProviderError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
                self.ops.mark_checkpoint_destroyed(checkpoint);
                Ok(MutationOutcome::CheckpointDestroyed(checkpoint))
            },
        )
        .await
    }

    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "event page limit must be 1..=1024".into(),
            ));
        }
        // One poll per call: refreshing the observation appends a state event on change.
        self.fetch_machine(machine).await?;
        let limit = usize::try_from(limit)
            .map_err(|_| ProviderError::Invalid("invalid event limit".into()))?;
        let (events, next_sequence) = self.ops.events(machine, after_sequence, limit);
        Ok(EventPage {
            events,
            next_sequence,
        })
    }

    /// Allocation-based estimate; see [`usage`]. Daytona has no per-sandbox metering endpoint.
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        if start_unix_ms >= end_unix_ms {
            return Err(ProviderError::Invalid(
                "usage interval must be non-empty".into(),
            ));
        }
        let id = machine.to_string();
        let sandbox = self.api.get(&id).await?;
        let observation = self.observe(&sandbox, None, None)?;
        let allocation = usage::Allocation {
            cpu: sandbox.cpu.unwrap_or(0),
            memory_gib: sandbox.memory.unwrap_or(0),
            disk_gib: sandbox.disk.unwrap_or(0),
        };
        usage::receipt(
            machine,
            &id,
            (start_unix_ms, end_unix_ms),
            (observation.created_at_unix_ms, now_unix_ms()),
            observation.contract.performance == Performance::Dedicated,
            allocation,
        )
    }

    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        if let Some(record) = self.ops.record_for_key(key) {
            return match (record.phase, record.outcome) {
                (OperationPhase::Succeeded, Some(outcome)) => Ok(outcome),
                (OperationPhase::Failed, _) => Err(ProviderError::Failed),
                (OperationPhase::Cancelled, _) => Err(ProviderError::Cancelled),
                _ => Err(ProviderError::Indeterminate(key)),
            };
        }
        self.recover_by_label(key).await
    }

    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        self.ops
            .record_for_key(key)
            .map(|record| record.id)
            .ok_or_else(|| ProviderError::NotFound(key.to_string()))
    }

    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.ops
            .inspect(operation)
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))
    }

    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        let (observation, sandboxes) = self
            .ops
            .cancel(operation)
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
        // Newest first: a fork child's own fork children were created after it, and Daytona
        // refuses to delete a sandbox that still has live fork children. Anything that cannot
        // be deleted stays recorded, so cancelling again retries it.
        let mut remaining = Vec::new();
        for sandbox in sandboxes.into_iter().rev() {
            match self.api.delete(&sandbox).await {
                Ok(()) | Err(ProviderError::NotFound(_)) => {}
                Err(error) => {
                    tracing::warn!(sandbox, %error, "delete of a cancelled operation's sandbox failed; cancel again to retry");
                    remaining.push(sandbox);
                }
            }
        }
        remaining.reverse();
        self.ops.restore_created(operation, remaining);
        Ok(observation)
    }

    /// Mutations complete inline, so the stream yields the current observation and, while it
    /// is still pending, one more once it leaves `Pending` or the ready timeout elapses.
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        let current = self.inspect_operation(operation).await?;
        if current.phase != OperationPhase::Pending {
            return Ok(futures::stream::once(async move { Ok(current) }).boxed());
        }
        let last = self
            .ops
            .watch(
                operation,
                self.config.poll_interval,
                self.config.ready_timeout,
            )
            .await;
        Ok(futures::stream::iter([Ok(current), last]).boxed())
    }
}

fn require_capability(
    machine: &MachineObservation,
    capability: Capability,
) -> Result<(), ProviderError> {
    if machine.contract.capabilities.contains(&capability) {
        Ok(())
    } else {
        Err(ProviderError::Unsupported(format!(
            "machine contract does not declare {capability:?}"
        )))
    }
}

/// Runs `command` in `sandbox` through the toolbox and requires exit code zero.
async fn run(api: &DaytonaApi, sandbox_id: &str, command: String) -> Result<(), ProviderError> {
    match run_status(api, sandbox_id, command).await? {
        Some(0) => Ok(()),
        _ => Err(ProviderError::Failed),
    }
}

/// Longest wait between retries of a fork refused because its source is still forking.
const MAX_FORK_BACKOFF: Duration = Duration::from_secs(2);
/// Attempts [`quiescent_archive_script`] makes before giving up.
const ARCHIVE_ATTEMPTS: u32 = 5;
/// Exit status of [`quiescent_archive_script`] when the workspace never held still.
const NOT_QUIESCENT: i64 = 75;

/// POSIX `sh` script that archives `workspace` into `tar_path.gz` only at a quiescent moment.
///
/// `tar` reads files one after another, so a workspace written during the read yields files
/// from different moments. The script archives, then re-reads the tree the same way (both
/// reads stream to stdout, so `tar` pads them identically) and compares checksums. It keeps
/// the archive only when both reads agree and neither `tar` saw a file change under it (a
/// failed re-read appends a marker to its stream, so its checksum can never match), which
/// means nothing changed across the whole read. It retries up to `attempts` times, one second
/// apart, and exits [`NOT_QUIESCENT`] otherwise.
fn quiescent_archive_script(workspace: &str, tar_path: &str, attempts: u32) -> String {
    let (workspace, tar) = (shell_quote(workspace), shell_quote(tar_path));
    format!(
        "mkdir -p {workspace} || exit 1; i=0; \
         while :; do \
           if tar -C {workspace} -cf - . > {tar}; then \
             a=$(cksum < {tar}) && b=$({{ tar -C {workspace} -cf - . || echo changed; }} | cksum) && [ \"$a\" = \"$b\" ] && break; \
           else r=$?; [ $r -eq 1 ] || {{ rm -f {tar}; exit $r; }}; fi; \
           i=$((i+1)); if [ $i -ge {attempts} ]; then rm -f {tar}; exit {NOT_QUIESCENT}; fi; \
           sleep 1; \
         done; \
         gzip -n -f {tar}"
    )
}

/// Runs `command` in the sandbox through the toolbox and returns its exit code.
async fn run_status(
    api: &DaytonaApi,
    sandbox_id: &str,
    command: String,
) -> Result<Option<i64>, ProviderError> {
    let output = api
        .execute(
            sandbox_id,
            &ExecuteRequest {
                command: format!("sh -c {}", shell_quote(&command)),
                cwd: None,
                timeout: Some(300),
            },
        )
        .await?;
    if output.exit_code != Some(0) {
        tracing::error!(sandbox = sandbox_id, exit = ?output.exit_code, output = ?output.result, "toolbox command failed");
    }
    Ok(output.exit_code)
}

/// POSIX single-quoted shell word.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Environment Daytona reports for `sandbox` (`env`), as string pairs.
fn inherited_env(sandbox: &Sandbox) -> BTreeMap<String, String> {
    sandbox
        .extra
        .get("env")
        .and_then(serde_json::Value::as_object)
        .map(|env| {
            env.iter()
                .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn config_defaults_and_redaction() {
        let mut config = DaytonaConfig::new("secret");
        assert_eq!(config.api_url, DEFAULT_API_URL);
        config.register_snapshot([7; 32], "base");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("secret"));
        assert!(rendered.contains("base"));
        assert!(DaytonaApi::new(&DaytonaConfig::new("")).is_err());
    }

    #[test]
    fn debug_output_never_renders_staged_boot_values() {
        let provider = DaytonaProvider::new(DaytonaConfig::new("api-secret")).unwrap();
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000003").unwrap();
        provider.stage_boot_env(
            key,
            BTreeMap::from([
                (
                    "ACYCLIC_BOOT_TOKEN".to_owned(),
                    "org-a.boot-token-secret".to_owned(),
                ),
                (
                    "ACYCLIC_HOST_ENDPOINT".to_owned(),
                    "http://host.example:8080".to_owned(),
                ),
            ]),
        );
        let rendered = format!("{provider:?}");
        assert!(!rendered.contains("boot-token-secret"), "{rendered}");
        assert!(!rendered.contains("host.example"), "{rendered}");
        assert!(!rendered.contains("api-secret"), "{rendered}");
        assert!(rendered.contains("staged_boot_envs: 1"), "{rendered}");
        assert!(rendered.contains("ACYCLIC_BOOT_TOKEN"), "{rendered}");
        let rendered = format!("{provider:#?}");
        assert!(!rendered.contains("boot-token-secret"), "{rendered}");
    }

    #[test]
    fn staged_boot_env_reaches_the_create_body() {
        let provider = DaytonaProvider::new(DaytonaConfig::new("k")).unwrap();
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000002").unwrap();
        assert!(provider.staged_boot_env(key).is_none());
        let env = BTreeMap::from([("ACYCLIC_BOOT_TOKEN".to_owned(), "t".to_owned())]);
        provider.stage_boot_env(key, env.clone());
        assert_eq!(provider.staged_boot_env(key), Some(env.clone()));
        let body = api::CreateSandboxRequest {
            env,
            ..api::CreateSandboxRequest::default()
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["env"]["ACYCLIC_BOOT_TOKEN"], "t");
        let bare = serde_json::to_value(api::CreateSandboxRequest::default()).unwrap();
        assert!(bare.get("env").is_none());
    }

    #[test]
    fn image_resolution_follows_registration_then_default() {
        let mut config = DaytonaConfig::new("k");
        config.register_snapshot([7; 32], "registered");
        let provider = DaytonaProvider::new(config).unwrap();
        assert_eq!(
            provider
                .resolve_snapshot(&Image::custom([7; 32]).unwrap())
                .unwrap(),
            "registered"
        );
        assert!(matches!(
            provider.resolve_snapshot(&Image::custom([9; 32]).unwrap()),
            Err(ProviderError::Unsupported(_))
        ));
        let mut config = DaytonaConfig::new("k");
        config.default_snapshot = Some("fallback".into());
        let provider = DaytonaProvider::new(config).unwrap();
        assert_eq!(
            provider
                .resolve_snapshot(&Image::custom([9; 32]).unwrap())
                .unwrap(),
            "fallback"
        );
        assert_eq!(CAPABILITIES.live_fork_memory, Assurance::Yes);
        assert_eq!(CAPABILITIES.usage_receipts, Assurance::Provisional);
        assert_eq!(CAPABILITIES.restore_cross_host, Assurance::No);
    }

    #[tokio::test]
    async fn unmappable_requests_fail_before_any_side_effect_and_release_the_key() {
        let mut config = DaytonaConfig::new("k");
        config.default_snapshot = Some("base".into());
        let provider = DaytonaProvider::new(config).unwrap();
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001").unwrap();
        let mut request = CreateMachine::new(key, Image::custom([7; 32]).unwrap(), [8; 32]);
        request.budgets.spend_micros = 5;
        provider.stage_boot_env(
            key,
            BTreeMap::from([("ACYCLIC_BOOT_TOKEN".to_owned(), "t".to_owned())]),
        );
        assert!(matches!(
            provider.create(request.clone()).await,
            Err(ProviderError::Unsupported(_))
        ));
        assert!(provider.registry().record_for_key(key).is_none());
        assert!(
            provider.staged_boot_env(key).is_none(),
            "a terminal create failure clears the staged boot environment"
        );
        let op = ops::operation_id(key);
        assert!(matches!(
            provider.inspect_operation(op).await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(matches!(
            provider.cancel(op).await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(matches!(
            provider.recover_operation(key).await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(matches!(
            provider.watch_operation(op).await.err(),
            Some(ProviderError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn fork_counts_are_bounded_before_any_request() {
        let provider = DaytonaProvider::new(DaytonaConfig::new("k")).unwrap();
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000004").unwrap();
        let too_many = NonZeroU32::new(MAX_FORK_CHILDREN + 1).unwrap();
        assert!(matches!(
            provider.fork_machine(MachineId::new(), too_many, key).await,
            Err(ProviderError::Invalid(_))
        ));
    }

    #[test]
    fn shell_words_and_inherited_environment() {
        assert_eq!(
            shell_quote("/home/daytona/work space"),
            "'/home/daytona/work space'"
        );
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
        let sandbox: Sandbox = serde_json::from_value(serde_json::json!({
            "id": "6f1d2c3b-4a5e-4f60-9b71-8c2d3e4f5a61",
            "env": { "ACYCLIC_HOST": "h", "NUMBER": 7 }
        }))
        .unwrap();
        assert_eq!(
            inherited_env(&sandbox),
            BTreeMap::from([("ACYCLIC_HOST".to_owned(), "h".to_owned())])
        );
    }

    #[test]
    fn container_contracts_declare_disk_fork_and_never_auto_suspend() {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000005").unwrap();
        let request = CreateMachine::new(key, Image::custom([7; 32]).unwrap(), [8; 32]);
        let container = DaytonaProvider::contract(
            &request,
            map::capabilities_for_class(Some("container")).unwrap(),
        )
        .unwrap();
        assert_eq!(container.fork_fidelity(), Some(ForkFidelity::DiskOnly));
        assert_eq!(container.suspension, SuspensionPolicy::Manual);
        let mut config = DaytonaConfig::new("k");
        config.register_network_policy([8; 32], map::NetworkPolicy::BlockAll);
        let body = map::create_request(&config, "s", key, None, &container).unwrap();
        assert!(
            serde_json::to_value(&body)
                .unwrap()
                .get("autoPauseInterval")
                .is_none()
        );
        let vm = DaytonaProvider::contract(
            &request,
            map::capabilities_for_class(Some("linux-vm")).unwrap(),
        )
        .unwrap();
        assert_eq!(vm.fork_fidelity(), Some(ForkFidelity::MemoryAndDisk));
        assert_eq!(vm.suspension, request.suspension);
        let mut require = request;
        require.compatibility =
            CompatibilityPolicy::Require(BTreeSet::from([Capability::LiveFork]));
        assert!(matches!(
            DaytonaProvider::contract(
                &require,
                map::capabilities_for_class(Some("container")).unwrap()
            ),
            Err(ProviderError::Unsupported(_))
        ));
    }

    const MACHINE: &str = "6f1d2c3b-4a5e-4f60-9b71-8c2d3e4f5a61";
    const OTHER: &str = "7a2e3d4c-5b6f-4a71-8c82-9d3e4f5a6b72";
    const CHILD: &str = "8b3f4e5d-6c7a-4b82-9d93-ae4f5a6b7c83";

    fn test_key(suffix: u8) -> IdempotencyKey {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000001{suffix:02x}")).unwrap()
    }

    fn request(key: IdempotencyKey) -> CreateMachine {
        CreateMachine::new(key, Image::custom([7; 32]).unwrap(), [8; 32])
    }

    fn contract() -> MachineContract {
        DaytonaProvider::contract(
            &request(test_key(0)),
            map::capabilities_for_class(Some("linux-vm")).unwrap(),
        )
        .unwrap()
    }

    fn mocked(mock: &mock::Mock, tenant: Option<&str>) -> Arc<DaytonaProvider> {
        let mut config = DaytonaConfig::new("secret-key");
        config.api_url = mock.url.clone();
        config.default_snapshot = Some("base".into());
        config.tenant = tenant.map(str::to_owned);
        config.poll_interval = Duration::from_millis(1);
        config.ready_timeout = Duration::from_secs(5);
        config.register_network_policy([8; 32], map::NetworkPolicy::BlockAll);
        Arc::new(DaytonaProvider::new(config).unwrap())
    }

    fn sandbox(id: &str, state: &str, labels: &BTreeMap<String, String>) -> String {
        serde_json::json!({ "id": id, "state": state, "labels": labels }).to_string()
    }

    fn owned_labels(key: IdempotencyKey, tenant: Option<&str>) -> BTreeMap<String, String> {
        map::labels(key, None, tenant, &contract())
    }

    fn gate() -> Arc<tokio::sync::Semaphore> {
        Arc::new(tokio::sync::Semaphore::new(0))
    }

    #[tokio::test]
    async fn cancelling_an_operation_on_an_existing_machine_never_deletes_it() {
        let paused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = gate();
        let (state, hold) = (Arc::clone(&paused), Arc::clone(&release));
        let mock = mock::Mock::start(move |request| {
            let (state, hold) = (Arc::clone(&state), Arc::clone(&hold));
            async move {
                if request.is("POST", &format!("/sandbox/{MACHINE}/pause")) {
                    hold.acquire().await.unwrap().forget();
                    state.store(true, std::sync::atomic::Ordering::SeqCst);
                    return (200, String::new());
                }
                let now = if state.load(std::sync::atomic::Ordering::SeqCst) {
                    "paused"
                } else {
                    "started"
                };
                (200, sandbox(MACHINE, now, &owned_labels(test_key(9), None)))
            }
        })
        .await;
        let provider = mocked(&mock, None);
        let machine = MachineId::parse(MACHINE).unwrap();
        let key = test_key(1);
        let task = tokio::spawn({
            let provider = Arc::clone(&provider);
            async move { provider.suspend(machine, key).await }
        });
        mock.wait_for("POST", &format!("/sandbox/{MACHINE}/pause"))
            .await;
        let cancelled = provider.cancel(ops::operation_id(key)).await.unwrap();
        assert_eq!(cancelled.phase, OperationPhase::Cancelled);
        release.add_permits(1);
        assert!(matches!(task.await.unwrap(), Err(ProviderError::Cancelled)));
        assert!(
            !mock.requests().iter().any(|r| r.method == "DELETE"),
            "cancelling a suspend must not delete the machine it acts on"
        );
    }

    #[tokio::test]
    async fn a_sandbox_created_after_cancellation_is_deleted() {
        let release = gate();
        let hold = Arc::clone(&release);
        let mock = mock::Mock::start(move |request| {
            let hold = Arc::clone(&hold);
            async move {
                if request.path.starts_with("/snapshots/") {
                    return (200, r#"{"id":"s","sandboxClass":"linux-vm"}"#.to_owned());
                }
                if request.is("POST", "/sandbox") {
                    hold.acquire().await.unwrap().forget();
                    let labels = owned_labels(test_key(2), None);
                    return (201, sandbox(CHILD, "creating", &labels));
                }
                (200, String::new())
            }
        })
        .await;
        let provider = mocked(&mock, None);
        let key = test_key(2);
        let task = tokio::spawn({
            let provider = Arc::clone(&provider);
            async move { provider.create(request(key)).await }
        });
        mock.wait_for("POST", "/sandbox").await;
        provider.cancel(ops::operation_id(key)).await.unwrap();
        release.add_permits(1);
        assert!(matches!(task.await.unwrap(), Err(ProviderError::Cancelled)));
        assert!(
            mock.requests()
                .iter()
                .any(|r| r.is("DELETE", &format!("/sandbox/{CHILD}"))),
            "the sandbox the in-flight create made must not outlive the cancellation"
        );
    }

    #[tokio::test]
    async fn create_adopts_only_its_own_name_holder() {
        let key = test_key(3);
        for (holder, adopted) in [
            (owned_labels(key, Some("other-tenant")), false),
            (owned_labels(test_key(4), None), false),
            (BTreeMap::new(), false),
            (owned_labels(key, None), true),
        ] {
            let labels = holder.clone();
            let mock = mock::Mock::start(move |request| {
                let labels = labels.clone();
                async move {
                    if request.path.starts_with("/snapshots/") {
                        return (200, r#"{"id":"s","sandboxClass":"linux-vm"}"#.to_owned());
                    }
                    if request.is("POST", "/sandbox") {
                        return (409, "name taken".to_owned());
                    }
                    (200, sandbox(OTHER, "started", &labels))
                }
            })
            .await;
            let outcome = mocked(&mock, None).create(request(key)).await;
            if adopted {
                assert!(
                    matches!(outcome, Ok(MutationOutcome::Created(ref m)) if m.id.to_string() == OTHER),
                    "{outcome:?}"
                );
            } else {
                assert!(
                    matches!(outcome, Err(ProviderError::Conflict(_))),
                    "{holder:?} -> {outcome:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn native_fork_never_adopts_another_request_or_parent_child() {
        let key = test_key(5);
        let child_name = map::sandbox_name(key, Some(0));
        // Child of another parent (not in this parent's forks), and a child of this parent
        // already relabelled for a different request under the same name.
        let foreign = map::labels(
            key,
            Some(map::ForkSlot { index: 0, count: 2 }),
            None,
            &contract(),
        );
        for (listed, labels) in [(false, owned_labels(test_key(9), None)), (true, foreign)] {
            let name = child_name.clone();
            let mock = mock::Mock::start(move |request| {
                let (name, labels) = (name.clone(), labels.clone());
                async move {
                    if request.is("POST", &format!("/sandbox/{MACHINE}/fork")) {
                        return (409, "name taken".to_owned());
                    }
                    if request.is("GET", &format!("/sandbox/{MACHINE}/forks")) {
                        let forks = if listed {
                            format!("[{}]", sandbox(CHILD, "started", &labels))
                        } else {
                            "[]".to_owned()
                        };
                        return (200, forks);
                    }
                    if request.is("GET", &format!("/sandbox/{name}")) {
                        return (200, sandbox(CHILD, "started", &labels));
                    }
                    (
                        200,
                        sandbox(MACHINE, "started", &owned_labels(test_key(9), None)),
                    )
                }
            })
            .await;
            let provider = mocked(&mock, None);
            let outcome = provider
                .fork_machine(
                    MachineId::parse(MACHINE).unwrap(),
                    NonZeroU32::new(1).unwrap(),
                    key,
                )
                .await;
            assert!(
                matches!(outcome, Err(ProviderError::Conflict(_))),
                "{outcome:?}"
            );
            assert!(
                !mock.requests().iter().any(|r| r.method == "PUT"),
                "a foreign child must never be relabelled"
            );
        }
    }

    #[tokio::test]
    async fn listing_and_reads_stay_within_the_configured_tenant() {
        let mock = mock::Mock::start(|request| async move {
            let ours = sandbox(MACHINE, "started", &owned_labels(test_key(6), Some("a")));
            let theirs = sandbox(OTHER, "started", &owned_labels(test_key(7), Some("b")));
            let untenanted = sandbox(CHILD, "started", &owned_labels(test_key(8), None));
            if request.is("GET", "/sandbox") {
                return (
                    200,
                    format!(r#"{{"items":[{ours},{theirs},{untenanted}]}}"#),
                );
            }
            if request.path.ends_with(OTHER) {
                return (200, theirs);
            }
            (404, String::new())
        })
        .await;
        let provider = mocked(&mock, Some("a"));
        let page = provider.list_machines(None, 16).await.unwrap();
        let ids: Vec<String> = page.machines.iter().map(|m| m.id.to_string()).collect();
        assert_eq!(ids, [MACHINE]);
        let filter = mock
            .requests()
            .into_iter()
            .find(|r| r.is("GET", "/sandbox"))
            .and_then(|r| r.query.into_iter().find(|(name, _)| name == "labels"))
            .unwrap()
            .1;
        assert!(filter.contains(r#""acyclic.tenant":"a""#), "{filter}");
        assert!(matches!(
            provider
                .inspect_machine(MachineId::parse(OTHER).unwrap())
                .await,
            Err(ProviderError::NotFound(_))
        ));
        let page = provider.list_machines(None, 16).await.unwrap();
        assert_eq!(
            page.machines.len(),
            1,
            "a foreign read must not enter the registry"
        );
    }

    #[tokio::test]
    async fn label_recovery_reports_a_partial_fork_as_indeterminate() {
        let key = test_key(10);
        let child = |id: &str, index: u32| {
            let slot = map::ForkSlot { index, count: 3 };
            sandbox(
                id,
                "started",
                &map::labels(key, Some(slot), None, &contract()),
            )
        };
        let partial = format!(r#"{{"items":[{},{}]}}"#, child(MACHINE, 0), child(CHILD, 2));
        let full = format!(
            r#"{{"items":[{},{},{}]}}"#,
            child(CHILD, 2),
            child(MACHINE, 0),
            child(OTHER, 1)
        );
        for (items, complete) in [(partial, false), (full, true)] {
            let mock = mock::Mock::start(move |_| {
                let items = items.clone();
                async move { (200, items) }
            })
            .await;
            let outcome = mocked(&mock, None).recover(key).await;
            if complete {
                let Ok(MutationOutcome::Forked(children)) = outcome else {
                    panic!("{outcome:?}")
                };
                let ids: Vec<String> = children.iter().map(|c| c.id.to_string()).collect();
                assert_eq!(ids, [MACHINE, OTHER, CHILD]);
            } else {
                assert!(
                    matches!(outcome, Err(ProviderError::Indeterminate(k)) if k == key),
                    "{outcome:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn execute_sends_the_key_only_to_the_proxy_daytona_reports() {
        for attacker in [true, false] {
            let own_url = Arc::new(Mutex::new(String::new()));
            let reported = Arc::clone(&own_url);
            let mock = mock::Mock::start(move |request| {
                let reported = Arc::clone(&reported);
                async move {
                    if request.method == "POST" {
                        return (200, r#"{"exitCode":0,"result":"ok"}"#.to_owned());
                    }
                    let proxy = if attacker {
                        "https://attacker.example/toolbox".to_owned()
                    } else {
                        format!("{}/toolbox", reported.lock().unwrap())
                    };
                    let body = serde_json::json!({ "id": MACHINE, "toolboxProxyUrl": proxy });
                    (200, body.to_string())
                }
            })
            .await;
            mock.url.clone_into(&mut own_url.lock().unwrap());
            let result = mocked(&mock, None)
                .api()
                .execute(
                    MACHINE,
                    &api::ExecuteRequest {
                        command: "true".into(),
                        cwd: None,
                        timeout: None,
                    },
                )
                .await;
            let posts: Vec<_> = mock
                .requests()
                .into_iter()
                .filter(|r| r.method == "POST")
                .collect();
            if attacker {
                assert!(
                    matches!(result, Err(ProviderError::Rejected(_))),
                    "{result:?}"
                );
                assert!(posts.is_empty());
            } else {
                assert_eq!(result.unwrap().exit_code, Some(0));
                assert_eq!(posts.len(), 1);
                assert_eq!(posts[0].path, format!("/toolbox/{MACHINE}/process/execute"));
                assert_eq!(posts[0].authorization.as_deref(), Some("Bearer secret-key"));
            }
        }
    }

    #[tokio::test]
    async fn an_unregistered_network_commitment_is_refused_before_any_request() {
        let mock = mock::Mock::start(|_| async { (500, String::new()) }).await;
        let provider = mocked(&mock, None);
        let mut unknown = request(test_key(11));
        unknown.network_policy_digest = [3; 32];
        assert!(matches!(
            provider.create(unknown).await,
            Err(ProviderError::Unsupported(_))
        ));
        assert!(mock.requests().is_empty(), "{:?}", mock.requests());
    }

    /// Fake Daytona fork API with the rule measured on real VMs: a sandbox runs one fork at a
    /// time, is `forking` for a while afterwards, and refuses another fork with 400 until it
    /// is `started` again.
    struct FakeForks {
        boxes: BTreeMap<String, FakeBox>,
        next: u32,
        refuse_first: bool,
        refused: u32,
        busy_violations: u32,
        max_forking: usize,
        forks: Vec<String>,
    }

    struct FakeBox {
        name: String,
        labels: BTreeMap<String, String>,
        source: Option<String>,
        ready_at: std::time::Instant,
        busy_until: std::time::Instant,
    }

    const FORK_BUSY: Duration = Duration::from_millis(40);
    const CHILD_BOOT: Duration = Duration::from_millis(10);

    impl FakeForks {
        fn new(parent_labels: BTreeMap<String, String>, refuse_first: bool) -> Self {
            let now = std::time::Instant::now();
            let parent = FakeBox {
                name: MACHINE.to_owned(),
                labels: parent_labels,
                source: None,
                ready_at: now,
                busy_until: now,
            };
            Self {
                boxes: BTreeMap::from([(MACHINE.to_owned(), parent)]),
                next: 1,
                refuse_first,
                refused: 0,
                busy_violations: 0,
                max_forking: 0,
                forks: Vec::new(),
            }
        }

        fn state(sandbox: &FakeBox, now: std::time::Instant) -> &'static str {
            if now < sandbox.ready_at {
                "creating"
            } else if now < sandbox.busy_until {
                "forking"
            } else {
                "started"
            }
        }

        fn resolve(&self, id_or_name: &str) -> Option<String> {
            self.boxes
                .iter()
                .find(|(id, value)| *id == id_or_name || value.name == id_or_name)
                .map(|(id, _)| id.clone())
        }

        fn json(&self, id: &str, now: std::time::Instant) -> serde_json::Value {
            let value = &self.boxes[id];
            serde_json::json!({
                "id": id, "name": value.name, "state": Self::state(value, now),
                "labels": value.labels,
            })
        }

        fn handle(&mut self, request: &mock::Request) -> mock::Reply {
            let now = std::time::Instant::now();
            let parts: Vec<&str> = request.path.trim_start_matches('/').split('/').collect();
            match (request.method.as_str(), parts.as_slice()) {
                ("POST", ["sandbox", source, "fork"]) => {
                    let Some(source) = self.resolve(source) else {
                        return (404, String::new());
                    };
                    let busy = Self::state(&self.boxes[&source], now) != "started";
                    if busy || std::mem::take(&mut self.refuse_first) {
                        self.refused += 1;
                        self.busy_violations += u32::from(busy);
                        let refusal = r#"{"message":"Sandbox must be in started state to fork"}"#;
                        return (400, refusal.to_owned());
                    }
                    let name: serde_json::Value = serde_json::from_str(&request.body).unwrap();
                    let name = name["name"].as_str().unwrap().to_owned();
                    if self.resolve(&name).is_some() {
                        return (409, "name taken".to_owned());
                    }
                    let id = format!("00000000-0000-4000-8000-{:012}", self.next);
                    self.next += 1;
                    let labels = self.boxes[&source].labels.clone();
                    self.boxes.get_mut(&source).unwrap().busy_until = now + FORK_BUSY;
                    let forking = self
                        .boxes
                        .values()
                        .filter(|value| Self::state(value, now) == "forking")
                        .count();
                    self.max_forking = self.max_forking.max(forking);
                    self.forks.push(source.clone());
                    self.boxes.insert(
                        id.clone(),
                        FakeBox {
                            name,
                            labels,
                            source: Some(source),
                            ready_at: now + CHILD_BOOT,
                            busy_until: now,
                        },
                    );
                    (200, self.json(&id, now).to_string())
                }
                ("GET", ["sandbox", source, "forks"]) => {
                    let source = self.resolve(source).unwrap_or_default();
                    let children: Vec<serde_json::Value> = self
                        .boxes
                        .iter()
                        .filter(|(_, value)| value.source.as_deref() == Some(source.as_str()))
                        .map(|(id, _)| self.json(id, now))
                        .collect();
                    (200, serde_json::Value::from(children).to_string())
                }
                ("PUT", ["sandbox", id, "labels"]) => {
                    let body: serde_json::Value = serde_json::from_str(&request.body).unwrap();
                    let labels = serde_json::from_value(body["labels"].clone()).unwrap();
                    let id = self.resolve(id).unwrap();
                    self.boxes.get_mut(&id).unwrap().labels = labels;
                    (200, String::new())
                }
                ("GET", ["sandbox", id]) => match self.resolve(id) {
                    Some(id) => (200, self.json(&id, now).to_string()),
                    None => (404, String::new()),
                },
                ("DELETE", ["sandbox", id]) => {
                    let id = self.resolve(id).unwrap_or_default();
                    self.boxes.remove(&id);
                    (200, String::new())
                }
                _ => (404, String::new()),
            }
        }
    }

    async fn fake_forks(refuse_first: bool) -> (mock::Mock, Arc<Mutex<FakeForks>>) {
        let fake = Arc::new(Mutex::new(FakeForks::new(
            owned_labels(test_key(12), None),
            refuse_first,
        )));
        let shared = Arc::clone(&fake);
        let mock = mock::Mock::start(move |request| {
            let reply = shared.lock().unwrap().handle(&request);
            async move { reply }
        })
        .await;
        (mock, fake)
    }

    #[tokio::test]
    async fn native_fork_fans_out_as_a_doubling_tree_one_fork_per_source() {
        let (mock, fake) = fake_forks(true).await;
        let provider = mocked(&mock, None);
        let key = test_key(13);
        let outcome = provider
            .fork_machine(
                MachineId::parse(MACHINE).unwrap(),
                NonZeroU32::new(16).unwrap(),
                key,
            )
            .await;
        let Ok(MutationOutcome::MachineForked {
            source,
            fidelity,
            children,
        }) = outcome
        else {
            panic!("{outcome:?}")
        };
        assert_eq!(source.to_string(), MACHINE);
        assert_eq!(fidelity, ForkFidelity::MemoryAndDisk);
        assert_eq!(children.len(), 16);
        let fake = fake.lock().unwrap();
        assert_eq!(
            fake.refused, 1,
            "only the injected refusal; sources are awaited"
        );
        assert_eq!(fake.busy_violations, 0);
        assert!(fake.max_forking > 1, "forks of one round run in parallel");
        assert_eq!(fake.forks.len(), 16);
        // Every child reports the requested machine as its parent and records the node it
        // was forked from, following the doubling rounds.
        let nodes: Vec<String> = std::iter::once(MACHINE.to_owned())
            .chain(children.iter().map(|child| child.id.to_string()))
            .collect();
        for (index, source) in map::fork_rounds(16).into_iter().flatten() {
            let child = &fake.boxes[&children[index as usize].id.to_string()];
            assert_eq!(child.name, map::sandbox_name(key, Some(index)));
            assert_eq!(child.labels[map::LABEL_PARENT], MACHINE);
            assert_eq!(child.labels[map::LABEL_FORK_SOURCE], nodes[source]);
            assert_eq!(child.source.as_deref(), Some(nodes[source].as_str()));
            assert_eq!(child.labels[map::LABEL_READY], "true");
            assert_eq!(child.labels[map::LABEL_COUNT], "16");
        }
        // The children form one complete fork for label recovery after a restart.
        let listed: Vec<Sandbox> = children
            .iter()
            .map(|child| Sandbox {
                id: child.id.to_string(),
                labels: fake.boxes[&child.id.to_string()].labels.clone(),
                ..Sandbox::default()
            })
            .collect();
        assert_eq!(map::complete_fork(&listed).unwrap().len(), 16);
    }

    #[tokio::test]
    async fn a_definitively_failed_fork_deletes_the_children_it_created() {
        // The second fork is refused with a non-retryable 400: child 0 must not be stranded.
        let forks = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let seen = Arc::clone(&forks);
        let mock = mock::Mock::start(move |request| {
            let seen = Arc::clone(&seen);
            async move {
                if request.is("POST", &format!("/sandbox/{MACHINE}/fork")) {
                    if seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        return (
                            200,
                            sandbox(CHILD, "started", &owned_labels(test_key(9), None)),
                        );
                    }
                    return (400, "invalid fork request".to_owned());
                }
                if request.method == "PUT" || request.method == "DELETE" {
                    return (200, String::new());
                }
                let id = if request.path.ends_with(CHILD) {
                    CHILD
                } else {
                    MACHINE
                };
                (
                    200,
                    sandbox(id, "started", &owned_labels(test_key(9), None)),
                )
            }
        })
        .await;
        let provider = mocked(&mock, None);
        let outcome = provider
            .fork_machine(
                MachineId::parse(MACHINE).unwrap(),
                NonZeroU32::new(2).unwrap(),
                test_key(14),
            )
            .await;
        assert!(
            matches!(outcome, Err(ProviderError::Invalid(_))),
            "{outcome:?}"
        );
        let deletes: Vec<String> = mock
            .requests()
            .into_iter()
            .filter(|r| r.method == "DELETE")
            .map(|r| r.path)
            .collect();
        assert_eq!(deletes, [format!("/sandbox/{CHILD}")]);
    }

    #[tokio::test]
    async fn cancelling_an_indeterminate_fork_deletes_its_children() {
        let mock = mock::Mock::start(|request| async move {
            if request.is("POST", &format!("/sandbox/{MACHINE}/fork")) {
                return (
                    200,
                    sandbox(CHILD, "started", &owned_labels(test_key(9), None)),
                );
            }
            if request.method == "PUT" || request.method == "DELETE" {
                return (200, String::new());
            }
            if request.path.ends_with(CHILD) {
                // The child never settles, so the fork ends indeterminate.
                return (
                    200,
                    sandbox(CHILD, "creating", &owned_labels(test_key(9), None)),
                );
            }
            (
                200,
                sandbox(MACHINE, "started", &owned_labels(test_key(9), None)),
            )
        })
        .await;
        let mut provider = Arc::into_inner(mocked(&mock, None)).unwrap();
        provider.config.ready_timeout = Duration::from_millis(50);
        let key = test_key(15);
        let outcome = provider
            .fork_machine(
                MachineId::parse(MACHINE).unwrap(),
                NonZeroU32::new(1).unwrap(),
                key,
            )
            .await;
        assert!(
            matches!(outcome, Err(ProviderError::Indeterminate(_))),
            "{outcome:?}"
        );
        assert!(!mock.requests().iter().any(|r| r.method == "DELETE"));
        let cancelled = provider.cancel(ops::operation_id(key)).await.unwrap();
        assert_eq!(cancelled.phase, OperationPhase::Cancelled);
        assert!(
            mock.requests()
                .iter()
                .any(|r| r.is("DELETE", &format!("/sandbox/{CHILD}")))
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_workspace_archive_is_kept_only_at_a_quiescent_instant() {
        let dir = std::env::temp_dir().join(format!("acyclic-quiescent-{}", std::process::id()));
        let workspace = dir.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("a"), "stable").unwrap();
        let tar = dir.join("out.tar");
        let script =
            quiescent_archive_script(workspace.to_str().unwrap(), tar.to_str().unwrap(), 2);
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(&script)
            .status()
            .unwrap();
        assert!(status.success());
        assert!(dir.join("out.tar.gz").exists());

        // Shadow `tar` so the workspace changes, or a read reports a change, deterministically
        // between the two reads of every attempt instead of racing a writer thread.
        let real_tar = std::process::Command::new("sh")
            .args(["-c", "command -v tar"])
            .output()
            .unwrap();
        let real_tar = String::from_utf8(real_tar.stdout).unwrap();
        let bin = dir.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let run_with_tar = |after_each_read: &str| {
            use std::os::unix::fs::PermissionsExt;
            let shim = bin.join("tar");
            std::fs::write(
                &shim,
                format!(
                    "#!/bin/sh\n{} \"$@\" || exit $?\n{after_each_read}\n",
                    real_tar.trim()
                ),
            )
            .unwrap();
            std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
            let _ = std::fs::remove_file(dir.join("out.tar.gz"));
            let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
            let status = std::process::Command::new("sh")
                .arg("-c")
                .arg(&script)
                .env("PATH", path)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(i32::try_from(NOT_QUIESCENT).unwrap()));
            assert!(!dir.join("out.tar.gz").exists() && !tar.exists());
        };
        // A workspace written between the two reads of every attempt never yields an archive.
        let busy = workspace.join("busy");
        run_with_tar(&format!("echo x >> '{}'", busy.display()));
        // Nor does one whose re-read sees a file change under it (`tar` exits 1) even though
        // both reads come out byte-identical.
        let calls = dir.join("calls");
        run_with_tar(&format!(
            "echo x >> '{c}'; [ $(wc -l < '{c}') -eq 1 ] || {{ : > '{c}'; exit 1; }}",
            c = calls.display()
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_replay_never_deletes_the_sandbox_it_adopted() {
        // After a restart the registry is empty, so a replayed create adopts the sandbox an
        // earlier (possibly reported-successful) attempt made. Cancelling the replay must not
        // destroy it.
        let key = test_key(16);
        let release = gate();
        let hold = Arc::clone(&release);
        let mock = mock::Mock::start(move |request| {
            let hold = Arc::clone(&hold);
            async move {
                if request.path.starts_with("/snapshots/") {
                    return (200, r#"{"id":"s","sandboxClass":"linux-vm"}"#.to_owned());
                }
                if request.is("POST", "/sandbox") {
                    return (409, "name taken".to_owned());
                }
                if request.is("GET", &format!("/sandbox/{OTHER}")) {
                    hold.acquire().await.unwrap().forget();
                }
                (200, sandbox(OTHER, "started", &owned_labels(key, None)))
            }
        })
        .await;
        let provider = mocked(&mock, None);
        let task = tokio::spawn({
            let provider = Arc::clone(&provider);
            async move { provider.create(request(key)).await }
        });
        mock.wait_for("GET", &format!("/sandbox/{OTHER}")).await;
        provider.cancel(ops::operation_id(key)).await.unwrap();
        release.add_permits(1);
        assert!(matches!(task.await.unwrap(), Err(ProviderError::Cancelled)));
        assert!(
            !mock.requests().iter().any(|r| r.method == "DELETE"),
            "an adopted sandbox belongs to the key's outcome, not to the cancelled replay"
        );
    }

    #[tokio::test]
    async fn cancel_deletes_fork_descendants_first_and_can_be_retried() {
        let refused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let once = Arc::clone(&refused);
        let mock = mock::Mock::start(move |request| {
            let once = Arc::clone(&once);
            async move {
                // Daytona refuses to delete a sandbox with live fork children; refuse once.
                if request.is("DELETE", "/sandbox/a")
                    && !once.swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    return (409, "sandbox has fork children".to_owned());
                }
                (200, String::new())
            }
        })
        .await;
        let provider = mocked(&mock, None);
        let key = test_key(17);
        let ops::Admission::Fresh(operation) = provider.registry().admit(key, &"fork").unwrap()
        else {
            panic!("expected fresh")
        };
        for id in ["a", "b", "c"] {
            assert!(provider.registry().bind_created(operation, id));
        }
        provider
            .registry()
            .fail(operation, &ProviderError::Indeterminate(key));
        provider.cancel(operation).await.unwrap();
        let deletes = |mock: &mock::Mock| -> Vec<String> {
            mock.requests()
                .into_iter()
                .filter(|r| r.method == "DELETE")
                .map(|r| r.path)
                .collect()
        };
        assert_eq!(deletes(&mock), ["/sandbox/c", "/sandbox/b", "/sandbox/a"]);
        assert_eq!(
            provider.registry().record(operation).unwrap().created,
            ["a"]
        );
        provider.cancel(operation).await.unwrap();
        assert_eq!(
            deletes(&mock).last().map(String::as_str),
            Some("/sandbox/a")
        );
        assert!(
            provider
                .registry()
                .record(operation)
                .unwrap()
                .created
                .is_empty()
        );
    }
}
