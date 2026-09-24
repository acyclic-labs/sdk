#![doc = include_str!("../README.md")]
#![deny(unsafe_code)]

pub mod api;
pub mod map;
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
    IdempotencyKey, Image, ImageQualification, MAX_EVENT_PAGE_SIZE, MAX_FORK_CHILDREN,
    MAX_PAGE_SIZE, MachineContract, MachineId, MachineObservation, MachinePage, MachineState,
    MachinesProvider, MutationOutcome, OperationId, OperationObservation, OperationPhase,
    OperationStream, Performance, ProviderAssurance, ProviderError, SuspensionPolicy, UsageReceipt,
};
use async_trait::async_trait;
use futures::StreamExt as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use api::{CreateSnapshotRequest, DaytonaApi, ForkRequest, Sandbox, Snapshot};
use ops::{Admission, OperationRegistry};

/// Default Daytona API base URL.
pub const DEFAULT_API_URL: &str = "https://app.daytona.io/api";

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
    /// Outbound domains allowed from sandboxes (Daytona `domainAllowList`); empty keeps the
    /// organization default.
    pub allow_domains: Vec<String>,
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
            allow_domains: Vec::new(),
            snapshots: BTreeMap::new(),
            tenant: None,
            organization_id: None,
            poll_interval: Duration::from_millis(500),
            ready_timeout: Duration::from_secs(180),
            request_timeout: Duration::from_secs(60),
        }
    }

    /// Reads `DAYTONA_API_KEY` (required), `DAYTONA_API_URL`, `DAYTONA_REGION`,
    /// `DAYTONA_SNAPSHOT`, `DAYTONA_ALLOW_DOMAINS` (comma-separated), `DAYTONA_TENANT`, and
    /// `DAYTONA_ORGANIZATION_ID`.
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
        config.allow_domains = read("DAYTONA_ALLOW_DOMAINS")
            .map(|list| {
                list.split(',')
                    .map(str::trim)
                    .filter(|d| !d.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        config.tenant = read("DAYTONA_TENANT");
        config.organization_id = read("DAYTONA_ORGANIZATION_ID");
        Ok(config)
    }

    /// Registers the snapshot that backs one image digest.
    pub fn register_snapshot(&mut self, digest: [u8; 32], snapshot: impl Into<String>) {
        self.snapshots.insert(map::hex(&digest), snapshot.into());
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
            .field("allow_domains", &self.allow_domains)
            .field("snapshots", &self.snapshots)
            .field("tenant", &self.tenant)
            .field("organization_id", &self.organization_id)
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

/// Feature declaration of this provider for VM-class sandboxes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DaytonaCapabilities {
    /// Checkpoints capture VM memory (hot snapshot, `includeMemory`).
    pub checkpoint_memory: Assurance,
    /// Native fork copies a running VM's memory and disk (`POST /sandbox/{id}/fork`), at any
    /// time, without an intermediate checkpoint.
    pub live_fork_memory: Assurance,
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
    LiveFork(MachineId, u32),
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
/// share its credentials.
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

    /// Image capabilities every qualified (VM-class) image receives: memory checkpoint, fork,
    /// and suspend/resume. No elastic sizing and no live movement.
    #[must_use]
    pub fn capabilities() -> BTreeSet<Capability> {
        BTreeSet::from([
            Capability::LiveCheckpoint,
            Capability::LiveFork,
            Capability::SuspendResume,
        ])
    }

    /// Opaque compatibility revision of this provider build.
    #[must_use]
    pub fn revision() -> [u8; 32] {
        Sha256::digest(b"acyclic-machines-daytona-v2").into()
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

    /// Rejects snapshots that do not boot a VM class. The sandbox class is a property of the
    /// snapshot; containers have no pause, memory snapshot, or fork, so they cannot honour the
    /// capabilities this provider declares.
    async fn require_vm_snapshot(&self, snapshot: &str) -> Result<(), ProviderError> {
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
        if map::is_vm_class(Some(&class)) {
            Ok(())
        } else {
            Err(ProviderError::Unsupported(format!(
                "Daytona snapshot {snapshot} boots class {class:?}; this provider needs a VM class for pause, memory checkpoints, and fork"
            )))
        }
    }

    fn contract(request: &CreateMachine) -> Result<MachineContract, ProviderError> {
        let capabilities = Self::capabilities();
        if let CompatibilityPolicy::Require(required) = &request.compatibility
            && (required.is_empty() || !required.is_subset(&capabilities))
        {
            return Err(ProviderError::Unsupported(
                "Daytona offers live checkpoint, live fork, and suspend/resume only".into(),
            ));
        }
        if request.budgets.spend_micros != 0 || request.budgets.concurrency != 0 {
            return Err(ProviderError::Unsupported(
                "Daytona cannot enforce per-machine spend or concurrency budgets; leave budgets zero".into(),
            ));
        }
        map::lifetime_fields(request.expiration)?;
        Ok(MachineContract {
            image: request.image.clone(),
            capabilities,
            compatibility: request.compatibility.clone(),
            compatibility_revision: Self::revision(),
            performance: request.performance,
            suspension: request.suspension,
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
    async fn create_or_adopt(
        &self,
        body: &api::CreateSandboxRequest,
    ) -> Result<Sandbox, ProviderError> {
        match self.api.create(body).await {
            Err(ProviderError::Conflict(detail)) => match &body.name {
                Some(name) => self
                    .api
                    .get(name)
                    .await
                    .map_err(|_| ProviderError::Conflict(detail)),
                None => Err(ProviderError::Conflict(detail)),
            },
            other => other,
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
                    .is_some_and(|record| !record.sandboxes.is_empty());
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

    /// Forks a running machine, memory and disk, into `count` new machines with Daytona's
    /// native VM fork, without taking a checkpoint first.
    ///
    /// This is the fast path for fork-join: it can run at any point in the parent's life and
    /// each child resumes exactly where the parent was, sharing its environment and
    /// credentials. Children are forked one after another (the parent passes through
    /// `forking` each time), named deterministically from `key`, and relabelled as this
    /// provider's fork children. Daytona keeps the parent/child relation in its fork tree and
    /// refuses to delete a parent while it has live fork children, so join (destroy) children
    /// before their parent.
    ///
    /// # Errors
    /// [`ProviderError::Invalid`] for a count above [`MAX_FORK_CHILDREN`],
    /// [`ProviderError::Conflict`] when the parent is not running, and otherwise the same
    /// failures as the trait mutations.
    pub async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count = count.get();
        if count > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply(key, &Intent::LiveFork(machine, count), async |operation| {
            let parent = self.fetch_machine(machine).await?;
            if parent.state != MachineState::Running {
                return Err(ProviderError::Conflict(
                    "only a running machine can be forked".into(),
                ));
            }
            let parent_id = machine.to_string();
            let contract = parent.contract;
            let mut ids = Vec::with_capacity(count as usize);
            for index in 0..count {
                let name = map::sandbox_name(key, Some(index));
                let child = match self
                    .api
                    .fork(
                        &parent_id,
                        &ForkRequest {
                            name: Some(name.clone()),
                        },
                    )
                    .await
                {
                    Err(ProviderError::Conflict(detail)) => self
                        .api
                        .get(&name)
                        .await
                        .map_err(|_| ProviderError::Conflict(detail))?,
                    other => other?,
                };
                self.ops.bind_sandbox(operation, &child.id);
                let ours = map::labels(
                    key,
                    map::KIND_FORK,
                    Some(index),
                    self.config.tenant.as_deref(),
                    &contract,
                );
                self.api
                    .replace_labels(&child.id, map::relabel(&child.labels, ours))
                    .await?;
                ids.push(child.id);
                self.wait_settled(&parent_id, key).await?;
            }
            let children = self.settle_children(ids, key, &contract, None).await?;
            Ok(MutationOutcome::Forked(children))
        })
        .await
    }

    async fn recover_by_label(
        &self,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let mut sandboxes = self.api.list(Some(&map::key_filter(key))).await?;
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
            map::KIND_FORK => {
                sandboxes.sort_by_key(|sandbox| {
                    sandbox
                        .labels
                        .get(map::LABEL_INDEX)
                        .and_then(|index| index.parse::<u32>().ok())
                        .unwrap_or(u32::MAX)
                });
                let children = sandboxes
                    .iter()
                    .map(|sandbox| self.observe(sandbox, None, None))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(MutationOutcome::Forked(children))
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
        self.require_vm_snapshot(&snapshot).await?;
        Ok(ImageQualification {
            image,
            capabilities: Self::capabilities(),
            compatibility_revision: Self::revision(),
        })
    }

    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let intent = Intent::Create(request.clone());
        let outcome = self
            .apply(key, &intent, async |operation| {
                let snapshot = self.resolve_snapshot(&request.image)?;
                let contract = Self::contract(&request)?;
                self.require_vm_snapshot(&snapshot).await?;
                let mut body = map::create_request(&self.config, &snapshot, key, None, &contract)?;
                body.env = self.staged_boot_env(key).unwrap_or_default();
                let created = self.create_or_adopt(&body).await?;
                self.ops.bind_sandbox(operation, &created.id);
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
        for sandbox in self.api.list(Some(&map::managed_filter())).await? {
            match self.observe(&sandbox, None, None) {
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
            if source.state != MachineState::Running {
                return Err(ProviderError::Conflict(
                    "Daytona memory snapshots require a started sandbox".into(),
                ));
            }
            let id = machine.to_string();
            let name = map::checkpoint_name(key);
            self.ops.bind_sandbox(operation, &id);
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
    /// [`DaytonaProvider::fork_machine`].
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
                    let body =
                        map::create_request(&self.config, &snapshot, key, Some(index), &contract)?;
                    let created = self.create_or_adopt(&body).await?;
                    self.ops.bind_sandbox(operation, &created.id);
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

    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::Suspend(machine), async |operation| {
            let current = self.fetch_machine(machine).await?;
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
            self.ops.bind_sandbox(operation, &id);
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
            self.ops.bind_sandbox(operation, &id);
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
            if current.state == MachineState::Destroyed {
                return Err(ProviderError::Conflict(
                    "destroyed machine cannot change policy".into(),
                ));
            }
            let id = machine.to_string();
            self.ops.bind_sandbox(operation, &id);
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
            let id = machine.to_string();
            self.ops.bind_sandbox(operation, &id);
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
        for sandbox in sandboxes {
            if let Err(error) = self.api.delete(&sandbox).await {
                tracing::warn!(sandbox, %error, "best-effort delete of a cancelled operation's sandbox failed");
            }
        }
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
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
}
