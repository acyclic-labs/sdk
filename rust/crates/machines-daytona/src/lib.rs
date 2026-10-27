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
pub mod usage;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{Mutex, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use acyclic_machines::{
    Capability, CheckpointId, CheckpointObservation, CompatibilityPolicy, CreateMachine, EventPage,
    ExecCommand, ExecStream, IdempotencyKey, Image, ImageQualification, MAX_PAGE_SIZE,
    MachineContract, MachineId, MachineObservation, MachineOutput, MachinePage, MachineState,
    MachinesProvider, MutationOutcome, OperationId, OperationObservation, OperationStream,
    ProviderAssurance, ProviderError, SuspensionPolicy, UsageReceipt,
};
use async_trait::async_trait;
use sha2::{Digest as _, Sha256};

use acyclic_machines::core::{
    Dispatch, NativeBinding, NativeResource, NativeResources, OperationRegistry, RawCreated,
    RawMachines, RawObservation,
};
#[cfg(test)]
use acyclic_machines::{ForkFidelity, MAX_FORK_CHILDREN, MutationIntent, core as ops};
use api::{CreateSnapshotRequest, DaytonaApi, ForkRequest, Sandbox};

/// Default Daytona API base URL.
pub const DEFAULT_API_URL: &str = "https://app.daytona.io/api";
/// Default directory used by guest execution.
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
    /// Absolute guest workspace directory. This does not confer atomic snapshot support.
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
/// sandboxes. Container sandboxes have no admitted atomic snapshot boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DaytonaCapabilities {
    /// Checkpoints capture VM memory (hot snapshot, `includeMemory`).
    pub checkpoint_memory: Assurance,
    /// Native fork copies a running VM's memory and disk (`POST /sandbox/{id}/fork`), at any
    /// time, without an intermediate checkpoint.
    pub live_fork_memory: Assurance,
    /// Container disk fork is unavailable without an atomic snapshot boundary.
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
    container_disk_fork: Assurance::No,
    suspend_keeps_memory: Assurance::Yes,
    network_policy: Assurance::Provisional,
    usage_receipts: Assurance::Provisional,
    restore_cross_host: Assurance::No,
};

/// Machines provider over Daytona VM sandboxes.
pub struct DaytonaProvider {
    config: DaytonaConfig,
    api: DaytonaApi,
    ops: OperationRegistry,
    boot_env: Mutex<BTreeMap<IdempotencyKey, BTreeMap<String, String>>>,
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

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

impl DaytonaProvider {
    /// Stages or replaces the environment for the matching create; creation clears it on completion.
    pub fn stage_boot_env(&self, key: IdempotencyKey, env: BTreeMap<String, String>) {
        self.boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, env);
    }

    /// Binds a provider to one Daytona organization.
    ///
    /// # Errors
    /// Returns [`ProviderError::Invalid`] when the configuration cannot produce a client.
    pub fn new(config: DaytonaConfig, ops: OperationRegistry) -> Result<Self, ProviderError> {
        let api = DaytonaApi::new(&config)?;
        Ok(Self {
            config,
            api,
            ops,
            boot_env: Mutex::default(),
        })
    }

    /// Boot environment staged for `key`, if any.
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
        let class = self
            .api
            .get_snapshot(snapshot)
            .await?
            .sandbox_class
            .unwrap_or_default();
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
            suspension,
            expiration: request.expiration,
            network_policy_digest: request.network_policy_digest,
            budgets: request.budgets,
        })
    }

    /// Records a sandbox observation, filling gaps from what the registry already knows.
    async fn observe(
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
        let known = match map::machine_id(&sandbox.id) {
            Ok(id) => self.ops.machine(id).await?,
            Err(_) => None,
        };
        let fallback = fallback.or(known.as_ref().map(|value| &value.contract));
        let last_checkpoint =
            last_checkpoint.or(known.as_ref().and_then(|value| value.last_checkpoint));
        let now = now_unix_ms();
        let observation = map::sandbox_to_observation(sandbox, fallback, last_checkpoint, now)?;
        self.ops.observe_machine(observation.clone(), now).await?;
        Ok(observation)
    }

    /// Reads a machine from Daytona, falling back to the registry once Daytona forgot it.
    async fn fetch_machine(&self, machine: MachineId) -> Result<MachineObservation, ProviderError> {
        self.ops.inspect_machine(self, machine, now_unix_ms()).await
    }

    fn raw_created(
        sandbox: &Sandbox,
        contract: &MachineContract,
        checkpoint: Option<CheckpointId>,
        adopted: bool,
    ) -> RawCreated {
        RawCreated {
            handle: sandbox.id.clone(),
            endpoint: None,
            observation: map::sandbox_to_observation(
                sandbox,
                Some(contract),
                checkpoint,
                now_unix_ms(),
            ),
            adopted,
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

    fn live_fork_labels(
        &self,
        key: IdempotencyKey,
        slot: map::ForkSlot,
        parent: &MachineObservation,
        source: &str,
    ) -> BTreeMap<String, String> {
        let mut labels = map::labels(key, Some(slot), self.tenant(), &parent.contract);
        labels.insert(map::LABEL_KIND.to_owned(), map::KIND_LIVE_FORK.to_owned());
        labels.insert(map::LABEL_PARENT.to_owned(), parent.id.to_string());
        labels.insert(map::LABEL_FORK_SOURCE.to_owned(), source.to_owned());
        labels.insert(map::LABEL_READY.to_owned(), "true".to_owned());
        labels
    }
}

#[async_trait]
impl NativeResources for DaytonaProvider {
    fn name(&self) -> &str {
        "daytona"
    }

    async fn raw_destroy(&self, resource: &NativeResource) -> Result<u64, ProviderError> {
        match resource {
            NativeResource::Machine(handle) => {
                self.api.delete(handle).await.map(|()| now_unix_ms())
            }
            NativeResource::Checkpoint(handle) => {
                // An accepted snapshot can be absent before materialization. Its
                // missing name is not proof of deletion; retain cleanup ownership.
                let snapshot =
                    self.api
                        .get_snapshot(handle)
                        .await
                        .map_err(|error| match error {
                            ProviderError::NotFound(_) => ProviderError::Unavailable,
                            error => error,
                        })?;
                self.api
                    .delete_snapshot(&snapshot.id)
                    .await
                    .map(|()| now_unix_ms())
            }
        }
    }
}

#[async_trait]
impl RawMachines for DaytonaProvider {
    async fn raw_create(&self, request: &CreateMachine) -> Result<RawCreated, ProviderError> {
        let key = request.idempotency_key;
        let snapshot = self.resolve_snapshot(&request.image)?;
        // Pure wire representability checks precede the native qualification read.
        Self::check_request(request)?;
        map::network_fields(&self.config, &request.network_policy_digest)?;
        let capabilities = self.snapshot_capabilities(&snapshot).await?;
        let contract = Self::contract(request, capabilities)?;
        let mut body = map::create_request(&self.config, &snapshot, key, None, &contract)?;
        body.env = self.staged_boot_env(key).unwrap_or_default();
        let (created, adopted) = self.create_or_adopt(&body).await?;
        Ok(Self::raw_created(&created, &contract, None, adopted))
    }

    async fn raw_observe(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
        known: Option<&MachineObservation>,
    ) -> Result<RawObservation, ProviderError> {
        let handle = native
            .map(|value| value.handle.clone())
            .unwrap_or_else(|| machine.to_string());
        let sandbox = self.api.get(&handle).await?;
        if !map::owned_by(&sandbox, self.tenant()) {
            return Err(ProviderError::NotFound(machine.to_string()));
        }
        Ok(RawObservation {
            settled: map::is_settled(sandbox.state.as_deref()),
            machine: map::sandbox_to_observation(
                &sandbox,
                known.map(|value| &value.contract),
                known.and_then(|value| value.last_checkpoint),
                now_unix_ms(),
            )?,
        })
    }
    async fn raw_suspend(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<(), ProviderError> {
        self.api
            .pause(&native.map_or_else(|| machine.to_string(), |value| value.handle.clone()))
            .await
    }
    async fn raw_wake(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<(), ProviderError> {
        self.api
            .start(&native.map_or_else(|| machine.to_string(), |value| value.handle.clone()))
            .await
    }
    async fn raw_set_suspension_policy(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
        policy: SuspensionPolicy,
    ) -> Result<(), ProviderError> {
        self.api
            .set_autopause(
                &native.map_or_else(|| machine.to_string(), |value| value.handle.clone()),
                map::autopause_minutes(policy),
            )
            .await
    }

    async fn raw_checkpoint(
        &self,
        source: &MachineObservation,
        native: Option<&NativeBinding>,
        key: IdempotencyKey,
    ) -> Result<String, ProviderError> {
        let handle = native
            .map(|value| value.handle.clone())
            .unwrap_or_else(|| source.id.to_string());
        let name = map::checkpoint_name(key);
        self.api
            .snapshot(
                &handle,
                &CreateSnapshotRequest {
                    name: name.clone(),
                    include_memory: true,
                },
            )
            .await?;
        Ok(name)
    }

    async fn raw_observe_checkpoint(
        &self,
        handle: &str,
        source: MachineId,
        contract: &MachineContract,
    ) -> Result<Option<CheckpointObservation>, ProviderError> {
        match self.api.get_snapshot(handle).await {
            Ok(value) if map::snapshot_settled(value.state.as_deref()) => {
                if !value
                    .state
                    .as_deref()
                    .is_some_and(|state| state.eq_ignore_ascii_case("active"))
                {
                    return Err(ProviderError::Failed);
                }
                map::snapshot_to_checkpoint(
                    &value,
                    Some(source),
                    contract.clone(),
                    true,
                    now_unix_ms(),
                )
                .map(Some)
            }
            Ok(_) | Err(ProviderError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn raw_fork(
        &self,
        checkpoint: CheckpointId,
        native: Option<&NativeBinding>,
        contract: &MachineContract,
        slot: map::ForkSlot,
        key: IdempotencyKey,
    ) -> Result<RawCreated, ProviderError> {
        let handle = native.map_or_else(|| checkpoint.to_string(), |native| native.handle.clone());
        let body = map::create_request(&self.config, &handle, key, Some(slot), contract)?;
        let (created, adopted) = self.create_or_adopt(&body).await?;
        Ok(Self::raw_created(
            &created,
            contract,
            Some(checkpoint),
            adopted,
        ))
    }

    async fn raw_fork_machine(
        &self,
        parent: &MachineObservation,
        source: MachineId,
        native: Option<&NativeBinding>,
        slot: map::ForkSlot,
        key: IdempotencyKey,
    ) -> Result<Dispatch<RawCreated>, ProviderError> {
        let handle = native.map_or_else(|| source.to_string(), |native| native.handle.clone());
        let name = map::sandbox_name(key, Some(slot.index));
        let request = ForkRequest {
            name: Some(name.clone()),
        };
        let (child, adopted) = match self.api.fork(&handle, &request).await {
            Ok(child) => (child, false),
            Err(ProviderError::Conflict(detail)) => (
                self.adopt_fork_child(
                    &handle,
                    &name,
                    &self.live_fork_labels(key, slot, parent, &handle),
                    detail,
                )
                .await?,
                true,
            ),
            Err(ProviderError::Invalid(detail)) if map::fork_refused_for_state(&detail) => {
                return Ok(Dispatch::Refused);
            }
            Err(error) => return Err(error),
        };
        Ok(Dispatch::Accepted(Self::raw_created(
            &child,
            &parent.contract,
            None,
            adopted,
        )))
    }

    async fn raw_finish_fork(
        &self,
        child: MachineId,
        native: Option<&NativeBinding>,
        parent: &MachineObservation,
        source: &str,
        slot: map::ForkSlot,
        key: IdempotencyKey,
    ) -> Result<(), ProviderError> {
        let handle = native.map_or_else(|| child.to_string(), |native| native.handle.clone());
        let child = self.api.get(&handle).await?;
        self.api
            .replace_labels(
                &handle,
                map::relabel(
                    &child.labels,
                    self.live_fork_labels(key, slot, parent, source),
                ),
            )
            .await
    }

    async fn raw_fork_children(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<Vec<MachineState>, ProviderError> {
        let handle = native.map_or_else(|| machine.to_string(), |native| native.handle.clone());
        Ok(self
            .api
            .forks(&handle)
            .await?
            .into_iter()
            .map(|child| map::machine_state(child.state.as_deref()))
            .collect())
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
    async fn exec(
        &self,
        _machine: MachineId,
        _command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        Err(ProviderError::Unsupported(
            "Daytona guest command streaming is not bound".into(),
        ))
    }
    async fn read_output(
        &self,
        _machine: MachineId,
        _offset: u64,
        _limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        Err(ProviderError::Unsupported(
            "Daytona retained console reads are not bound".into(),
        ))
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
        let outcome = self
            .ops
            .create_machine(
                self,
                request,
                self.config.poll_interval,
                self.config.ready_timeout,
            )
            .await;
        // The staged environment is only useful to the create request itself. Whatever the
        // outcome, a later attempt stages afresh; keeping it would retain the token for nothing.
        self.boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&key);
        outcome.map(|settled| settled.outcome)
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
        let tenant = self.tenant();
        let listed = self.api.list(Some(&map::managed_filter(tenant))).await?;
        for sandbox in listed
            .iter()
            .filter(|sandbox| map::owned_by(sandbox, tenant))
        {
            match self.observe(sandbox, None, None).await {
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(sandbox = sandbox.id, %error, "skipping sandbox that cannot be observed");
                }
            }
        }
        self.ops.machine_page(after, limit).await
    }

    /// Takes a hot snapshot (filesystem and memory) of a running VM. The snapshot is an
    /// independent Daytona object: it outlives its source and can be restored any number of
    /// times by [`MachinesProvider::fork`].
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.ops
            .create_checkpoint(
                self,
                machine,
                key,
                self.config.poll_interval,
                self.config.ready_timeout,
                now_unix_ms(),
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.ops.inspect_checkpoint(self, checkpoint).await
    }

    /// Restores a checkpoint into `count` new sandboxes by creating each from the hot
    /// snapshot. To fork a *running* machine without a checkpoint, use
    /// [`MachinesProvider::fork_machine`].
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.ops
            .fork_checkpoint(
                self,
                checkpoint,
                count,
                key,
                self.config.poll_interval,
                self.config.ready_timeout,
            )
            .await
            .map(|settled| settled.outcome)
    }

    /// Native memory-and-disk VM fork. Containers are refused without an atomic
    /// snapshot boundary. Children are labelled with their source (`acyclic.parent`).
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.ops
            .fork_machine(
                self,
                machine,
                count,
                key,
                self.config.poll_interval,
                self.config.ready_timeout,
                now_unix_ms(),
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.ops
            .transition(
                self,
                machine,
                key,
                false,
                self.config.poll_interval,
                self.config.ready_timeout,
                now_unix_ms(),
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.ops
            .transition(
                self,
                machine,
                key,
                true,
                self.config.poll_interval,
                self.config.ready_timeout,
                now_unix_ms(),
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.ops
            .set_suspension_policy(self, machine, policy, key, now_unix_ms())
            .await
            .map(|settled| settled.outcome)
    }

    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.ops
            .destroy_machine(self, machine, key, now_unix_ms())
            .await
            .map(|settled| settled.outcome)
    }

    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.ops
            .destroy_checkpoint(self, checkpoint, key)
            .await
            .map(|settled| settled.outcome)
    }

    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        self.ops
            .event_page(self, machine, after_sequence, limit, now_unix_ms())
            .await
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
        let observation = self.observe(&sandbox, None, None).await?;
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
            map::legacy_dedicated_cpu(&sandbox.labels)?,
            allocation,
        )
    }

    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        Ok(self.ops.settled(key).await?.outcome)
    }

    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        self.ops
            .record_for_key(key)
            .await?
            .map(|record| record.id)
            .ok_or_else(|| ProviderError::NotFound(key.to_string()))
    }

    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.ops
            .inspect(operation)
            .await?
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))
    }

    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        self.ops.cancel_with(self, operation).await
    }

    /// Current and future observations follow the canonical Streams cursor.
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        self.ops.watch(operation).await
    }
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "tests index collections whose shape they just built or asserted"
)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use acyclic_machines::OperationPhase;

    fn registry() -> OperationRegistry {
        OperationRegistry::new(
            Arc::new(acyclic_stream::MemoryStream::new(Default::default())),
            "test",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn guest_io_is_unsupported_without_making_daytona_requests() {
        let mock = mock::Mock::start(|_| async { (500, "unexpected request".into()) }).await;
        let mut config = DaytonaConfig::new("fake");
        config.api_url = mock.url.clone();
        let provider = DaytonaProvider::new(config, registry()).unwrap();
        let machine = MachineId::parse("00000000-0000-4000-8000-000000000001").unwrap();
        let command = ExecCommand::new(vec!["echo".into()], 1).unwrap();
        assert!(matches!(
            provider.exec(machine, command).await,
            Err(ProviderError::Unsupported(_))
        ));
        assert!(matches!(
            provider.read_output(machine, 0, 1).await,
            Err(ProviderError::Unsupported(_))
        ));
        assert!(mock.requests().is_empty());
    }

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
        let provider = DaytonaProvider::new(DaytonaConfig::new("api-secret"), registry()).unwrap();
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
        let provider = DaytonaProvider::new(DaytonaConfig::new("k"), registry()).unwrap();
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
        let provider = DaytonaProvider::new(config, registry()).unwrap();
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
        let provider = DaytonaProvider::new(config, registry()).unwrap();
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
        let provider = DaytonaProvider::new(config, registry()).unwrap();
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
        assert!(
            provider
                .registry()
                .record_for_key(key)
                .await
                .unwrap()
                .is_none()
        );
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
        let provider = DaytonaProvider::new(DaytonaConfig::new("k"), registry()).unwrap();
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000004").unwrap();
        let too_many = NonZeroU32::new(MAX_FORK_CHILDREN + 1).unwrap();
        assert!(matches!(
            provider.fork_machine(MachineId::new(), too_many, key).await,
            Err(ProviderError::Invalid(_))
        ));
    }

    #[test]
    fn container_contracts_refuse_non_atomic_fork_and_never_auto_suspend() {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000005").unwrap();
        let request = CreateMachine::new(key, Image::custom([7; 32]).unwrap(), [8; 32]);
        let container = DaytonaProvider::contract(
            &request,
            map::capabilities_for_class(Some("container")).unwrap(),
        )
        .unwrap();
        assert_eq!(container.fork_fidelity(), None);
        assert_eq!(CAPABILITIES.container_disk_fork, Assurance::No);
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
        Arc::new(DaytonaProvider::new(config, registry()).unwrap())
    }

    #[tokio::test]
    async fn restart_replays_settlement_without_native_label_lookup()
    -> Result<(), Box<dyn std::error::Error>> {
        // The HTTP stand-in below is an internal dispatch oracle, not a provider capture.
        let key = test_key(18);
        let mock = mock::Mock::start(move |request| async move {
            if request.path.starts_with("/snapshots/") {
                (200, r#"{"id":"s","sandboxClass":"linux-vm"}"#.into())
            } else {
                (200, sandbox(MACHINE, "started", &owned_labels(key, None)))
            }
        })
        .await;
        let dir = tempfile::tempdir()?;
        let mut config = DaytonaConfig::new("internal-test");
        config.api_url = mock.url.clone();
        config.default_snapshot = Some("base".into());
        config.register_network_policy([8; 32], map::NetworkPolicy::BlockAll);
        let streams = acyclic_stream::LocalStream::open(
            dir.path(),
            acyclic_stream::LocalStreamLimits::default(),
        )
        .await?;
        let provider = DaytonaProvider::new(
            config.clone(),
            OperationRegistry::new(Arc::new(streams), "account")?,
        )?;
        let original = provider.create(request(key)).await?;
        let settlement = provider.registry().settled(key).await?;
        let calls = mock.requests().len();
        drop(provider);
        let streams = acyclic_stream::LocalStream::open(
            dir.path(),
            acyclic_stream::LocalStreamLimits::default(),
        )
        .await?;
        let provider = DaytonaProvider::new(
            config,
            OperationRegistry::new(Arc::new(streams), "account")?,
        )?;
        assert_eq!(provider.create(request(key)).await?, original);
        assert_eq!(provider.recover(key).await?, original);
        assert_eq!(provider.registry().join(key).await?, settlement);
        assert_eq!(mock.requests().len(), calls);
        Ok(())
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
    async fn api_calls_emit_spans_without_paths_or_bodies() {
        use std::sync::Mutex;
        use tracing_subscriber::layer::{Context, SubscriberExt as _};

        type Seen = Arc<Mutex<Vec<(&'static str, &'static str, String)>>>;
        struct Capture(Seen);
        struct Fields<'a>(&'static str, &'a Seen);
        impl tracing::field::Visit for Fields<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let value = format!("{value:?}").trim_matches('"').to_owned();
                self.1.lock().unwrap().push((self.0, field.name(), value));
            }
        }
        impl<S> tracing_subscriber::Layer<S> for Capture
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                _: &tracing::span::Id,
                _: Context<'_, S>,
            ) {
                attrs.record(&mut Fields(attrs.metadata().name(), &self.0));
            }
            fn on_record(
                &self,
                id: &tracing::span::Id,
                values: &tracing::span::Record<'_>,
                ctx: Context<'_, S>,
            ) {
                values.record(&mut Fields(ctx.span(id).unwrap().name(), &self.0));
            }
        }
        // A second live dispatcher keeps callsites consulting this test's subscriber.
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        let seen = Seen::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&seen))),
        );
        let mock = mock::Mock::start(|request| async move {
            if request.is("GET", &format!("/sandbox/{MACHINE}")) {
                (
                    200,
                    sandbox(MACHINE, "started", &owned_labels(test_key(9), None)),
                )
            } else if request.is("GET", "/sandbox/malformed") {
                (200, "{".into())
            } else {
                (404, "no sandbox at /secret/path".into())
            }
        })
        .await;
        let provider = mocked(&mock, None);
        provider.api.get(MACHINE).await.unwrap();
        assert!(matches!(
            provider.api.delete("missing").await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(matches!(
            provider.api.get("malformed").await,
            Err(ProviderError::Rejected(_))
        ));

        let seen = seen.lock().unwrap();
        // Only the well-formed `get` is ok; the malformed body fails inside its span.
        assert_eq!(
            seen.iter()
                .filter(|(_, f, v)| (*f, v.as_str()) == ("outcome", "ok"))
                .count(),
            1
        );
        for expected in [
            ("op", "get"),
            ("http.status", "200"),
            ("outcome", "ok"),
            ("op", "delete"),
            ("http.status", "404"),
            ("error.kind", "not_found"),
            ("error.kind", "rejected"),
        ] {
            assert!(
                seen.iter()
                    .any(|(s, f, v)| *s == "acyclic.machines.daytona.call"
                        && (*f, v.as_str()) == expected),
                "{expected:?} not in {seen:?}"
            );
        }
        for (_, field, value) in seen.iter() {
            assert!(!["path", "body", "authorization"].contains(field));
            assert!(
                !value.contains('/') && !value.contains("missing"),
                "{value}"
            );
        }
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
    async fn provider_labels_cannot_invent_a_settled_operation() {
        let key = test_key(10);
        let mock = mock::Mock::start(|_| async {
            (500, "journal recovery must not use provider labels".into())
        })
        .await;
        let provider = mocked(&mock, None);
        assert!(matches!(
            provider.recover(key).await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(mock.requests().is_empty());
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
        for (index, source) in acyclic_machines::core::fork_rounds(16)
            .into_iter()
            .flatten()
        {
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

    #[tokio::test]
    async fn cancelling_a_replay_never_deletes_the_sandbox_it_adopted() {
        // A name conflict can expose a sandbox created by another attempt.
        // Cancelling this attempt must never delete an adopted sandbox.
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
        let ops::Admission::Fresh(operation) =
            provider.registry().admit(key, &"fork").await.unwrap()
        else {
            panic!("expected fresh")
        };
        for id in ["a", "b", "c"] {
            assert!(
                provider
                    .registry()
                    .bind_created(operation, &NativeResource::Machine(id.into()))
                    .await
                    .unwrap()
            );
        }
        provider
            .registry()
            .fail(operation, &ProviderError::Indeterminate(key))
            .await
            .unwrap();
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
            provider
                .registry()
                .record(operation)
                .await
                .unwrap()
                .unwrap()
                .created,
            [NativeResource::Machine("a".into())]
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
                .await
                .unwrap()
                .unwrap()
                .created
                .is_empty()
        );
    }
    #[tokio::test]
    async fn cancel_retains_an_accepted_snapshot_that_has_not_materialized() {
        // Internal refusal oracle; this is not a captured provider response.
        let mock = mock::Mock::start(|_| async { (404, "{}".into()) }).await;
        let provider = mocked(&mock, None);
        let key = test_key(30);
        let ops::Admission::Fresh(operation) = provider
            .registry()
            .admit(
                key,
                &MutationIntent::Checkpoint(MachineId::parse(MACHINE).unwrap()),
            )
            .await
            .unwrap()
        else {
            panic!("expected fresh");
        };
        let resource = NativeResource::Checkpoint("accepted-pending-snapshot".into());
        provider
            .registry()
            .claim(provider.as_ref(), operation, &resource, false)
            .await
            .unwrap();
        provider.cancel(operation).await.unwrap();
        assert_eq!(
            provider.registry().created(operation).await.unwrap(),
            [resource]
        );
        let requests = mock.requests();
        assert_eq!(requests.len(), 1);
        assert!(
            requests
                .first()
                .unwrap()
                .is("GET", "/snapshots/accepted-pending-snapshot")
        );
        assert_eq!(
            provider.inspect_operation(operation).await.unwrap().phase,
            acyclic_machines::OperationPhase::Cancelled
        );
    }
}
