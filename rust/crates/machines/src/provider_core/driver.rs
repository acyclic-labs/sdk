//! Provider transport boundary. Lifecycle bookkeeping stays in the shared core.

use crate::{
    ExecCommand, ExpirationPolicy, MachineContract, MachineState, ProviderError, SuspensionPolicy,
};
use async_trait::async_trait;
use std::collections::BTreeMap;

/// Network policy associated with an admitted contract commitment.
#[derive(Clone, Debug)]
pub enum NetworkPolicy {
    /// Deny outbound connections.
    BlockAll,
    /// Permit only these DNS names, using the provider's enforcement mechanism.
    AllowDomains(Vec<String>),
}

/// One provider create, with all portable decisions already resolved.
#[derive(Clone)]
pub struct SandboxSpec {
    /// Stable operation name for provider lookup; never a retry authorization.
    pub name: String,
    /// Provider image/template registered for the immutable contract image.
    pub image: String,
    /// An immutable native snapshot to restore instead of booting the image.
    pub snapshot: Option<ProviderSnapshot>,
    /// Exact admitted contract.
    pub contract: MachineContract,
    /// Authorized outbound policy.
    pub network: NetworkPolicy,
    /// Environment supplied by the application, never included in diagnostics.
    pub env: BTreeMap<String, String>,
}

/// A provider's atomic disk snapshot, never a copy assembled by reading files.
#[derive(Clone, Debug)]
pub struct ProviderSnapshot {
    /// Opaque restore handle interpreted only by the originating adapter.
    pub handle: String,
    /// Conservative availability deadline, if the provider expires snapshots.
    pub expires_at_unix_ms: Option<u64>,
}

/// Physical disposition after the shared core revokes a checkpoint reference.
#[derive(Clone, Copy, Debug)]
pub enum SnapshotRelease {
    /// The provider acknowledged deletion (or reported it already missing).
    Deleted,
    /// The provider retains data under its native retention policy. This is not
    /// evidence of physical deletion; only the SDK reference has been revoked.
    ProviderRetained,
}

impl SandboxSpec {
    /// The admitted provider lifetime, rounded up for APIs accepting seconds.
    pub fn lifetime_seconds(&self) -> Result<u32, ProviderError> {
        let duration = match self.contract.expiration {
            ExpirationPolicy::MaxAge(duration) => duration,
            ExpirationPolicy::AtUnixMs(deadline) => std::time::Duration::from_millis(
                deadline.saturating_sub(super::remote::now_unix_ms()),
            ),
            _ => return Err(ProviderError::Unsupported(
                "finite provider lifetime admission failed; recovery: select MaxAge or AtUnixMs"
                    .into(),
            )),
        };
        u32::try_from(
            duration
                .as_secs()
                .saturating_add(u64::from(duration.subsec_nanos() != 0)),
        )
        .map_err(|_| {
            ProviderError::Invalid(
                "provider lifetime overflow; recovery: choose a shorter maximum age".into(),
            )
        })
    }
}

/// Provider resource identity and its stable logical endpoint.
#[derive(Clone, Debug)]
pub struct SandboxInstance {
    /// Opaque provider resource identity.
    pub id: String,
    /// Provider endpoint, without embedded credentials.
    pub endpoint: String,
    /// Provisioned allocation reported by the provider or fixed by its deployment.
    pub allocation: super::Allocation,
}

/// Thin adapter boundary for remote sandbox transports.
///
/// Implementations dispatch each mutation once. They must report lost mutation
/// replies as unavailable, and must never retry an exec after uncertain dispatch.
/// Capability declarations and credential handling belong in the adapter;
/// operation replay, event history and pagination do not.
#[async_trait]
pub trait SandboxDriver: Clone + Send + Sync + 'static {
    /// Provider name used in diagnostics and compatibility commitments.
    fn name(&self) -> &'static str;

    /// Whether this transport implements memory-preserving suspend and wake.
    fn supports_suspend(&self) -> bool;

    /// True only for a native boundary covering every writer at one instant,
    /// whose children cold boot without inheriting memory or running processes.
    fn supports_disk_snapshots(&self) -> bool {
        false
    }

    /// Native minimum retention, measured conservatively from dispatch time.
    fn snapshot_retention(&self) -> Option<std::time::Duration> {
        None
    }

    /// Captures one immutable disk generation while leaving its source running.
    /// Lost replies must remain uncertain and must never authorize redispatch.
    async fn snapshot(
        &self,
        _instance: &SandboxInstance,
        _request_id: &str,
    ) -> Result<String, ProviderError> {
        Err(super::snapshot_unavailable())
    }

    /// Releases a native handle after SDK fork admission has been revoked.
    async fn release_snapshot(
        &self,
        _snapshot: &ProviderSnapshot,
    ) -> Result<SnapshotRelease, ProviderError> {
        Err(super::snapshot_unavailable())
    }

    /// Provider's enforced lifetime ceiling. A `ProviderDefault` request negotiates
    /// to this explicit maximum age in the returned contract. Explicit deadlines
    /// beyond it are refused before dispatch.
    fn maximum_lifetime(&self) -> Option<std::time::Duration> {
        None
    }

    /// Declares native domain allowlist enforcement. The core admits policy;
    /// adapters only encode an already admitted policy in their create primitive.
    fn supports_domain_egress(&self) -> bool {
        false
    }

    /// Creates one ready sandbox using a registered image and enforced network policy.
    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxInstance, ProviderError>;

    /// Reads the provider's current lifecycle state.
    async fn state(&self, instance: &SandboxInstance) -> Result<MachineState, ProviderError>;

    /// Deletes one provider resource. Missing resources may return `NotFound`.
    async fn destroy(&self, instance: &SandboxInstance) -> Result<(), ProviderError>;

    /// Suspends the existing sandbox while retaining its memory.
    async fn suspend(&self, _instance: &SandboxInstance) -> Result<(), ProviderError> {
        Err(ProviderError::Unsupported(
            "memory-preserving suspension is unavailable".into(),
        ))
    }

    /// Resumes a suspended sandbox with the same provider identity.
    async fn wake(&self, _instance: &SandboxInstance) -> Result<(), ProviderError> {
        Err(ProviderError::Unsupported(
            "memory-preserving wake is unavailable".into(),
        ))
    }

    /// Changes the provider's automatic suspension policy.
    async fn set_suspension_policy(
        &self,
        _instance: &SandboxInstance,
        policy: SuspensionPolicy,
    ) -> Result<(), ProviderError> {
        match policy {
            SuspensionPolicy::Manual => Ok(()),
            SuspensionPolicy::AfterIdle(_) => Err(ProviderError::Unsupported(
                "automatic idle suspension is unavailable; recovery: select SuspensionPolicy::Manual".into(),
            )),
        }
    }

    /// Whether the native exec primitive can accept owned binary stdin.
    fn supports_stdin(&self) -> bool {
        false
    }

    /// Dispatches one native command. The shared core supplies process ownership
    /// and cancellation; stdin is present only after supports_stdin admission.
    async fn exec(
        &self,
        instance: &SandboxInstance,
        argv: Vec<String>,
        timeout_ms: u32,
        input: Option<Vec<u8>>,
    ) -> Result<crate::ExecStream, ProviderError>;

    /// Prepares common ownership before dispatch, including a lost start reply.
    fn start(
        &self,
        instance: &SandboxInstance,
        command: &ExecCommand,
        input: Option<Vec<u8>>,
    ) -> Result<super::ExecSession, ProviderError> {
        if input.is_some() && !self.supports_stdin() {
            return Err(ProviderError::Unsupported("provider stdin admission failed; recovery: choose a provider with native stdin support".into()));
        }
        let driver = self.clone();
        let instance = instance.clone();
        Ok(super::process_group_io(
            command,
            input,
            move |argv, timeout, input| {
                let driver = driver.clone();
                let instance = instance.clone();
                Box::pin(async move { driver.exec(&instance, argv, timeout, input).await })
            },
        ))
    }

    /// Reads exact guest bytes without text conversion.
    async fn read_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
    ) -> Result<Vec<u8>, ProviderError>;

    /// Uploads an exact file as bytes, without text conversion.
    async fn write_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
        data: &[u8],
    ) -> Result<(), ProviderError>;
}
