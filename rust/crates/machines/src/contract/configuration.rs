use super::*;

/// Optional image/runtime capability.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Capability {
    ElasticCpu,
    ElasticMemory,
    LiveCheckpoint,
    LiveFork,
    SuspendResume,
    LiveMovement,
    /// [`MachinesProvider::fork_machine`] copies a running machine's persistent disk, but not its memory or processes.
    DiskFork,
}

/// What the children of one [`MachinesProvider::fork_machine`] inherited from their source.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ForkFidelity {
    /// Children resume from the source's memory, processes, and disk at the fork instant.
    MemoryAndDisk,
    /// Children boot fresh over a copy of the source's persistent disk (provider-defined; see [`Capability::DiskFork`]) taken at one consistent instant.
    DiskOnly,
}

impl ForkFidelity {
    /// The fidelity a machine with `capabilities` forks at, or `None` when it cannot be forked live at all and the caller must fall back to checkpoint fork or a restart.
    pub fn for_capabilities(capabilities: &BTreeSet<Capability>) -> Option<Self> {
        if capabilities.contains(&Capability::LiveFork) {
            Some(Self::MemoryAndDisk)
        } else if capabilities.contains(&Capability::DiskFork) {
            Some(Self::DiskOnly)
        } else {
            None
        }
    }
}

/// Capability admission rule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompatibilityPolicy {
    /// Enable only the exact proven set.
    BestEffort,
    /// Reject unless every requested capability is proven.
    Require(BTreeSet<Capability>),
}

/// Automatic suspension policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SuspensionPolicy {
    Manual,
    AfterIdle(Duration),
}

/// Automatic destruction policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExpirationPolicy {
    Never,
    MaxAge(Duration),
    AtUnixMs(u64),
    Idle(Duration),
}

/// Optional customer cost and concurrency limits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Budgets {
    /// Spend ceiling in integer micros; zero delegates to account policy.
    pub spend_micros: u64,
    /// Concurrent-machine ceiling; zero delegates to account policy.
    pub concurrency: u32,
}

/// Shape-free create request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateMachine {
    /// Caller-retained mutation identity.
    pub idempotency_key: IdempotencyKey,
    /// Immutable image, or `None` to select the provider's installed default.
    /// The settled machine contract always retains the resolved immutable image.
    pub image: Option<Image>,
    /// Exact canonical volume reference, admitted by the service under the original scope.
    /// The reference carries no authority or physical guest mapping capability.
    pub selected_volume_ref: Option<wire::selection::VolumeRef>,
    /// Capability policy.
    pub compatibility: CompatibilityPolicy,
    /// Suspension policy.
    pub suspension: SuspensionPolicy,
    /// Expiration policy.
    pub expiration: ExpirationPolicy,
    /// Commitment to a separately authorized network policy.
    pub network_policy_digest: [u8; 32],
    /// Cost and concurrency limits.
    pub budgets: Budgets,
}

impl CreateMachine {
    /// Creates a best-effort request with 15-second idle suspension and no expiry.
    pub fn new(
        idempotency_key: IdempotencyKey,
        image: Option<Image>,
        network_policy_digest: [u8; 32],
    ) -> Self {
        Self {
            idempotency_key,
            image,
            selected_volume_ref: None,
            compatibility: CompatibilityPolicy::BestEffort,
            suspension: SuspensionPolicy::AfterIdle(DEFAULT_IDLE_SUSPEND),
            expiration: ExpirationPolicy::Never,
            network_policy_digest,
            budgets: Budgets::default(),
        }
    }
}

/// Exact public contract retained for one machine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachineContract {
    /// Immutable source image.
    pub image: Image,
    /// Exact selected workspace retained by the original hosted guest admission.
    pub selected_volume_ref: Option<wire::selection::VolumeRef>,
    /// Exact enabled capabilities.
    pub capabilities: BTreeSet<Capability>,
    /// Original capability policy.
    pub compatibility: CompatibilityPolicy,
    /// Opaque customer compatibility revision.
    pub compatibility_revision: [u8; 32],
    /// Suspension policy.
    pub suspension: SuspensionPolicy,
    /// Expiration policy.
    pub expiration: ExpirationPolicy,
    /// Network-policy commitment.
    pub network_policy_digest: [u8; 32],
    /// Cost and concurrency limits.
    pub budgets: Budgets,
    /// Where commands run, declared by the provider.
    pub platform: Platform,
    /// Whether the provider or the device owner owns this Machine's existence.
    pub lifecycle: LifecycleClass,
}

impl MachineContract {
    /// The fidelity [`MachinesProvider::fork_machine`] offers for a machine under this contract; `None` means live fork is unsupported for it.
    pub fn fork_fidelity(&self) -> Option<ForkFidelity> {
        ForkFidelity::for_capabilities(&self.capabilities)
            .filter(|_| self.lifecycle == LifecycleClass::Provisioned)
    }

    /// Whether exec and guest writes wake this Machine from `Suspended` themselves,
    /// inside their own deadline: a provisioned Machine under [`SuspensionPolicy::AfterIdle`].
    pub fn wakes_on_demand(&self) -> bool {
        self.lifecycle == LifecycleClass::Provisioned
            && matches!(self.suspension, SuspensionPolicy::AfterIdle(_))
    }

    /// Refuses a lifecycle mutation an attached device does not offer.
    pub fn provisioned(&self) -> Result<(), ProviderError> {
        match self.lifecycle {
            LifecycleClass::Provisioned => Ok(()),
            LifecycleClass::Attached => Err(ProviderError::Unsupported(
                "an attached device has no provider lifecycle; recovery: exec on it, or destroy it to detach".into(),
            )),
        }
    }
}

/// Who owns a Machine's existence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LifecycleClass {
    /// The provider creates, suspends, wakes, checkpoints, forks and destroys it.
    Provisioned,
    /// A device its owner attached. Only exec, guest IO and observation apply;
    /// [`MachinesProvider::destroy_machine`] detaches it without touching the device.
    Attached,
}

/// Operating system commands run on.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperatingSystem {
    Linux,
    Macos,
    Windows,
}

/// Processor architecture commands run on.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Architecture {
    X64,
    Arm64,
}

/// What kind of computer a Machine is.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeviceKind {
    VirtualMachine,
    Container,
    Desktop,
    Server,
}

/// The command language a shell caller writes for.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Shell {
    Bash,
    PowerShell,
}

/// How guest paths are spelled.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PathStyle {
    Posix,
    Windows,
}

/// Where a Machine's commands run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Platform {
    pub os: OperatingSystem,
    pub arch: Architecture,
    pub device_kind: DeviceKind,
    pub shell: Shell,
    pub path_style: PathStyle,
}

impl Platform {
    /// The compiling host's OS, architecture, native shell and path style, or
    /// `None` on a target outside the public platform set.
    pub const fn host(device_kind: DeviceKind) -> Option<Self> {
        let os = if cfg!(target_os = "linux") {
            OperatingSystem::Linux
        } else if cfg!(target_os = "macos") {
            OperatingSystem::Macos
        } else if cfg!(windows) {
            OperatingSystem::Windows
        } else {
            return None;
        };
        let arch = if cfg!(target_arch = "x86_64") {
            Architecture::X64
        } else if cfg!(target_arch = "aarch64") {
            Architecture::Arm64
        } else {
            return None;
        };
        let windows = matches!(os, OperatingSystem::Windows);
        Some(Self {
            os,
            arch,
            device_kind,
            shell: if windows {
                Shell::PowerShell
            } else {
                Shell::Bash
            },
            path_style: if windows {
                PathStyle::Windows
            } else {
                PathStyle::Posix
            },
        })
    }
}

/// Qualification result for one immutable image.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImageQualification {
    /// Qualified image.
    pub image: Image,
    /// Exact enabled capabilities.
    pub capabilities: BTreeSet<Capability>,
    /// Opaque customer compatibility revision.
    pub compatibility_revision: [u8; 32],
}
