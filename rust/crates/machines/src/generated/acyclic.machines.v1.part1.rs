impl ExpirationKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "EXPIRATION_KIND_UNSPECIFIED",
            Self::Never => "EXPIRATION_KIND_NEVER",
            Self::MaxAge => "EXPIRATION_KIND_MAX_AGE",
            Self::At => "EXPIRATION_KIND_AT",
            Self::Idle => "EXPIRATION_KIND_IDLE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "EXPIRATION_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "EXPIRATION_KIND_NEVER" => Some(Self::Never),
            "EXPIRATION_KIND_MAX_AGE" => Some(Self::MaxAge),
            "EXPIRATION_KIND_AT" => Some(Self::At),
            "EXPIRATION_KIND_IDLE" => Some(Self::Idle),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum OperationStatus {
    Unspecified = 0,
    Pending = 1,
    Succeeded = 2,
    Cancelled = 3,
    Indeterminate = 4,
    Failed = 5,
}
impl OperationStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "OPERATION_STATUS_UNSPECIFIED",
            Self::Pending => "OPERATION_STATUS_PENDING",
            Self::Succeeded => "OPERATION_STATUS_SUCCEEDED",
            Self::Cancelled => "OPERATION_STATUS_CANCELLED",
            Self::Indeterminate => "OPERATION_STATUS_INDETERMINATE",
            Self::Failed => "OPERATION_STATUS_FAILED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "OPERATION_STATUS_UNSPECIFIED" => Some(Self::Unspecified),
            "OPERATION_STATUS_PENDING" => Some(Self::Pending),
            "OPERATION_STATUS_SUCCEEDED" => Some(Self::Succeeded),
            "OPERATION_STATUS_CANCELLED" => Some(Self::Cancelled),
            "OPERATION_STATUS_INDETERMINATE" => Some(Self::Indeterminate),
            "OPERATION_STATUS_FAILED" => Some(Self::Failed),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MachineStatus {
    Unspecified = 0,
    Starting = 1,
    Running = 2,
    Suspending = 3,
    Suspended = 4,
    Waking = 5,
    Destroying = 6,
    Destroyed = 7,
    Failed = 8,
    Indeterminate = 9,
}
impl MachineStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "MACHINE_STATUS_UNSPECIFIED",
            Self::Starting => "MACHINE_STATUS_STARTING",
            Self::Running => "MACHINE_STATUS_RUNNING",
            Self::Suspending => "MACHINE_STATUS_SUSPENDING",
            Self::Suspended => "MACHINE_STATUS_SUSPENDED",
            Self::Waking => "MACHINE_STATUS_WAKING",
            Self::Destroying => "MACHINE_STATUS_DESTROYING",
            Self::Destroyed => "MACHINE_STATUS_DESTROYED",
            Self::Failed => "MACHINE_STATUS_FAILED",
            Self::Indeterminate => "MACHINE_STATUS_INDETERMINATE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "MACHINE_STATUS_UNSPECIFIED" => Some(Self::Unspecified),
            "MACHINE_STATUS_STARTING" => Some(Self::Starting),
            "MACHINE_STATUS_RUNNING" => Some(Self::Running),
            "MACHINE_STATUS_SUSPENDING" => Some(Self::Suspending),
            "MACHINE_STATUS_SUSPENDED" => Some(Self::Suspended),
            "MACHINE_STATUS_WAKING" => Some(Self::Waking),
            "MACHINE_STATUS_DESTROYING" => Some(Self::Destroying),
            "MACHINE_STATUS_DESTROYED" => Some(Self::Destroyed),
            "MACHINE_STATUS_FAILED" => Some(Self::Failed),
            "MACHINE_STATUS_INDETERMINATE" => Some(Self::Indeterminate),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ForkFidelity {
    Unspecified = 0,
    /// Children resume from the source's memory, processes, and disk at the fork instant.
    MemoryAndDisk = 1,
    /// Children boot fresh over a copy of the source's persistent disk (provider-defined; see
    /// CAPABILITY_DISK_FORK) taken at one consistent instant; no process state is inherited.
    DiskOnly = 2,
}
impl ForkFidelity {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "FORK_FIDELITY_UNSPECIFIED",
            Self::MemoryAndDisk => "FORK_FIDELITY_MEMORY_AND_DISK",
            Self::DiskOnly => "FORK_FIDELITY_DISK_ONLY",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "FORK_FIDELITY_UNSPECIFIED" => Some(Self::Unspecified),
            "FORK_FIDELITY_MEMORY_AND_DISK" => Some(Self::MemoryAndDisk),
            "FORK_FIDELITY_DISK_ONLY" => Some(Self::DiskOnly),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PressureKind {
    Unspecified = 0,
    CustomerBudget = 1,
    MachineLimit = 2,
    ServiceSaturation = 3,
}
impl PressureKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "PRESSURE_KIND_UNSPECIFIED",
            Self::CustomerBudget => "PRESSURE_KIND_CUSTOMER_BUDGET",
            Self::MachineLimit => "PRESSURE_KIND_MACHINE_LIMIT",
            Self::ServiceSaturation => "PRESSURE_KIND_SERVICE_SATURATION",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "PRESSURE_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "PRESSURE_KIND_CUSTOMER_BUDGET" => Some(Self::CustomerBudget),
            "PRESSURE_KIND_MACHINE_LIMIT" => Some(Self::MachineLimit),
            "PRESSURE_KIND_SERVICE_SATURATION" => Some(Self::ServiceSaturation),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EventKind {
    Unspecified = 0,
    State = 1,
    Pressure = 2,
    Capacity = 3,
}
impl EventKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "EVENT_KIND_UNSPECIFIED",
            Self::State => "EVENT_KIND_STATE",
            Self::Pressure => "EVENT_KIND_PRESSURE",
            Self::Capacity => "EVENT_KIND_CAPACITY",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "EVENT_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "EVENT_KIND_STATE" => Some(Self::State),
            "EVENT_KIND_PRESSURE" => Some(Self::Pressure),
            "EVENT_KIND_CAPACITY" => Some(Self::Capacity),
            _ => None,
        }
    }
}
#[cfg(feature = "grpc")]
include!("acyclic.machines.v1.tonic.rs");
