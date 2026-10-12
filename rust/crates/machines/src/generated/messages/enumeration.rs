#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ImageKind {
    Unspecified = 0,
    ManagedOci = 1,
    Custom = 2,
    Checkpoint = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Capability {
    Unspecified = 0,
    ElasticCpu = 1,
    ElasticMemory = 2,
    LiveCheckpoint = 3,
    LiveFork = 4,
    SuspendResume = 5,
    LiveMovement = 6,
    /// ForkMachine copies a running machine's persistent disk, but not its memory or processes,
    DiskFork = 7,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CompatibilityMode {
    Unspecified = 0,
    BestEffort = 1,
    Require = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ExpirationKind {
    Unspecified = 0,
    Never = 1,
    MaxAge = 2,
    At = 3,
    Idle = 4,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ForkFidelity {
    Unspecified = 0,
    /// Children resume from the source's memory, processes, and disk at the fork instant.
    MemoryAndDisk = 1,
    /// Children boot fresh over a copy of the source's persistent disk (provider-defined; see
    DiskOnly = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PressureKind {
    Unspecified = 0,
    CustomerBudget = 1,
    MachineLimit = 2,
    ServiceSaturation = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EventKind {
    Unspecified = 0,
    State = 1,
    Pressure = 2,
    Capacity = 3,
}
