#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct QualifyImageRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub image: ::core::option::Option<Image>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct CreateMachineRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub idempotency_key: ::core::option::Option<IdempotencyKey>,
    #[prost(message, optional, tag = "3")]
    pub image: ::core::option::Option<Image>,
    #[prost(message, optional, tag = "4")]
    pub compatibility: ::core::option::Option<CompatibilityPolicy>,
    #[prost(message, optional, tag = "6")]
    pub suspension: ::core::option::Option<SuspensionPolicy>,
    #[prost(message, optional, tag = "7")]
    pub expiration: ::core::option::Option<ExpirationPolicy>,
    #[prost(bytes = "vec", tag = "8")]
    pub network_policy_digest: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "9")]
    pub budgets: ::core::option::Option<Budgets>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct MachineMutationRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub idempotency_key: ::core::option::Option<IdempotencyKey>,
    #[prost(message, optional, tag = "3")]
    pub machine: ::core::option::Option<MachineId>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct CheckpointMachineRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub idempotency_key: ::core::option::Option<IdempotencyKey>,
    #[prost(message, optional, tag = "3")]
    pub machine: ::core::option::Option<MachineId>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ForkCheckpointRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub idempotency_key: ::core::option::Option<IdempotencyKey>,
    #[prost(message, optional, tag = "3")]
    pub checkpoint: ::core::option::Option<CheckpointId>,
    #[prost(uint32, tag = "4")]
    pub count: u32,
}
/// Forks a running machine, without an intermediate checkpoint, into `count` fresh
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ForkMachineRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub idempotency_key: ::core::option::Option<IdempotencyKey>,
    #[prost(message, optional, tag = "3")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(uint32, tag = "4")]
    pub count: u32,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct SetSuspensionPolicyRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub idempotency_key: ::core::option::Option<IdempotencyKey>,
    #[prost(message, optional, tag = "3")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(message, optional, tag = "4")]
    pub policy: ::core::option::Option<SuspensionPolicy>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct CheckpointMutationRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub idempotency_key: ::core::option::Option<IdempotencyKey>,
    #[prost(message, optional, tag = "3")]
    pub checkpoint: ::core::option::Option<CheckpointId>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct RecoverRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub idempotency_key: ::core::option::Option<IdempotencyKey>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct InspectMachineRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub machine: ::core::option::Option<MachineId>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct InspectCheckpointRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub checkpoint: ::core::option::Option<CheckpointId>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ListMachinesRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub after: ::core::option::Option<MachineId>,
    #[prost(uint32, tag = "3")]
    pub limit: u32,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct OperationRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub operation: ::core::option::Option<OperationId>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct OperationState {
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<OperationId>,
    #[prost(enumeration = "OperationStatus", tag = "2")]
    pub status: i32,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Endpoint {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub uri: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MachineState {
    #[prost(message, optional, tag = "1")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(enumeration = "MachineStatus", tag = "2")]
    pub status: i32,
    #[prost(message, optional, tag = "3")]
    pub contract: ::core::option::Option<MachineContract>,
    #[prost(message, repeated, tag = "4")]
    pub endpoints: ::prost::alloc::vec::Vec<Endpoint>,
    #[prost(message, optional, tag = "5")]
    pub last_checkpoint: ::core::option::Option<CheckpointId>,
    #[prost(uint64, tag = "6")]
    pub created_at_unix_ms: u64,
    #[prost(uint64, tag = "7")]
    pub changed_at_unix_ms: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MachinePage {
    #[prost(message, repeated, tag = "1")]
    pub machines: ::prost::alloc::vec::Vec<MachineState>,
    #[prost(message, optional, tag = "2")]
    pub next: ::core::option::Option<MachineId>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct CheckpointState {
    #[prost(message, optional, tag = "1")]
    pub checkpoint: ::core::option::Option<CheckpointId>,
    #[prost(message, optional, tag = "2")]
    pub source: ::core::option::Option<MachineId>,
    #[prost(message, optional, tag = "3")]
    pub contract: ::core::option::Option<MachineContract>,
    #[prost(bool, tag = "4")]
    pub forkable: bool,
    #[prost(uint64, tag = "5")]
    pub created_at_unix_ms: u64,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct MachineAdmission {
    #[prost(message, optional, tag = "1")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(message, optional, tag = "2")]
    pub operation: ::core::option::Option<OperationId>,
    #[prost(message, optional, tag = "3")]
    pub contract: ::core::option::Option<MachineContract>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct CheckpointAdmission {
    #[prost(message, optional, tag = "1")]
    pub checkpoint: ::core::option::Option<CheckpointId>,
    #[prost(message, optional, tag = "2")]
    pub source: ::core::option::Option<MachineId>,
    #[prost(message, optional, tag = "3")]
    pub operation: ::core::option::Option<OperationId>,
    #[prost(message, optional, tag = "4")]
    pub contract: ::core::option::Option<MachineContract>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForkAdmission {
    #[prost(message, optional, tag = "1")]
    pub checkpoint: ::core::option::Option<CheckpointId>,
    #[prost(message, repeated, tag = "2")]
    pub children: ::prost::alloc::vec::Vec<MachineId>,
    #[prost(message, optional, tag = "3")]
    pub operation: ::core::option::Option<OperationId>,
    #[prost(message, optional, tag = "4")]
    pub contract: ::core::option::Option<MachineContract>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForkMachineAdmission {
    #[prost(message, optional, tag = "1")]
    pub source: ::core::option::Option<MachineId>,
    #[prost(message, repeated, tag = "2")]
    pub children: ::prost::alloc::vec::Vec<MachineId>,
    #[prost(message, optional, tag = "3")]
    pub operation: ::core::option::Option<OperationId>,
    #[prost(message, optional, tag = "4")]
    pub contract: ::core::option::Option<MachineContract>,
    #[prost(enumeration = "ForkFidelity", tag = "5")]
    pub fidelity: i32,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct PolicyAdmission {
    #[prost(message, optional, tag = "1")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(message, optional, tag = "2")]
    pub operation: ::core::option::Option<OperationId>,
    #[prost(message, optional, tag = "3")]
    pub policy: ::core::option::Option<SuspensionPolicy>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct MutationAdmission {
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<OperationId>,
    #[prost(message, optional, tag = "2")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(message, optional, tag = "3")]
    pub checkpoint: ::core::option::Option<CheckpointId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoveredAdmission {
    #[prost(message, optional, tag = "1")]
    pub operation: ::core::option::Option<OperationId>,
    #[prost(
        oneof = "recovered_admission::Result",
        tags = "2, 3, 4, 5, 6, 7, 8, 9, 10"
    )]
    pub result: ::core::option::Option<recovered_admission::Result>,
}
/// Nested message and enum types in `RecoveredAdmission`.
pub mod recovered_admission {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "2")]
        Create(super::MachineAdmission),
        #[prost(message, tag = "3")]
        Checkpoint(super::CheckpointAdmission),
        #[prost(message, tag = "4")]
        Fork(super::ForkAdmission),
        #[prost(message, tag = "5")]
        Suspend(super::MutationAdmission),
        #[prost(message, tag = "6")]
        Wake(super::MutationAdmission),
        #[prost(message, tag = "7")]
        DestroyMachine(super::MutationAdmission),
        #[prost(message, tag = "8")]
        SetSuspensionPolicy(super::PolicyAdmission),
        #[prost(message, tag = "9")]
        DestroyCheckpoint(super::MutationAdmission),
        #[prost(message, tag = "10")]
        ForkMachine(super::ForkMachineAdmission),
    }
}
/// Terminal simulator result. Unlike an admission, this contains the checked
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForkedMachines {
    #[prost(message, repeated, tag = "1")]
    pub machines: ::prost::alloc::vec::Vec<MachineState>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ForkedLiveMachines {
    #[prost(message, optional, tag = "1")]
    pub source: ::core::option::Option<MachineId>,
    #[prost(enumeration = "ForkFidelity", tag = "2")]
    pub fidelity: i32,
    #[prost(message, repeated, tag = "3")]
    pub children: ::prost::alloc::vec::Vec<MachineState>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct PolicySet {
    #[prost(message, optional, tag = "1")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(message, optional, tag = "2")]
    pub policy: ::core::option::Option<SuspensionPolicy>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MutationOutcome {
    #[prost(oneof = "mutation_outcome::Result", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9")]
    pub result: ::core::option::Option<mutation_outcome::Result>,
}
/// Nested message and enum types in `MutationOutcome`.
pub mod mutation_outcome {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "1")]
        Created(super::MachineState),
        #[prost(message, tag = "2")]
        Checkpointed(super::CheckpointState),
        #[prost(message, tag = "3")]
        Forked(super::ForkedMachines),
        #[prost(message, tag = "4")]
        Suspended(super::MachineId),
        #[prost(message, tag = "5")]
        Woken(super::MachineId),
        #[prost(message, tag = "6")]
        SuspensionPolicySet(super::PolicySet),
        #[prost(message, tag = "7")]
        MachineDestroyed(super::MachineId),
        #[prost(message, tag = "8")]
        CheckpointDestroyed(super::CheckpointId),
        #[prost(message, tag = "9")]
        MachineForked(super::ForkedLiveMachines),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OperationPage {
    #[prost(message, repeated, tag = "1")]
    pub operations: ::prost::alloc::vec::Vec<OperationState>,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct MachineEvent {
    #[prost(message, optional, tag = "1")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(uint64, tag = "2")]
    pub sequence: u64,
    #[prost(uint64, tag = "3")]
    pub observed_at_unix_ms: u64,
    #[prost(enumeration = "EventKind", tag = "4")]
    pub kind: i32,
    #[prost(enumeration = "MachineStatus", tag = "5")]
    pub state: i32,
    #[prost(enumeration = "PressureKind", tag = "6")]
    pub pressure: i32,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct EventsRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(uint64, tag = "3")]
    pub after_sequence: u64,
    #[prost(uint32, tag = "4")]
    pub limit: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventPage {
    #[prost(message, repeated, tag = "1")]
    pub events: ::prost::alloc::vec::Vec<MachineEvent>,
    #[prost(uint64, tag = "2")]
    pub next_sequence: u64,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UsageRequest {
    #[prost(message, optional, tag = "1")]
    pub protocol: ::core::option::Option<ProtocolVersion>,
    #[prost(message, optional, tag = "2")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(uint64, tag = "3")]
    pub start_unix_ms: u64,
    #[prost(uint64, tag = "4")]
    pub end_unix_ms: u64,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UsageReceipt {
    #[prost(message, optional, tag = "1")]
    pub machine: ::core::option::Option<MachineId>,
    #[prost(uint64, tag = "2")]
    pub start_unix_ms: u64,
    #[prost(uint64, tag = "3")]
    pub end_unix_ms: u64,
    #[prost(uint64, tag = "4")]
    pub elastic_cpu_ns: u64,
    #[prost(uint64, tag = "5")]
    pub dedicated_cpu_ns: u64,
    #[prost(uint64, tag = "6")]
    pub private_resident_byte_seconds: u64,
    #[prost(uint64, tag = "7")]
    pub durable_private_bytes: u64,
    #[prost(bytes = "vec", tag = "11")]
    pub lineage_receipt_sha256: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "9")]
    pub egress_bytes: u64,
    #[prost(bytes = "vec", tag = "10")]
    pub receipt: ::prost::alloc::vec::Vec<u8>,
}
