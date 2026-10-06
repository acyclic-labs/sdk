//! Rust-owned Machines v1 contract metadata.
//!
//! This model is the generation input; the archived protobuf is used only as a compatibility oracle.

use super::FieldType::*;
use super::{
    Cardinality, ContractSpec, EnumSpec, EnumValueSpec, FieldSpec, FieldType, MessageSpec,
    MethodSpec, OneofSpec, RouteSpec, ServiceSpec,
};
use prost::Message;

const fn field(
    name: &'static str,
    number: u32,
    cardinality: Cardinality,
    field_type: FieldType,
    json_name: &'static str,
) -> FieldSpec {
    super::field(name, number, cardinality, field_type, json_name)
}
const fn oneof_field(
    name: &'static str,
    number: u32,
    field_type: FieldType,
    json_name: &'static str,
    oneof: &'static str,
) -> FieldSpec {
    super::oneof_field(name, number, field_type, json_name, oneof)
}
const fn oneof(name: &'static str, synthetic: bool) -> OneofSpec {
    super::oneof(name, synthetic)
}
macro_rules! message { ($name:ident, $wire_name:literal, [$($field:expr),* $(,)?], [$($oneof:expr),* $(,)?], [$($range:expr),* $(,)?], [$($reserved:expr),* $(,)?]) => { #[allow(non_upper_case_globals)] const $name: MessageSpec = MessageSpec { name: $wire_name, fields: &[$($field),*], oneofs: &[$($oneof),*], nested_messages: &[], is_map_entry: false, reserved_ranges: &[$($range),*], reserved_names: &[$($reserved),*] }; }; }

pub const IMAGEKIND: EnumSpec = EnumSpec {
    name: "ImageKind",
    values: &[
        EnumValueSpec {
            name: "IMAGE_KIND_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "IMAGE_KIND_MANAGED_OCI",
            number: 1,
        },
        EnumValueSpec {
            name: "IMAGE_KIND_CUSTOM",
            number: 2,
        },
        EnumValueSpec {
            name: "IMAGE_KIND_CHECKPOINT",
            number: 3,
        },
    ],
};

pub const CAPABILITY: EnumSpec = EnumSpec {
    name: "Capability",
    values: &[
        EnumValueSpec {
            name: "CAPABILITY_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "CAPABILITY_ELASTIC_CPU",
            number: 1,
        },
        EnumValueSpec {
            name: "CAPABILITY_ELASTIC_MEMORY",
            number: 2,
        },
        EnumValueSpec {
            name: "CAPABILITY_LIVE_CHECKPOINT",
            number: 3,
        },
        EnumValueSpec {
            name: "CAPABILITY_LIVE_FORK",
            number: 4,
        },
        EnumValueSpec {
            name: "CAPABILITY_SUSPEND_RESUME",
            number: 5,
        },
        EnumValueSpec {
            name: "CAPABILITY_LIVE_MOVEMENT",
            number: 6,
        },
        EnumValueSpec {
            name: "CAPABILITY_DISK_FORK",
            number: 7,
        },
    ],
};

pub const COMPATIBILITYMODE: EnumSpec = EnumSpec {
    name: "CompatibilityMode",
    values: &[
        EnumValueSpec {
            name: "COMPATIBILITY_MODE_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "COMPATIBILITY_MODE_BEST_EFFORT",
            number: 1,
        },
        EnumValueSpec {
            name: "COMPATIBILITY_MODE_REQUIRE",
            number: 2,
        },
    ],
};

pub const EXPIRATIONKIND: EnumSpec = EnumSpec {
    name: "ExpirationKind",
    values: &[
        EnumValueSpec {
            name: "EXPIRATION_KIND_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "EXPIRATION_KIND_NEVER",
            number: 1,
        },
        EnumValueSpec {
            name: "EXPIRATION_KIND_MAX_AGE",
            number: 2,
        },
        EnumValueSpec {
            name: "EXPIRATION_KIND_AT",
            number: 3,
        },
        EnumValueSpec {
            name: "EXPIRATION_KIND_IDLE",
            number: 4,
        },
    ],
};

pub const OPERATIONSTATUS: EnumSpec = EnumSpec {
    name: "OperationStatus",
    values: &[
        EnumValueSpec {
            name: "OPERATION_STATUS_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "OPERATION_STATUS_PENDING",
            number: 1,
        },
        EnumValueSpec {
            name: "OPERATION_STATUS_SUCCEEDED",
            number: 2,
        },
        EnumValueSpec {
            name: "OPERATION_STATUS_CANCELLED",
            number: 3,
        },
        EnumValueSpec {
            name: "OPERATION_STATUS_INDETERMINATE",
            number: 4,
        },
        EnumValueSpec {
            name: "OPERATION_STATUS_FAILED",
            number: 5,
        },
    ],
};

pub const MACHINESTATUS: EnumSpec = EnumSpec {
    name: "MachineStatus",
    values: &[
        EnumValueSpec {
            name: "MACHINE_STATUS_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_STARTING",
            number: 1,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_RUNNING",
            number: 2,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_SUSPENDING",
            number: 3,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_SUSPENDED",
            number: 4,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_WAKING",
            number: 5,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_DESTROYING",
            number: 6,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_DESTROYED",
            number: 7,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_FAILED",
            number: 8,
        },
        EnumValueSpec {
            name: "MACHINE_STATUS_INDETERMINATE",
            number: 9,
        },
    ],
};

pub const FORKFIDELITY: EnumSpec = EnumSpec {
    name: "ForkFidelity",
    values: &[
        EnumValueSpec {
            name: "FORK_FIDELITY_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "FORK_FIDELITY_MEMORY_AND_DISK",
            number: 1,
        },
        EnumValueSpec {
            name: "FORK_FIDELITY_DISK_ONLY",
            number: 2,
        },
    ],
};

pub const PRESSUREKIND: EnumSpec = EnumSpec {
    name: "PressureKind",
    values: &[
        EnumValueSpec {
            name: "PRESSURE_KIND_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "PRESSURE_KIND_CUSTOMER_BUDGET",
            number: 1,
        },
        EnumValueSpec {
            name: "PRESSURE_KIND_MACHINE_LIMIT",
            number: 2,
        },
        EnumValueSpec {
            name: "PRESSURE_KIND_SERVICE_SATURATION",
            number: 3,
        },
    ],
};

pub const EVENTKIND: EnumSpec = EnumSpec {
    name: "EventKind",
    values: &[
        EnumValueSpec {
            name: "EVENT_KIND_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "EVENT_KIND_STATE",
            number: 1,
        },
        EnumValueSpec {
            name: "EVENT_KIND_PRESSURE",
            number: 2,
        },
        EnumValueSpec {
            name: "EVENT_KIND_CAPACITY",
            number: 3,
        },
    ],
};

message!(
    PROTOCOLVERSION,
    "ProtocolVersion",
    [
        field("major", 1, Cardinality::Singular, Uint32, "major"),
        field("minor", 2, Cardinality::Singular, Uint32, "minor")
    ],
    [],
    [],
    []
);
message!(
    OPERATIONID,
    "OperationId",
    [field("value", 1, Cardinality::Singular, Bytes, "value")],
    [],
    [],
    []
);
message!(
    IDEMPOTENCYKEY,
    "IdempotencyKey",
    [field("value", 1, Cardinality::Singular, Bytes, "value")],
    [],
    [],
    []
);
message!(
    MACHINEID,
    "MachineId",
    [field("value", 1, Cardinality::Singular, Bytes, "value")],
    [],
    [],
    []
);
message!(
    CHECKPOINTID,
    "CheckpointId",
    [field("value", 1, Cardinality::Singular, Bytes, "value")],
    [],
    [],
    []
);
message!(
    IMAGE,
    "Image",
    [
        field("kind", 1, Cardinality::Singular, Enum("ImageKind"), "kind"),
        oneof_field(
            "managed_digest",
            2,
            Bytes,
            "managedDigest",
            "immutable_reference"
        ),
        oneof_field(
            "custom_digest",
            3,
            Bytes,
            "customDigest",
            "immutable_reference"
        ),
        oneof_field(
            "checkpoint",
            4,
            Message("CheckpointId"),
            "checkpoint",
            "immutable_reference"
        )
    ],
    [oneof("immutable_reference", false)],
    [],
    []
);
message!(
    COMPATIBILITYPOLICY,
    "CompatibilityPolicy",
    [
        field(
            "mode",
            1,
            Cardinality::Singular,
            Enum("CompatibilityMode"),
            "mode"
        ),
        field(
            "required",
            2,
            Cardinality::Repeated,
            Enum("Capability"),
            "required"
        )
    ],
    [],
    [],
    []
);
message!(
    IMAGEQUALIFICATION,
    "ImageQualification",
    [
        field("image", 1, Cardinality::Singular, Message("Image"), "image"),
        field(
            "capabilities",
            2,
            Cardinality::Repeated,
            Enum("Capability"),
            "capabilities"
        ),
        field(
            "compatibility_revision",
            3,
            Cardinality::Singular,
            Bytes,
            "compatibilityRevision"
        )
    ],
    [],
    [],
    []
);
message!(
    SUSPENSIONPOLICY,
    "SuspensionPolicy",
    [
        oneof_field("manual", 1, Bool, "manual", "policy"),
        oneof_field("after_idle_ms", 2, Uint64, "afterIdleMs", "policy")
    ],
    [oneof("policy", false)],
    [],
    []
);
message!(
    EXPIRATIONPOLICY,
    "ExpirationPolicy",
    [
        field(
            "kind",
            1,
            Cardinality::Singular,
            Enum("ExpirationKind"),
            "kind"
        ),
        field("value_ms", 2, Cardinality::Singular, Uint64, "valueMs")
    ],
    [],
    [],
    []
);
message!(
    BUDGETS,
    "Budgets",
    [
        field(
            "spend_micros",
            1,
            Cardinality::Singular,
            Uint64,
            "spendMicros"
        ),
        field(
            "concurrency",
            2,
            Cardinality::Singular,
            Uint32,
            "concurrency"
        )
    ],
    [],
    [],
    []
);
message!(
    MACHINECONTRACT,
    "MachineContract",
    [
        field("image", 1, Cardinality::Singular, Message("Image"), "image"),
        field(
            "capabilities",
            2,
            Cardinality::Repeated,
            Enum("Capability"),
            "capabilities"
        ),
        field(
            "compatibility",
            3,
            Cardinality::Singular,
            Message("CompatibilityPolicy"),
            "compatibility"
        ),
        field(
            "compatibility_revision",
            4,
            Cardinality::Singular,
            Bytes,
            "compatibilityRevision"
        ),
        field(
            "suspension",
            6,
            Cardinality::Singular,
            Message("SuspensionPolicy"),
            "suspension"
        ),
        field(
            "expiration",
            7,
            Cardinality::Singular,
            Message("ExpirationPolicy"),
            "expiration"
        ),
        field(
            "network_policy_digest",
            8,
            Cardinality::Singular,
            Bytes,
            "networkPolicyDigest"
        ),
        field(
            "budgets",
            9,
            Cardinality::Singular,
            Message("Budgets"),
            "budgets"
        )
    ],
    [],
    [(5, 5)],
    ["performance"]
);
message!(
    QUALIFYIMAGEREQUEST,
    "QualifyImageRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field("image", 2, Cardinality::Singular, Message("Image"), "image")
    ],
    [],
    [],
    []
);
message!(
    CREATEMACHINEREQUEST,
    "CreateMachineRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            Message("IdempotencyKey"),
            "idempotencyKey"
        ),
        field("image", 3, Cardinality::Singular, Message("Image"), "image"),
        field(
            "compatibility",
            4,
            Cardinality::Singular,
            Message("CompatibilityPolicy"),
            "compatibility"
        ),
        field(
            "suspension",
            6,
            Cardinality::Singular,
            Message("SuspensionPolicy"),
            "suspension"
        ),
        field(
            "expiration",
            7,
            Cardinality::Singular,
            Message("ExpirationPolicy"),
            "expiration"
        ),
        field(
            "network_policy_digest",
            8,
            Cardinality::Singular,
            Bytes,
            "networkPolicyDigest"
        ),
        field(
            "budgets",
            9,
            Cardinality::Singular,
            Message("Budgets"),
            "budgets"
        )
    ],
    [],
    [(5, 5)],
    ["performance"]
);
message!(
    MACHINEMUTATIONREQUEST,
    "MachineMutationRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            Message("IdempotencyKey"),
            "idempotencyKey"
        ),
        field(
            "machine",
            3,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        )
    ],
    [],
    [],
    []
);
message!(
    CHECKPOINTMACHINEREQUEST,
    "CheckpointMachineRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            Message("IdempotencyKey"),
            "idempotencyKey"
        ),
        field(
            "machine",
            3,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        )
    ],
    [],
    [],
    []
);
message!(
    FORKCHECKPOINTREQUEST,
    "ForkCheckpointRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            Message("IdempotencyKey"),
            "idempotencyKey"
        ),
        field(
            "checkpoint",
            3,
            Cardinality::Singular,
            Message("CheckpointId"),
            "checkpoint"
        ),
        field("count", 4, Cardinality::Singular, Uint32, "count")
    ],
    [],
    [(5, 5)],
    ["performance"]
);
message!(
    FORKMACHINEREQUEST,
    "ForkMachineRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            Message("IdempotencyKey"),
            "idempotencyKey"
        ),
        field(
            "machine",
            3,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field("count", 4, Cardinality::Singular, Uint32, "count")
    ],
    [],
    [],
    []
);
message!(
    SETSUSPENSIONPOLICYREQUEST,
    "SetSuspensionPolicyRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            Message("IdempotencyKey"),
            "idempotencyKey"
        ),
        field(
            "machine",
            3,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "policy",
            4,
            Cardinality::Singular,
            Message("SuspensionPolicy"),
            "policy"
        )
    ],
    [],
    [],
    []
);
message!(
    CHECKPOINTMUTATIONREQUEST,
    "CheckpointMutationRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            Message("IdempotencyKey"),
            "idempotencyKey"
        ),
        field(
            "checkpoint",
            3,
            Cardinality::Singular,
            Message("CheckpointId"),
            "checkpoint"
        )
    ],
    [],
    [],
    []
);
message!(
    RECOVERREQUEST,
    "RecoverRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            Message("IdempotencyKey"),
            "idempotencyKey"
        )
    ],
    [],
    [],
    []
);
message!(
    INSPECTMACHINEREQUEST,
    "InspectMachineRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "machine",
            2,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        )
    ],
    [],
    [],
    []
);
message!(
    INSPECTCHECKPOINTREQUEST,
    "InspectCheckpointRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "checkpoint",
            2,
            Cardinality::Singular,
            Message("CheckpointId"),
            "checkpoint"
        )
    ],
    [],
    [],
    []
);
message!(
    LISTMACHINESREQUEST,
    "ListMachinesRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "after",
            2,
            Cardinality::Singular,
            Message("MachineId"),
            "after"
        ),
        field("limit", 3, Cardinality::Singular, Uint32, "limit")
    ],
    [],
    [],
    []
);
message!(
    OPERATIONREQUEST,
    "OperationRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "operation",
            2,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        )
    ],
    [],
    [],
    []
);
message!(
    OPERATIONSTATE,
    "OperationState",
    [
        field(
            "operation",
            1,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        ),
        field(
            "status",
            2,
            Cardinality::Singular,
            Enum("OperationStatus"),
            "status"
        )
    ],
    [],
    [],
    []
);
message!(
    ENDPOINT,
    "Endpoint",
    [
        field("name", 1, Cardinality::Singular, String, "name"),
        field("uri", 2, Cardinality::Singular, String, "uri")
    ],
    [],
    [],
    []
);
message!(
    MACHINESTATE,
    "MachineState",
    [
        field(
            "machine",
            1,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "status",
            2,
            Cardinality::Singular,
            Enum("MachineStatus"),
            "status"
        ),
        field(
            "contract",
            3,
            Cardinality::Singular,
            Message("MachineContract"),
            "contract"
        ),
        field(
            "endpoints",
            4,
            Cardinality::Repeated,
            Message("Endpoint"),
            "endpoints"
        ),
        field(
            "last_checkpoint",
            5,
            Cardinality::Singular,
            Message("CheckpointId"),
            "lastCheckpoint"
        ),
        field(
            "created_at_unix_ms",
            6,
            Cardinality::Singular,
            Uint64,
            "createdAtUnixMs"
        ),
        field(
            "changed_at_unix_ms",
            7,
            Cardinality::Singular,
            Uint64,
            "changedAtUnixMs"
        )
    ],
    [],
    [],
    []
);
message!(
    MACHINEPAGE,
    "MachinePage",
    [
        field(
            "machines",
            1,
            Cardinality::Repeated,
            Message("MachineState"),
            "machines"
        ),
        field(
            "next",
            2,
            Cardinality::Singular,
            Message("MachineId"),
            "next"
        )
    ],
    [],
    [],
    []
);
message!(
    CHECKPOINTSTATE,
    "CheckpointState",
    [
        field(
            "checkpoint",
            1,
            Cardinality::Singular,
            Message("CheckpointId"),
            "checkpoint"
        ),
        field(
            "source",
            2,
            Cardinality::Singular,
            Message("MachineId"),
            "source"
        ),
        field(
            "contract",
            3,
            Cardinality::Singular,
            Message("MachineContract"),
            "contract"
        ),
        field("forkable", 4, Cardinality::Singular, Bool, "forkable"),
        field(
            "created_at_unix_ms",
            5,
            Cardinality::Singular,
            Uint64,
            "createdAtUnixMs"
        )
    ],
    [],
    [],
    []
);
message!(
    MACHINEADMISSION,
    "MachineAdmission",
    [
        field(
            "machine",
            1,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "operation",
            2,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        ),
        field(
            "contract",
            3,
            Cardinality::Singular,
            Message("MachineContract"),
            "contract"
        )
    ],
    [],
    [],
    []
);
message!(
    CHECKPOINTADMISSION,
    "CheckpointAdmission",
    [
        field(
            "checkpoint",
            1,
            Cardinality::Singular,
            Message("CheckpointId"),
            "checkpoint"
        ),
        field(
            "source",
            2,
            Cardinality::Singular,
            Message("MachineId"),
            "source"
        ),
        field(
            "operation",
            3,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        ),
        field(
            "contract",
            4,
            Cardinality::Singular,
            Message("MachineContract"),
            "contract"
        )
    ],
    [],
    [],
    []
);
message!(
    FORKADMISSION,
    "ForkAdmission",
    [
        field(
            "checkpoint",
            1,
            Cardinality::Singular,
            Message("CheckpointId"),
            "checkpoint"
        ),
        field(
            "children",
            2,
            Cardinality::Repeated,
            Message("MachineId"),
            "children"
        ),
        field(
            "operation",
            3,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        ),
        field(
            "contract",
            4,
            Cardinality::Singular,
            Message("MachineContract"),
            "contract"
        )
    ],
    [],
    [],
    []
);
message!(
    FORKMACHINEADMISSION,
    "ForkMachineAdmission",
    [
        field(
            "source",
            1,
            Cardinality::Singular,
            Message("MachineId"),
            "source"
        ),
        field(
            "children",
            2,
            Cardinality::Repeated,
            Message("MachineId"),
            "children"
        ),
        field(
            "operation",
            3,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        ),
        field(
            "contract",
            4,
            Cardinality::Singular,
            Message("MachineContract"),
            "contract"
        ),
        field(
            "fidelity",
            5,
            Cardinality::Singular,
            Enum("ForkFidelity"),
            "fidelity"
        )
    ],
    [],
    [],
    []
);
message!(
    POLICYADMISSION,
    "PolicyAdmission",
    [
        field(
            "machine",
            1,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "operation",
            2,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        ),
        field(
            "policy",
            3,
            Cardinality::Singular,
            Message("SuspensionPolicy"),
            "policy"
        )
    ],
    [],
    [],
    []
);
message!(
    MUTATIONADMISSION,
    "MutationAdmission",
    [
        field(
            "operation",
            1,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        ),
        field(
            "machine",
            2,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "checkpoint",
            3,
            Cardinality::Singular,
            Message("CheckpointId"),
            "checkpoint"
        )
    ],
    [],
    [],
    []
);
message!(
    RECOVEREDADMISSION,
    "RecoveredAdmission",
    [
        field(
            "operation",
            1,
            Cardinality::Singular,
            Message("OperationId"),
            "operation"
        ),
        oneof_field("create", 2, Message("MachineAdmission"), "create", "result"),
        oneof_field(
            "checkpoint",
            3,
            Message("CheckpointAdmission"),
            "checkpoint",
            "result"
        ),
        oneof_field("fork", 4, Message("ForkAdmission"), "fork", "result"),
        oneof_field(
            "suspend",
            5,
            Message("MutationAdmission"),
            "suspend",
            "result"
        ),
        oneof_field("wake", 6, Message("MutationAdmission"), "wake", "result"),
        oneof_field(
            "destroy_machine",
            7,
            Message("MutationAdmission"),
            "destroyMachine",
            "result"
        ),
        oneof_field(
            "set_suspension_policy",
            8,
            Message("PolicyAdmission"),
            "setSuspensionPolicy",
            "result"
        ),
        oneof_field(
            "destroy_checkpoint",
            9,
            Message("MutationAdmission"),
            "destroyCheckpoint",
            "result"
        ),
        oneof_field(
            "fork_machine",
            10,
            Message("ForkMachineAdmission"),
            "forkMachine",
            "result"
        )
    ],
    [oneof("result", false)],
    [],
    []
);
message!(
    FORKEDMACHINES,
    "ForkedMachines",
    [field(
        "machines",
        1,
        Cardinality::Repeated,
        Message("MachineState"),
        "machines"
    )],
    [],
    [],
    []
);
message!(
    FORKEDLIVEMACHINES,
    "ForkedLiveMachines",
    [
        field(
            "source",
            1,
            Cardinality::Singular,
            Message("MachineId"),
            "source"
        ),
        field(
            "fidelity",
            2,
            Cardinality::Singular,
            Enum("ForkFidelity"),
            "fidelity"
        ),
        field(
            "children",
            3,
            Cardinality::Repeated,
            Message("MachineState"),
            "children"
        )
    ],
    [],
    [],
    []
);
message!(
    POLICYSET,
    "PolicySet",
    [
        field(
            "machine",
            1,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "policy",
            2,
            Cardinality::Singular,
            Message("SuspensionPolicy"),
            "policy"
        )
    ],
    [],
    [],
    []
);
message!(
    MUTATIONOUTCOME,
    "MutationOutcome",
    [
        oneof_field("created", 1, Message("MachineState"), "created", "result"),
        oneof_field(
            "checkpointed",
            2,
            Message("CheckpointState"),
            "checkpointed",
            "result"
        ),
        oneof_field("forked", 3, Message("ForkedMachines"), "forked", "result"),
        oneof_field("suspended", 4, Message("MachineId"), "suspended", "result"),
        oneof_field("woken", 5, Message("MachineId"), "woken", "result"),
        oneof_field(
            "suspension_policy_set",
            6,
            Message("PolicySet"),
            "suspensionPolicySet",
            "result"
        ),
        oneof_field(
            "machine_destroyed",
            7,
            Message("MachineId"),
            "machineDestroyed",
            "result"
        ),
        oneof_field(
            "checkpoint_destroyed",
            8,
            Message("CheckpointId"),
            "checkpointDestroyed",
            "result"
        ),
        oneof_field(
            "machine_forked",
            9,
            Message("ForkedLiveMachines"),
            "machineForked",
            "result"
        )
    ],
    [oneof("result", false)],
    [],
    []
);
message!(
    OPERATIONPAGE,
    "OperationPage",
    [field(
        "operations",
        1,
        Cardinality::Repeated,
        Message("OperationState"),
        "operations"
    )],
    [],
    [],
    []
);
message!(
    MACHINEEVENT,
    "MachineEvent",
    [
        field(
            "machine",
            1,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field("sequence", 2, Cardinality::Singular, Uint64, "sequence"),
        field(
            "observed_at_unix_ms",
            3,
            Cardinality::Singular,
            Uint64,
            "observedAtUnixMs"
        ),
        field("kind", 4, Cardinality::Singular, Enum("EventKind"), "kind"),
        field(
            "state",
            5,
            Cardinality::Singular,
            Enum("MachineStatus"),
            "state"
        ),
        field(
            "pressure",
            6,
            Cardinality::Singular,
            Enum("PressureKind"),
            "pressure"
        )
    ],
    [],
    [],
    []
);
message!(
    EVENTSREQUEST,
    "EventsRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "machine",
            2,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "after_sequence",
            3,
            Cardinality::Singular,
            Uint64,
            "afterSequence"
        ),
        field("limit", 4, Cardinality::Singular, Uint32, "limit")
    ],
    [],
    [],
    []
);
message!(
    EVENTPAGE,
    "EventPage",
    [
        field(
            "events",
            1,
            Cardinality::Repeated,
            Message("MachineEvent"),
            "events"
        ),
        field(
            "next_sequence",
            2,
            Cardinality::Singular,
            Uint64,
            "nextSequence"
        )
    ],
    [],
    [],
    []
);
message!(
    USAGEREQUEST,
    "UsageRequest",
    [
        field(
            "protocol",
            1,
            Cardinality::Singular,
            Message("ProtocolVersion"),
            "protocol"
        ),
        field(
            "machine",
            2,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "start_unix_ms",
            3,
            Cardinality::Singular,
            Uint64,
            "startUnixMs"
        ),
        field("end_unix_ms", 4, Cardinality::Singular, Uint64, "endUnixMs")
    ],
    [],
    [],
    []
);
message!(
    USAGERECEIPT,
    "UsageReceipt",
    [
        field(
            "machine",
            1,
            Cardinality::Singular,
            Message("MachineId"),
            "machine"
        ),
        field(
            "start_unix_ms",
            2,
            Cardinality::Singular,
            Uint64,
            "startUnixMs"
        ),
        field("end_unix_ms", 3, Cardinality::Singular, Uint64, "endUnixMs"),
        field(
            "elastic_cpu_ns",
            4,
            Cardinality::Singular,
            Uint64,
            "elasticCpuNs"
        ),
        field(
            "dedicated_cpu_ns",
            5,
            Cardinality::Singular,
            Uint64,
            "dedicatedCpuNs"
        ),
        field(
            "private_resident_byte_seconds",
            6,
            Cardinality::Singular,
            Uint64,
            "privateResidentByteSeconds"
        ),
        field(
            "durable_private_bytes",
            7,
            Cardinality::Singular,
            Uint64,
            "durablePrivateBytes"
        ),
        field(
            "lineage_receipt_sha256",
            11,
            Cardinality::Singular,
            Bytes,
            "lineageReceiptSha256"
        ),
        field(
            "egress_bytes",
            9,
            Cardinality::Singular,
            Uint64,
            "egressBytes"
        ),
        field("receipt", 10, Cardinality::Singular, Bytes, "receipt")
    ],
    [],
    [(8, 8)],
    ["lineage_shared_bytes"]
);
pub const MACHINESSERVICE: ServiceSpec = ServiceSpec {
    name: "MachinesService",
    methods: &[
        MethodSpec {
            name: "QualifyImage",
            input: "QualifyImageRequest",
            output: "ImageQualification",
            docs: "Qualifies an image against the protocol and capability contract.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Create",
            input: "CreateMachineRequest",
            output: "MachineAdmission",
            docs: "Admits a machine with lifecycle and budget policy.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Checkpoint",
            input: "CheckpointMachineRequest",
            output: "CheckpointAdmission",
            docs: "Creates an immutable checkpoint for a machine.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Fork",
            input: "ForkCheckpointRequest",
            output: "ForkAdmission",
            docs: "Forks a checkpoint into fresh machines.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "ForkMachine",
            input: "ForkMachineRequest",
            output: "ForkMachineAdmission",
            docs: "Forks a running machine into fresh children, preserving the declared fidelity.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Suspend",
            input: "MachineMutationRequest",
            output: "MutationAdmission",
            docs: "Requests suspension of a machine.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Wake",
            input: "MachineMutationRequest",
            output: "MutationAdmission",
            docs: "Requests wake of a suspended machine.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "SetSuspensionPolicy",
            input: "SetSuspensionPolicyRequest",
            output: "PolicyAdmission",
            docs: "Replaces a machine suspension policy.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "DestroyMachine",
            input: "MachineMutationRequest",
            output: "MutationAdmission",
            docs: "Destroys a machine and records the mutation outcome.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "DestroyCheckpoint",
            input: "CheckpointMutationRequest",
            output: "MutationAdmission",
            docs: "Destroys a checkpoint and records the mutation outcome.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Recover",
            input: "RecoverRequest",
            output: "RecoveredAdmission",
            docs: "Recovers the outcome of an indeterminate operation.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "InspectMachine",
            input: "InspectMachineRequest",
            output: "MachineState",
            docs: "Reads the current machine state.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "InspectCheckpoint",
            input: "InspectCheckpointRequest",
            output: "CheckpointState",
            docs: "Reads the current checkpoint state.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "ListMachines",
            input: "ListMachinesRequest",
            output: "MachinePage",
            docs: "Lists a bounded page of machines.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Events",
            input: "EventsRequest",
            output: "EventPage",
            docs: "Reads a bounded machine event page.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Usage",
            input: "UsageRequest",
            output: "UsageReceipt",
            docs: "Reads usage for a bounded machine time interval.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Cancel",
            input: "OperationRequest",
            output: "OperationState",
            docs: "Requests cancellation of an admitted operation.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "InspectOperation",
            input: "OperationRequest",
            output: "OperationState",
            docs: "Reads the current operation state.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "WatchOperation",
            input: "OperationRequest",
            output: "OperationState",
            docs: "Streams ordered operation state from a sequence cursor.",
            client_streaming: false,
            server_streaming: true,
        },
    ],
};

pub const MACHINES_ROUTES: &[RouteSpec] = &[];

pub const MACHINES: ContractSpec = ContractSpec {
    file_name: "machines/v1/machines.proto",
    syntax: "proto3",
    package: "acyclic.machines.v1",
    dependencies: &[],
    options: super::FileOptionsSpec {
        go_package: "github.com/acyclic-labs/sdk/go/gen/machines/v1;machinesv1",
    },
    messages: &[
        PROTOCOLVERSION,
        OPERATIONID,
        IDEMPOTENCYKEY,
        MACHINEID,
        CHECKPOINTID,
        IMAGE,
        COMPATIBILITYPOLICY,
        IMAGEQUALIFICATION,
        SUSPENSIONPOLICY,
        EXPIRATIONPOLICY,
        BUDGETS,
        MACHINECONTRACT,
        QUALIFYIMAGEREQUEST,
        CREATEMACHINEREQUEST,
        MACHINEMUTATIONREQUEST,
        CHECKPOINTMACHINEREQUEST,
        FORKCHECKPOINTREQUEST,
        FORKMACHINEREQUEST,
        SETSUSPENSIONPOLICYREQUEST,
        CHECKPOINTMUTATIONREQUEST,
        RECOVERREQUEST,
        INSPECTMACHINEREQUEST,
        INSPECTCHECKPOINTREQUEST,
        LISTMACHINESREQUEST,
        OPERATIONREQUEST,
        OPERATIONSTATE,
        ENDPOINT,
        MACHINESTATE,
        MACHINEPAGE,
        CHECKPOINTSTATE,
        MACHINEADMISSION,
        CHECKPOINTADMISSION,
        FORKADMISSION,
        FORKMACHINEADMISSION,
        POLICYADMISSION,
        MUTATIONADMISSION,
        RECOVEREDADMISSION,
        FORKEDMACHINES,
        FORKEDLIVEMACHINES,
        POLICYSET,
        MUTATIONOUTCOME,
        OPERATIONPAGE,
        MACHINEEVENT,
        EVENTSREQUEST,
        EVENTPAGE,
        USAGEREQUEST,
        USAGERECEIPT,
    ],
    enums: &[
        IMAGEKIND,
        CAPABILITY,
        COMPATIBILITYMODE,
        EXPIRATIONKIND,
        OPERATIONSTATUS,
        MACHINESTATUS,
        FORKFIDELITY,
        PRESSUREKIND,
        EVENTKIND,
    ],
    services: &[MACHINESSERVICE],
    routes: MACHINES_ROUTES,
};

/// Documentation owned by the Machines model and consumed by contract exporters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MachinesDoc {
    pub name: &'static str,
    pub text: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MachinesFieldDoc {
    pub message: &'static str,
    pub name: &'static str,
    pub text: &'static str,
}

pub const MACHINES_MESSAGE_DOCS: &[MachinesDoc] = &[
    MachinesDoc {
        name: "ProtocolVersion",
        text: "Identifies the protocol version required by a Machines request.",
    },
    MachinesDoc {
        name: "OperationId",
        text: "Identifies an admitted asynchronous machine operation.",
    },
    MachinesDoc {
        name: "IdempotencyKey",
        text: "Binds retries of one machine mutation intent.",
    },
    MachinesDoc {
        name: "MachineId",
        text: "Identifies a machine.",
    },
    MachinesDoc {
        name: "CheckpointId",
        text: "Identifies an immutable machine checkpoint.",
    },
    MachinesDoc {
        name: "Image",
        text: "Describes a managed, custom, or checkpoint image source.",
    },
    MachinesDoc {
        name: "CompatibilityPolicy",
        text: "Declares whether image compatibility is best-effort or required.",
    },
    MachinesDoc {
        name: "ImageQualification",
        text: "Reports image capabilities and compatibility revision.",
    },
    MachinesDoc {
        name: "SuspensionPolicy",
        text: "Declares manual or idle based machine suspension.",
    },
    MachinesDoc {
        name: "ExpirationPolicy",
        text: "Declares whether and when a machine expires.",
    },
    MachinesDoc {
        name: "Budgets",
        text: "Declares spend and concurrency bounds for a machine contract.",
    },
    MachinesDoc {
        name: "MachineContract",
        text: "Captures image, capability, lifecycle, network, and budget policy.",
    },
    MachinesDoc {
        name: "QualifyImageRequest",
        text: "Requests qualification of an image against a protocol version.",
    },
    MachinesDoc {
        name: "CreateMachineRequest",
        text: "Requests admission of a machine with lifecycle and budget policy.",
    },
    MachinesDoc {
        name: "MachineMutationRequest",
        text: "Identifies a machine for a lifecycle mutation.",
    },
    MachinesDoc {
        name: "CheckpointMachineRequest",
        text: "Requests an immutable checkpoint of a machine.",
    },
    MachinesDoc {
        name: "ForkCheckpointRequest",
        text: "Requests fresh machines forked from a checkpoint.",
    },
    MachinesDoc {
        name: "ForkMachineRequest",
        text: "Requests fresh live-fork children from a machine.",
    },
    MachinesDoc {
        name: "SetSuspensionPolicyRequest",
        text: "Requests replacement of a machine suspension policy.",
    },
    MachinesDoc {
        name: "CheckpointMutationRequest",
        text: "Identifies a checkpoint for a mutation.",
    },
    MachinesDoc {
        name: "RecoverRequest",
        text: "Requests recovery of an indeterminate mutation operation.",
    },
    MachinesDoc {
        name: "InspectMachineRequest",
        text: "Identifies a machine to inspect.",
    },
    MachinesDoc {
        name: "InspectCheckpointRequest",
        text: "Identifies a checkpoint to inspect.",
    },
    MachinesDoc {
        name: "ListMachinesRequest",
        text: "Requests a bounded page of machines.",
    },
    MachinesDoc {
        name: "OperationRequest",
        text: "Identifies an operation to inspect, cancel, or watch.",
    },
    MachinesDoc {
        name: "OperationState",
        text: "Reports operation status and its stable identity.",
    },
    MachinesDoc {
        name: "Endpoint",
        text: "Reports a reachable endpoint associated with a machine.",
    },
    MachinesDoc {
        name: "MachineState",
        text: "Reports machine lifecycle state, contract, endpoints, and timestamps.",
    },
    MachinesDoc {
        name: "MachinePage",
        text: "Returns a bounded page of machine states and a continuation token.",
    },
    MachinesDoc {
        name: "CheckpointState",
        text: "Reports checkpoint lineage, contract, and forkability.",
    },
    MachinesDoc {
        name: "MachineAdmission",
        text: "Confirms machine creation and its admitted operation and contract.",
    },
    MachinesDoc {
        name: "CheckpointAdmission",
        text: "Confirms checkpoint creation and its source and operation.",
    },
    MachinesDoc {
        name: "ForkAdmission",
        text: "Confirms checkpoint fork children and the admitted operation.",
    },
    MachinesDoc {
        name: "ForkMachineAdmission",
        text: "Confirms live fork children and declared fork fidelity.",
    },
    MachinesDoc {
        name: "PolicyAdmission",
        text: "Confirms a suspension policy mutation.",
    },
    MachinesDoc {
        name: "MutationAdmission",
        text: "Confirms a machine or checkpoint mutation and resulting identities.",
    },
    MachinesDoc {
        name: "RecoveredAdmission",
        text: "Reports recovered mutation outcomes associated with an operation.",
    },
    MachinesDoc {
        name: "ForkedMachines",
        text: "Returns machine identities created by a checkpoint fork.",
    },
    MachinesDoc {
        name: "ForkedLiveMachines",
        text: "Returns live-fork children and the declared fidelity.",
    },
    MachinesDoc {
        name: "PolicySet",
        text: "Reports the machine and policy after a successful policy mutation.",
    },
    MachinesDoc {
        name: "MutationOutcome",
        text: "Reports which machine lifecycle outcomes were admitted.",
    },
    MachinesDoc {
        name: "OperationPage",
        text: "Returns a bounded page of operation observations.",
    },
    MachinesDoc {
        name: "MachineEvent",
        text: "Reports one ordered machine lifecycle or pressure event.",
    },
    MachinesDoc {
        name: "EventsRequest",
        text: "Requests a bounded machine event page from a sequence cursor.",
    },
    MachinesDoc {
        name: "EventPage",
        text: "Returns machine events and the next sequence cursor.",
    },
    MachinesDoc {
        name: "UsageRequest",
        text: "Requests usage for a machine and half-open time interval.",
    },
    MachinesDoc {
        name: "UsageReceipt",
        text: "Reports machine usage counters and the provider receipt.",
    },
];

pub const MACHINES_FIELD_DOCS: &[MachinesFieldDoc] = &[
    MachinesFieldDoc {
        message: "ProtocolVersion",
        name: "major",
        text: "The major value carried by ProtocolVersion.",
    },
    MachinesFieldDoc {
        message: "ProtocolVersion",
        name: "minor",
        text: "The minor value carried by ProtocolVersion.",
    },
    MachinesFieldDoc {
        message: "OperationId",
        name: "value",
        text: "The value value carried by OperationId.",
    },
    MachinesFieldDoc {
        message: "IdempotencyKey",
        name: "value",
        text: "The value value carried by IdempotencyKey.",
    },
    MachinesFieldDoc {
        message: "MachineId",
        name: "value",
        text: "The value value carried by MachineId.",
    },
    MachinesFieldDoc {
        message: "CheckpointId",
        name: "value",
        text: "The value value carried by CheckpointId.",
    },
    MachinesFieldDoc {
        message: "Image",
        name: "kind",
        text: "The kind value carried by Image.",
    },
    MachinesFieldDoc {
        message: "Image",
        name: "managed_digest",
        text: "The managed digest value carried by Image.",
    },
    MachinesFieldDoc {
        message: "Image",
        name: "custom_digest",
        text: "The custom digest value carried by Image.",
    },
    MachinesFieldDoc {
        message: "Image",
        name: "checkpoint",
        text: "The checkpoint value carried by Image.",
    },
    MachinesFieldDoc {
        message: "CompatibilityPolicy",
        name: "mode",
        text: "The mode value carried by CompatibilityPolicy.",
    },
    MachinesFieldDoc {
        message: "CompatibilityPolicy",
        name: "required",
        text: "The required value carried by CompatibilityPolicy.",
    },
    MachinesFieldDoc {
        message: "ImageQualification",
        name: "image",
        text: "The image value carried by ImageQualification.",
    },
    MachinesFieldDoc {
        message: "ImageQualification",
        name: "capabilities",
        text: "The capabilities value carried by ImageQualification.",
    },
    MachinesFieldDoc {
        message: "ImageQualification",
        name: "compatibility_revision",
        text: "The compatibility revision value carried by ImageQualification.",
    },
    MachinesFieldDoc {
        message: "SuspensionPolicy",
        name: "manual",
        text: "The manual value carried by SuspensionPolicy.",
    },
    MachinesFieldDoc {
        message: "SuspensionPolicy",
        name: "after_idle_ms",
        text: "The after idle ms value carried by SuspensionPolicy.",
    },
    MachinesFieldDoc {
        message: "ExpirationPolicy",
        name: "kind",
        text: "The kind value carried by ExpirationPolicy.",
    },
    MachinesFieldDoc {
        message: "ExpirationPolicy",
        name: "value_ms",
        text: "The value ms value carried by ExpirationPolicy.",
    },
    MachinesFieldDoc {
        message: "Budgets",
        name: "spend_micros",
        text: "The spend micros value carried by Budgets.",
    },
    MachinesFieldDoc {
        message: "Budgets",
        name: "concurrency",
        text: "The concurrency value carried by Budgets.",
    },
    MachinesFieldDoc {
        message: "MachineContract",
        name: "image",
        text: "The image value carried by MachineContract.",
    },
    MachinesFieldDoc {
        message: "MachineContract",
        name: "capabilities",
        text: "The capabilities value carried by MachineContract.",
    },
    MachinesFieldDoc {
        message: "MachineContract",
        name: "compatibility",
        text: "The compatibility value carried by MachineContract.",
    },
    MachinesFieldDoc {
        message: "MachineContract",
        name: "compatibility_revision",
        text: "The compatibility revision value carried by MachineContract.",
    },
    MachinesFieldDoc {
        message: "MachineContract",
        name: "suspension",
        text: "The suspension value carried by MachineContract.",
    },
    MachinesFieldDoc {
        message: "MachineContract",
        name: "expiration",
        text: "The expiration value carried by MachineContract.",
    },
    MachinesFieldDoc {
        message: "MachineContract",
        name: "network_policy_digest",
        text: "The network policy digest value carried by MachineContract.",
    },
    MachinesFieldDoc {
        message: "MachineContract",
        name: "budgets",
        text: "The budgets value carried by MachineContract.",
    },
    MachinesFieldDoc {
        message: "QualifyImageRequest",
        name: "protocol",
        text: "The protocol value carried by QualifyImageRequest.",
    },
    MachinesFieldDoc {
        message: "QualifyImageRequest",
        name: "image",
        text: "The image value carried by QualifyImageRequest.",
    },
    MachinesFieldDoc {
        message: "CreateMachineRequest",
        name: "protocol",
        text: "The protocol value carried by CreateMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CreateMachineRequest",
        name: "idempotency_key",
        text: "The idempotency key value carried by CreateMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CreateMachineRequest",
        name: "image",
        text: "The image value carried by CreateMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CreateMachineRequest",
        name: "compatibility",
        text: "The compatibility value carried by CreateMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CreateMachineRequest",
        name: "suspension",
        text: "The suspension value carried by CreateMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CreateMachineRequest",
        name: "expiration",
        text: "The expiration value carried by CreateMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CreateMachineRequest",
        name: "network_policy_digest",
        text: "The network policy digest value carried by CreateMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CreateMachineRequest",
        name: "budgets",
        text: "The budgets value carried by CreateMachineRequest.",
    },
    MachinesFieldDoc {
        message: "MachineMutationRequest",
        name: "protocol",
        text: "The protocol value carried by MachineMutationRequest.",
    },
    MachinesFieldDoc {
        message: "MachineMutationRequest",
        name: "idempotency_key",
        text: "The idempotency key value carried by MachineMutationRequest.",
    },
    MachinesFieldDoc {
        message: "MachineMutationRequest",
        name: "machine",
        text: "The machine value carried by MachineMutationRequest.",
    },
    MachinesFieldDoc {
        message: "CheckpointMachineRequest",
        name: "protocol",
        text: "The protocol value carried by CheckpointMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CheckpointMachineRequest",
        name: "idempotency_key",
        text: "The idempotency key value carried by CheckpointMachineRequest.",
    },
    MachinesFieldDoc {
        message: "CheckpointMachineRequest",
        name: "machine",
        text: "The machine value carried by CheckpointMachineRequest.",
    },
    MachinesFieldDoc {
        message: "ForkCheckpointRequest",
        name: "protocol",
        text: "The protocol value carried by ForkCheckpointRequest.",
    },
    MachinesFieldDoc {
        message: "ForkCheckpointRequest",
        name: "idempotency_key",
        text: "The idempotency key value carried by ForkCheckpointRequest.",
    },
    MachinesFieldDoc {
        message: "ForkCheckpointRequest",
        name: "checkpoint",
        text: "The checkpoint value carried by ForkCheckpointRequest.",
    },
    MachinesFieldDoc {
        message: "ForkCheckpointRequest",
        name: "count",
        text: "The count value carried by ForkCheckpointRequest.",
    },
    MachinesFieldDoc {
        message: "ForkMachineRequest",
        name: "protocol",
        text: "The protocol value carried by ForkMachineRequest.",
    },
    MachinesFieldDoc {
        message: "ForkMachineRequest",
        name: "idempotency_key",
        text: "The idempotency key value carried by ForkMachineRequest.",
    },
    MachinesFieldDoc {
        message: "ForkMachineRequest",
        name: "machine",
        text: "The machine value carried by ForkMachineRequest.",
    },
    MachinesFieldDoc {
        message: "ForkMachineRequest",
        name: "count",
        text: "The count value carried by ForkMachineRequest.",
    },
    MachinesFieldDoc {
        message: "SetSuspensionPolicyRequest",
        name: "protocol",
        text: "The protocol value carried by SetSuspensionPolicyRequest.",
    },
    MachinesFieldDoc {
        message: "SetSuspensionPolicyRequest",
        name: "idempotency_key",
        text: "The idempotency key value carried by SetSuspensionPolicyRequest.",
    },
    MachinesFieldDoc {
        message: "SetSuspensionPolicyRequest",
        name: "machine",
        text: "The machine value carried by SetSuspensionPolicyRequest.",
    },
    MachinesFieldDoc {
        message: "SetSuspensionPolicyRequest",
        name: "policy",
        text: "The policy value carried by SetSuspensionPolicyRequest.",
    },
    MachinesFieldDoc {
        message: "CheckpointMutationRequest",
        name: "protocol",
        text: "The protocol value carried by CheckpointMutationRequest.",
    },
    MachinesFieldDoc {
        message: "CheckpointMutationRequest",
        name: "idempotency_key",
        text: "The idempotency key value carried by CheckpointMutationRequest.",
    },
    MachinesFieldDoc {
        message: "CheckpointMutationRequest",
        name: "checkpoint",
        text: "The checkpoint value carried by CheckpointMutationRequest.",
    },
    MachinesFieldDoc {
        message: "RecoverRequest",
        name: "protocol",
        text: "The protocol value carried by RecoverRequest.",
    },
    MachinesFieldDoc {
        message: "RecoverRequest",
        name: "idempotency_key",
        text: "The idempotency key value carried by RecoverRequest.",
    },
    MachinesFieldDoc {
        message: "InspectMachineRequest",
        name: "protocol",
        text: "The protocol value carried by InspectMachineRequest.",
    },
    MachinesFieldDoc {
        message: "InspectMachineRequest",
        name: "machine",
        text: "The machine value carried by InspectMachineRequest.",
    },
    MachinesFieldDoc {
        message: "InspectCheckpointRequest",
        name: "protocol",
        text: "The protocol value carried by InspectCheckpointRequest.",
    },
    MachinesFieldDoc {
        message: "InspectCheckpointRequest",
        name: "checkpoint",
        text: "The checkpoint value carried by InspectCheckpointRequest.",
    },
    MachinesFieldDoc {
        message: "ListMachinesRequest",
        name: "protocol",
        text: "The protocol value carried by ListMachinesRequest.",
    },
    MachinesFieldDoc {
        message: "ListMachinesRequest",
        name: "after",
        text: "The after value carried by ListMachinesRequest.",
    },
    MachinesFieldDoc {
        message: "ListMachinesRequest",
        name: "limit",
        text: "The limit value carried by ListMachinesRequest.",
    },
    MachinesFieldDoc {
        message: "OperationRequest",
        name: "protocol",
        text: "The protocol value carried by OperationRequest.",
    },
    MachinesFieldDoc {
        message: "OperationRequest",
        name: "operation",
        text: "The operation value carried by OperationRequest.",
    },
    MachinesFieldDoc {
        message: "OperationState",
        name: "operation",
        text: "The operation value carried by OperationState.",
    },
    MachinesFieldDoc {
        message: "OperationState",
        name: "status",
        text: "The status value carried by OperationState.",
    },
    MachinesFieldDoc {
        message: "Endpoint",
        name: "name",
        text: "The name value carried by Endpoint.",
    },
    MachinesFieldDoc {
        message: "Endpoint",
        name: "uri",
        text: "The uri value carried by Endpoint.",
    },
    MachinesFieldDoc {
        message: "MachineState",
        name: "machine",
        text: "The machine value carried by MachineState.",
    },
    MachinesFieldDoc {
        message: "MachineState",
        name: "status",
        text: "The status value carried by MachineState.",
    },
    MachinesFieldDoc {
        message: "MachineState",
        name: "contract",
        text: "The contract value carried by MachineState.",
    },
    MachinesFieldDoc {
        message: "MachineState",
        name: "endpoints",
        text: "The endpoints value carried by MachineState.",
    },
    MachinesFieldDoc {
        message: "MachineState",
        name: "last_checkpoint",
        text: "The last checkpoint value carried by MachineState.",
    },
    MachinesFieldDoc {
        message: "MachineState",
        name: "created_at_unix_ms",
        text: "The created at unix ms value carried by MachineState.",
    },
    MachinesFieldDoc {
        message: "MachineState",
        name: "changed_at_unix_ms",
        text: "The changed at unix ms value carried by MachineState.",
    },
    MachinesFieldDoc {
        message: "MachinePage",
        name: "machines",
        text: "The machines value carried by MachinePage.",
    },
    MachinesFieldDoc {
        message: "MachinePage",
        name: "next",
        text: "The next value carried by MachinePage.",
    },
    MachinesFieldDoc {
        message: "CheckpointState",
        name: "checkpoint",
        text: "The checkpoint value carried by CheckpointState.",
    },
    MachinesFieldDoc {
        message: "CheckpointState",
        name: "source",
        text: "The source value carried by CheckpointState.",
    },
    MachinesFieldDoc {
        message: "CheckpointState",
        name: "contract",
        text: "The contract value carried by CheckpointState.",
    },
    MachinesFieldDoc {
        message: "CheckpointState",
        name: "forkable",
        text: "The forkable value carried by CheckpointState.",
    },
    MachinesFieldDoc {
        message: "CheckpointState",
        name: "created_at_unix_ms",
        text: "The created at unix ms value carried by CheckpointState.",
    },
    MachinesFieldDoc {
        message: "MachineAdmission",
        name: "machine",
        text: "The machine value carried by MachineAdmission.",
    },
    MachinesFieldDoc {
        message: "MachineAdmission",
        name: "operation",
        text: "The operation value carried by MachineAdmission.",
    },
    MachinesFieldDoc {
        message: "MachineAdmission",
        name: "contract",
        text: "The contract value carried by MachineAdmission.",
    },
    MachinesFieldDoc {
        message: "CheckpointAdmission",
        name: "checkpoint",
        text: "The checkpoint value carried by CheckpointAdmission.",
    },
    MachinesFieldDoc {
        message: "CheckpointAdmission",
        name: "source",
        text: "The source value carried by CheckpointAdmission.",
    },
    MachinesFieldDoc {
        message: "CheckpointAdmission",
        name: "operation",
        text: "The operation value carried by CheckpointAdmission.",
    },
    MachinesFieldDoc {
        message: "CheckpointAdmission",
        name: "contract",
        text: "The contract value carried by CheckpointAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkAdmission",
        name: "checkpoint",
        text: "The checkpoint value carried by ForkAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkAdmission",
        name: "children",
        text: "The children value carried by ForkAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkAdmission",
        name: "operation",
        text: "The operation value carried by ForkAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkAdmission",
        name: "contract",
        text: "The contract value carried by ForkAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkMachineAdmission",
        name: "source",
        text: "The source value carried by ForkMachineAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkMachineAdmission",
        name: "children",
        text: "The children value carried by ForkMachineAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkMachineAdmission",
        name: "operation",
        text: "The operation value carried by ForkMachineAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkMachineAdmission",
        name: "contract",
        text: "The contract value carried by ForkMachineAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkMachineAdmission",
        name: "fidelity",
        text: "The fidelity value carried by ForkMachineAdmission.",
    },
    MachinesFieldDoc {
        message: "PolicyAdmission",
        name: "machine",
        text: "The machine value carried by PolicyAdmission.",
    },
    MachinesFieldDoc {
        message: "PolicyAdmission",
        name: "operation",
        text: "The operation value carried by PolicyAdmission.",
    },
    MachinesFieldDoc {
        message: "PolicyAdmission",
        name: "policy",
        text: "The policy value carried by PolicyAdmission.",
    },
    MachinesFieldDoc {
        message: "MutationAdmission",
        name: "operation",
        text: "The operation value carried by MutationAdmission.",
    },
    MachinesFieldDoc {
        message: "MutationAdmission",
        name: "machine",
        text: "The machine value carried by MutationAdmission.",
    },
    MachinesFieldDoc {
        message: "MutationAdmission",
        name: "checkpoint",
        text: "The checkpoint value carried by MutationAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "operation",
        text: "The operation value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "create",
        text: "The create value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "checkpoint",
        text: "The checkpoint value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "fork",
        text: "The fork value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "suspend",
        text: "The suspend value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "wake",
        text: "The wake value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "destroy_machine",
        text: "The destroy machine value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "set_suspension_policy",
        text: "The set suspension policy value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "destroy_checkpoint",
        text: "The destroy checkpoint value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "RecoveredAdmission",
        name: "fork_machine",
        text: "The fork machine value carried by RecoveredAdmission.",
    },
    MachinesFieldDoc {
        message: "ForkedMachines",
        name: "machines",
        text: "The machines value carried by ForkedMachines.",
    },
    MachinesFieldDoc {
        message: "ForkedLiveMachines",
        name: "source",
        text: "The source value carried by ForkedLiveMachines.",
    },
    MachinesFieldDoc {
        message: "ForkedLiveMachines",
        name: "fidelity",
        text: "The fidelity value carried by ForkedLiveMachines.",
    },
    MachinesFieldDoc {
        message: "ForkedLiveMachines",
        name: "children",
        text: "The children value carried by ForkedLiveMachines.",
    },
    MachinesFieldDoc {
        message: "PolicySet",
        name: "machine",
        text: "The machine value carried by PolicySet.",
    },
    MachinesFieldDoc {
        message: "PolicySet",
        name: "policy",
        text: "The policy value carried by PolicySet.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "created",
        text: "The created value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "checkpointed",
        text: "The checkpointed value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "forked",
        text: "The forked value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "suspended",
        text: "The suspended value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "woken",
        text: "The woken value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "suspension_policy_set",
        text: "The suspension policy set value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "machine_destroyed",
        text: "The machine destroyed value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "checkpoint_destroyed",
        text: "The checkpoint destroyed value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "MutationOutcome",
        name: "machine_forked",
        text: "The machine forked value carried by MutationOutcome.",
    },
    MachinesFieldDoc {
        message: "OperationPage",
        name: "operations",
        text: "The operations value carried by OperationPage.",
    },
    MachinesFieldDoc {
        message: "MachineEvent",
        name: "machine",
        text: "The machine value carried by MachineEvent.",
    },
    MachinesFieldDoc {
        message: "MachineEvent",
        name: "sequence",
        text: "The sequence value carried by MachineEvent.",
    },
    MachinesFieldDoc {
        message: "MachineEvent",
        name: "observed_at_unix_ms",
        text: "The observed at unix ms value carried by MachineEvent.",
    },
    MachinesFieldDoc {
        message: "MachineEvent",
        name: "kind",
        text: "The kind value carried by MachineEvent.",
    },
    MachinesFieldDoc {
        message: "MachineEvent",
        name: "state",
        text: "The state value carried by MachineEvent.",
    },
    MachinesFieldDoc {
        message: "MachineEvent",
        name: "pressure",
        text: "The pressure value carried by MachineEvent.",
    },
    MachinesFieldDoc {
        message: "EventsRequest",
        name: "protocol",
        text: "The protocol value carried by EventsRequest.",
    },
    MachinesFieldDoc {
        message: "EventsRequest",
        name: "machine",
        text: "The machine value carried by EventsRequest.",
    },
    MachinesFieldDoc {
        message: "EventsRequest",
        name: "after_sequence",
        text: "The after sequence value carried by EventsRequest.",
    },
    MachinesFieldDoc {
        message: "EventsRequest",
        name: "limit",
        text: "The limit value carried by EventsRequest.",
    },
    MachinesFieldDoc {
        message: "EventPage",
        name: "events",
        text: "The events value carried by EventPage.",
    },
    MachinesFieldDoc {
        message: "EventPage",
        name: "next_sequence",
        text: "The next sequence value carried by EventPage.",
    },
    MachinesFieldDoc {
        message: "UsageRequest",
        name: "protocol",
        text: "The protocol value carried by UsageRequest.",
    },
    MachinesFieldDoc {
        message: "UsageRequest",
        name: "machine",
        text: "The machine value carried by UsageRequest.",
    },
    MachinesFieldDoc {
        message: "UsageRequest",
        name: "start_unix_ms",
        text: "The start unix ms value carried by UsageRequest.",
    },
    MachinesFieldDoc {
        message: "UsageRequest",
        name: "end_unix_ms",
        text: "The end unix ms value carried by UsageRequest.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "machine",
        text: "The machine value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "start_unix_ms",
        text: "The start unix ms value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "end_unix_ms",
        text: "The end unix ms value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "elastic_cpu_ns",
        text: "The elastic cpu ns value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "dedicated_cpu_ns",
        text: "The dedicated cpu ns value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "private_resident_byte_seconds",
        text: "The private resident byte seconds value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "durable_private_bytes",
        text: "The durable private bytes value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "lineage_receipt_sha256",
        text: "The lineage receipt sha256 value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "egress_bytes",
        text: "The egress bytes value carried by UsageReceipt.",
    },
    MachinesFieldDoc {
        message: "UsageReceipt",
        name: "receipt",
        text: "The receipt value carried by UsageReceipt.",
    },
];

pub const MACHINES_SERVICE_DOCS: &[MachinesDoc] = &[MachinesDoc {
    name: "MachinesService",
    text: "Qualifies images and manages machine, checkpoint, fork, operation, event, and usage lifecycles.",
}];

pub fn machines_message_docs(name: &str) -> &'static str {
    MACHINES_MESSAGE_DOCS
        .iter()
        .find(|doc| doc.name == name)
        .map(|doc| doc.text)
        .unwrap_or_else(|| panic!("missing Machines message docs for {name}"))
}

pub fn machines_field_docs(message: &str, name: &str) -> &'static str {
    MACHINES_FIELD_DOCS
        .iter()
        .find(|doc| doc.message == message && doc.name == name)
        .map(|doc| doc.text)
        .unwrap_or_else(|| panic!("missing Machines field docs for {message}.{name}"))
}

pub fn machines_service_docs(name: &str) -> &'static str {
    MACHINES_SERVICE_DOCS
        .iter()
        .find(|doc| doc.name == name)
        .map(|doc| doc.text)
        .unwrap_or_else(|| panic!("missing Machines service docs for {name}"))
}

pub fn machines_descriptor() -> Vec<u8> {
    MACHINES.descriptor_set().encode_to_vec()
}
pub fn machines_proto() -> std::string::String {
    MACHINES.render_proto()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_documentation_covers_messages_fields_and_services() {
        assert_eq!(MACHINES_MESSAGE_DOCS.len(), MACHINES.messages.len());
        assert_eq!(
            MACHINES_FIELD_DOCS.len(),
            MACHINES
                .messages
                .iter()
                .map(|message| message.fields.len())
                .sum::<usize>()
        );
        for message in MACHINES.messages {
            for field in message.fields {
                assert_eq!(
                    MACHINES_FIELD_DOCS
                        .iter()
                        .filter(|doc| doc.message == message.name && doc.name == field.name)
                        .count(),
                    1,
                    "missing or duplicate docs for {}.{}",
                    message.name,
                    field.name
                );
            }
        }
        assert_eq!(MACHINES_SERVICE_DOCS.len(), MACHINES.services.len());
        assert!(machines_proto().contains("Describes a managed, custom, or checkpoint image"));
        assert!(machines_proto().contains("The idempotency key value carried"));
    }
}
