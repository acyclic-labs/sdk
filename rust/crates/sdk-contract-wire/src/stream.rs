//! Rust-owned Stream v2 contract metadata.
//!
//! The active Stream protobuf remains an archived compatibility fixture. This
//! module is the structured input for descriptor and source generation.

use prost::Message;

use super::{
    Cardinality, ContractSpec, EnumSpec, EnumValueSpec, FieldSpec, FieldType, MessageSpec,
    MethodSpec, OneofSpec, RouteSpec, ServiceSpec,
};

const fn field(
    name: &'static str,
    number: u32,
    cardinality: Cardinality,
    field_type: FieldType,
    json_name: &'static str,
) -> FieldSpec {
    super::field(name, number, cardinality, field_type, json_name)
}
const fn optional(
    name: &'static str,
    number: u32,
    field_type: FieldType,
    json_name: &'static str,
    oneof: &'static str,
) -> FieldSpec {
    super::optional_field(name, number, field_type, json_name, oneof)
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

macro_rules! message {
    ($name:ident, $wire_name:literal, [$($field:expr),* $(,)?], [$($oneof:expr),* $(,)?], [$($range:expr),* $(,)?], [$($reserved:expr),* $(,)?]) => {
        #[allow(non_upper_case_globals)]
        const $name: MessageSpec = MessageSpec {
            name: $wire_name,
            fields: &[$($field),*],
            oneofs: &[$($oneof),*],
            nested_messages: &[],
            is_map_entry: false,
            reserved_ranges: &[$($range),*],
            reserved_names: &[$($reserved),*],
        };
    };
}

message!(
    Record,
    "Record",
    [
        field(
            "sequence",
            1,
            Cardinality::Singular,
            FieldType::Uint64,
            "sequence"
        ),
        field("value", 2, Cardinality::Singular, FieldType::Bytes, "value"),
        field(
            "commit_id",
            3,
            Cardinality::Singular,
            FieldType::Bytes,
            "commitId"
        ),
        field(
            "committed_at_micros",
            4,
            Cardinality::Singular,
            FieldType::Uint64,
            "committedAtMicros"
        ),
    ],
    [],
    [],
    []
);
message!(
    AppendRequest,
    "AppendRequest",
    [
        field("path", 1, Cardinality::Singular, FieldType::String, "path"),
        field(
            "records",
            2,
            Cardinality::Repeated,
            FieldType::Bytes,
            "records"
        ),
        optional("if_tail", 3, FieldType::Uint64, "ifTail", "_if_tail"),
        optional(
            "idempotency_key",
            4,
            FieldType::Bytes,
            "idempotencyKey",
            "_idempotency_key"
        ),
    ],
    [oneof("_if_tail", true), oneof("_idempotency_key", true)],
    [],
    []
);
message!(
    AppendReceipt,
    "AppendReceipt",
    [
        field(
            "start",
            1,
            Cardinality::Singular,
            FieldType::Uint64,
            "start"
        ),
        field("end", 2, Cardinality::Singular, FieldType::Uint64, "end"),
        field("tail", 3, Cardinality::Singular, FieldType::Uint64, "tail"),
        field(
            "commit_id",
            4,
            Cardinality::Singular,
            FieldType::Bytes,
            "commitId"
        ),
    ],
    [],
    [],
    []
);
message!(
    TailConflict,
    "TailConflict",
    [field(
        "actual_tail",
        1,
        Cardinality::Singular,
        FieldType::Uint64,
        "actualTail"
    )],
    [],
    [],
    []
);
message!(
    AppendResponse,
    "AppendResponse",
    [
        oneof_field(
            "committed",
            1,
            FieldType::Message("AppendReceipt"),
            "committed",
            "outcome"
        ),
        oneof_field(
            "conflict",
            2,
            FieldType::Message("TailConflict"),
            "conflict",
            "outcome"
        ),
    ],
    [oneof("outcome", false)],
    [],
    []
);
message!(
    TailRequest,
    "TailRequest",
    [field(
        "path",
        1,
        Cardinality::Singular,
        FieldType::String,
        "path"
    )],
    [],
    [],
    []
);
message!(
    TailResponse,
    "TailResponse",
    [field(
        "tail",
        1,
        Cardinality::Singular,
        FieldType::Uint64,
        "tail"
    )],
    [],
    [(2, 2)],
    ["trim_point"]
);
message!(
    ForkRequest,
    "ForkRequest",
    [
        field(
            "source",
            1,
            Cardinality::Singular,
            FieldType::String,
            "source"
        ),
        field(
            "destination",
            2,
            Cardinality::Singular,
            FieldType::String,
            "destination"
        ),
        optional("at_tail", 3, FieldType::Uint64, "atTail", "_at_tail"),
        optional(
            "idempotency_key",
            4,
            FieldType::Bytes,
            "idempotencyKey",
            "_idempotency_key"
        ),
    ],
    [oneof("_at_tail", true), oneof("_idempotency_key", true)],
    [],
    []
);
message!(
    ForkReceipt,
    "ForkReceipt",
    [
        field(
            "source",
            1,
            Cardinality::Singular,
            FieldType::String,
            "source"
        ),
        field(
            "destination",
            2,
            Cardinality::Singular,
            FieldType::String,
            "destination"
        ),
        field(
            "forked_at",
            3,
            Cardinality::Singular,
            FieldType::Uint64,
            "forkedAt"
        ),
        field("tail", 4, Cardinality::Singular, FieldType::Uint64, "tail"),
        field(
            "commit_id",
            5,
            Cardinality::Singular,
            FieldType::Bytes,
            "commitId"
        ),
    ],
    [],
    [],
    []
);
message!(
    ReadRequest,
    "ReadRequest",
    [
        field("path", 1, Cardinality::Singular, FieldType::String, "path"),
        field("from", 2, Cardinality::Singular, FieldType::Uint64, "from"),
        field(
            "limit",
            3,
            Cardinality::Singular,
            FieldType::Uint32,
            "limit"
        ),
    ],
    [],
    [],
    []
);
message!(
    FollowRequest,
    "FollowRequest",
    [
        field("path", 1, Cardinality::Singular, FieldType::String, "path"),
        field("from", 2, Cardinality::Singular, FieldType::Uint64, "from"),
    ],
    [],
    [],
    []
);
message!(
    ReadResponse,
    "ReadResponse",
    [field(
        "record",
        1,
        Cardinality::Singular,
        FieldType::Message("Record"),
        "record"
    )],
    [],
    [],
    []
);
message!(
    ChildrenRequest,
    "ChildrenRequest",
    [
        optional("parent", 1, FieldType::String, "parent", "_parent"),
        field(
            "limit",
            2,
            Cardinality::Singular,
            FieldType::Uint32,
            "limit"
        ),
    ],
    [oneof("_parent", true)],
    [],
    []
);
message!(
    Child,
    "Child",
    [field(
        "path",
        1,
        Cardinality::Singular,
        FieldType::String,
        "path"
    )],
    [],
    [],
    []
);
message!(
    ChildrenResponse,
    "ChildrenResponse",
    [field(
        "child",
        1,
        Cardinality::Singular,
        FieldType::Message("Child"),
        "child"
    )],
    [],
    [],
    []
);
message!(
    ChildrenPageRequest,
    "ChildrenPageRequest",
    [
        optional("parent", 1, FieldType::String, "parent", "_parent"),
        optional("after", 2, FieldType::String, "after", "_after"),
        optional(
            "hierarchy_version",
            3,
            FieldType::Bytes,
            "hierarchyVersion",
            "_hierarchy_version"
        ),
        field(
            "limit",
            4,
            Cardinality::Singular,
            FieldType::Uint32,
            "limit"
        ),
    ],
    [
        oneof("_parent", true),
        oneof("_after", true),
        oneof("_hierarchy_version", true)
    ],
    [],
    []
);
message!(
    ChildrenPageResponse,
    "ChildrenPageResponse",
    [
        field(
            "hierarchy_version",
            1,
            Cardinality::Singular,
            FieldType::Bytes,
            "hierarchyVersion"
        ),
        field(
            "children",
            2,
            Cardinality::Repeated,
            FieldType::Message("Child"),
            "children"
        ),
        optional(
            "next_after",
            3,
            FieldType::String,
            "nextAfter",
            "_next_after"
        ),
    ],
    [oneof("_next_after", true)],
    [],
    []
);
message!(
    TailCondition,
    "TailCondition",
    [
        field("path", 1, Cardinality::Singular, FieldType::String, "path"),
        field(
            "expected",
            2,
            Cardinality::Singular,
            FieldType::Uint64,
            "expected"
        ),
    ],
    [],
    [],
    []
);
message!(
    AbsentCondition,
    "AbsentCondition",
    [field(
        "path",
        1,
        Cardinality::Singular,
        FieldType::String,
        "path"
    )],
    [],
    [],
    []
);
message!(
    CommitCondition,
    "CommitCondition",
    [
        oneof_field(
            "tail",
            1,
            FieldType::Message("TailCondition"),
            "tail",
            "condition"
        ),
        oneof_field(
            "absent",
            2,
            FieldType::Message("AbsentCondition"),
            "absent",
            "condition"
        ),
    ],
    [oneof("condition", false)],
    [],
    []
);
message!(
    AppendMutation,
    "AppendMutation",
    [
        field("path", 1, Cardinality::Singular, FieldType::String, "path"),
        field(
            "records",
            2,
            Cardinality::Repeated,
            FieldType::Bytes,
            "records"
        ),
    ],
    [],
    [],
    []
);
message!(
    ForkMutation,
    "ForkMutation",
    [
        field(
            "source",
            1,
            Cardinality::Singular,
            FieldType::String,
            "source"
        ),
        field(
            "destination",
            2,
            Cardinality::Singular,
            FieldType::String,
            "destination"
        ),
        field(
            "at_tail",
            3,
            Cardinality::Singular,
            FieldType::Uint64,
            "atTail"
        ),
        field(
            "records",
            4,
            Cardinality::Repeated,
            FieldType::Bytes,
            "records"
        ),
    ],
    [],
    [],
    []
);
message!(
    CommitMutation,
    "CommitMutation",
    [
        oneof_field(
            "append",
            1,
            FieldType::Message("AppendMutation"),
            "append",
            "mutation"
        ),
        oneof_field(
            "fork",
            2,
            FieldType::Message("ForkMutation"),
            "fork",
            "mutation"
        ),
    ],
    [oneof("mutation", false)],
    [(3, 3), (4, 4)],
    ["trim", "delete"]
);
message!(
    CommitRequest,
    "CommitRequest",
    [
        field(
            "conditions",
            1,
            Cardinality::Repeated,
            FieldType::Message("CommitCondition"),
            "conditions"
        ),
        field(
            "mutations",
            2,
            Cardinality::Repeated,
            FieldType::Message("CommitMutation"),
            "mutations"
        ),
        field(
            "idempotency_key",
            3,
            Cardinality::Singular,
            FieldType::Bytes,
            "idempotencyKey"
        ),
        optional(
            "deadline_unix_millis",
            4,
            FieldType::Uint64,
            "deadlineUnixMillis",
            "_deadline_unix_millis"
        ),
    ],
    [oneof("_deadline_unix_millis", true)],
    [],
    []
);
message!(
    CommittedAppend,
    "CommittedAppend",
    [
        field("path", 1, Cardinality::Singular, FieldType::String, "path"),
        field(
            "start",
            2,
            Cardinality::Singular,
            FieldType::Uint64,
            "start"
        ),
        field("end", 3, Cardinality::Singular, FieldType::Uint64, "end"),
        field("tail", 4, Cardinality::Singular, FieldType::Uint64, "tail"),
        field(
            "records",
            5,
            Cardinality::Repeated,
            FieldType::Message("Record"),
            "records"
        ),
    ],
    [],
    [],
    []
);
message!(
    CommittedFork,
    "CommittedFork",
    [
        field(
            "source",
            1,
            Cardinality::Singular,
            FieldType::String,
            "source"
        ),
        field(
            "destination",
            2,
            Cardinality::Singular,
            FieldType::String,
            "destination"
        ),
        field(
            "forked_at",
            3,
            Cardinality::Singular,
            FieldType::Uint64,
            "forkedAt"
        ),
        field("tail", 4, Cardinality::Singular, FieldType::Uint64, "tail"),
        field(
            "records",
            5,
            Cardinality::Repeated,
            FieldType::Message("Record"),
            "records"
        ),
    ],
    [],
    [],
    []
);
message!(
    CommittedMutation,
    "CommittedMutation",
    [
        oneof_field(
            "append",
            1,
            FieldType::Message("CommittedAppend"),
            "append",
            "mutation"
        ),
        oneof_field(
            "fork",
            2,
            FieldType::Message("CommittedFork"),
            "fork",
            "mutation"
        ),
    ],
    [oneof("mutation", false)],
    [(3, 3), (4, 4)],
    ["trim", "delete"]
);
message!(
    CommittedEnvelope,
    "CommittedEnvelope",
    [
        field(
            "commit_id",
            1,
            Cardinality::Singular,
            FieldType::Bytes,
            "commitId"
        ),
        field(
            "mutations",
            2,
            Cardinality::Repeated,
            FieldType::Message("CommittedMutation"),
            "mutations"
        ),
    ],
    [],
    [],
    []
);
message!(
    TailCommitConflict,
    "TailCommitConflict",
    [
        field("path", 1, Cardinality::Singular, FieldType::String, "path"),
        field(
            "expected",
            2,
            Cardinality::Singular,
            FieldType::Uint64,
            "expected"
        ),
        optional("actual", 3, FieldType::Uint64, "actual", "_actual"),
    ],
    [oneof("_actual", true)],
    [],
    []
);
message!(
    ExistsCommitConflict,
    "ExistsCommitConflict",
    [field(
        "path",
        1,
        Cardinality::Singular,
        FieldType::String,
        "path"
    )],
    [],
    [],
    []
);
message!(
    CommitConflict,
    "CommitConflict",
    [
        oneof_field(
            "tail",
            1,
            FieldType::Message("TailCommitConflict"),
            "tail",
            "conflict"
        ),
        oneof_field(
            "exists",
            2,
            FieldType::Message("ExistsCommitConflict"),
            "exists",
            "conflict"
        ),
    ],
    [oneof("conflict", false)],
    [(3, 3)],
    ["retired"]
);
message!(
    CommitConflicts,
    "CommitConflicts",
    [field(
        "conflicts",
        1,
        Cardinality::Repeated,
        FieldType::Message("CommitConflict"),
        "conflicts"
    )],
    [],
    [],
    []
);
message!(
    CommitResponse,
    "CommitResponse",
    [
        oneof_field(
            "committed",
            1,
            FieldType::Message("CommittedEnvelope"),
            "committed",
            "outcome"
        ),
        oneof_field(
            "conflict",
            2,
            FieldType::Message("CommitConflicts"),
            "conflict",
            "outcome"
        ),
    ],
    [oneof("outcome", false)],
    [],
    []
);
message!(
    ReadCommitRequest,
    "ReadCommitRequest",
    [field(
        "commit_id",
        1,
        Cardinality::Singular,
        FieldType::Bytes,
        "commitId"
    )],
    [],
    [],
    []
);
message!(
    InspectIdempotencyRequest,
    "InspectIdempotencyRequest",
    [field(
        "idempotency_key",
        1,
        Cardinality::Singular,
        FieldType::Bytes,
        "idempotencyKey"
    )],
    [],
    [],
    []
);
message!(
    IdempotencyObservation,
    "IdempotencyObservation",
    [
        field(
            "idempotency_key",
            1,
            Cardinality::Singular,
            FieldType::Bytes,
            "idempotencyKey"
        ),
        field(
            "request_digest",
            2,
            Cardinality::Singular,
            FieldType::Bytes,
            "requestDigest"
        ),
        oneof_field(
            "append",
            3,
            FieldType::Message("AppendResponse"),
            "append",
            "outcome"
        ),
        oneof_field(
            "fork",
            4,
            FieldType::Message("ForkReceipt"),
            "fork",
            "outcome"
        ),
        oneof_field(
            "commit",
            7,
            FieldType::Message("CommitResponse"),
            "commit",
            "outcome"
        ),
    ],
    [oneof("outcome", false)],
    [(5, 5), (6, 6)],
    ["trim", "delete"]
);
message!(
    InspectIdempotencyResponse,
    "InspectIdempotencyResponse",
    [optional(
        "observation",
        1,
        FieldType::Message("IdempotencyObservation"),
        "observation",
        "_observation"
    )],
    [oneof("_observation", true)],
    [],
    []
);
message!(
    TokenGrant,
    "TokenGrant",
    [
        field("path", 1, Cardinality::Singular, FieldType::String, "path"),
        optional("subtree", 2, FieldType::Bool, "subtree", "_subtree"),
        field(
            "operations",
            3,
            Cardinality::Repeated,
            FieldType::String,
            "operations"
        ),
    ],
    [oneof("_subtree", true)],
    [],
    []
);
message!(
    CreateTokenRequest,
    "CreateTokenRequest",
    [
        field(
            "expires_in",
            1,
            Cardinality::Singular,
            FieldType::String,
            "expiresIn"
        ),
        field(
            "allow",
            2,
            Cardinality::Repeated,
            FieldType::Message("TokenGrant"),
            "allow"
        ),
    ],
    [],
    [],
    []
);

pub const STREAM_LIMIT: EnumSpec = EnumSpec {
    name: "StreamLimit",
    values: &[
        EnumValueSpec {
            name: "STREAM_LIMIT_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "STREAM_LIMIT_MAX_RECORD_BYTES",
            number: 65536,
        },
        EnumValueSpec {
            name: "STREAM_LIMIT_MAX_ITEMS",
            number: 1024,
        },
        EnumValueSpec {
            name: "STREAM_LIMIT_MAX_COMMAND_BYTES",
            number: 1056768,
        },
        EnumValueSpec {
            name: "STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES",
            number: 256,
        },
        EnumValueSpec {
            name: "STREAM_LIMIT_MAX_PATH_BYTES",
            number: 65535,
        },
    ],
};

pub const STREAM_SERVICE: ServiceSpec = ServiceSpec {
    name: "StreamService",
    methods: &[
        MethodSpec {
            name: "InspectIdempotency",
            input: "InspectIdempotencyRequest",
            output: "InspectIdempotencyResponse",
            docs: "Returns the admitted outcome for an idempotency key, when present.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Append",
            input: "AppendRequest",
            output: "AppendResponse",
            docs: "Appends records to a stream with an optional expected tail.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Tail",
            input: "TailRequest",
            output: "TailResponse",
            docs: "Returns the current tail sequence for a stream.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Fork",
            input: "ForkRequest",
            output: "ForkReceipt",
            docs: "Forks a stream path at an admitted sequence.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Read",
            input: "ReadRequest",
            output: "ReadResponse",
            docs: "Reads records from a stream page by page.",
            client_streaming: false,
            server_streaming: true,
        },
        MethodSpec {
            name: "Follow",
            input: "FollowRequest",
            output: "ReadResponse",
            docs: "Follows a stream and yields records as they become available.",
            client_streaming: false,
            server_streaming: true,
        },
        MethodSpec {
            name: "Children",
            input: "ChildrenRequest",
            output: "ChildrenResponse",
            docs: "Lists child stream paths under a parent path.",
            client_streaming: false,
            server_streaming: true,
        },
        MethodSpec {
            name: "ChildrenPage",
            input: "ChildrenPageRequest",
            output: "ChildrenPageResponse",
            docs: "Returns one ordered page of child stream paths.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Commit",
            input: "CommitRequest",
            output: "CommitResponse",
            docs: "Atomically commits stream mutations after checking conditions.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "ReadCommit",
            input: "ReadCommitRequest",
            output: "CommittedEnvelope",
            docs: "Reads a committed envelope by commit identifier.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};

pub const STREAM: ContractSpec = ContractSpec {
    file_name: "stream/v2/stream.proto",
    syntax: "proto3",
    package: "acyclic.stream.v2",
    dependencies: &[],
    options: super::FileOptionsSpec {
        go_package: "github.com/acyclic-labs/sdk/go/gen/stream/v2;streamv2",
    },
    messages: &[
        Record,
        AppendRequest,
        AppendReceipt,
        TailConflict,
        AppendResponse,
        TailRequest,
        TailResponse,
        ForkRequest,
        ForkReceipt,
        ReadRequest,
        FollowRequest,
        ReadResponse,
        ChildrenRequest,
        Child,
        ChildrenResponse,
        ChildrenPageRequest,
        ChildrenPageResponse,
        TailCondition,
        AbsentCondition,
        CommitCondition,
        AppendMutation,
        ForkMutation,
        CommitMutation,
        CommitRequest,
        CommittedAppend,
        CommittedFork,
        CommittedMutation,
        CommittedEnvelope,
        TailCommitConflict,
        ExistsCommitConflict,
        CommitConflict,
        CommitConflicts,
        CommitResponse,
        ReadCommitRequest,
        InspectIdempotencyRequest,
        IdempotencyObservation,
        InspectIdempotencyResponse,
        TokenGrant,
        CreateTokenRequest,
    ],
    enums: &[STREAM_LIMIT],
    services: &[STREAM_SERVICE],
    routes: STREAM_ROUTES,
};

/// Rust-owned HTTP projection for the hosted Stream transport.
pub const STREAM_ROUTES: &[RouteSpec] = &[
    RouteSpec {
        method: "POST",
        path: "/v1/stream/idempotency/inspect",
        operation_id: "inspectIdempotency",
        rpc: "acyclic.stream.v2.StreamService/InspectIdempotency",
        request: "InspectIdempotencyRequest",
        response: "InspectIdempotencyResponse",
        docs: "Returns the admitted outcome for an idempotency key, when present.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/append",
        operation_id: "append",
        rpc: "acyclic.stream.v2.StreamService/Append",
        request: "AppendRequest",
        response: "AppendResponse",
        docs: "Appends records to a stream with an optional expected tail.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/tail",
        operation_id: "tail",
        rpc: "acyclic.stream.v2.StreamService/Tail",
        request: "TailRequest",
        response: "TailResponse",
        docs: "Returns the current tail sequence for a stream.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/fork",
        operation_id: "fork",
        rpc: "acyclic.stream.v2.StreamService/Fork",
        request: "ForkRequest",
        response: "ForkReceipt",
        docs: "Forks a stream path at an admitted sequence.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/read",
        operation_id: "read",
        rpc: "acyclic.stream.v2.StreamService/Read",
        request: "ReadRequest",
        response: "ReadResponse",
        docs: "Reads records from a stream page by page.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/follow",
        operation_id: "follow",
        rpc: "acyclic.stream.v2.StreamService/Follow",
        request: "FollowRequest",
        response: "ReadResponse",
        docs: "Follows a stream and yields records as they become available.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/children",
        operation_id: "children",
        rpc: "acyclic.stream.v2.StreamService/Children",
        request: "ChildrenRequest",
        response: "ChildrenResponse",
        docs: "Lists child stream paths under a parent path.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/children/page",
        operation_id: "childrenPage",
        rpc: "acyclic.stream.v2.StreamService/ChildrenPage",
        request: "ChildrenPageRequest",
        response: "ChildrenPageResponse",
        docs: "Returns one ordered page of child stream paths.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/commit",
        operation_id: "commit",
        rpc: "acyclic.stream.v2.StreamService/Commit",
        request: "CommitRequest",
        response: "CommitResponse",
        docs: "Atomically commits stream mutations after checking conditions.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/stream/commits/read",
        operation_id: "readCommit",
        rpc: "acyclic.stream.v2.StreamService/ReadCommit",
        request: "ReadCommitRequest",
        response: "CommittedEnvelope",
        docs: "Reads a committed envelope by commit identifier.",
    },
];

pub fn stream_descriptor() -> Vec<u8> {
    STREAM.descriptor_set().encode_to_vec()
}

pub fn stream_proto() -> String {
    STREAM.render_proto()
}
