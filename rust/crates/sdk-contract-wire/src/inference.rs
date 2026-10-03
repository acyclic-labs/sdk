//! Rust-owned Inference v1 contract metadata.
//!
//! The archived protobuf remains a compatibility oracle; normal generation consumes
//! this structured model, including routes and validation metadata.

use super::FieldType::*;
use super::{
    Cardinality, ContractSpec, EnumSpec, EnumValueSpec, FieldSpec, FieldType, MessageSpec,
    MethodSpec, OneofSpec, RouteSpec, ServiceSpec,
};
use acyclic_sdk_contract_options::{OptionTarget, RawOptionValue, RawOptions, option_spec_by_name};

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
    ($name:ident, $wire_name:literal, [$($field:expr),* $(,)?], [$($oneof:expr),* $(,)?]) => {
        #[allow(non_upper_case_globals)]
        const $name: MessageSpec = MessageSpec { name: $wire_name, fields: &[$($field),*], oneofs: &[$($oneof),*], nested_messages: &[], is_map_entry: false, reserved_ranges: &[], reserved_names: &[] };
    };
}

message!(LISTMODELSREQUEST, "ListModelsRequest", [], []);
message!(
    LISTMODELSRESPONSE,
    "ListModelsResponse",
    [field(
        "models",
        1,
        Cardinality::Repeated,
        Message("ModelCapability"),
        "models"
    )],
    []
);
message!(
    MODELCAPABILITY,
    "ModelCapability",
    [
        field("model", 1, Cardinality::Singular, String, "model"),
        field(
            "execution_profile",
            2,
            Cardinality::Singular,
            Bytes,
            "executionProfile"
        ),
        field(
            "maximum_context",
            3,
            Cardinality::Singular,
            Uint64,
            "maximumContext"
        ),
        field(
            "maximum_output",
            4,
            Cardinality::Singular,
            Uint64,
            "maximumOutput"
        ),
        field("features", 5, Cardinality::Repeated, String, "features"),
        field(
            "retention_profiles",
            6,
            Cardinality::Repeated,
            Message("RetentionProfile"),
            "retentionProfiles"
        ),
        field(
            "idle_kv_profiles",
            7,
            Cardinality::Repeated,
            Message("RetentionProfile"),
            "idleKvProfiles"
        )
    ],
    []
);
message!(
    RETENTIONPROFILE,
    "RetentionProfile",
    [
        field("profile", 1, Cardinality::Singular, Bytes, "profile"),
        field(
            "minimum_duration_ms",
            2,
            Cardinality::Singular,
            Uint64,
            "minimumDurationMs"
        ),
        field(
            "maximum_duration_ms",
            3,
            Cardinality::Singular,
            Uint64,
            "maximumDurationMs"
        )
    ],
    []
);
message!(
    RETAINWARMREQUEST,
    "RetainWarmRequest",
    [
        field(
            "identity",
            1,
            Cardinality::Singular,
            Message("RequestIdentity"),
            "identity"
        ),
        field("context", 2, Cardinality::Singular, Bytes, "context"),
        field(
            "latency_profile",
            3,
            Cardinality::Singular,
            Bytes,
            "latencyProfile"
        ),
        field(
            "expires_at_ms",
            4,
            Cardinality::Singular,
            Uint64,
            "expiresAtMs"
        ),
        field(
            "idle_kv",
            5,
            Cardinality::Singular,
            Message("IdleKvPolicy"),
            "idleKv"
        )
    ],
    []
);
message!(
    IDLEKVPOLICY,
    "IdleKvPolicy",
    [
        field("profile", 1, Cardinality::Singular, Bytes, "profile"),
        field(
            "idle_timeout_ms",
            2,
            Cardinality::Singular,
            Uint64,
            "idleTimeoutMs"
        )
    ],
    []
);
message!(
    IDLEKVRETENTION,
    "IdleKvRetention",
    [
        field(
            "policy",
            1,
            Cardinality::Singular,
            Message("IdleKvPolicy"),
            "policy"
        ),
        field(
            "retained_at_ms",
            2,
            Cardinality::Singular,
            Uint64,
            "retainedAtMs"
        ),
        optional(
            "last_used_at_ms",
            3,
            Uint64,
            "lastUsedAtMs",
            "_last_used_at_ms"
        ),
        optional("last_run_id", 4, Bytes, "lastRunId", "_last_run_id")
    ],
    [oneof("_last_used_at_ms", true), oneof("_last_run_id", true)]
);
message!(
    INSPECTWARMREQUEST,
    "InspectWarmRequest",
    [field(
        "commitment",
        1,
        Cardinality::Singular,
        Bytes,
        "commitment"
    )],
    []
);
message!(
    RENEWWARMREQUEST,
    "RenewWarmRequest",
    [
        field(
            "identity",
            1,
            Cardinality::Singular,
            Message("RequestIdentity"),
            "identity"
        ),
        field("commitment", 2, Cardinality::Singular, Bytes, "commitment"),
        field(
            "expires_at_ms",
            3,
            Cardinality::Singular,
            Uint64,
            "expiresAtMs"
        ),
        optional(
            "idle_timeout_ms",
            4,
            Uint64,
            "idleTimeoutMs",
            "_idle_timeout_ms"
        )
    ],
    [oneof("_idle_timeout_ms", true)]
);
message!(
    RELEASEWARMREQUEST,
    "ReleaseWarmRequest",
    [
        field(
            "identity",
            1,
            Cardinality::Singular,
            Message("RequestIdentity"),
            "identity"
        ),
        field("commitment", 2, Cardinality::Singular, Bytes, "commitment")
    ],
    []
);
message!(
    WARMVIEW,
    "WarmView",
    [
        field("commitment", 1, Cardinality::Singular, Bytes, "commitment"),
        field("context", 2, Cardinality::Singular, Bytes, "context"),
        field(
            "model_profile",
            3,
            Cardinality::Singular,
            Bytes,
            "modelProfile"
        ),
        field(
            "latency_profile",
            4,
            Cardinality::Singular,
            Bytes,
            "latencyProfile"
        ),
        field(
            "expires_at_ms",
            5,
            Cardinality::Singular,
            Uint64,
            "expiresAtMs"
        ),
        field(
            "state",
            6,
            Cardinality::Singular,
            Enum("WarmState"),
            "state"
        ),
        field(
            "evidence_digest",
            7,
            Cardinality::Singular,
            Bytes,
            "evidenceDigest"
        ),
        field(
            "admission_receipt_id",
            8,
            Cardinality::Singular,
            Bytes,
            "admissionReceiptId"
        ),
        field("sequence", 9, Cardinality::Singular, Uint64, "sequence"),
        field(
            "idle_kv",
            10,
            Cardinality::Singular,
            Message("IdleKvRetention"),
            "idleKv"
        )
    ],
    []
);
message!(
    EVALUATIONARTIFACT,
    "EvaluationArtifact",
    [
        field("digest", 1, Cardinality::Singular, Bytes, "digest"),
        field("media_type", 2, Cardinality::Singular, String, "mediaType"),
        field(
            "logical_size",
            3,
            Cardinality::Singular,
            Uint64,
            "logicalSize"
        )
    ],
    []
);
message!(
    EVALUATIONCASE,
    "EvaluationCase",
    [
        field("case_id", 1, Cardinality::Singular, Bytes, "caseId"),
        field("input", 2, Cardinality::Singular, Bytes, "input"),
        optional(
            "input_artifact_digest",
            3,
            Bytes,
            "inputArtifactDigest",
            "_input_artifact_digest"
        )
    ],
    [oneof("_input_artifact_digest", true)]
);
message!(
    EVALUATIONSUITE,
    "EvaluationSuite",
    [
        field("identity", 1, Cardinality::Singular, String, "identity"),
        field("digest", 2, Cardinality::Singular, Bytes, "digest"),
        field(
            "cases",
            3,
            Cardinality::Repeated,
            Message("EvaluationCase"),
            "cases"
        )
    ],
    []
);
message!(
    EVALUATIONGRADER,
    "EvaluationGrader",
    [
        field("handle", 1, Cardinality::Singular, Bytes, "handle"),
        field(
            "artifact_digest",
            2,
            Cardinality::Singular,
            Bytes,
            "artifactDigest"
        )
    ],
    []
);
message!(
    EVALUATIONMETRIC,
    "EvaluationMetric",
    [
        field("identity", 1, Cardinality::Singular, String, "identity"),
        field(
            "aggregation",
            2,
            Cardinality::Singular,
            Enum("EvaluationAggregation"),
            "aggregation"
        )
    ],
    []
);
message!(
    EVALUATIONSPEC,
    "EvaluationSpec",
    [
        field(
            "candidates",
            1,
            Cardinality::Repeated,
            Message("EvaluationArtifact"),
            "candidates"
        ),
        field(
            "suite",
            2,
            Cardinality::Singular,
            Message("EvaluationSuite"),
            "suite"
        ),
        field(
            "grader",
            3,
            Cardinality::Singular,
            Message("EvaluationGrader"),
            "grader"
        ),
        field(
            "metrics",
            4,
            Cardinality::Repeated,
            Message("EvaluationMetric"),
            "metrics"
        ),
        field(
            "maximum_case_results",
            5,
            Cardinality::Singular,
            Uint64,
            "maximumCaseResults"
        ),
        field("spec_digest", 6, Cardinality::Singular, Bytes, "specDigest")
    ],
    []
);
message!(
    CREATEEVALUATIONREQUEST,
    "CreateEvaluationRequest",
    [
        field(
            "identity",
            1,
            Cardinality::Singular,
            Message("RequestIdentity"),
            "identity"
        ),
        field(
            "spec",
            2,
            Cardinality::Singular,
            Message("EvaluationSpec"),
            "spec"
        )
    ],
    []
);
message!(
    INSPECTEVALUATIONREQUEST,
    "InspectEvaluationRequest",
    [field(
        "evaluation_id",
        1,
        Cardinality::Singular,
        Bytes,
        "evaluationId"
    )],
    []
);
message!(
    EXACTRATIONAL,
    "ExactRational",
    [
        field("numerator", 1, Cardinality::Singular, Sint64, "numerator"),
        field(
            "denominator",
            2,
            Cardinality::Singular,
            Uint64,
            "denominator"
        )
    ],
    []
);
message!(
    EVALUATIONMETRICVALUE,
    "EvaluationMetricValue",
    [
        field(
            "metric_identity",
            1,
            Cardinality::Singular,
            String,
            "metricIdentity"
        ),
        field(
            "value",
            2,
            Cardinality::Singular,
            Message("ExactRational"),
            "value"
        )
    ],
    []
);
message!(
    EVALUATIONCASERESULT,
    "EvaluationCaseResult",
    [
        field(
            "candidate_digest",
            1,
            Cardinality::Singular,
            Bytes,
            "candidateDigest"
        ),
        field("case_id", 2, Cardinality::Singular, Bytes, "caseId"),
        field(
            "observation",
            3,
            Cardinality::Singular,
            Message("EvaluationGraderObservation"),
            "observation"
        ),
        field(
            "metrics",
            4,
            Cardinality::Repeated,
            Message("EvaluationMetricValue"),
            "metrics"
        ),
        field(
            "outcome",
            5,
            Cardinality::Singular,
            Enum("EvaluationCaseOutcome"),
            "outcome"
        )
    ],
    []
);
message!(
    EVALUATIONGRADEROBSERVATION,
    "EvaluationGraderObservation",
    [
        field(
            "native_output_digest",
            1,
            Cardinality::Singular,
            Bytes,
            "nativeOutputDigest"
        ),
        field(
            "observation_digest",
            2,
            Cardinality::Singular,
            Bytes,
            "observationDigest"
        ),
        field(
            "binding_digest",
            3,
            Cardinality::Singular,
            Bytes,
            "bindingDigest"
        )
    ],
    []
);
message!(
    EVALUATIONAGGREGATE,
    "EvaluationAggregate",
    [
        field(
            "candidate_digest",
            1,
            Cardinality::Singular,
            Bytes,
            "candidateDigest"
        ),
        field(
            "metric_identity",
            2,
            Cardinality::Singular,
            String,
            "metricIdentity"
        ),
        field(
            "aggregation",
            3,
            Cardinality::Singular,
            Enum("EvaluationAggregation"),
            "aggregation"
        ),
        field(
            "value",
            4,
            Cardinality::Singular,
            Message("ExactRational"),
            "value"
        )
    ],
    []
);
message!(
    EVALUATIONRESULT,
    "EvaluationResult",
    [
        field("spec_digest", 1, Cardinality::Singular, Bytes, "specDigest"),
        field(
            "case_results",
            2,
            Cardinality::Repeated,
            Message("EvaluationCaseResult"),
            "caseResults"
        ),
        field(
            "aggregates",
            3,
            Cardinality::Repeated,
            Message("EvaluationAggregate"),
            "aggregates"
        ),
        field(
            "result_digest",
            4,
            Cardinality::Singular,
            Bytes,
            "resultDigest"
        )
    ],
    []
);
message!(
    EVALUATIONVIEW,
    "EvaluationView",
    [
        field(
            "evaluation_id",
            1,
            Cardinality::Singular,
            Bytes,
            "evaluationId"
        ),
        field(
            "spec",
            2,
            Cardinality::Singular,
            Message("EvaluationSpec"),
            "spec"
        ),
        field(
            "state",
            3,
            Cardinality::Singular,
            Enum("EvaluationState"),
            "state"
        ),
        optional(
            "result",
            4,
            Message("EvaluationResult"),
            "result",
            "_result"
        ),
        field("sequence", 5, Cardinality::Singular, Uint64, "sequence")
    ],
    [oneof("_result", true)]
);
message!(EMPTY, "Empty", [], []);
message!(
    REQUESTIDENTITY,
    "RequestIdentity",
    [
        field(
            "client_instance",
            1,
            Cardinality::Singular,
            Bytes,
            "clientInstance"
        ),
        field("request_id", 2, Cardinality::Singular, Bytes, "requestId")
    ],
    []
);
message!(
    ITEM,
    "Item",
    [
        field("id", 1, Cardinality::Singular, Bytes, "id"),
        field("kind", 2, Cardinality::Singular, Enum("ItemKind"), "kind"),
        field("payload", 3, Cardinality::Singular, Bytes, "payload"),
        field("link", 4, Cardinality::Singular, Bytes, "link"),
        field(
            "continuation_profile",
            5,
            Cardinality::Singular,
            Bytes,
            "continuationProfile"
        )
    ],
    []
);
message!(
    CREATECONTEXTREQUEST,
    "CreateContextRequest",
    [
        field(
            "identity",
            1,
            Cardinality::Singular,
            Message("RequestIdentity"),
            "identity"
        ),
        field("model", 2, Cardinality::Singular, String, "model"),
        field("items", 3, Cardinality::Repeated, Message("Item"), "items")
    ],
    []
);
message!(
    INSPECTCONTEXTREQUEST,
    "InspectContextRequest",
    [field(
        "revision",
        1,
        Cardinality::Singular,
        Bytes,
        "revision"
    )],
    []
);
message!(
    INSERT,
    "Insert",
    [
        field("target", 1, Cardinality::Singular, Bytes, "target"),
        field("item", 2, Cardinality::Singular, Message("Item"), "item")
    ],
    []
);
message!(
    REPLACE,
    "Replace",
    [
        field("target", 1, Cardinality::Singular, Bytes, "target"),
        field("payload", 2, Cardinality::Singular, Bytes, "payload")
    ],
    []
);
message!(
    EDIT,
    "Edit",
    [
        oneof_field("append", 1, Message("Item"), "append", "action"),
        oneof_field(
            "insert_before",
            2,
            Message("Insert"),
            "insertBefore",
            "action"
        ),
        oneof_field(
            "insert_after",
            3,
            Message("Insert"),
            "insertAfter",
            "action"
        ),
        oneof_field("replace", 4, Message("Replace"), "replace", "action"),
        oneof_field("delete", 5, Bytes, "delete", "action")
    ],
    [oneof("action", false)]
);
message!(
    EDITS,
    "Edits",
    [field(
        "edits",
        1,
        Cardinality::Repeated,
        Message("Edit"),
        "edits"
    )],
    []
);
message!(
    TRUNCATE,
    "Truncate",
    [optional("through", 1, Bytes, "through", "_through")],
    [oneof("_through", true)]
);
message!(
    COMPACT,
    "Compact",
    [
        field("selected", 1, Cardinality::Repeated, Bytes, "selected"),
        field(
            "replacement",
            2,
            Cardinality::Repeated,
            Message("Item"),
            "replacement"
        )
    ],
    []
);
message!(
    TRANSFER,
    "Transfer",
    [field("model", 1, Cardinality::Singular, String, "model")],
    []
);
message!(
    MUTATECONTEXTREQUEST,
    "MutateContextRequest",
    [
        field(
            "identity",
            1,
            Cardinality::Singular,
            Message("RequestIdentity"),
            "identity"
        ),
        field("source", 2, Cardinality::Singular, Bytes, "source"),
        oneof_field("edit", 3, Message("Edits"), "edit", "action"),
        oneof_field("fork", 4, Message("Empty"), "fork", "action"),
        oneof_field("truncate", 5, Message("Truncate"), "truncate", "action"),
        oneof_field("compact", 6, Message("Compact"), "compact", "action"),
        oneof_field("release", 7, Message("Empty"), "release", "action"),
        oneof_field("transfer", 8, Message("Transfer"), "transfer", "action")
    ],
    [oneof("action", false)]
);
message!(
    MUTATIONRECEIPT,
    "MutationReceipt",
    [
        field("revision", 1, Cardinality::Singular, Bytes, "revision"),
        field(
            "command_digest",
            2,
            Cardinality::Singular,
            Bytes,
            "commandDigest"
        ),
        field("sequence", 3, Cardinality::Singular, Uint64, "sequence"),
        field("retained", 4, Cardinality::Singular, Bool, "retained")
    ],
    []
);
message!(
    CONTEXTVIEW,
    "ContextView",
    [
        field("revision", 1, Cardinality::Singular, Bytes, "revision"),
        optional("parent", 2, Bytes, "parent", "_parent"),
        field("lineage", 3, Cardinality::Singular, Bytes, "lineage"),
        field(
            "execution_profile",
            4,
            Cardinality::Singular,
            Bytes,
            "executionProfile"
        ),
        field(
            "content_digest",
            5,
            Cardinality::Singular,
            Bytes,
            "contentDigest"
        ),
        field("items", 6, Cardinality::Repeated, Message("Item"), "items"),
        field("model", 7, Cardinality::Singular, String, "model"),
        field(
            "provenance",
            8,
            Cardinality::Singular,
            Message("ContextProvenance"),
            "provenance"
        )
    ],
    [oneof("_parent", true)]
);
message!(
    CONTEXTPROVENANCE,
    "ContextProvenance",
    [
        oneof_field("created", 1, Message("Empty"), "created", "origin"),
        oneof_field(
            "derived",
            2,
            Message("ProvenanceSource"),
            "derived",
            "origin"
        ),
        oneof_field("forked", 3, Message("ProvenanceSource"), "forked", "origin"),
        oneof_field(
            "transferred",
            4,
            Message("TransferProvenance"),
            "transferred",
            "origin"
        ),
        oneof_field(
            "generated",
            5,
            Message("GenerationProvenance"),
            "generated",
            "origin"
        ),
        oneof_field(
            "run_input",
            6,
            Message("RunInputProvenance"),
            "runInput",
            "origin"
        )
    ],
    [oneof("origin", false)]
);
message!(
    PROVENANCESOURCE,
    "ProvenanceSource",
    [field("source", 1, Cardinality::Singular, Bytes, "source")],
    []
);
message!(
    TRANSFERPROVENANCE,
    "TransferProvenance",
    [
        field("source", 1, Cardinality::Singular, Bytes, "source"),
        field(
            "reused_compatible_state",
            2,
            Cardinality::Singular,
            Bool,
            "reusedCompatibleState"
        )
    ],
    []
);
message!(
    GENERATIONPROVENANCE,
    "GenerationProvenance",
    [
        field("run_id", 1, Cardinality::Singular, Bytes, "runId"),
        field(
            "terminal_receipt_digest",
            2,
            Cardinality::Singular,
            Bytes,
            "terminalReceiptDigest"
        )
    ],
    []
);
message!(
    RUNINPUTPROVENANCE,
    "RunInputProvenance",
    [
        field("source", 1, Cardinality::Singular, Bytes, "source"),
        field("run_id", 2, Cardinality::Singular, Bytes, "runId"),
        field(
            "maximum_output",
            3,
            Cardinality::Singular,
            Uint64,
            "maximumOutput"
        ),
        optional("seed", 4, Uint64, "seed", "_seed")
    ],
    [oneof("_seed", true)]
);
message!(
    GENERATERUNREQUEST,
    "GenerateRunRequest",
    [
        field(
            "identity",
            1,
            Cardinality::Singular,
            Message("RequestIdentity"),
            "identity"
        ),
        field("context", 2, Cardinality::Singular, Bytes, "context"),
        field("input", 3, Cardinality::Singular, Message("Item"), "input"),
        field(
            "maximum_output",
            4,
            Cardinality::Singular,
            Uint64,
            "maximumOutput"
        ),
        optional("seed", 5, Uint64, "seed", "_seed")
    ],
    [oneof("_seed", true)]
);
message!(
    GENERATERUNRESPONSE,
    "GenerateRunResponse",
    [field(
        "run",
        1,
        Cardinality::Singular,
        Message("RunView"),
        "run"
    )],
    []
);
message!(
    INSPECTRUNREQUEST,
    "InspectRunRequest",
    [field("run_id", 1, Cardinality::Singular, Bytes, "runId")],
    []
);
message!(
    WATCHRUNREQUEST,
    "WatchRunRequest",
    [
        field("run_id", 1, Cardinality::Singular, Bytes, "runId"),
        field(
            "from_sequence",
            2,
            Cardinality::Singular,
            Uint64,
            "fromSequence"
        )
    ],
    []
);
message!(
    LOGICALUSAGE,
    "LogicalUsage",
    [
        field(
            "new_prefill",
            1,
            Cardinality::Singular,
            Uint64,
            "newPrefill"
        ),
        field(
            "generated_output",
            2,
            Cardinality::Singular,
            Uint64,
            "generatedOutput"
        ),
        field(
            "effective_context_reads",
            3,
            Cardinality::Singular,
            Uint64,
            "effectiveContextReads"
        ),
        field(
            "retained_byte_millis",
            4,
            Cardinality::Singular,
            Uint64,
            "retainedByteMillis"
        )
    ],
    []
);
message!(
    USAGERECEIPT,
    "UsageReceipt",
    [
        field("receipt_id", 1, Cardinality::Singular, Bytes, "receiptId"),
        field(
            "model_profile",
            2,
            Cardinality::Singular,
            Bytes,
            "modelProfile"
        ),
        field(
            "meter_revision",
            3,
            Cardinality::Singular,
            Bytes,
            "meterRevision"
        ),
        field(
            "usage",
            4,
            Cardinality::Singular,
            Message("LogicalUsage"),
            "usage"
        ),
        field(
            "rate_card_revision",
            5,
            Cardinality::Singular,
            Bytes,
            "rateCardRevision"
        )
    ],
    []
);
message!(
    RUNRESULT,
    "RunResult",
    [
        field("output", 1, Cardinality::Singular, Bytes, "output"),
        optional("context", 2, Message("ContextView"), "context", "_context"),
        field(
            "terminal",
            3,
            Cardinality::Singular,
            Enum("RunTerminal"),
            "terminal"
        ),
        optional("receipt", 4, Message("UsageReceipt"), "receipt", "_receipt")
    ],
    [oneof("_context", true), oneof("_receipt", true)]
);
message!(
    RUNVIEW,
    "RunView",
    [
        field("run_id", 1, Cardinality::Singular, Bytes, "runId"),
        field("input", 2, Cardinality::Singular, Bytes, "input"),
        field("model", 3, Cardinality::Singular, String, "model"),
        field(
            "last_sequence",
            4,
            Cardinality::Singular,
            Uint64,
            "lastSequence"
        ),
        field(
            "cancellation_requested",
            5,
            Cardinality::Singular,
            Bool,
            "cancellationRequested"
        ),
        optional("result", 6, Message("RunResult"), "result", "_result")
    ],
    [oneof("_result", true)]
);
message!(
    RUNEVENT,
    "RunEvent",
    [
        field("sequence", 1, Cardinality::Singular, Uint64, "sequence"),
        oneof_field("output", 2, Bytes, "output", "event"),
        oneof_field("usage", 3, Message("LogicalUsage"), "usage", "event"),
        oneof_field("terminal", 4, Enum("RunTerminal"), "terminal", "event"),
        oneof_field("progress", 5, Message("RunProgress"), "progress", "event")
    ],
    [oneof("event", false)]
);
message!(
    RUNPROGRESS,
    "RunProgress",
    [field("kind", 1, Cardinality::Singular, String, "kind")],
    []
);

pub const WARMSTATE: EnumSpec = EnumSpec {
    name: "WarmState",
    values: &[
        EnumValueSpec {
            name: "WARM_STATE_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "WARM_STATE_ACTIVE",
            number: 1,
        },
        EnumValueSpec {
            name: "WARM_STATE_EXPIRED",
            number: 2,
        },
        EnumValueSpec {
            name: "WARM_STATE_BREACHED",
            number: 3,
        },
        EnumValueSpec {
            name: "WARM_STATE_RELEASED",
            number: 4,
        },
    ],
};

pub const EVALUATIONAGGREGATION: EnumSpec = EnumSpec {
    name: "EvaluationAggregation",
    values: &[
        EnumValueSpec {
            name: "EVALUATION_AGGREGATION_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "EVALUATION_AGGREGATION_MEAN",
            number: 1,
        },
        EnumValueSpec {
            name: "EVALUATION_AGGREGATION_SUM",
            number: 2,
        },
        EnumValueSpec {
            name: "EVALUATION_AGGREGATION_MINIMUM",
            number: 3,
        },
        EnumValueSpec {
            name: "EVALUATION_AGGREGATION_MAXIMUM",
            number: 4,
        },
    ],
};

pub const EVALUATIONCASEOUTCOME: EnumSpec = EnumSpec {
    name: "EvaluationCaseOutcome",
    values: &[
        EnumValueSpec {
            name: "EVALUATION_CASE_OUTCOME_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "EVALUATION_CASE_OUTCOME_SCORED",
            number: 1,
        },
        EnumValueSpec {
            name: "EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED",
            number: 2,
        },
        EnumValueSpec {
            name: "EVALUATION_CASE_OUTCOME_GRADER_FAILED",
            number: 3,
        },
    ],
};

pub const EVALUATIONSTATE: EnumSpec = EnumSpec {
    name: "EvaluationState",
    values: &[
        EnumValueSpec {
            name: "EVALUATION_STATE_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "EVALUATION_STATE_ADMITTED",
            number: 1,
        },
        EnumValueSpec {
            name: "EVALUATION_STATE_RUNNING",
            number: 2,
        },
        EnumValueSpec {
            name: "EVALUATION_STATE_COMPLETED",
            number: 3,
        },
        EnumValueSpec {
            name: "EVALUATION_STATE_FAILED",
            number: 4,
        },
        EnumValueSpec {
            name: "EVALUATION_STATE_CANCELLED",
            number: 5,
        },
    ],
};

pub const ITEMKIND: EnumSpec = EnumSpec {
    name: "ItemKind",
    values: &[
        EnumValueSpec {
            name: "ITEM_KIND_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "ITEM_KIND_INSTRUCTION",
            number: 1,
        },
        EnumValueSpec {
            name: "ITEM_KIND_SYSTEM",
            number: 2,
        },
        EnumValueSpec {
            name: "ITEM_KIND_DEVELOPER",
            number: 3,
        },
        EnumValueSpec {
            name: "ITEM_KIND_USER",
            number: 4,
        },
        EnumValueSpec {
            name: "ITEM_KIND_ASSISTANT",
            number: 5,
        },
        EnumValueSpec {
            name: "ITEM_KIND_TOOL_DEFINITION",
            number: 6,
        },
        EnumValueSpec {
            name: "ITEM_KIND_TOOL_CALL",
            number: 7,
        },
        EnumValueSpec {
            name: "ITEM_KIND_TOOL_RESULT",
            number: 8,
        },
        EnumValueSpec {
            name: "ITEM_KIND_IMAGE",
            number: 9,
        },
        EnumValueSpec {
            name: "ITEM_KIND_AUDIO",
            number: 10,
        },
        EnumValueSpec {
            name: "ITEM_KIND_FILE",
            number: 11,
        },
        EnumValueSpec {
            name: "ITEM_KIND_CONTINUATION",
            number: 12,
        },
    ],
};

pub const RUNTERMINAL: EnumSpec = EnumSpec {
    name: "RunTerminal",
    values: &[
        EnumValueSpec {
            name: "RUN_TERMINAL_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "RUN_TERMINAL_COMPLETED",
            number: 1,
        },
        EnumValueSpec {
            name: "RUN_TERMINAL_OUTPUT_LIMITED",
            number: 2,
        },
        EnumValueSpec {
            name: "RUN_TERMINAL_TOOL_CALL",
            number: 3,
        },
        EnumValueSpec {
            name: "RUN_TERMINAL_REFUSAL",
            number: 4,
        },
        EnumValueSpec {
            name: "RUN_TERMINAL_CANCELLED",
            number: 5,
        },
        EnumValueSpec {
            name: "RUN_TERMINAL_FAILED",
            number: 6,
        },
        EnumValueSpec {
            name: "RUN_TERMINAL_INDETERMINATE",
            number: 7,
        },
    ],
};

pub const MODELSSERVICE: ServiceSpec = ServiceSpec {
    name: "ModelsService",
    methods: &[MethodSpec {
        name: "List",
        input: "ListModelsRequest",
        output: "ListModelsResponse",
        docs: "Lists model capabilities and retention profiles.",
        client_streaming: false,
        server_streaming: false,
    }],
};

pub const CONTEXTSSERVICE: ServiceSpec = ServiceSpec {
    name: "ContextsService",
    methods: &[
        MethodSpec {
            name: "Create",
            input: "CreateContextRequest",
            output: "MutationReceipt",
            docs: "Creates an immutable context revision.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Inspect",
            input: "InspectContextRequest",
            output: "ContextView",
            docs: "Reads an immutable context revision by digest.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Mutate",
            input: "MutateContextRequest",
            output: "MutationReceipt",
            docs: "Admits one immutable context mutation or release.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};

pub const WARMCONTEXTSSERVICE: ServiceSpec = ServiceSpec {
    name: "WarmContextsService",
    methods: &[
        MethodSpec {
            name: "Retain",
            input: "RetainWarmRequest",
            output: "WarmView",
            docs: "Admits an explicit warm-retention commitment.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Inspect",
            input: "InspectWarmRequest",
            output: "WarmView",
            docs: "Reads the current warm-retention commitment.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Renew",
            input: "RenewWarmRequest",
            output: "WarmView",
            docs: "Extends a warm-retention commitment.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Release",
            input: "ReleaseWarmRequest",
            output: "WarmView",
            docs: "Releases a warm-retention commitment.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};

pub const RUNSSERVICE: ServiceSpec = ServiceSpec {
    name: "RunsService",
    methods: &[
        MethodSpec {
            name: "Generate",
            input: "GenerateRunRequest",
            output: "GenerateRunResponse",
            docs: "Admits a recoverable generation run.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Inspect",
            input: "InspectRunRequest",
            output: "RunView",
            docs: "Reads the current generation run view.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Watch",
            input: "WatchRunRequest",
            output: "RunEvent",
            docs: "Streams ordered run events from a sequence cursor.",
            client_streaming: false,
            server_streaming: true,
        },
        MethodSpec {
            name: "Cancel",
            input: "InspectRunRequest",
            output: "RunView",
            docs: "Requests cancellation of a generation run.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};

pub const EVALUATIONSSERVICE: ServiceSpec = ServiceSpec {
    name: "EvaluationsService",
    methods: &[
        MethodSpec {
            name: "Create",
            input: "CreateEvaluationRequest",
            output: "EvaluationView",
            docs: "Admits an immutable evaluation specification.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "Inspect",
            input: "InspectEvaluationRequest",
            output: "EvaluationView",
            docs: "Reads an immutable evaluation view.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};

pub const INFERENCE_ROUTES: &[RouteSpec] = &[
    RouteSpec {
        method: "POST",
        path: "/v1/inference/models/list",
        operation_id: "modelsList",
        rpc: "inference.customer.v1.ModelsService/List",
        request: "ListModelsRequest",
        response: "ListModelsResponse",
        docs: "Lists model capabilities and retention profiles.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/contexts/create",
        operation_id: "contextsCreate",
        rpc: "inference.customer.v1.ContextsService/Create",
        request: "CreateContextRequest",
        response: "MutationReceipt",
        docs: "Creates an immutable context revision.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/contexts/inspect",
        operation_id: "contextsInspect",
        rpc: "inference.customer.v1.ContextsService/Inspect",
        request: "InspectContextRequest",
        response: "ContextView",
        docs: "Reads an immutable context revision by digest.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/contexts/mutate",
        operation_id: "contextsMutate",
        rpc: "inference.customer.v1.ContextsService/Mutate",
        request: "MutateContextRequest",
        response: "MutationReceipt",
        docs: "Admits one immutable context mutation or release.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/warm/retain",
        operation_id: "warmContextsRetain",
        rpc: "inference.customer.v1.WarmContextsService/Retain",
        request: "RetainWarmRequest",
        response: "WarmView",
        docs: "Admits an explicit warm-retention commitment.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/warm/inspect",
        operation_id: "warmContextsInspect",
        rpc: "inference.customer.v1.WarmContextsService/Inspect",
        request: "InspectWarmRequest",
        response: "WarmView",
        docs: "Reads the current warm-retention commitment.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/warm/renew",
        operation_id: "warmContextsRenew",
        rpc: "inference.customer.v1.WarmContextsService/Renew",
        request: "RenewWarmRequest",
        response: "WarmView",
        docs: "Extends a warm-retention commitment.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/warm/release",
        operation_id: "warmContextsRelease",
        rpc: "inference.customer.v1.WarmContextsService/Release",
        request: "ReleaseWarmRequest",
        response: "WarmView",
        docs: "Releases a warm-retention commitment.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/runs/generate",
        operation_id: "runsGenerate",
        rpc: "inference.customer.v1.RunsService/Generate",
        request: "GenerateRunRequest",
        response: "GenerateRunResponse",
        docs: "Admits a recoverable generation run.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/runs/inspect",
        operation_id: "runsInspect",
        rpc: "inference.customer.v1.RunsService/Inspect",
        request: "InspectRunRequest",
        response: "RunView",
        docs: "Reads the current generation run view.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/runs/watch",
        operation_id: "runsWatch",
        rpc: "inference.customer.v1.RunsService/Watch",
        request: "WatchRunRequest",
        response: "RunEvent",
        docs: "Streams ordered run events from a sequence cursor.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/runs/cancel",
        operation_id: "runsCancel",
        rpc: "inference.customer.v1.RunsService/Cancel",
        request: "InspectRunRequest",
        response: "RunView",
        docs: "Requests cancellation of a generation run.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/evaluations/create",
        operation_id: "evaluationsCreate",
        rpc: "inference.customer.v1.EvaluationsService/Create",
        request: "CreateEvaluationRequest",
        response: "EvaluationView",
        docs: "Admits an immutable evaluation specification.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/inference/evaluations/inspect",
        operation_id: "evaluationsInspect",
        rpc: "inference.customer.v1.EvaluationsService/Inspect",
        request: "InspectEvaluationRequest",
        response: "EvaluationView",
        docs: "Reads an immutable evaluation view.",
    },
];

pub const INFERENCE: ContractSpec = ContractSpec {
    file_name: "inference/v1/inference.proto",
    syntax: "proto3",
    package: "inference.customer.v1",
    dependencies: &["validation/v1/options.proto"],
    options: super::FileOptionsSpec {
        go_package: "github.com/acyclic-labs/sdk/go/gen/inference/v1;inferencev1",
    },
    messages: &[
        LISTMODELSREQUEST,
        LISTMODELSRESPONSE,
        MODELCAPABILITY,
        RETENTIONPROFILE,
        RETAINWARMREQUEST,
        IDLEKVPOLICY,
        IDLEKVRETENTION,
        INSPECTWARMREQUEST,
        RENEWWARMREQUEST,
        RELEASEWARMREQUEST,
        WARMVIEW,
        EVALUATIONARTIFACT,
        EVALUATIONCASE,
        EVALUATIONSUITE,
        EVALUATIONGRADER,
        EVALUATIONMETRIC,
        EVALUATIONSPEC,
        CREATEEVALUATIONREQUEST,
        INSPECTEVALUATIONREQUEST,
        EXACTRATIONAL,
        EVALUATIONMETRICVALUE,
        EVALUATIONCASERESULT,
        EVALUATIONGRADEROBSERVATION,
        EVALUATIONAGGREGATE,
        EVALUATIONRESULT,
        EVALUATIONVIEW,
        REQUESTIDENTITY,
        ITEM,
        CREATECONTEXTREQUEST,
        INSPECTCONTEXTREQUEST,
        EMPTY,
        INSERT,
        REPLACE,
        EDIT,
        EDITS,
        TRUNCATE,
        COMPACT,
        TRANSFER,
        MUTATECONTEXTREQUEST,
        MUTATIONRECEIPT,
        CONTEXTVIEW,
        CONTEXTPROVENANCE,
        PROVENANCESOURCE,
        TRANSFERPROVENANCE,
        GENERATIONPROVENANCE,
        RUNINPUTPROVENANCE,
        GENERATERUNREQUEST,
        GENERATERUNRESPONSE,
        INSPECTRUNREQUEST,
        WATCHRUNREQUEST,
        LOGICALUSAGE,
        USAGERECEIPT,
        RUNRESULT,
        RUNVIEW,
        RUNEVENT,
        RUNPROGRESS,
    ],
    enums: &[
        WARMSTATE,
        EVALUATIONAGGREGATION,
        EVALUATIONCASEOUTCOME,
        EVALUATIONSTATE,
        ITEMKIND,
        RUNTERMINAL,
    ],
    services: &[
        MODELSSERVICE,
        CONTEXTSSERVICE,
        WARMCONTEXTSSERVICE,
        RUNSSERVICE,
        EVALUATIONSSERVICE,
    ],
    routes: INFERENCE_ROUTES,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationValue {
    Bool(bool),
    U32(u32),
    U64(u64),
    Text(&'static str),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationOption {
    pub subject: &'static str,
    pub name: &'static str,
    pub value: ValidationValue,
}
pub const INFERENCE_OPTIONS: &[ValidationOption] = &[
    ValidationOption {
        subject: "ListModelsResponse.models",
        name: "min_items",
        value: ValidationValue::U32(1),
    },
    ValidationOption {
        subject: "ListModelsResponse.models",
        name: "max_items",
        value: ValidationValue::U32(4096),
    },
    ValidationOption {
        subject: "ModelCapability.model",
        name: "nonempty_max_bytes",
        value: ValidationValue::U32(256),
    },
    ValidationOption {
        subject: "ModelCapability.execution_profile",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "ModelCapability.maximum_context",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "ModelCapability.maximum_output",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "ModelCapability.features",
        name: "min_items",
        value: ValidationValue::U32(1),
    },
    ValidationOption {
        subject: "ModelCapability.features",
        name: "max_items",
        value: ValidationValue::U32(64),
    },
    ValidationOption {
        subject: "ModelCapability.features",
        name: "nonempty_max_item_bytes",
        value: ValidationValue::U32(64),
    },
    ValidationOption {
        subject: "ModelCapability.retention_profiles",
        name: "max_items",
        value: ValidationValue::U32(64),
    },
    ValidationOption {
        subject: "ModelCapability.idle_kv_profiles",
        name: "max_items",
        value: ValidationValue::U32(64),
    },
    ValidationOption {
        subject: "RetentionProfile.profile",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "RetentionProfile.minimum_duration_ms",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RetainWarmRequest.identity",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RetainWarmRequest.context",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "IdleKvPolicy.profile",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "IdleKvPolicy.idle_timeout_ms",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "IdleKvRetention.policy",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "IdleKvRetention.retained_at_ms",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "IdleKvRetention.last_used_at_ms",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "IdleKvRetention.last_run_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "InspectWarmRequest.commitment",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "RenewWarmRequest.identity",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RenewWarmRequest.commitment",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "RenewWarmRequest.idle_timeout_ms",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "ReleaseWarmRequest.identity",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "ReleaseWarmRequest.commitment",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "WarmView.commitment",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "WarmView.context",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "WarmView.model_profile",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "WarmView.expires_at_ms",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "WarmView.state",
        name: "known_nonzero_enum",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "WarmView.evidence_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "WarmView.admission_receipt_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "WarmView.sequence",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationArtifact.digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationArtifact.media_type",
        name: "nonempty_max_bytes",
        value: ValidationValue::U32(256),
    },
    ValidationOption {
        subject: "EvaluationArtifact.logical_size",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationCase.case_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "EvaluationCase.input_artifact_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationSuite.identity",
        name: "nonempty_max_bytes",
        value: ValidationValue::U32(256),
    },
    ValidationOption {
        subject: "EvaluationSuite.digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationSuite.cases",
        name: "min_items",
        value: ValidationValue::U32(1),
    },
    ValidationOption {
        subject: "EvaluationSuite.cases",
        name: "max_items",
        value: ValidationValue::U32(4096),
    },
    ValidationOption {
        subject: "EvaluationGrader.handle",
        name: "nonempty_max_bytes",
        value: ValidationValue::U32(4096),
    },
    ValidationOption {
        subject: "EvaluationGrader.artifact_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationMetric.identity",
        name: "nonempty_max_bytes",
        value: ValidationValue::U32(256),
    },
    ValidationOption {
        subject: "EvaluationMetric.aggregation",
        name: "known_nonzero_enum",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationSpec.candidates",
        name: "min_items",
        value: ValidationValue::U32(1),
    },
    ValidationOption {
        subject: "EvaluationSpec.candidates",
        name: "max_items",
        value: ValidationValue::U32(256),
    },
    ValidationOption {
        subject: "EvaluationSpec.suite",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationSpec.grader",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationSpec.metrics",
        name: "min_items",
        value: ValidationValue::U32(1),
    },
    ValidationOption {
        subject: "EvaluationSpec.metrics",
        name: "max_items",
        value: ValidationValue::U32(64),
    },
    ValidationOption {
        subject: "EvaluationSpec.maximum_case_results",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationSpec.maximum_case_results",
        name: "max_uint64",
        value: ValidationValue::U64(65536),
    },
    ValidationOption {
        subject: "EvaluationSpec.spec_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "CreateEvaluationRequest.identity",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "CreateEvaluationRequest.spec",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "InspectEvaluationRequest.evaluation_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "ExactRational.denominator",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationMetricValue.value",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationCaseResult.candidate_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationCaseResult.case_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "EvaluationCaseResult.observation",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationCaseResult.outcome",
        name: "known_nonzero_enum",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationGraderObservation.native_output_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationGraderObservation.observation_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationGraderObservation.binding_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationAggregate.candidate_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationAggregate.aggregation",
        name: "known_nonzero_enum",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationAggregate.value",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationResult.spec_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationResult.result_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "EvaluationView.evaluation_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "EvaluationView.spec",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationView.state",
        name: "known_nonzero_enum",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "EvaluationView.sequence",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RequestIdentity.client_instance",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "RequestIdentity.request_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "CreateContextRequest.identity",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "InspectContextRequest.revision",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "MutateContextRequest.identity",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "MutateContextRequest.source",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "MutateContextRequest.action",
        name: "required_oneof",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "MutationReceipt.revision",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "MutationReceipt.command_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "MutationReceipt.sequence",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "ContextView.revision",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "ContextView.parent",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "ContextView.lineage",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "ContextView.execution_profile",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "ContextView.content_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "ContextView.model",
        name: "nonempty_max_bytes",
        value: ValidationValue::U32(256),
    },
    ValidationOption {
        subject: "ContextView.provenance",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "ContextProvenance.origin",
        name: "required_oneof",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "ProvenanceSource.source",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "TransferProvenance.source",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "GenerationProvenance.run_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "GenerationProvenance.terminal_receipt_digest",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "RunInputProvenance.source",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "RunInputProvenance.run_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "RunInputProvenance.maximum_output",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "GenerateRunRequest.identity",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "GenerateRunRequest.context",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "GenerateRunRequest.input",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "GenerateRunRequest.maximum_output",
        name: "positive_uint64",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "GenerateRunResponse.run",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "InspectRunRequest.run_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "WatchRunRequest.run_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "UsageReceipt.receipt_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "UsageReceipt.model_profile",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "UsageReceipt.meter_revision",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "UsageReceipt.usage",
        name: "required_message",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "UsageReceipt.rate_card_revision",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "RunResult.terminal",
        name: "known_nonzero_enum",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RunView.run_id",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(16),
    },
    ValidationOption {
        subject: "RunView.input",
        name: "nonzero_fixed_bytes",
        value: ValidationValue::U32(32),
    },
    ValidationOption {
        subject: "RunView.model",
        name: "nonempty_max_bytes",
        value: ValidationValue::U32(256),
    },
    ValidationOption {
        subject: "RunEvent.terminal",
        name: "known_nonzero_enum",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RunEvent.event",
        name: "required_oneof",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RunTerminal.RUN_TERMINAL_CANCELLED",
        name: "partial_terminal",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RunTerminal.RUN_TERMINAL_FAILED",
        name: "partial_terminal",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "RunTerminal.RUN_TERMINAL_INDETERMINATE",
        name: "partial_terminal",
        value: ValidationValue::Bool(true),
    },
    ValidationOption {
        subject: "ModelsService.List",
        name: "http_path",
        value: ValidationValue::Text("models/list"),
    },
    ValidationOption {
        subject: "ContextsService.Create",
        name: "http_path",
        value: ValidationValue::Text("contexts/create"),
    },
    ValidationOption {
        subject: "ContextsService.Inspect",
        name: "http_path",
        value: ValidationValue::Text("contexts/inspect"),
    },
    ValidationOption {
        subject: "ContextsService.Mutate",
        name: "http_path",
        value: ValidationValue::Text("contexts/mutate"),
    },
    ValidationOption {
        subject: "WarmContextsService.Retain",
        name: "http_path",
        value: ValidationValue::Text("warm/retain"),
    },
    ValidationOption {
        subject: "WarmContextsService.Inspect",
        name: "http_path",
        value: ValidationValue::Text("warm/inspect"),
    },
    ValidationOption {
        subject: "WarmContextsService.Renew",
        name: "http_path",
        value: ValidationValue::Text("warm/renew"),
    },
    ValidationOption {
        subject: "WarmContextsService.Release",
        name: "http_path",
        value: ValidationValue::Text("warm/release"),
    },
    ValidationOption {
        subject: "RunsService.Generate",
        name: "http_path",
        value: ValidationValue::Text("runs/generate"),
    },
    ValidationOption {
        subject: "RunsService.Inspect",
        name: "http_path",
        value: ValidationValue::Text("runs/inspect"),
    },
    ValidationOption {
        subject: "RunsService.Watch",
        name: "http_path",
        value: ValidationValue::Text("runs/watch"),
    },
    ValidationOption {
        subject: "RunsService.Cancel",
        name: "http_path",
        value: ValidationValue::Text("runs/cancel"),
    },
    ValidationOption {
        subject: "EvaluationsService.Create",
        name: "http_path",
        value: ValidationValue::Text("evaluations/create"),
    },
    ValidationOption {
        subject: "EvaluationsService.Inspect",
        name: "http_path",
        value: ValidationValue::Text("evaluations/inspect"),
    },
];

/// Encode the options owned by the Inference model for one descriptor subject.
///
/// The model's validation table remains convenient for documentation and
/// projections, while this conversion feeds the shared Rust-owned raw wire
/// path used by descriptor exporters. Unknown or duplicate assignments cannot
/// be silently emitted because `RawOptions::try_push` validates each identity.
pub fn inference_raw_options(subject: &str) -> RawOptions {
    let mut options = RawOptions::new();
    for assignment in INFERENCE_OPTIONS
        .iter()
        .filter(|assignment| assignment.subject == subject)
    {
        let spec = OptionTarget::all()
            .iter()
            .find_map(|target| option_spec_by_name(*target, assignment.name))
            .unwrap_or_else(|| panic!("unknown inference option {}", assignment.name));
        let value = match assignment.value {
            ValidationValue::Bool(value) => RawOptionValue::Bool(value),
            ValidationValue::U32(value) => RawOptionValue::U32(value),
            ValidationValue::U64(value) => RawOptionValue::U64(value),
            ValidationValue::Text(value) => RawOptionValue::String(value.to_owned()),
        };
        options.try_push(spec, value).unwrap_or_else(|error| {
            panic!("invalid inference option {}: {error}", assignment.name)
        });
    }
    options
}

/// Documentation owned by the Inference model and consumed by contract exporters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InferenceDoc {
    pub name: &'static str,
    pub text: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InferenceFieldDoc {
    pub message: &'static str,
    pub name: &'static str,
    pub text: &'static str,
}

pub const INFERENCE_MESSAGE_DOCS: &[InferenceDoc] = &[
    InferenceDoc {
        name: "ListModelsRequest",
        text: "Requests the model capabilities visible to the caller.",
    },
    InferenceDoc {
        name: "ListModelsResponse",
        text: "Returns model capabilities and their retention profiles.",
    },
    InferenceDoc {
        name: "ModelCapability",
        text: "Describes a model profile, limits, features, and warm-retention options.",
    },
    InferenceDoc {
        name: "RetentionProfile",
        text: "Describes a bounded duration profile for retained context state.",
    },
    InferenceDoc {
        name: "RetainWarmRequest",
        text: "Requests a warm retention commitment for an existing context.",
    },
    InferenceDoc {
        name: "IdleKvPolicy",
        text: "Describes the idle KV retention profile attached to a warm context.",
    },
    InferenceDoc {
        name: "IdleKvRetention",
        text: "Records idle KV retention policy and the last observed use.",
    },
    InferenceDoc {
        name: "InspectWarmRequest",
        text: "Identifies a warm retention commitment for inspection.",
    },
    InferenceDoc {
        name: "RenewWarmRequest",
        text: "Extends a warm retention commitment and its idle policy.",
    },
    InferenceDoc {
        name: "ReleaseWarmRequest",
        text: "Releases a warm retention commitment.",
    },
    InferenceDoc {
        name: "WarmView",
        text: "Reports the admitted warm commitment and its current state.",
    },
    InferenceDoc {
        name: "EvaluationArtifact",
        text: "Identifies an evaluation input or output artifact by digest and size.",
    },
    InferenceDoc {
        name: "EvaluationCase",
        text: "Defines one evaluation input case and its optional artifact reference.",
    },
    InferenceDoc {
        name: "EvaluationSuite",
        text: "Groups evaluation cases under an immutable suite identity.",
    },
    InferenceDoc {
        name: "EvaluationGrader",
        text: "Identifies the grader used to observe an evaluation case.",
    },
    InferenceDoc {
        name: "EvaluationMetric",
        text: "Defines a named aggregation applied to evaluation observations.",
    },
    InferenceDoc {
        name: "EvaluationSpec",
        text: "Defines candidates, cases, grader, metrics, limits, and specification digest.",
    },
    InferenceDoc {
        name: "CreateEvaluationRequest",
        text: "Requests admission of an immutable evaluation specification.",
    },
    InferenceDoc {
        name: "InspectEvaluationRequest",
        text: "Identifies an evaluation to inspect.",
    },
    InferenceDoc {
        name: "ExactRational",
        text: "Represents a rational value without floating-point rounding.",
    },
    InferenceDoc {
        name: "EvaluationMetricValue",
        text: "Associates a metric identity with an exact value.",
    },
    InferenceDoc {
        name: "EvaluationCaseResult",
        text: "Records one candidate result, observation, metrics, and outcome.",
    },
    InferenceDoc {
        name: "EvaluationGraderObservation",
        text: "Records grader output and binding digests for a case.",
    },
    InferenceDoc {
        name: "EvaluationAggregate",
        text: "Records an aggregate metric value for one candidate.",
    },
    InferenceDoc {
        name: "EvaluationResult",
        text: "Contains case results, aggregates, and the immutable result digest.",
    },
    InferenceDoc {
        name: "EvaluationView",
        text: "Reports an admitted evaluation, its state, result, and sequence.",
    },
    InferenceDoc {
        name: "Empty",
        text: "An empty context mutation operation.",
    },
    InferenceDoc {
        name: "RequestIdentity",
        text: "Binds a mutation or run request to a caller-supplied identity.",
    },
    InferenceDoc {
        name: "Item",
        text: "Represents one typed item in an immutable context revision.",
    },
    InferenceDoc {
        name: "CreateContextRequest",
        text: "Requests creation of an immutable context revision.",
    },
    InferenceDoc {
        name: "InspectContextRequest",
        text: "Identifies a context revision to inspect.",
    },
    InferenceDoc {
        name: "Insert",
        text: "Inserts an item at a target position.",
    },
    InferenceDoc {
        name: "Replace",
        text: "Replaces the payload at a target position.",
    },
    InferenceDoc {
        name: "Edit",
        text: "Describes one append, insert, replace, or delete edit.",
    },
    InferenceDoc {
        name: "Edits",
        text: "Groups ordered edits into one context mutation.",
    },
    InferenceDoc {
        name: "Truncate",
        text: "Truncates a context through a selected position.",
    },
    InferenceDoc {
        name: "Compact",
        text: "Compacts selected context content into a replacement item.",
    },
    InferenceDoc {
        name: "Transfer",
        text: "Transfers a context to a selected model profile.",
    },
    InferenceDoc {
        name: "MutateContextRequest",
        text: "Requests one immutable context mutation or release operation.",
    },
    InferenceDoc {
        name: "MutationReceipt",
        text: "Confirms an admitted context revision and its command digest.",
    },
    InferenceDoc {
        name: "ContextView",
        text: "Reports an immutable context revision and its provenance.",
    },
    InferenceDoc {
        name: "ContextProvenance",
        text: "Records how a context revision was created or derived.",
    },
    InferenceDoc {
        name: "ProvenanceSource",
        text: "Identifies the source revision for a derived context.",
    },
    InferenceDoc {
        name: "TransferProvenance",
        text: "Records a model transfer and compatible-state reuse.",
    },
    InferenceDoc {
        name: "GenerationProvenance",
        text: "Records the run and terminal receipt that generated a revision.",
    },
    InferenceDoc {
        name: "RunInputProvenance",
        text: "Records the context and run inputs used for generation.",
    },
    InferenceDoc {
        name: "GenerateRunRequest",
        text: "Requests a recoverable generation run from a context revision.",
    },
    InferenceDoc {
        name: "GenerateRunResponse",
        text: "Returns the admitted generation run.",
    },
    InferenceDoc {
        name: "InspectRunRequest",
        text: "Identifies a generation run to inspect.",
    },
    InferenceDoc {
        name: "WatchRunRequest",
        text: "Identifies a run and starting event sequence to watch.",
    },
    InferenceDoc {
        name: "LogicalUsage",
        text: "Reports bounded logical usage counters for a run.",
    },
    InferenceDoc {
        name: "UsageReceipt",
        text: "Reports metered usage and the receipt revision for a run.",
    },
    InferenceDoc {
        name: "RunResult",
        text: "Reports generation output, terminal state, context, and usage receipt.",
    },
    InferenceDoc {
        name: "RunView",
        text: "Reports the current recoverable run state and latest result.",
    },
    InferenceDoc {
        name: "RunEvent",
        text: "Reports one ordered output, usage, progress, or terminal event.",
    },
    InferenceDoc {
        name: "RunProgress",
        text: "Reports progress classification for a generation run.",
    },
];

pub const INFERENCE_FIELD_DOCS: &[InferenceFieldDoc] = &[
    InferenceFieldDoc {
        message: "ListModelsResponse",
        name: "models",
        text: "The model capabilities visible to the caller.",
    },
    InferenceFieldDoc {
        message: "RequestIdentity",
        name: "client_instance",
        text: "The caller instance identity.",
    },
    InferenceFieldDoc {
        message: "RequestIdentity",
        name: "request_id",
        text: "The caller request identity.",
    },
    InferenceFieldDoc {
        message: "ModelCapability",
        name: "model",
        text: "The model value carried by ModelCapability.",
    },
    InferenceFieldDoc {
        message: "ModelCapability",
        name: "execution_profile",
        text: "The execution profile value carried by ModelCapability.",
    },
    InferenceFieldDoc {
        message: "ModelCapability",
        name: "maximum_context",
        text: "The maximum context value carried by ModelCapability.",
    },
    InferenceFieldDoc {
        message: "ModelCapability",
        name: "maximum_output",
        text: "The maximum output value carried by ModelCapability.",
    },
    InferenceFieldDoc {
        message: "ModelCapability",
        name: "features",
        text: "The features value carried by ModelCapability.",
    },
    InferenceFieldDoc {
        message: "ModelCapability",
        name: "retention_profiles",
        text: "The retention profiles value carried by ModelCapability.",
    },
    InferenceFieldDoc {
        message: "ModelCapability",
        name: "idle_kv_profiles",
        text: "The idle kv profiles value carried by ModelCapability.",
    },
    InferenceFieldDoc {
        message: "RetentionProfile",
        name: "profile",
        text: "The profile value carried by RetentionProfile.",
    },
    InferenceFieldDoc {
        message: "RetentionProfile",
        name: "minimum_duration_ms",
        text: "The minimum duration ms value carried by RetentionProfile.",
    },
    InferenceFieldDoc {
        message: "RetentionProfile",
        name: "maximum_duration_ms",
        text: "The maximum duration ms value carried by RetentionProfile.",
    },
    InferenceFieldDoc {
        message: "RetainWarmRequest",
        name: "identity",
        text: "The identity value carried by RetainWarmRequest.",
    },
    InferenceFieldDoc {
        message: "RetainWarmRequest",
        name: "context",
        text: "The context value carried by RetainWarmRequest.",
    },
    InferenceFieldDoc {
        message: "RetainWarmRequest",
        name: "latency_profile",
        text: "The latency profile value carried by RetainWarmRequest.",
    },
    InferenceFieldDoc {
        message: "RetainWarmRequest",
        name: "expires_at_ms",
        text: "The expires at ms value carried by RetainWarmRequest.",
    },
    InferenceFieldDoc {
        message: "RetainWarmRequest",
        name: "idle_kv",
        text: "The idle kv value carried by RetainWarmRequest.",
    },
    InferenceFieldDoc {
        message: "IdleKvPolicy",
        name: "profile",
        text: "The profile value carried by IdleKvPolicy.",
    },
    InferenceFieldDoc {
        message: "IdleKvPolicy",
        name: "idle_timeout_ms",
        text: "The idle timeout ms value carried by IdleKvPolicy.",
    },
    InferenceFieldDoc {
        message: "IdleKvRetention",
        name: "policy",
        text: "The policy value carried by IdleKvRetention.",
    },
    InferenceFieldDoc {
        message: "IdleKvRetention",
        name: "retained_at_ms",
        text: "The retained at ms value carried by IdleKvRetention.",
    },
    InferenceFieldDoc {
        message: "IdleKvRetention",
        name: "last_used_at_ms",
        text: "The last used at ms value carried by IdleKvRetention.",
    },
    InferenceFieldDoc {
        message: "IdleKvRetention",
        name: "last_run_id",
        text: "The last run id value carried by IdleKvRetention.",
    },
    InferenceFieldDoc {
        message: "InspectWarmRequest",
        name: "commitment",
        text: "The commitment value carried by InspectWarmRequest.",
    },
    InferenceFieldDoc {
        message: "RenewWarmRequest",
        name: "identity",
        text: "The identity value carried by RenewWarmRequest.",
    },
    InferenceFieldDoc {
        message: "RenewWarmRequest",
        name: "commitment",
        text: "The commitment value carried by RenewWarmRequest.",
    },
    InferenceFieldDoc {
        message: "RenewWarmRequest",
        name: "expires_at_ms",
        text: "The expires at ms value carried by RenewWarmRequest.",
    },
    InferenceFieldDoc {
        message: "RenewWarmRequest",
        name: "idle_timeout_ms",
        text: "The idle timeout ms value carried by RenewWarmRequest.",
    },
    InferenceFieldDoc {
        message: "ReleaseWarmRequest",
        name: "identity",
        text: "The identity value carried by ReleaseWarmRequest.",
    },
    InferenceFieldDoc {
        message: "ReleaseWarmRequest",
        name: "commitment",
        text: "The commitment value carried by ReleaseWarmRequest.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "commitment",
        text: "The commitment value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "context",
        text: "The context value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "model_profile",
        text: "The model profile value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "latency_profile",
        text: "The latency profile value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "expires_at_ms",
        text: "The expires at ms value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "state",
        text: "The state value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "evidence_digest",
        text: "The evidence digest value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "admission_receipt_id",
        text: "The admission receipt id value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "sequence",
        text: "The sequence value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "WarmView",
        name: "idle_kv",
        text: "The idle kv value carried by WarmView.",
    },
    InferenceFieldDoc {
        message: "EvaluationArtifact",
        name: "digest",
        text: "The digest value carried by EvaluationArtifact.",
    },
    InferenceFieldDoc {
        message: "EvaluationArtifact",
        name: "media_type",
        text: "The media type value carried by EvaluationArtifact.",
    },
    InferenceFieldDoc {
        message: "EvaluationArtifact",
        name: "logical_size",
        text: "The logical size value carried by EvaluationArtifact.",
    },
    InferenceFieldDoc {
        message: "EvaluationCase",
        name: "case_id",
        text: "The case id value carried by EvaluationCase.",
    },
    InferenceFieldDoc {
        message: "EvaluationCase",
        name: "input",
        text: "The input value carried by EvaluationCase.",
    },
    InferenceFieldDoc {
        message: "EvaluationCase",
        name: "input_artifact_digest",
        text: "The input artifact digest value carried by EvaluationCase.",
    },
    InferenceFieldDoc {
        message: "EvaluationSuite",
        name: "identity",
        text: "The identity value carried by EvaluationSuite.",
    },
    InferenceFieldDoc {
        message: "EvaluationSuite",
        name: "digest",
        text: "The digest value carried by EvaluationSuite.",
    },
    InferenceFieldDoc {
        message: "EvaluationSuite",
        name: "cases",
        text: "The cases value carried by EvaluationSuite.",
    },
    InferenceFieldDoc {
        message: "EvaluationGrader",
        name: "handle",
        text: "The handle value carried by EvaluationGrader.",
    },
    InferenceFieldDoc {
        message: "EvaluationGrader",
        name: "artifact_digest",
        text: "The artifact digest value carried by EvaluationGrader.",
    },
    InferenceFieldDoc {
        message: "EvaluationMetric",
        name: "identity",
        text: "The identity value carried by EvaluationMetric.",
    },
    InferenceFieldDoc {
        message: "EvaluationMetric",
        name: "aggregation",
        text: "The aggregation value carried by EvaluationMetric.",
    },
    InferenceFieldDoc {
        message: "EvaluationSpec",
        name: "candidates",
        text: "The candidates value carried by EvaluationSpec.",
    },
    InferenceFieldDoc {
        message: "EvaluationSpec",
        name: "suite",
        text: "The suite value carried by EvaluationSpec.",
    },
    InferenceFieldDoc {
        message: "EvaluationSpec",
        name: "grader",
        text: "The grader value carried by EvaluationSpec.",
    },
    InferenceFieldDoc {
        message: "EvaluationSpec",
        name: "metrics",
        text: "The metrics value carried by EvaluationSpec.",
    },
    InferenceFieldDoc {
        message: "EvaluationSpec",
        name: "maximum_case_results",
        text: "The maximum case results value carried by EvaluationSpec.",
    },
    InferenceFieldDoc {
        message: "EvaluationSpec",
        name: "spec_digest",
        text: "The spec digest value carried by EvaluationSpec.",
    },
    InferenceFieldDoc {
        message: "CreateEvaluationRequest",
        name: "identity",
        text: "The identity value carried by CreateEvaluationRequest.",
    },
    InferenceFieldDoc {
        message: "CreateEvaluationRequest",
        name: "spec",
        text: "The spec value carried by CreateEvaluationRequest.",
    },
    InferenceFieldDoc {
        message: "InspectEvaluationRequest",
        name: "evaluation_id",
        text: "The evaluation id value carried by InspectEvaluationRequest.",
    },
    InferenceFieldDoc {
        message: "ExactRational",
        name: "numerator",
        text: "The numerator value carried by ExactRational.",
    },
    InferenceFieldDoc {
        message: "ExactRational",
        name: "denominator",
        text: "The denominator value carried by ExactRational.",
    },
    InferenceFieldDoc {
        message: "EvaluationMetricValue",
        name: "metric_identity",
        text: "The metric identity value carried by EvaluationMetricValue.",
    },
    InferenceFieldDoc {
        message: "EvaluationMetricValue",
        name: "value",
        text: "The value value carried by EvaluationMetricValue.",
    },
    InferenceFieldDoc {
        message: "EvaluationCaseResult",
        name: "candidate_digest",
        text: "The candidate digest value carried by EvaluationCaseResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationCaseResult",
        name: "case_id",
        text: "The case id value carried by EvaluationCaseResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationCaseResult",
        name: "observation",
        text: "The observation value carried by EvaluationCaseResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationCaseResult",
        name: "metrics",
        text: "The metrics value carried by EvaluationCaseResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationCaseResult",
        name: "outcome",
        text: "The outcome value carried by EvaluationCaseResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationGraderObservation",
        name: "native_output_digest",
        text: "The native output digest value carried by EvaluationGraderObservation.",
    },
    InferenceFieldDoc {
        message: "EvaluationGraderObservation",
        name: "observation_digest",
        text: "The observation digest value carried by EvaluationGraderObservation.",
    },
    InferenceFieldDoc {
        message: "EvaluationGraderObservation",
        name: "binding_digest",
        text: "The binding digest value carried by EvaluationGraderObservation.",
    },
    InferenceFieldDoc {
        message: "EvaluationAggregate",
        name: "candidate_digest",
        text: "The candidate digest value carried by EvaluationAggregate.",
    },
    InferenceFieldDoc {
        message: "EvaluationAggregate",
        name: "metric_identity",
        text: "The metric identity value carried by EvaluationAggregate.",
    },
    InferenceFieldDoc {
        message: "EvaluationAggregate",
        name: "aggregation",
        text: "The aggregation value carried by EvaluationAggregate.",
    },
    InferenceFieldDoc {
        message: "EvaluationAggregate",
        name: "value",
        text: "The value value carried by EvaluationAggregate.",
    },
    InferenceFieldDoc {
        message: "EvaluationResult",
        name: "spec_digest",
        text: "The spec digest value carried by EvaluationResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationResult",
        name: "case_results",
        text: "The case results value carried by EvaluationResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationResult",
        name: "aggregates",
        text: "The aggregates value carried by EvaluationResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationResult",
        name: "result_digest",
        text: "The result digest value carried by EvaluationResult.",
    },
    InferenceFieldDoc {
        message: "EvaluationView",
        name: "evaluation_id",
        text: "The evaluation id value carried by EvaluationView.",
    },
    InferenceFieldDoc {
        message: "EvaluationView",
        name: "spec",
        text: "The spec value carried by EvaluationView.",
    },
    InferenceFieldDoc {
        message: "EvaluationView",
        name: "state",
        text: "The state value carried by EvaluationView.",
    },
    InferenceFieldDoc {
        message: "EvaluationView",
        name: "result",
        text: "The result value carried by EvaluationView.",
    },
    InferenceFieldDoc {
        message: "EvaluationView",
        name: "sequence",
        text: "The sequence value carried by EvaluationView.",
    },
    InferenceFieldDoc {
        message: "Item",
        name: "id",
        text: "The id value carried by Item.",
    },
    InferenceFieldDoc {
        message: "Item",
        name: "kind",
        text: "The kind value carried by Item.",
    },
    InferenceFieldDoc {
        message: "Item",
        name: "payload",
        text: "The payload value carried by Item.",
    },
    InferenceFieldDoc {
        message: "Item",
        name: "link",
        text: "The link value carried by Item.",
    },
    InferenceFieldDoc {
        message: "Item",
        name: "continuation_profile",
        text: "The continuation profile value carried by Item.",
    },
    InferenceFieldDoc {
        message: "CreateContextRequest",
        name: "identity",
        text: "The identity value carried by CreateContextRequest.",
    },
    InferenceFieldDoc {
        message: "CreateContextRequest",
        name: "model",
        text: "The model value carried by CreateContextRequest.",
    },
    InferenceFieldDoc {
        message: "CreateContextRequest",
        name: "items",
        text: "The items value carried by CreateContextRequest.",
    },
    InferenceFieldDoc {
        message: "InspectContextRequest",
        name: "revision",
        text: "The revision value carried by InspectContextRequest.",
    },
    InferenceFieldDoc {
        message: "Insert",
        name: "target",
        text: "The target value carried by Insert.",
    },
    InferenceFieldDoc {
        message: "Insert",
        name: "item",
        text: "The item value carried by Insert.",
    },
    InferenceFieldDoc {
        message: "Replace",
        name: "target",
        text: "The target value carried by Replace.",
    },
    InferenceFieldDoc {
        message: "Replace",
        name: "payload",
        text: "The payload value carried by Replace.",
    },
    InferenceFieldDoc {
        message: "Edit",
        name: "append",
        text: "The append value carried by Edit.",
    },
    InferenceFieldDoc {
        message: "Edit",
        name: "insert_before",
        text: "The insert before value carried by Edit.",
    },
    InferenceFieldDoc {
        message: "Edit",
        name: "insert_after",
        text: "The insert after value carried by Edit.",
    },
    InferenceFieldDoc {
        message: "Edit",
        name: "replace",
        text: "The replace value carried by Edit.",
    },
    InferenceFieldDoc {
        message: "Edit",
        name: "delete",
        text: "The delete value carried by Edit.",
    },
    InferenceFieldDoc {
        message: "Edits",
        name: "edits",
        text: "The edits value carried by Edits.",
    },
    InferenceFieldDoc {
        message: "Truncate",
        name: "through",
        text: "The through value carried by Truncate.",
    },
    InferenceFieldDoc {
        message: "Compact",
        name: "selected",
        text: "The selected value carried by Compact.",
    },
    InferenceFieldDoc {
        message: "Compact",
        name: "replacement",
        text: "The replacement value carried by Compact.",
    },
    InferenceFieldDoc {
        message: "Transfer",
        name: "model",
        text: "The model value carried by Transfer.",
    },
    InferenceFieldDoc {
        message: "MutateContextRequest",
        name: "identity",
        text: "The identity value carried by MutateContextRequest.",
    },
    InferenceFieldDoc {
        message: "MutateContextRequest",
        name: "source",
        text: "The source value carried by MutateContextRequest.",
    },
    InferenceFieldDoc {
        message: "MutateContextRequest",
        name: "edit",
        text: "The edit value carried by MutateContextRequest.",
    },
    InferenceFieldDoc {
        message: "MutateContextRequest",
        name: "fork",
        text: "The fork value carried by MutateContextRequest.",
    },
    InferenceFieldDoc {
        message: "MutateContextRequest",
        name: "truncate",
        text: "The truncate value carried by MutateContextRequest.",
    },
    InferenceFieldDoc {
        message: "MutateContextRequest",
        name: "compact",
        text: "The compact value carried by MutateContextRequest.",
    },
    InferenceFieldDoc {
        message: "MutateContextRequest",
        name: "release",
        text: "The release value carried by MutateContextRequest.",
    },
    InferenceFieldDoc {
        message: "MutateContextRequest",
        name: "transfer",
        text: "The transfer value carried by MutateContextRequest.",
    },
    InferenceFieldDoc {
        message: "MutationReceipt",
        name: "revision",
        text: "The revision value carried by MutationReceipt.",
    },
    InferenceFieldDoc {
        message: "MutationReceipt",
        name: "command_digest",
        text: "The command digest value carried by MutationReceipt.",
    },
    InferenceFieldDoc {
        message: "MutationReceipt",
        name: "sequence",
        text: "The sequence value carried by MutationReceipt.",
    },
    InferenceFieldDoc {
        message: "MutationReceipt",
        name: "retained",
        text: "The retained value carried by MutationReceipt.",
    },
    InferenceFieldDoc {
        message: "ContextView",
        name: "revision",
        text: "The revision value carried by ContextView.",
    },
    InferenceFieldDoc {
        message: "ContextView",
        name: "parent",
        text: "The parent value carried by ContextView.",
    },
    InferenceFieldDoc {
        message: "ContextView",
        name: "lineage",
        text: "The lineage value carried by ContextView.",
    },
    InferenceFieldDoc {
        message: "ContextView",
        name: "execution_profile",
        text: "The execution profile value carried by ContextView.",
    },
    InferenceFieldDoc {
        message: "ContextView",
        name: "content_digest",
        text: "The content digest value carried by ContextView.",
    },
    InferenceFieldDoc {
        message: "ContextView",
        name: "items",
        text: "The items value carried by ContextView.",
    },
    InferenceFieldDoc {
        message: "ContextView",
        name: "model",
        text: "The model value carried by ContextView.",
    },
    InferenceFieldDoc {
        message: "ContextView",
        name: "provenance",
        text: "The provenance value carried by ContextView.",
    },
    InferenceFieldDoc {
        message: "ContextProvenance",
        name: "created",
        text: "The created value carried by ContextProvenance.",
    },
    InferenceFieldDoc {
        message: "ContextProvenance",
        name: "derived",
        text: "The derived value carried by ContextProvenance.",
    },
    InferenceFieldDoc {
        message: "ContextProvenance",
        name: "forked",
        text: "The forked value carried by ContextProvenance.",
    },
    InferenceFieldDoc {
        message: "ContextProvenance",
        name: "transferred",
        text: "The transferred value carried by ContextProvenance.",
    },
    InferenceFieldDoc {
        message: "ContextProvenance",
        name: "generated",
        text: "The generated value carried by ContextProvenance.",
    },
    InferenceFieldDoc {
        message: "ContextProvenance",
        name: "run_input",
        text: "The run input value carried by ContextProvenance.",
    },
    InferenceFieldDoc {
        message: "ProvenanceSource",
        name: "source",
        text: "The source value carried by ProvenanceSource.",
    },
    InferenceFieldDoc {
        message: "TransferProvenance",
        name: "source",
        text: "The source value carried by TransferProvenance.",
    },
    InferenceFieldDoc {
        message: "TransferProvenance",
        name: "reused_compatible_state",
        text: "The reused compatible state value carried by TransferProvenance.",
    },
    InferenceFieldDoc {
        message: "GenerationProvenance",
        name: "run_id",
        text: "The run id value carried by GenerationProvenance.",
    },
    InferenceFieldDoc {
        message: "GenerationProvenance",
        name: "terminal_receipt_digest",
        text: "The terminal receipt digest value carried by GenerationProvenance.",
    },
    InferenceFieldDoc {
        message: "RunInputProvenance",
        name: "source",
        text: "The source value carried by RunInputProvenance.",
    },
    InferenceFieldDoc {
        message: "RunInputProvenance",
        name: "run_id",
        text: "The run id value carried by RunInputProvenance.",
    },
    InferenceFieldDoc {
        message: "RunInputProvenance",
        name: "maximum_output",
        text: "The maximum output value carried by RunInputProvenance.",
    },
    InferenceFieldDoc {
        message: "RunInputProvenance",
        name: "seed",
        text: "The seed value carried by RunInputProvenance.",
    },
    InferenceFieldDoc {
        message: "GenerateRunRequest",
        name: "identity",
        text: "The identity value carried by GenerateRunRequest.",
    },
    InferenceFieldDoc {
        message: "GenerateRunRequest",
        name: "context",
        text: "The context value carried by GenerateRunRequest.",
    },
    InferenceFieldDoc {
        message: "GenerateRunRequest",
        name: "input",
        text: "The input value carried by GenerateRunRequest.",
    },
    InferenceFieldDoc {
        message: "GenerateRunRequest",
        name: "maximum_output",
        text: "The maximum output value carried by GenerateRunRequest.",
    },
    InferenceFieldDoc {
        message: "GenerateRunRequest",
        name: "seed",
        text: "The seed value carried by GenerateRunRequest.",
    },
    InferenceFieldDoc {
        message: "GenerateRunResponse",
        name: "run",
        text: "The run value carried by GenerateRunResponse.",
    },
    InferenceFieldDoc {
        message: "InspectRunRequest",
        name: "run_id",
        text: "The run id value carried by InspectRunRequest.",
    },
    InferenceFieldDoc {
        message: "WatchRunRequest",
        name: "run_id",
        text: "The run id value carried by WatchRunRequest.",
    },
    InferenceFieldDoc {
        message: "WatchRunRequest",
        name: "from_sequence",
        text: "The from sequence value carried by WatchRunRequest.",
    },
    InferenceFieldDoc {
        message: "LogicalUsage",
        name: "new_prefill",
        text: "The new prefill value carried by LogicalUsage.",
    },
    InferenceFieldDoc {
        message: "LogicalUsage",
        name: "generated_output",
        text: "The generated output value carried by LogicalUsage.",
    },
    InferenceFieldDoc {
        message: "LogicalUsage",
        name: "effective_context_reads",
        text: "The effective context reads value carried by LogicalUsage.",
    },
    InferenceFieldDoc {
        message: "LogicalUsage",
        name: "retained_byte_millis",
        text: "The retained byte millis value carried by LogicalUsage.",
    },
    InferenceFieldDoc {
        message: "UsageReceipt",
        name: "receipt_id",
        text: "The receipt id value carried by UsageReceipt.",
    },
    InferenceFieldDoc {
        message: "UsageReceipt",
        name: "model_profile",
        text: "The model profile value carried by UsageReceipt.",
    },
    InferenceFieldDoc {
        message: "UsageReceipt",
        name: "meter_revision",
        text: "The meter revision value carried by UsageReceipt.",
    },
    InferenceFieldDoc {
        message: "UsageReceipt",
        name: "usage",
        text: "The usage value carried by UsageReceipt.",
    },
    InferenceFieldDoc {
        message: "UsageReceipt",
        name: "rate_card_revision",
        text: "The rate card revision value carried by UsageReceipt.",
    },
    InferenceFieldDoc {
        message: "RunResult",
        name: "output",
        text: "The output value carried by RunResult.",
    },
    InferenceFieldDoc {
        message: "RunResult",
        name: "context",
        text: "The context value carried by RunResult.",
    },
    InferenceFieldDoc {
        message: "RunResult",
        name: "terminal",
        text: "The terminal value carried by RunResult.",
    },
    InferenceFieldDoc {
        message: "RunResult",
        name: "receipt",
        text: "The receipt value carried by RunResult.",
    },
    InferenceFieldDoc {
        message: "RunView",
        name: "run_id",
        text: "The run id value carried by RunView.",
    },
    InferenceFieldDoc {
        message: "RunView",
        name: "input",
        text: "The input value carried by RunView.",
    },
    InferenceFieldDoc {
        message: "RunView",
        name: "model",
        text: "The model value carried by RunView.",
    },
    InferenceFieldDoc {
        message: "RunView",
        name: "last_sequence",
        text: "The last sequence value carried by RunView.",
    },
    InferenceFieldDoc {
        message: "RunView",
        name: "cancellation_requested",
        text: "The cancellation requested value carried by RunView.",
    },
    InferenceFieldDoc {
        message: "RunView",
        name: "result",
        text: "The result value carried by RunView.",
    },
    InferenceFieldDoc {
        message: "RunEvent",
        name: "sequence",
        text: "The sequence value carried by RunEvent.",
    },
    InferenceFieldDoc {
        message: "RunEvent",
        name: "output",
        text: "The output value carried by RunEvent.",
    },
    InferenceFieldDoc {
        message: "RunEvent",
        name: "usage",
        text: "The usage value carried by RunEvent.",
    },
    InferenceFieldDoc {
        message: "RunEvent",
        name: "terminal",
        text: "The terminal value carried by RunEvent.",
    },
    InferenceFieldDoc {
        message: "RunEvent",
        name: "progress",
        text: "The progress value carried by RunEvent.",
    },
    InferenceFieldDoc {
        message: "RunProgress",
        name: "kind",
        text: "The kind value carried by RunProgress.",
    },
];

pub const INFERENCE_SERVICE_DOCS: &[InferenceDoc] = &[
    InferenceDoc {
        name: "ModelsService",
        text: "Lists model capabilities and retention profiles.",
    },
    InferenceDoc {
        name: "ContextsService",
        text: "Creates, inspects, and mutates immutable context revisions.",
    },
    InferenceDoc {
        name: "WarmContextsService",
        text: "Admits and manages explicit warm-retention commitments.",
    },
    InferenceDoc {
        name: "RunsService",
        text: "Admits, inspects, watches, and cancels recoverable generation runs.",
    },
    InferenceDoc {
        name: "EvaluationsService",
        text: "Admits and inspects immutable evaluation results.",
    },
];

pub fn inference_message_docs(name: &str) -> &'static str {
    INFERENCE_MESSAGE_DOCS
        .iter()
        .find(|doc| doc.name == name)
        .map(|doc| doc.text)
        .unwrap_or_else(|| panic!("missing Inference message docs for {name}"))
}

pub fn inference_field_docs(message: &str, name: &str) -> &'static str {
    INFERENCE_FIELD_DOCS
        .iter()
        .find(|doc| doc.message == message && doc.name == name)
        .map(|doc| doc.text)
        .unwrap_or_else(|| panic!("missing Inference field docs for {message}.{name}"))
}

pub fn inference_service_docs(name: &str) -> &'static str {
    INFERENCE_SERVICE_DOCS
        .iter()
        .find(|doc| doc.name == name)
        .map(|doc| doc.text)
        .unwrap_or_else(|| panic!("missing Inference service docs for {name}"))
}

pub fn inference_descriptor() -> Vec<u8> {
    super::inference_descriptor_with_options()
}
pub fn inference_proto() -> std::string::String {
    INFERENCE.render_proto()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_inference_assignment_uses_the_shared_typed_option_path() {
        for assignment in INFERENCE_OPTIONS {
            let options = inference_raw_options(assignment.subject);
            assert!(
                options
                    .fields()
                    .iter()
                    .any(|field| field.spec.name == assignment.name),
                "missing {} on {}",
                assignment.name,
                assignment.subject
            );
        }
    }

    #[test]
    fn typed_options_retain_wire_order_and_width() {
        let options = inference_raw_options("EvaluationSpec.maximum_case_results");
        assert_eq!(options.fields()[1].spec.number, 51008);
        assert!(
            options
                .encode()
                .ends_with(&[0x80, 0xF4, 0x18, 0x80, 0x80, 0x04])
        );
    }

    #[test]
    fn model_documentation_covers_messages_fields_and_services() {
        assert_eq!(INFERENCE_MESSAGE_DOCS.len(), INFERENCE.messages.len());
        assert_eq!(
            INFERENCE_FIELD_DOCS.len(),
            INFERENCE
                .messages
                .iter()
                .map(|message| message.fields.len())
                .sum::<usize>()
        );
        for message in INFERENCE.messages {
            for field in message.fields {
                assert_eq!(
                    INFERENCE_FIELD_DOCS
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
        assert_eq!(INFERENCE_SERVICE_DOCS.len(), INFERENCE.services.len());
        assert!(inference_proto().contains("Describes a model profile, limits"));
        assert!(inference_proto().contains("The maximum context value carried"));
    }
}
