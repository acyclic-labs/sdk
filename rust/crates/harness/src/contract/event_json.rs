//! Admission for the concrete history EventPayload schema, not arbitrary serde.

use super::{Error, JsonBounds, JsonCodecCharge, Result};
use super::json::{JsonValueSchema, JsonValueShape};
use crate::{
    conversation::{Attachment, ConversationMessage, FileRef},
    core::{ExtensionConfiguration, ExtensionDependency},
    fork::{CapturedResource, ForkOmission, ForkSeed, ReferenceGrant, SharedGrant},
    merge::ProjectMergeReceipt,
};
use serde::__private229::de::Content;
use serde_json::Value;
use std::mem::size_of;
use uuid::Uuid;

const CONTENT_LAYERS: u64 = 4;

const fn maximum(values: &[usize]) -> usize {
    let mut result = 0;
    let mut index = 0;
    while index < values.len() {
        if values[index] > result { result = values[index]; }
        index += 1;
    }
    result
}

const TYPED_COLLECTION_SLOT_BYTES: usize = maximum(&[
        size_of::<ExtensionDependency>(),
        size_of::<ExtensionConfiguration>(),
        size_of::<CapturedResource>(),
        size_of::<ForkOmission>(),
        size_of::<SharedGrant>(),
        size_of::<ReferenceGrant>(),
        size_of::<Attachment>(),
        size_of::<(String, FileRef)>(),
]);

const BOXED_PAYLOAD_BYTES: usize = maximum(&[
    size_of::<ForkSeed>(), size_of::<ProjectMergeReceipt>(),
    size_of::<ConversationMessage>(), size_of::<FileRef>(),
]);

const DIAGNOSTIC_LABELS: &str = concat!(
    "lifecycle_transitioned    custom    extension_state_migrated    extensions_selected    extension_configured    ",
    "effect_planned    effect_dispatched    effect_resolved    fork_published    project_merge_published    ",
    "conversation_bound    conversation_message_appended    model_context_selected    interaction_opened    ",
    "interaction_resolved    operation_id    parent    parent_revision    child    child_agent    attached_agents    ",
    "resources    omissions    child_private_volume    child_private_generation    inherited_context    summary    ",
    "inherited_through_sequence    shared_grants    reference_grants    history_witness    attachment_manifests    ",
    "boundary    provider    namespace    family    version    kind    id    class    owner    key    volume    ",
    "path    descriptor    display_name    sha256    byte_length    media_type    from    to    reason    record    ",
    "migration    previous    selected    configurations    extension    name    schema_digest    ",
    "implementation_digest    fork_policy    content    effect_id    guarantee    effect_kind    request    ",
    "request_digest    result_schema    result_schema_digest    attempt_id    observation    status    proof    ",
    "state    result    message    seed    receipt    agent    selection    ticket    resolution    source    ",
    "revision    required    outcome    evidence    reader    attachment_manifest    operations    checkpoint    ",
    "limits    history_limits    context    payloads    references    source_project    source_generation    ",
    "target_project    expected_target_generation    result_generation    provider_operation_id    provider_proof    ",
    "notice    format    statement    sequence    attachments    items    manifest    item_count    reply_to    ",
    "tool_call_id    extensions    conversation_revision    message_ids    deadline_unix_ms    approval    ",
    "action_digest    expected_version    detail    answer    reference    value    paths    history    project    ",
    "private_volume    process    artifact    shared_volume    captured    unsupported    in_flight    ",
    "indeterminate    planned    dispatched    succeeded    failed    inline    answered    approved    declined    ",
    "cancelled    expired    denied    session    agent_private    session_shared    user    assistant    system    ",
    "tool_call    tool_result    interaction    permission    fork    merge    question    choice    form    ",
    "approval    inherit    reset    omit    at_most_once    idempotent_retry    exactly_once    pending    ",
    "active    waiting    completed    failed    cancelled    conversation    session    agent    turn    task    ",
    "file_bytes    path_bytes    attachments    render_bytes    model_steps    model_events_per_step    ",
    "tool_calls_per_step    context_messages    maximum_events    maximum_bytes    authorized_source    ",
    "native_snapshot    caller_task    caller_admission_digest    caller_fence    source_task    graph_root    ",
    "maximum_work    policy_scope    capabilities    issuer    agent    parent_proof    ",
    "ProjectSourceAuthorization    Scope    LeaseFence    reservation_id    placement    read    write    list    ",
    "fork    merge    EventPayload    ForkSeed    ProjectMergeReceipt    ConversationMessage    ResourceRevision    ",
    "VolumeOwner    ReferencedAttachmentsWire    EffectStatus    FileRefWire    ProviderRefWire    ",
    "ResourceRefWire    unknown variant ``, expected one of     unknown field ``, expected one of     ",
    "duplicate field ``    missing field ``    invalid type: , expected     invalid value: , expected     ",
    "invalid length , expected      at line  column     a sequence    a map    a string    a byte array    ",
    "an integer    an array of length 32    a valid UUID string    invalid request:     storage failure:     ",
    "interaction outcome must be an object    interaction outcome kind must be a string    ",
    "unknown interaction outcome field     SHA-256 digest must contain exactly 32 bytes    ",
    "typed resource kind mismatch    provider identity is invalid    resource key is empty    ",
    "resource version is invalid    attachment manifest metadata is invalid    ",
);

const DIAGNOSTIC_LABEL_BYTES: u64 = DIAGNOSTIC_LABELS.len() as u64;

const PREPARATION_DIAGNOSTIC_LABELS: &str = concat!(
    "Model    ModelMessage    ModelRole    ModelContent    ModelContentPart    FileProjectionPolicy    ",
    "ModelDataPart    ToolResultContent    NativeConfigurationBinding    ImageDetail    NativeMediaIntent    ",
    "NativeMediaPolicy    ModelRequest    ModelEvent    ToolDefinition    EventReference    ExtensionDependency    ",
    "ExtensionConfiguration    Context    CanonicalContextCheckpoint    CompactionReference    ",
    "CompactionRetention    ContextSummary    ResponseProjection    InheritedConversationPrefix    ",
    "ModelContextSelection    WorkflowCommand    MachineIdentity    MachineStatus    MachineTransition    ",
    "MachineCheckpoint    WorkflowRecord    WorkflowAdmission    FileDescriptorWire    ResourceKind    workspace    ",
    "generation    artifact    sandbox    checkpoint    stream    context    run    model    messages    tools    ",
    "options    max_output_tokens    role    text    file    policy    call_id    callId    arguments    json    ",
    "parts    metadata    delta    reasoning    input_schema    output_schema    projection_schema    description    ",
    "configuration    intent    image    audio    video    document    auto    low    high    maximum_duration_ms    ",
    "maximum_frames    maximum_pages    bounded_full    native    format_version    parent_agent    ",
    "through_sequence    message_digest    current_input_index    source_digest    maximum_messages    retention    ",
    "retained    roles    native_media    step    source_messages    output    canonical    compaction    machine    ",
    "digest    commands    suspended    idempotency_key    input_digest    prior    input    transition    next    ",
    "initial    state    value    Capabilities    file byte length exceeds cross-language precision    ",
    "media type is invalid    file or volume label is invalid    volume-relative path is invalid    ",
    "data did not match any variant of untagged enum ModelContent    struct     struct variant     tuple variant     ",
    "newtype variant     unit variant     RecursiveForkSourceIntent    RecursiveForkToolPlan    ",
    "RecursiveMergeToolPlan    ForkToolArguments    MergeToolArguments    MergeToolSelection    MergeToolSide    ",
    "Plan    Stage    State    InitialPlan    SourcePin    Receipt    ConfiguredRecursiveSourceCheckpoint    ",
    "SourceCheckpoint    CommandResults    CommandResult    ResourceCreationProofRef    ",
    "ResourcePreparationRequest    TaskAdmissionRecord    ComponentIdentity    ExecutionPlacement    ",
    "TaskRunLimits    ExtensionAdmission    Limits    ExtensionConfiguration    ExtensionDependency    ",
    "EventReference    ForkRequest    ForkHistoryPolicy    ForkPreparation    ForkSelection    AttestedBoundary    ",
    "ForkReport    ForkSeed    Capture    CapturedResource    ForkOmission    SharedGrant    ReferenceGrant    ",
    "SummaryForkSelection    SummaryForkCapture    ResourceRevision    WorkflowCommand    MachineIdentity    ",
    "arguments    caller    source_task    child_operation_id    allocation_digest    request    preparation    ",
    "child_message_pointer    child_message_file_pointer    source_checkpoint    operation_id    destination    ",
    "source    source_project    source_generation    authorized_source    target_project    ",
    "expected_target_generation    notice    id    msg    selections    conflict    side    kind    plan    phase    ",
    "stage    intent    command    report    receipt    value    next    snapshot    request_digest    commands    ",
    "provider    resource    evidence    child    resources    creation_grant    maximum_resources    ",
    "maximum_work    maximum_bytes    payload    input_files    read_authorization    task    machine    input    ",
    "input_schema    output_schema    parent    resource_proofs    grants    limits    run_limits    policy    ",
    "extensions    execution    name    version    digest    build    environment    readiness_revision    ",
    "concurrency    max_steps    deadline_epoch_ms    selected    configurations    extension    schema_digest    ",
    "content    parent_revision    child_agent    attached_agents    boundary    child_project_volume    ",
    "child_private_volume    child_result_volume    inherited_through_sequence    maximum_inherited_messages    ",
    "maximum_inherited_bytes    maximum_inherited_references    summary    required    revision    reference    ",
    "volume    generation    paths    implementation_digest    captures    child_private_generation    ",
    "child_result_generation    inherited_context    shared_grants    reference_grants    history_witness    ",
    "attachment_manifests    selection    outcome    omissions    operations    file    reader    ",
    "attachment_manifest    checkpoint    history_limits    context    payloads    references    fork    merge    ",
    "prepare    observe    fork_publish    child_admit    merge_apply    merge_publish    ready    source_pinned    ",
    "waiting    complete    settled    base    target    fresh    history    project    private_volume    process    ",
    "artifact    shared_volume    extension    captured    unsupported    in_flight    indeterminate    Snapshot    ",
    "SnapshotWire    format_version    authority    revision    events    projection    state_digest    ",
    "attestation    Projection    lifecycle    active_extensions    active_extension_revision    extension_states    ",
    "configured_extensions    active_configurations    effects    indexed_terminal_effects    effects_archived    ",
    "forks    published_merges    conversation    latest_context_checkpoint    interactions    bindings    ",
    "VolumeRefWire    provider    id    class    owner    VolumeClass    project    agent_private    ",
    "session_shared    VolumeOwner    kind    agent    session    FileDescriptorWire    sha256    byte_length    ",
    "media_type    SHA-256 digest must contain exactly 32 bytes    FileRefWire    volume    path    version    ",
    "descriptor    display_name    Attachment    file    label    ReferencedAttachmentsWire    inline    manifest    ",
    "items    item_count    MessageKind    user    assistant    system    tool_call    tool_result    interaction    ",
    "permission    fork    merge    ConversationMessage    sequence    content    attachments    reply_to    ",
    "tool_call_id    extensions    ConversationState    ConversationStateWire    messages    archived_prefix    ",
    "ArchivedConversationPrefix    through_sequence    history_digest    checkpoint    invalid request:     ",
    "file or volume label is invalid    volume-relative path is invalid    media type is invalid    ",
    "attachment manifest metadata is invalid    message identity is nil    message sequence must start at one    ",
    "extension key must be namespaced    tool linkage does not match message kind    ",
    "archived conversation prefix is unbound    conversation order or identity is invalid    Authority    ",
    "AggregateKind    kind    id    agent    conversation    session    turn    task    LifecycleState    pending    ",
    "active    waiting    completed    failed    cancelled    EffectGuarantee    exactly_once    idempotent_retry    ",
    "at_most_once    EffectStatus    state    planned    dispatched    succeeded    result    message    ",
    "indeterminate    EffectState    provider    guarantee    effect_kind    request    request_digest    ",
    "result_schema    result_schema_digest    status    attempts    ExtensionForkPolicy    inherit    reset    ",
    "reject    ExtensionRecord    name    version    schema_digest    implementation_digest    fork_policy    ",
    "content    ExtensionDependency    ExtensionConfiguration    extension    InteractionTicket    ",
    "deadline_unix_ms    approval    ApprovalBinding    operation_id    action_digest    InteractionKind    ",
    "question    choice    form    InteractionResolution    expected_version    outcome    detail    ",
    "InteractionOutcome    answered    answer    approved    declined    expired    denied    ",
    "interaction outcome must be an object    interaction outcome kind must be a string    ",
    "unknown interaction outcome field     NativeProcessRequest    NativeProcessResult    NativeProcessStop    ",
    "NativeProcessStopKind    NativeViewOptions    NativeVolumeMapping    NativeViewManifest    ",
    "NativeVolumeNamespace    executable    argv    cwd    environment    timeout_ms    control_timeout_ms    ",
    "cancellation_poll_ms    maximum_output_bytes    maximum_result_bytes    mcp_stdio    standard_input    view    ",
    "root    maximum_volumes    work_per_volume    options    volumes    volume    generation    namespace    ",
    "path    writable    authority_revision    materialized_directory    lazy_mount    snapshot    success    ",
    "exit_code    stdout    stderr    stop    kind    message    cleanup_completed    timeout    output_limit    ",
    "control_stopped    capture_failed    McpStdioRequest    McpStdioMethod    initialization    operation    ",
    "method    params    maximum_bytes    tools/list    tools/call    LazySnapshotRef    SourceReference    id    ",
    "workspace_id    authored_generation    source    overlay    shadows    identity    epoch    WorkCounters    ",
    "authorityRecordsRead    authorityRecordsAppended    authorityBytesRead    authorityBytesWritten    ",
    "objectProbes    backendReadOperations    backendWriteOperations    durabilityOperations    pageReads    ",
    "pageWrites    objectBytesRead    objectBytesWritten    bytesHashed    bytesCopied    bytesEncoded    ",
    "sourceBytesRead    sourcePathComponents    sourceEntriesVisited    outputBytes    itemsExamined    ",
    "itemsReturned    allocationOperations    peakAllocationBytes    materializations    path string    ",
    "an array of length 16    TimerTaskCommand    TaskObserveCommand    TaskAdmitCommand    ",
    "PreparedTaskAdmitCommand    TaskCancelCommand    ModelTaskCommand    ToolTaskCommand    MailSendTaskCommand    ",
    "MailReceiveTaskCommand    SelectedModelContext    deadline_unix_ms    admission_operation    name    version    ",
    "input    selected_context    max_steps    max_output_tokens    revision    arguments    ",
    "selected_output_volume    mailbox_operation    admission    CommittedMailResult    InboxItem    task_id    ",
    "message_id    recipient    payload    after    selection    messages    Outcome    Succeeded    Failed    ",
    "Cancelled    Indeterminate    message    operation_id    ",
);

/// Compile-time label/punctuation upper bound for the concrete preparation graphs listed above: stock records/commands, recursive/resource/task DTOs, configured Snapshot/conversation, native process/MCP, refs and Event/SourceAuth.
pub const fn preparation_serde_diagnostic_label_bytes() -> u64 {
    PREPARATION_DIAGNOSTIC_LABELS.len() as u64
}

/// Diagnostic capacity for the finite concrete SDK preparation graphs above.
pub(crate) fn audited_preparation_diagnostic_bytes(
    input_bytes: u64, native_code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    multiply_with_code(diagnostic_bytes(input_bytes, preparation_serde_diagnostic_label_bytes(), native_code)?, 2, native_code)
}

/// Pure quota arithmetic for an SDK-issued shape and an audited closed DTO. `schema_label_bytes` MUST cover actual field/discriminator/type names, list punctuation and custom STATIC diagnostics.
pub fn json_typed_diagnostic_owned_bytes(
    shape: JsonValueShape, schema_label_bytes: u64, native_code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    multiply_with_code(diagnostic_bytes(shape.input_bytes(), schema_label_bytes, native_code)?, 2, native_code)
}


const COMMON_DIAGNOSTIC_TEXT: &str = concat!(
    "unknown variant ``, expected one of unknown field ``, expected one of ",
    "duplicate field ``missing field ``invalid type: , expected ",
    "invalid value: , expected invalid length , expected  at line  column ",
    "a sequencea mapa stringa byte arrayan integeran array of length 32a valid UUID string",
    "invalid request: storage failure: ",
);

/// For audited serde-derived records and the moving reference converters only: source interpolation is debug-escaped at <=6 bytes/input byte, with at most two diagnostic buffers overlapping during the original error conversions. `schema_label_bytes` must include the concrete field/discriminator/type labels, their list punctuation, and custom static diagnostics.
pub(crate) fn audited_serde_diagnostic_bytes(input_bytes: u64, schema_label_bytes: u64) -> Result<u64> {
    diagnostic_bytes(input_bytes, schema_label_bytes, None)
}

fn diagnostic_bytes(input_bytes: u64, schema_label_bytes: u64, native_code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    let sum = |values: &[u64]| sum_with_code(values, native_code);
    let multiply = |value, factor| multiply_with_code(value, factor, native_code);
    let static_bytes = sum(&[schema_label_bytes, COMMON_DIAGNOSTIC_TEXT.len() as u64,
        2 * u64::from(usize::BITS)])?;
    sum(&[multiply(input_bytes, 6 * 2)?, multiply(static_bytes, 2)?])
}

fn ownership_overflow(native_code: Option<crate::wire::ErrorCode>) -> Error {
    match native_code {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(code) => Error::NativeBoundary { code, reason: "event JSON admission overflow" },
        _ => Error::Invalid("event JSON admission overflow".into()),
    }
}

pub(super) fn multiply_with_code(value: u64, factor: u64, native_code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    value.checked_mul(factor).ok_or_else(|| ownership_overflow(native_code))
}
pub(super) fn sum_with_code(values: &[u64], native_code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    values.iter().try_fold(0u64, |total, value| total.checked_add(*value)
        .ok_or_else(|| ownership_overflow(native_code)))
}

/// Admits lexical structure and reserves the original concrete EventPayload decode's logical quota BEFORE serde can buffer tagged fields or diagnostics.
pub(crate) fn event_payload_json_admission<F: FnMut(JsonCodecCharge) -> Result<()> + ?Sized>(
    bytes: &[u8], bounds: JsonBounds, charge: &mut F,
) -> Result<u64> {
    let bounds = JsonBounds {
        maximum_bytes: bounds.maximum_bytes.min(acyclic_stream::MAX_RECORD_BYTES as u64),
        maximum_depth: bounds.maximum_depth.min(256),
        ..bounds
    };
    let value_shape = super::json::admit_json_value_shape(
        bytes, bounds, JsonValueSchema::EventPayload, &mut |event| charge(event),
    )?;
    let admission = json_decode_owned_bytes(value_shape, false, None)?.0;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes: admission, value_shape })?;
    Ok(admission)
}

pub(crate) fn json_decode_owned_bytes(
    shape: JsonValueShape, snapshot: bool, native_code: Option<crate::wire::ErrorCode>,
) -> Result<(u64, u64, u64)> {
    let multiply = |value, factor| multiply_with_code(value, factor, native_code);
    let sum = |values: &[u64]| sum_with_code(values, native_code);
    let input_bytes = shape.input_bytes();
    let lexical_items = shape.lexical_items();
    let slots = multiply(lexical_items, 2 + 8)?;
    let content_slot = size_of::<(Content<'static>, Content<'static>)>()
        .max(size_of::<Content<'static>>()) as u64;
    let serde_content_bytes = multiply(multiply(slots, content_slot)?, CONTENT_LAYERS)?;
    let typed_slot = if snapshot { multiply(crate::core::Snapshot::LOGICAL_SCHEMA_SLOT_BYTES, 3)? }
        else { TYPED_COLLECTION_SLOT_BYTES as u64 };
    let typed_collection_bytes = multiply(slots, typed_slot)?;
    let value_slot = size_of::<Value>().max(size_of::<(String, Value)>()) as u64;
    let value_overlap_bytes = sum(&[
        multiply(slots, value_slot)?,
        super::json::json_value_virtual_owned_bytes(shape).map_err(|error| match (native_code, error) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            (Some(code), Error::NativeBoundary { reason, .. }) => Error::NativeBoundary { code, reason },
            (_, error) => error,
        })?,
    ])?;
    let boxed_payload_bytes = multiply(if snapshot { lexical_items } else { 1 }, BOXED_PAYLOAD_BYTES as u64)?;
    let string_payload_bytes = multiply(input_bytes, CONTENT_LAYERS + 1 + 1)?;
    let scratch_bytes = multiply(input_bytes.max(8), 2)?;
    let mut diagnostic_bytes = diagnostic_bytes(input_bytes, DIAGNOSTIC_LABEL_BYTES, native_code)?;
    let mut conversation_bytes = 0;
    if snapshot {
        diagnostic_bytes = sum(&[diagnostic_bytes,
            multiply(super::snapshot_json::SNAPSHOT_DIAGNOSTICS.len() as u64 + 4, 2)?])?;
        let cache = sum(&[
            multiply(lexical_items, size_of::<[u8; 32]>() as u64)?,
            multiply(slots, size_of::<usize>() as u64)?,
            multiply(lexical_items, 2 * size_of::<(Uuid, usize)>() as u64)?,
        ])?;
        const UUID_TEXT_BYTES: u64 = 2 * size_of::<Uuid>() as u64 + 4;
        let canonical_items = multiply(lexical_items, 1 + 2 * 8)?;
        let canonical_slots = multiply(canonical_items, 2 + 8)?;
        conversation_bytes = sum(&[
            cache, multiply(canonical_slots, value_slot)?,
            input_bytes, multiply(canonical_items, UUID_TEXT_BYTES)?,
            multiply(input_bytes, 6)?, multiply(canonical_items, UUID_TEXT_BYTES + 2 + 2)?,
            multiply(UUID_TEXT_BYTES.max(8), 2)?,
        ])?;
    }
    let maximum_owned_bytes = sum(&[serde_content_bytes, typed_collection_bytes, value_overlap_bytes,
        boxed_payload_bytes, string_payload_bytes, scratch_bytes, diagnostic_bytes, conversation_bytes])?;
    Ok((maximum_owned_bytes, diagnostic_bytes, conversation_bytes))
}

