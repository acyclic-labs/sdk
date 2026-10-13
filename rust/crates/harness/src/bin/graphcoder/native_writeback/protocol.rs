use super::*;
pub(super) fn publication_digest(
    binding: &NativePublicationConfig,
    journal: &MaterializationJournal,
) -> Result<[u8; 32]> {
    if journal.plan.operation_id != fs_operation(binding.operation_id)
        || journal.plan.intent
            != (acyclic_fs::MaterializationIntent::GenerationTransition {
                from: binding.from,
                to: binding.to,
            })
        || journal.root_identity != Some(binding.source_root_identity)
        || journal.preparation_identity.is_none()
        || (journal.phase != MaterializationPhase::Preparing
            && journal.preimages.len() != journal.plan.edits.len())
    {
        return Err(Error::Conflict("original publication journal differs from configured source, target or real root identity".into()));
    }
    Ok(*blake3::hash(&canonical_bytes(&(
        "graphcoder/native-publication/v1",
        binding,
        &journal.plan,
        journal.root_identity,
        journal.preparation_identity,
        &journal.preimages,
    ))?)
    .as_bytes())
}
pub(super) fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    acyclic_harness::canonical_json_bytes_bounded(
        value,
        acyclic_harness::JsonBounds {
            maximum_bytes: MAX_FRAME_BYTES as u64,
            maximum_items: u64::MAX,
            maximum_depth: u32::MAX,
        },
        None,
        &mut |_| Ok(()),
    )
}
pub(super) fn fs_operation(operation: OperationId) -> acyclic_fs::OperationId {
    acyclic_fs::OperationId::from_bytes(operation.into_bytes())
}
pub(super) fn interaction_id(binding: &NativePublicationConfig) -> InteractionId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"graphcoder/native-publication/interaction/v1\0");
    hash.update(&binding.caller.into_bytes());
    hash.update(&binding.operation_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    InteractionId::from_bytes(bytes)
}
pub(super) fn approval_operation(operation: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"graphcoder/native-publication/decision/v1\0");
    hash.update(&operation.into_bytes());
    hash.update(&[1]);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
pub(super) fn publication_status(
    original: Option<&MaterializationJournal>,
    approval: Option<&(InteractionTicket, Option<InteractionResolution>)>,
) -> &'static str {
    let status = match original.map(|journal| journal.phase) {
        Some(MaterializationPhase::Applied) => "applied",
        Some(MaterializationPhase::Preparing | MaterializationPhase::Applying) => "recovering",
        Some(MaterializationPhase::RollingBack | MaterializationPhase::RolledBack) => "rejected",
        Some(MaterializationPhase::Prepared) => "pending",
        None => "unopened",
    };
    match approval
        .and_then(|(_, resolution)| resolution.as_ref())
        .map(|resolution| &resolution.outcome)
    {
        Some(InteractionOutcome::Approved) if !matches!(status, "applied" | "recovering") => {
            "approved"
        }
        Some(InteractionOutcome::Indeterminate { .. }) => "pending",
        Some(InteractionOutcome::Approved) | None => status,
        _ => "rejected",
    }
}

pub(super) fn publication_failure(
    binding: &NativePublicationConfig,
    message: String,
    work: WorkCounters,
) -> serde_json::Value {
    serde_json::json!({
        "operation_id": binding.operation_id, "caller": binding.caller, "status": "failed",
        "interaction": null, "resolution": null, "action_digest": null,
        "publication": null, "work": work, "failure": message, "approved_frame_hex": null,
    })
}
