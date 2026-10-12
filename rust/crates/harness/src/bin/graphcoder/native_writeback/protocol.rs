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
pub(super) fn publication_options(
    binding: &NativePublicationConfig,
) -> acyclic_fs::MaterializeOptions {
    acyclic_fs::MaterializeOptions {
        destination: binding.operation_directory.join("target"),
        expected_root_identity: None,
        maximum_directory_entries: binding.maximum_directory_entries,
        maximum_extent_spans: binding.maximum_extent_spans,
        transfer_bytes: binding.transfer_bytes,
    }
}
pub(super) fn publication<'a>(
    binding: &'a NativePublicationConfig,
    excluded: &'a [&'a str],
    options: &'a acyclic_fs::MaterializeOptions,
    cancellation: &'a acyclic_fs::CancellationToken,
) -> acyclic_fs::NativeWorkspacePublication<'a> {
    acyclic_fs::NativeWorkspacePublication {
        root: &binding.root,
        operation_directory: &binding.operation_directory,
        source_root_identity: acyclic_fs::NativeRootIdentity::from_bytes(
            binding.source_root_identity,
        ),
        operation_id: fs_operation(binding.operation_id),
        from: binding.from,
        to: binding.to,
        excluded_names: excluded,
        options,
        budget: binding.budget,
        cancellation,
    }
}
pub(super) fn publication_base(
    binding: &NativePublicationConfig,
    work: WorkCounters,
) -> NativePublicationResult {
    NativePublicationResult {
        operation_id: binding.operation_id,
        caller: binding.caller,
        status: "unopened",
        interaction: None,
        resolution: None,
        action_digest: None,
        publication: None,
        work,
        failure: None,
        approved_frame_hex: None,
    }
}
pub(super) fn publication_failure(
    binding: &NativePublicationConfig,
    message: String,
    work: WorkCounters,
) -> NativePublicationResult {
    let mut result = publication_base(binding, work);
    result.status = "failed";
    result.failure = Some(message);
    result
}
pub(super) fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}
pub(super) fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
