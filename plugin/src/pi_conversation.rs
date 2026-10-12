//! Canonical Pi task conversation binding, not a shadow conversation journal.

use acyclic_harness::{Error, OperationId, Result,
    conversation::Limits,
    core::{AggregateKind, Authority, AuthorityIssuer, AuthorityVerifier, EventPayload, Scope},
    durable_host::TaskJournalOwner, store::HistoryReader};
use acyclic_stream::{StreamClient, StreamProvider};

fn native_invalid(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason }
}

/// Reads the original atomic operation index, including archived bindings.
async fn observe_binding_with_native_work<P: StreamProvider>(
    stream: &StreamClient<P>, authority: &Authority, verifier: &AuthorityVerifier,
    scope: &Scope, operation: OperationId, maximum_event_bytes: u64,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<Option<u64>> {
    if maximum_event_bytes == 0 {
        return Err(native_invalid("Pi binding requires a positive immutable event limit"));
    }
    let reader = HistoryReader::new_with_native_work(stream, authority, verifier, work)?;
    let original = reader.0.operation_event_bounded_with_native_work(
        operation, maximum_event_bytes, work).await?;
    let Some(event) = original.0.as_ref() else { return Ok(None); };
    match &event.payload {
        EventPayload::ConversationBound { agent }
            if Some(*agent) == scope.agent()
                && event.scope.agent() == scope.agent()
                && event.scope.id() == scope.id()
                && event.scope.capabilities() == scope.capabilities()
                && event.causal_parent.is_none() =>
        {
            event.revision.checked_sub(1).map(Some)
                .ok_or_else(|| native_invalid("Pi binding event has no original command revision"))
        }
        _ => Err(native_invalid("Pi binding differs from its original committed creation")),
    }
}

/// Original finite native ledger; no ordinary scope clone, fresh meter, or shadow binding cache.
#[allow(clippy::too_many_arguments, reason = "original producer custody and guarded creator are explicit")]
pub(crate) async fn bind_task_conversation<P: StreamProvider + 'static>(
    stream: &StreamClient<P>, authority: (Authority, AuthorityIssuer, Scope),
    authority_custody: acyclic_fs::NativeAllocation, owner: &TaskJournalOwner<P>,
    exact_volume: &acyclic_harness::conversation::VolumeRef,
    original_creation: acyclic_fs::OperationId, limits: Limits,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let authority = (authority, authority_custody);
    if authority.0.0.kind != AggregateKind::Conversation
        || authority.0.2.agent().is_none()
        || !authority.0.2.capabilities().contains("conversation:bind")
    {
        return Err(native_invalid("Pi binding requires its original conversation authority"));
    }
    let task = work.display(&format_args!("{}", owner.task_id()))
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    if task.0 != authority.0.0.id {
        return Err(native_invalid("Pi conversation binding differs from the original task owner"));
    }
    // Pi's task/audience association is checked above; the SDK owns the
    // canonical original Bind/reopen, current-fence guard and immutable ACK.
    // Its exact command bytes retain their intrinsic original custody until
    // after the genuine publication completes; they are never extracted.
    let encoded = acyclic_harness::filesystem::bind_original_root_conversation_with_native_work(
        stream, authority.0,
        authority.1, owner, exact_volume, original_creation, limits, work,
    ).await?;
    drop(encoded);
    Ok(())
}

/// Cold observation of the original binding uses the same authenticated immutable operation query; it cannot populate a current owner's cache.
#[allow(clippy::too_many_arguments, reason = "original authority custody and immutable operation are explicit")]
pub(crate) async fn observe_task_conversation<P: StreamProvider>(
    stream: &StreamClient<P>, authority: (Authority, AuthorityIssuer, Scope),
    authority_custody: acyclic_fs::NativeAllocation,
    exact_volume: &acyclic_harness::conversation::VolumeRef,
    original_creation: acyclic_fs::OperationId,
    maximum_event_bytes: u64, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<bool> {
    let authority = (authority, authority_custody);
    if authority.0.0.kind != AggregateKind::Conversation
        || authority.0.2.agent().is_none()
        || !authority.0.2.capabilities().contains("conversation:bind")
    {
        return Err(native_invalid("Pi binding requires its original conversation authority"));
    }
    let verifier = authority.0.1.verifier_with_native_work(work)?;
    let operation = acyclic_harness::filesystem::native_root_output_binding_operation(
        &authority.0.0, exact_volume, original_creation, work)?;
    Ok(observe_binding_with_native_work(stream, &authority.0.0, &verifier.0,
        &authority.0.2, operation, maximum_event_bytes, work).await?.is_some())
}
