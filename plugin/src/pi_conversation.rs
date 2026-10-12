//! Canonical Pi task conversation binding, not a shadow conversation journal.

use acyclic_harness::{Error, OperationId, Result,
    conversation::Limits,
    core::{AggregateKind, Authority, AuthorityIssuer, AuthorityVerifier, EventPayload, Scope},
    durable_host::TaskJournalOwner, store::HistoryReader};
use acyclic_stream::{StreamClient, StreamProvider};

struct NativeConversationAuthority {
    authority: Authority,
    issuer: AuthorityIssuer,
    scope: Scope,
    _custody: acyclic_fs::NativeAllocation,
}

struct NativeOwned<T> {
    value: T,
    _custody: acyclic_fs::NativeAllocation,
}

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
    let reader = NativeOwned { value: reader.0, _custody: reader.1 };
    let original = reader.value.operation_event_bounded_with_native_work(
        operation, maximum_event_bytes, work).await?;
    let original = NativeOwned { value: original.0, _custody: original.1 };
    let Some(event) = original.value.as_ref() else { return Ok(None); };
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
    let authority = NativeConversationAuthority { authority: authority.0, issuer: authority.1,
        scope: authority.2, _custody: authority_custody };
    if authority.authority.kind != AggregateKind::Conversation
        || authority.scope.agent().is_none()
        || !authority.scope.capabilities().contains("conversation:bind")
    {
        return Err(native_invalid("Pi binding requires its original conversation authority"));
    }
    let task = work.display(&format_args!("{}", owner.task_id()))
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let task = NativeOwned { value: task.0, _custody: task.1 };
    if task.value != authority.authority.id {
        return Err(native_invalid("Pi conversation binding differs from the original task owner"));
    }
    // Pi's task/audience association is checked above; the SDK owns the
    // canonical original Bind/reopen, current-fence guard and immutable ACK.
    // Its exact command bytes retain their intrinsic original custody until
    // after the genuine publication completes; they are never extracted.
    let encoded = acyclic_harness::filesystem::bind_original_root_conversation_with_native_work(
        stream, (authority.authority, authority.issuer, authority.scope),
        authority._custody, owner, exact_volume, original_creation, limits, work,
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
    let authority = NativeConversationAuthority { authority: authority.0, issuer: authority.1,
        scope: authority.2, _custody: authority_custody };
    if authority.authority.kind != AggregateKind::Conversation
        || authority.scope.agent().is_none()
        || !authority.scope.capabilities().contains("conversation:bind")
    {
        return Err(native_invalid("Pi binding requires its original conversation authority"));
    }
    let verifier = authority.issuer.verifier_with_native_work(work)?;
    let verifier = NativeOwned { value: verifier.0, _custody: verifier.1 };
    let operation = acyclic_harness::filesystem::native_root_output_binding_operation(
        &authority.authority, exact_volume, original_creation, work)?;
    Ok(observe_binding_with_native_work(stream, &authority.authority, &verifier.value,
        &authority.scope, operation, maximum_event_bytes, work).await?.is_some())
}
