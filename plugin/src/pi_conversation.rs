//! Original task conversation binding and bounded cold observation.

use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{
    Error, Result,
    conversation::{Limits, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, EventPayload, Scope},
    durable_host::TaskJournalOwner,
    filesystem::{
        PreparationIo, bind_original_root_conversation_with_native_work,
        native_root_output_binding_operation,
    },
    store::{HistoryIo, HistoryReader},
};
use acyclic_stream::LocalStream;
use std::sync::Arc;

fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}

pub(crate) async fn bind_task_conversation(
    stream: &Arc<LocalStream>,
    authority: (Authority, AuthorityIssuer, Scope),
    custody: NativeAllocation,
    owner: &TaskJournalOwner<LocalStream>,
    volume: &VolumeRef,
    creation: acyclic_fs::OperationId,
    limits: Limits,
    work: &NativeMaterializationWork,
) -> Result<()> {
    if authority.0.id
        != uuid::Uuid::from_bytes(owner.task_id().into_bytes())
            .hyphenated()
            .encode_lower(&mut [0; 36])
    {
        return Err(invalid(
            "Pi conversation binding differs from the original task owner",
        ));
    }
    bind_original_root_conversation_with_native_work(
        stream, authority, custody, owner, volume, creation, limits, work,
    )
    .await
    .map(drop)
}

pub(crate) async fn observe_task_conversation(
    stream: &Arc<LocalStream>,
    authority: (Authority, AuthorityIssuer, Scope),
    custody: NativeAllocation,
    volume: &VolumeRef,
    creation: acyclic_fs::OperationId,
    maximum_event_bytes: u64,
    work: &NativeMaterializationWork,
) -> Result<bool> {
    let ((audience, issuer, scope), _custody) = (authority, custody);
    if audience.kind != AggregateKind::Conversation
        || scope.agent().is_none()
        || !scope.capabilities().contains("conversation:bind")
    {
        return Err(invalid(
            "Pi binding requires its original conversation authority",
        ));
    }
    if maximum_event_bytes == 0 {
        return Err(invalid(
            "Pi binding requires a positive immutable event limit",
        ));
    }
    let io = PreparationIo::from_native_work(work)?;
    let mut native_io = &io;
    let mut observer: Option<&mut dyn HistoryIo> = Some(&mut native_io);
    let verifier = issuer.verifier(&mut observer)?;
    let operation = native_root_output_binding_operation(&audience, volume, creation, work)?;
    let reader = HistoryReader::new(stream, &audience, &verifier, &mut observer)?;
    let (event, _) = reader
        .operation_event(operation, maximum_event_bytes, &mut observer)
        .await?;
    match event.as_ref() {
        None => Ok(false),
        Some(event) => match &event.payload {
            EventPayload::ConversationBound { agent }
                if Some(*agent) == scope.agent()
                    && event.scope.agent() == scope.agent()
                    && event.scope.id() == scope.id()
                    && event.scope.capabilities() == scope.capabilities()
                    && event.causal_parent.is_none() =>
            {
                if event.revision == 0 {
                    return Err(invalid("Pi binding event has no original command revision"));
                }
                Ok(true)
            }
            _ => Err(invalid(
                "Pi binding differs from its original committed creation",
            )),
        },
    }
}
