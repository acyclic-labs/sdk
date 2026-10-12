use crate::{Error, Result, core::Scope, conversation::VolumeRef, durable_host::TaskJournalOwner};
use acyclic_fs::NativeMaterializationWork;
use acyclic_stream::StreamProvider;

pub(super) fn boundary(code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    Error::NativeBoundary { code, reason }
}

/// Canonical native ROOT bind identity; this derives an operation, not authority.
pub fn native_root_output_binding_operation(
    audience: &crate::core::Authority, volume: &VolumeRef,
    creation: acyclic_fs::OperationId, work: &NativeMaterializationWork,
) -> Result<crate::OperationId> {
    let (encoded, _custody) = encode_record(
        &("harness/native-root-output-bind/v2", audience, volume,
            crate::OperationId::from_bytes(creation.into_bytes())), work,
    )?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: encoded.len() as u64, ..Default::default()
    })?;
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&blake3::hash(&encoded).as_bytes()[..16]);
    Ok(crate::OperationId::from_bytes(bytes))
}

/// Authenticates the original ROOT Bind and returns its canonical command bytes.
#[allow(clippy::too_many_arguments, reason = "original creator, owner and paid authority are distinct")]
pub async fn bind_original_root_conversation_with_native_work<P: StreamProvider>(
    stream: &std::sync::Arc<P>,
    authority: (crate::core::Authority, crate::core::AuthorityIssuer, Scope),
    authority_custody: acyclic_fs::NativeAllocation,
    owner: &TaskJournalOwner<P>,
    private_volume: &VolumeRef,
    original_creation: acyclic_fs::OperationId,
    limits: crate::conversation::Limits,
    work: &NativeMaterializationWork,
) -> Result<acyclic_stream::OwnedBytes> {
    let _authority_custody = authority_custody;
    let (audience, issuer, scope) = authority;
    let invalid = |reason| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid, reason,
    };
    if audience.kind != crate::core::AggregateKind::Conversation
        || scope.agent().is_none()
        || !scope.capabilities().contains("conversation:bind")
        || limits.file_bytes == 0
    {
        return Err(invalid("ROOT binding requires its original conversation authority and finite event allowance"));
    }
    owner.verify(false, &mut Some(&mut crate::filesystem::PreparationIo::from_native_work(work).map(crate::store::HistoryWork::new)?)).await?;
    let verifier = allocate(work, |io| issuer.verifier(&mut Some(&mut io.history_io())))?;
    let operation = native_root_output_binding_operation(
        &audience, private_volume, original_creation, work,
    )?;
    let mut reader_io = crate::filesystem::PreparationIo::from_native_work(work).map(crate::store::HistoryWork::new)?;
    let reader = crate::store::HistoryReader::new(stream, &audience, &verifier.0, &mut Some(&mut reader_io))?;
    let reader_custody = reader_io.into_custody()?;
    let event = reader.operation_event_bounded_with_native_work(
        operation, limits.file_bytes, work,
    ).await?;
    let original_revision = match event.0.as_ref() {
        None => None,
        Some(event) => match &event.payload {
            crate::core::EventPayload::ConversationBound { agent }
                if Some(*agent) == scope.agent()
                    && event.scope.agent() == scope.agent()
                    && event.scope.id() == scope.id()
                    && event.scope.capabilities() == scope.capabilities()
                    && event.causal_parent.is_none() =>
            {
                Some(event.revision.checked_sub(1)
                    .ok_or_else(|| invalid("original ROOT binding has no command revision"))?)
            }
            _ => return Err(invalid("ROOT binding differs from the protected original creation")),
        },
    };
    drop(event);
    drop(reader);
    drop(reader_custody);
    let mut aggregate = crate::store::StreamAggregate::open_with_native_work(
        stream, audience,
        verifier.0, crate::core::SchemaRegistry::new(), work,
    ).await?;
    aggregate.0.set_limits(limits)?;
    if original_revision.is_none()
        && aggregate.0.reducer().conversation().and_then(|state| state.agent).is_some()
    {
        return Err(invalid("ROOT conversation changed during original binding admission"));
    }
    let key = work.display(&format_args!("harness/native-root-output-bind/v2/{operation}"))?;
    let key = (crate::IdempotencyKey::new(key.0)?, key.1);
    let agent = scope.agent().expect("validated ROOT binding actor");
    let command = crate::core::Command {
        operation_id: operation, idempotency_key: key.0,
        expected_revision: original_revision.unwrap_or_else(|| aggregate.0.reducer().revision()),
        causal_parent: None, action: crate::core::Action::BindConversation { agent }, scope,
    };
    let mut io = crate::filesystem::PreparationIo::from_native_work(work).map(crate::store::HistoryWork::new)?;
    let encoded = crate::store::history::canonical_bytes_with_work(
        &command, crate::store::default_projection_read_limits().maximum_bytes,
        &mut Some(&mut io),
    )?;
    let encoded_custody = crate::store::history::HistoryIo::take_native_custody(
        &mut io, encoded.capacity() as u64,
    )?;
    let encoded = acyclic_stream::OwnedBytes::from_vec_with_guard(
        encoded, encoded_custody, work.runtime_meter(),
    ).map_err(|failure| native_owned_bytes_failure(failure, work))?;
    crate::store::history::HistoryIo::retain_owned(
        &mut io, std::mem::size_of::<crate::core::ApplyResult>() as u64)?;
    let applied = aggregate.0.execute(command, Some(owner), &mut Some(&mut io)).await?;
    drop(applied);
    Ok(encoded)
}

pub(super) fn native_codec_error_custody(error: Error, custody: acyclic_fs::NativeAllocation) -> Error {
    match error {
        Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody: Some(custody) },
        Error::NativeDecode { code, source, custody: None } => Error::NativeDecode { code, source, custody: Some(custody) },
        Error::NativeEncode { code, source, custody: None } => Error::NativeEncode { code, source, custody: Some(custody) },
        error => error,
    }
}

pub(super) fn encode_record<T: serde::Serialize + ?Sized>(
    value: &T, work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, acyclic_fs::NativeAllocation)> {
    let io = super::PreparationIo::from_native_work(work)?;
    io.retain_owned(crate::contract::sdk_json_diagnostic_bytes())?;
    let remaining = work.remaining()?;
    let bytes = crate::store::history::canonical_bytes_for_bounds_with_work(value,
        crate::contract::JsonBounds { maximum_bytes: remaining.bytes_encoded,
            maximum_items: remaining.items_examined, maximum_depth: 256 },
        &mut Some(&mut &io));
    match bytes {
        Ok(bytes) => {
            let custody = io.into_native_custody(bytes.capacity() as u64)?;
            Ok((bytes, custody))
        }
        Err(error) => {
            let held = io.owned_bytes()?;
            Err(native_codec_error_custody(error, io.into_native_custody(held)?))
        }
    }
}

pub(super) fn native_owned_bytes_failure(
    failure: acyclic_stream::PreparedOwnedBytesFailure<acyclic_fs::NativeAllocation>,
    work: &NativeMaterializationWork,
) -> Error {
    match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => Error::from(work.runtime_error(error)),
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
        => boundary(crate::wire::ErrorCode::Storage, "native owned byte buffer violated its fixed initialized ownership invariant"),
    }
}

pub(super) fn allocate<T>(work: &NativeMaterializationWork,
    copy: impl FnOnce(&super::PreparationIo) -> Result<T>,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    let io = super::PreparationIo::from_native_work(work)?;
    let copied = copy(&io)?;
    let retained = io.owned_bytes()?;
    Ok((copied, io.into_native_custody(retained)?))
}
