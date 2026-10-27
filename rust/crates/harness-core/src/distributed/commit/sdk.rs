use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_call_result<T>(
    original: &acyclic_fs::NativeMaterializationWork,
    result: acyclic_stream::NativeStreamResult<T>,
) -> Result<std::result::Result<acyclic_stream::NativeStreamOutput<T>, StreamError>> {
    result.map_err(|error| Error::NativeWork {
        cause: original.runtime_error(error),
        custody: None,
    })
}

pub(in crate::distributed) async fn inspect_idempotency<P: StreamProvider + ?Sized>(
    client: &Arc<P>,
    key: StreamIdempotencyKey,
    binding: &acyclic_stream::request::CommitBinding,
    operation: OperationId,
    quota: &mut CodecQuota<'_, '_>,
    phase: Option<&mut CommitPhase>,
) -> Result<IdempotencyOutcome> {
    let observation = async {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned()
        {
            let result = client
                .native_inspect_idempotency(key.clone(), original.runtime_meter())
                .await;
            let output = native_call_result(&original, result)?
                .map_err(|_| Error::Indeterminate(operation))?;
            let (observation, retained) =
                crate::store::history::adopt_native_sdk_output(&original, output, quota.work())?;
            quota.adopt(retained)?;
            return Ok(observation);
        }
        if let Some(io) = quota.work().as_deref_mut() {
            io.begin_call()?;
        }
        client
            .inspect_idempotency(key.clone())
            .await
            .map_err(|_| Error::Indeterminate(operation))
    }
    .await?
    .ok_or(Error::Indeterminate(operation))?;
    if let IdempotencyOutcome::Commit(envelope) = &observation.outcome {
        observation::observe_envelope(envelope, None, quota)?;
    }
    if observation.idempotency_key != key || observation.request_digest != binding.digest() {
        return Err(guarded_boundary(
            quota.work(), wire::ErrorCode::Conflict, "commit recovery identity reused",
        ));
    }
    if let IdempotencyOutcome::Commit(envelope) = &observation.outcome {
        let matches = binding.matches_with(envelope, |bytes| {
            if let Some(io) = quota.work().as_deref_mut() {
                io.hashed_bytes(bytes)?;
            }
            Ok::<(), Error>(())
        })?.map_err(|error| operations::stream_error_with_work(error, quota.work()))?;
        if !matches {
            return Err(guarded_boundary(
                quota.work(), wire::ErrorCode::Conflict, "commit recovery identity reused",
            ));
        }
    }
    if matches!(observation.outcome, IdempotencyOutcome::Commit(_))
        && let Some(phase) = phase
    {
        *phase = CommitPhase::Committed;
    }
    if let Some(io) = quota.work().as_deref_mut() {
        io.check_cancellation()?;
    }
    Ok(observation.outcome)
}
