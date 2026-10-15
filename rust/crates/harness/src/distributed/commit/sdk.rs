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

pub(super) async fn inspect_idempotency<P: StreamProvider>(
    client: &Arc<P>,
    key: StreamIdempotencyKey,
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
                .native_inspect_idempotency(key, original.runtime_meter())
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
            .inspect_idempotency(key)
            .await
            .map_err(|_| Error::Indeterminate(operation))
    }
    .await?
    .ok_or(Error::Indeterminate(operation))?;
    if let IdempotencyOutcome::Commit(outcome) = &observation.outcome {
        observe_commit(outcome, false, phase, quota)?;
    }
    if let Some(io) = quota.work().as_deref_mut() {
        io.check_cancellation()?;
    }
    Ok(observation.outcome)
}
