use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) type PreparedNativeRecovery =
    acyclic_stream::NativeProviderFuture<'static, Option<acyclic_stream::IdempotencyObservation>>;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(crate) type PreparedNativeRecovery = std::convert::Infallible;

pub(crate) type SdkCallError = std::result::Result<StreamError, Error>;

pub(crate) fn sdk_source(error: &SdkCallError) -> &StreamError {
    match error {
        Ok(source) => source,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Err(Error::NativeStream { source, .. }) => source,
        Err(_) => unreachable!("non-SDK failures use the outer result"),
    }
}

pub(crate) fn sdk_error(error: SdkCallError, work: &Option<&mut dyn HistoryIo>) -> Error {
    match error {
        Ok(source) => operations::stream_error_with_work(source, work),
        Err(error) => error,
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_call_result<T>(
    original: &acyclic_fs::NativeMaterializationWork,
    result: acyclic_stream::NativeStreamResult<T>,
) -> Result<std::result::Result<acyclic_stream::NativeStreamOutput<T>, SdkCallError>> {
    match crate::store::history::native_sdk_result(original, result) {
        Ok(output) => Ok(Ok(output)),
        Err(error @ Error::NativeStream { .. }) => Ok(Err(Err(error))),
        Err(error) => Err(error),
    }
}

pub(crate) async fn inspect_idempotency<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    key: StreamIdempotencyKey,
    custody: &mut CodecQuota<'_, '_>,
    phase: Option<&mut CommitPhase>,
    recovery: Option<PreparedNativeRecovery>,
) -> Result<std::result::Result<Option<IdempotencyOutcome>, SdkCallError>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if recovery.is_none()
        && custody
            .work()
            .as_deref()
            .is_some_and(|observer| observer.requires_prepared_native_dispatch())
    {
        return Err(coordinator_boundary(
            custody.work(),
            wire::ErrorCode::Invalid,
            "accepted completion requires its pre-action SDK recovery",
        ));
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = recovery;
    let inspected = async {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = custody
            .work()
            .as_deref()
            .and_then(|observer| observer.original_native_work())
            .cloned()
        {
            let inspected = native_call_result(
                &original,
                match recovery {
                    Some(recovery) => {
                        drop(key);
                        recovery.await
                    }
                    None => {
                        client
                            .native_inspect_idempotency(key, original.runtime_meter())
                            .await
                    }
                },
            )?;
            let output = match inspected {
                Ok(output) => output,
                Err(error) => return Ok(Err(error)),
            };
            if let Some(observation) = output.value()
                && let IdempotencyOutcome::Commit(outcome) = &observation.outcome
            {
                observe_commit(outcome, false, phase, custody)?;
            }
            let (observation, retained) =
                crate::store::history::adopt_native_sdk_output(&original, output, custody.work())?;
            custody.adopt(retained)?;
            return Ok(Ok(observation));
        }
        if let Some(work) = custody.work().as_deref_mut() {
            work.begin_call()?;
        }
        match client.inspect_idempotency(key).await {
            Err(source) => Ok::<_, Error>(Err(SdkCallError::Ok(source))),
            Ok(observation) => {
                if let Some(observation) = &observation
                    && let IdempotencyOutcome::Commit(outcome) = &observation.outcome
                {
                    observe_commit(outcome, false, phase, custody)?;
                }
                Ok(Ok(observation))
            }
        }
    }
    .await?;
    let observation = match inspected {
        Ok(Some(observation)) => observation,
        Ok(None) => return Ok(Ok(None)),
        Err(error) => return Ok(Err(error)),
    };
    if let Some(work) = custody.work().as_deref_mut() {
        work.check_cancellation()?;
    }
    Ok(Ok(Some(observation.outcome)))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn receive_native_commit(
    original: &acyclic_fs::NativeMaterializationWork,
    committed: acyclic_stream::NativeStreamResult<acyclic_stream::CommitOutcome>,
    phase: Option<&mut CommitPhase>,
    vectors: u64,
    custody: &mut CodecQuota<'_, '_>,
) -> Result<std::result::Result<acyclic_stream::CommitOutcome, SdkCallError>> {
    let mut phase = phase;
    if let Ok(Err(error)) = &committed
        && *error != StreamError::Unavailable
        && let Some(phase) = phase.as_deref_mut()
    {
        *phase = CommitPhase::Uncommitted;
    }
    let released = custody.relinquish(vectors);
    let output = match native_call_result(original, committed)? {
        Ok(output) => output,
        Err(error) => return Ok(Err(error)),
    };
    let observed = observe_commit(output.value(), true, phase, custody);
    released.and(observed)?;
    let (outcome, retained) =
        crate::store::history::adopt_native_sdk_output(original, output, custody.work())?;
    custody.adopt(retained)?;
    Ok(Ok(outcome))
}

pub(crate) async fn resolve_commit<P: StreamProvider>(
    committed: std::result::Result<acyclic_stream::CommitOutcome, SdkCallError>,
    client: &Arc<P>,
    key: StreamIdempotencyKey,
    operation: Option<OperationId>,
    phase: Option<&mut CommitPhase>,
    recovery: Option<PreparedNativeRecovery>,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<acyclic_stream::CommitOutcome> {
    let outcome = match committed {
        Ok(outcome) => outcome,
        Err(error) => {
            let Some(operation) =
                operation.filter(|_| *sdk_source(&error) == StreamError::Unavailable)
            else {
                return Err(sdk_error(error, quota.work()));
            };
            match inspect_idempotency(client, key, quota, phase, recovery).await? {
                Ok(Some(IdempotencyOutcome::Commit(outcome))) => outcome,
                Ok(Some(_)) => {
                    return Err(coordinator_boundary(
                        quota.work(),
                        wire::ErrorCode::Conflict,
                        "commit recovery identity reused",
                    ));
                }
                Ok(None) | Err(_) => return Err(Error::Indeterminate(operation)),
            }
        }
    };
    if let Some(io) = quota.work().as_deref_mut() {
        io.check_cancellation()?;
    }
    Ok(outcome)
}
