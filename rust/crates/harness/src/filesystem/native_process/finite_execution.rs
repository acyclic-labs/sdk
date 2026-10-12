use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::filesystem::native_execution::{NativeProcessNamespaceScopes, NativeViewPublicationError};
use std::process::{Command, Stdio};

pub(super) struct NativePublicationSlot {
    slot: Box<std::mem::MaybeUninit<NativeViewPublicationError>>,
    failure_custody: Option<NativeAllocation>,
    custody: NativeAllocation,
}

impl NativePublicationSlot {
    fn new(work: &NativeMaterializationWork) -> Result<Self> {
        let custody = work.claim(std::mem::size_of::<NativeViewPublicationError>() as u64)?;
        let failure_custody = Some(work.retain_owned(0)?);
        Ok(Self { slot: Box::new_uninit(), failure_custody, custody })
    }

    pub(super) fn original_failure(&mut self, cause: Error, work: WorkCounters) -> NativeViewPublicationError {
        NativeViewPublicationError {
            cause, accounting_failure: None, work, destination: None, completed: Vec::new(), published: Vec::new(), diagnostic: None,
            custody: self.failure_custody.take(),
        }
    }

    fn retain(self, failure: NativeViewPublicationError) -> Error {
        Error::NativePublication { failure: Box::write(self.slot, failure),
            custody: Some(self.custody) }
    }
}

#[inline(never)]
fn spawn_capture<F, T>(
    callback: F, original: &NativeMaterializationWork,
) -> Result<tokio::task::JoinHandle<T>>
where F: FnOnce() -> T + Send + 'static, T: Send + 'static {
    let custody = original.claim(std::mem::size_of::<(F, NativeAllocation)>() as u64)?;
    let task = (callback, custody);
    Ok(tokio::task::spawn_blocking(move || {
        let (callback, _custody) = task;
        callback()
    }))
}

#[inline(never)]
pub(super) fn execute<'a, P: StreamProvider + 'static>(
    owner: &'a Arc<TaskJournalOwner<P>>, view: &'a dyn NativeProcessView<P>,
    authority: &'a NativeProcessAuthority, request: &'a Arc<NativeProcessRequest>,
    dispatch: &'a EffectDispatch, schema: &'a serde_json::Value,
    supervisor: &'a Arc<NativeSupervisorControl>, original: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (NativeExecution, NativeAllocation)>> {
    finite_binding::require_view_family(view, original)?;
    crate::NativeFutureBundle::new(original, || async move {
        let mut custody = Some(original.retain_owned(0)?);
        let mut publication_slot = None;
        let result: Result<NativeExecution> = async {
            verify(owner, original)?.await?;
            grants(owner, &request.view, original)?;
            {
                view.operation_cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                authority.results.supports_native_stage(original)?;
                if !authority.result_content.supports_native_read(authority.results.volume(), original)? {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                        reason: "native result reader has no original finite SDK engine" });
                }
                publication_slot = Some(NativePublicationSlot::new(original)?);
                original.admit(WorkCounters { bytes_encoded: u64::from(request.maximum_result_bytes),
                    bytes_hashed: u64::from(request.maximum_result_bytes),
                    bytes_copied: u64::from(request.maximum_output_bytes),
                    items_examined: u64::from(request.maximum_output_bytes).saturating_add(16),
                    ..WorkCounters::default() })?;
                view.validate_working_directory(&request.cwd, original)?.await?;
                view.validate_for_dispatch(original)?.await?;

    }
            if view.manifest() != &request.view {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native dispatch view changed after observation" });
            }
            verify(owner, original)?.await?;
            let scopes = Some(view.retain_native_namespace_scopes(original)?.await?);
            let captured = {crate::NativeFutureBundle::new(original,
                    || capture(owner, request, supervisor, view.operation_cancellation(), original, scopes),
                )?.await?
    };
            let capture_custody = captured.1;
            let captured = captured.0;
            let publish = captured.stop.is_none();
            let cleanup_completed = captured.stop.as_ref().is_none_or(|stop| stop.cleanup_completed);
            let bytes = {
                let io = crate::filesystem::PreparationIo::from_native_work(original)?;
                let bytes = io.encode(&captured, u64::from(request.maximum_result_bytes), true)?;
                let decoded = io.decode_preparation::<serde_json::Value>(
                    &bytes, crate::wire::ErrorCode::Invalid)?;
                crate::contract::validate_json_schema_value(schema, &decoded.value, "native process result", Some(&io))?;
                drop(decoded);
                bytes

    };
            drop(captured);
            drop(capture_custody);
            let result = {
                let (result, guard) = authority.results.stage_once_with_native_work(
                    OperationId::from_bytes(dispatch.attempt_id.into_bytes()), "native-process/result.json",
                    &bytes, "application/json", "Native process result", original)?.await?;
                let mut guard = guard;
                custody.as_mut().expect("original custody").absorb(&mut guard)?;
                original.charge(WorkCounters { items_examined: 1,
                    ..WorkCounters::default() })?;
                result.descriptor().verify_with_native_work(&bytes, original)?;
                if result.descriptor().media_type() != "application/json" {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "native effect result must be JSON content" });
                }
                authority.result_content.verify_with_native_work(
                    &result, u64::from(request.maximum_result_bytes), original,
                )?.await?;
                result

    };
            drop(bytes);
            if result.volume() != authority.results.volume() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native result publisher returned another owner volume" });
            }
            let mut published = Vec::new();
            if publish {
                {
                    let admitted = async {
                        verify(owner, original)?.await?;
                        grants(owner, &request.view, original)?;
                        view.publish(owner, original)
                    }.await;
                    let publication = match admitted {
                        Ok(future) => match future.await.unwrap_or_else(|cause| Err(
                            publication_slot.as_mut().expect("preadmitted failure slot").original_failure(cause, original.receipt()))) {
                            Ok(receipt) => {
                                let checked = async {
                                    if view.manifest() != &request.view {
                                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                                            reason: "native publication view changed after publication" });
                                    }
                                    verify(owner, original)?.await
                                }.await;
                                match checked {
                                    Ok(()) => {
                                        let mut publication_custody = receipt.custody;
                                        let completed = receipt.completed;
                                        let actual = receipt.published;
                                        drop(completed);
                                        custody.as_mut().expect("original execution custody")
                                            .absorb(&mut publication_custody)
                                            .expect("same original prepaid publication ledger");
                                        published = actual;
                                        Ok(())
                                    },
                                    Err(cause) => Err(NativeViewPublicationError {
                                        cause, accounting_failure: None, work: receipt.work,
                                        destination: None, completed: receipt.completed, published: receipt.published,
                                        diagnostic: None, custody: Some(receipt.custody),
                                    }),
                                }
                            }
                            Err(failure) => Err(failure),
                        },
                        Err(cause) => Err(publication_slot.as_mut().expect("preadmitted failure slot").original_failure(cause, original.receipt())),
                    };
                    if let Err(mut failure) = publication {
                        failure.diagnostic = Some(result);
                        let mut result_custody = custody.take().expect("original result custody");
                        failure.custody.as_mut().expect("admitted original publication failure custody")
                            .absorb(&mut result_custody).expect("same original admitted ledger");
                        return Err(publication_slot.take().expect("preadmitted failure slot").retain(failure));
                    }

    }
            }
            let work = original.receipt();
            Ok(if cleanup_completed {
                NativeExecution { status: EffectStatus::Succeeded { result }, diagnostic: None,
                    published, work }
            } else {
                NativeExecution { status: EffectStatus::Indeterminate, diagnostic: Some(result),
                    published, work }
            })
        }.await;
        match result {
            Ok(execution) => Ok((execution, custody.expect("successful execution retains its original payload"))),
            Err(Error::NativeWork { cause, custody: None }) => Err(Error::NativeWork { cause, custody }),
            Err(error) => Err(error),
        }
    })
}

#[inline(never)]
fn verify<'a, P: StreamProvider>(
    owner: &'a TaskJournalOwner<P>, work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, ()>> {
    crate::NativeFutureBundle::new(work, || owner.verify_with_native_work(false, work))
}

fn grants<P: StreamProvider>(owner: &TaskJournalOwner<P>, manifest: &NativeViewManifest,
    original: &NativeMaterializationWork) -> Result<()> {
    for mapping in &manifest.volumes {
        original.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
        owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Read, original)?;
        if mapping.writable {
            owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Write, original)?;
        }
    }
    Ok(())
}

async fn capture<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>, request: &Arc<NativeProcessRequest>,
    supervisor: &Arc<NativeSupervisorControl>, cancellation: &acyclic_fs::CancellationToken,
    original: &NativeMaterializationWork,
    scopes: Option<NativeProcessNamespaceScopes>,
) -> Result<(NativeProcessResult, NativeAllocation)> {
    verify(owner, original)?.await?;
    let mut custody = original.retain_owned(0)?;
    let maximum = request.maximum_output_bytes as usize;
    let input = request.mcp_stdio.as_ref().map_or_else(
        || request.standard_input.as_ref().map_or(0, Vec::len),
        |request| request.maximum_bytes as usize,
    );
    let mut allocated = [Vec::new(), Vec::new(), Vec::new()];
    for (buffer, capacity) in allocated.iter_mut().zip([maximum, maximum, input]) {
        if capacity != 0 {
            custody.absorb(&mut original.claim(capacity as u64)
                ?)?;
        }
        buffer.try_reserve_exact(capacity).map_err(|error| Error::from(
            acyclic_fs::NativeTreeMaterializationError::AllocationFailed(error)))?;
        if buffer.capacity() > capacity {
            custody.absorb(
                &mut original.retain_owned((buffer.capacity() - capacity) as u64)?,
            )?;
        }
    }
    let buffers = allocated;
    let check_owner = Arc::clone(owner);
    let invocation = Arc::clone(request);
    let capture_supervisor = Arc::clone(supervisor);
    let invocation_work = original.clone();
    let original = original.clone();
    let cancellation = cancellation.clone();
    let runtime = tokio::runtime::Handle::current();
    let inputs = (buffers, scopes, custody);
    let (output, original_failure, custody) = spawn_capture(move || {
        let (buffers, scopes, custody) = inputs;
        let mut custody = custody;
        let mut scopes = scopes;
        let failure = Mutex::new(None);
        let root_failure = |error: Error| {
            *failure.lock().expect("capture failure custody") = Some(error);
            std::io::Error::from(std::io::ErrorKind::Other)
        };
        let check = || -> std::io::Result<()> {
            let result = runtime.block_on(tokio::time::timeout(
                Duration::from_millis(u64::from(invocation.control_timeout_ms)), async {
                    cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                    verify(check_owner.as_ref(), &original)?.await
                }));
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => { *failure.lock().expect("capture failure custody") = Some(error); Err(std::io::ErrorKind::Interrupted.into()) },
                Err(error) => {
                    *failure.lock().expect("capture failure custody") =
                        Some(Error::NativeTimeout { source: error, custody: None });
                    Err(std::io::ErrorKind::Interrupted.into())
                }
            }
        };
        let output = (|| -> std::io::Result<_> {
            check()?;
            let exchange = invocation.mcp_stdio.as_ref().map(|request|
                request.exchange_with_native_work(&original))
                .transpose().map_err(|error| root_failure(error))?;
            let _command_custody = {
                let mut bytes = invocation.executable.as_os_str().len().checked_add(invocation.cwd.as_os_str().len()).ok_or(std::io::ErrorKind::OutOfMemory)?;
                for argument in &invocation.argv { bytes = bytes.checked_add(argument.len()).ok_or(std::io::ErrorKind::OutOfMemory)?; }
                for (key, value) in &invocation.environment { bytes = bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or(std::io::ErrorKind::OutOfMemory)?; }
                original.charge(WorkCounters { bytes_copied: bytes as u64,
                    items_examined: (invocation.argv.len() + invocation.environment.len()) as u64, ..WorkCounters::default() })
                    .map_err(|error| root_failure(Error::from(error)))?;
                original.claim(bytes as u64).map_err(|error| root_failure(Error::from(error)))?

};
            let mut command = Command::new(&invocation.executable);
            command.args(&invocation.argv).current_dir(&invocation.cwd).env_clear().envs(&invocation.environment)
                .stdin(if invocation.mcp_stdio.is_some() || invocation.standard_input.is_some() { Stdio::piped() } else { Stdio::null() })
                .stdout(Stdio::piped()).stderr(Stdio::piped());
            check()?;
            capture_supervisor.record_capture(NativeCapturePhase::Spawning)?;
            let process_custody = original.retain_owned(0).map_err(|error| root_failure(Error::from(error)))?;
            let mut process = None;
            let spawned = {acyclic_native_runtime::spawn_process_tree_with_meter_custody(
                    &mut command, original.runtime_meter(), &mut process,
                )
};
            if let Err(error) = spawned {
                if let Some(process) = process {
                    let retained = NativeRetainedProcess { process,
                        _scopes: scopes.take(),
                        _custody: process_custody };
                    *capture_supervisor.process.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retained);
                }
                return Err(error);
            }
            let mut process = process.expect("actual canonical spawn installed custody");
            if let Err(error) = capture_supervisor.record_capture(NativeCapturePhase::Running) {
                {
                    let retained = NativeRetainedProcess { process,
                        _scopes: scopes.take(),
                        _custody: process_custody };
                    *capture_supervisor.process.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retained);
                }
                return Err(error);
            }
            let mut next_check = std::time::Instant::now();
            let poll = || {
                capture_supervisor.poll_stop()?;
                if std::time::Instant::now() >= next_check {
                    check()?;
                    next_check = std::time::Instant::now() + Duration::from_millis(u64::from(invocation.cancellation_poll_ms));
                }
                Ok(())
            };
            let timeout = Duration::from_millis(u64::from(invocation.timeout_ms));
            let maximum = invocation.maximum_output_bytes as usize;
            let output = if let Some((exchange, first, exchange_custody)) = exchange {
                let mut exchange = exchange;
                let request = invocation.mcp_stdio.as_ref().expect("original prepared exchange");
                let mut response_custody = None;
                let observe = |chunk: &[u8]| {
                    response_custody.take();
                    let bytes = {exchange.push_with_native_work(chunk, &original).map(|(bytes, guard)| { response_custody = Some(guard); bytes })
}.map_err(|error| root_failure(error))?;
                    Ok(acyclic_native_runtime::ProcessInput { bytes, complete: exchange.is_complete() })
                };
                let output = process.wait_with_output(timeout, maximum,
                    Some(acyclic_native_runtime::CaptureInput::Exchange { initial: &first, maximum: request.maximum_bytes as usize }),
                    poll, observe, Some(original.runtime_meter()), Some(buffers));
                drop(exchange); drop(first); drop(response_custody); drop(exchange_custody);
                output
            } else if let Some(input) = &invocation.standard_input {
                process.wait_with_output(timeout, maximum, Some(acyclic_native_runtime::CaptureInput::Literal(input)),
                    poll, |_| Ok(acyclic_native_runtime::ProcessInput::default()), Some(original.runtime_meter()), Some(buffers))
            } else {
                process.wait_with_output(timeout, maximum, None,
                    poll, |_| Ok(acyclic_native_runtime::ProcessInput::default()), Some(original.runtime_meter()), Some(buffers))
            };
            if !process.is_reaped()
                || output.as_ref().is_err_and(|failure| !failure.cleanup_completed)
            {
                let retained = NativeRetainedProcess { process,
                    _scopes: scopes.take(),
                    _custody: process_custody };
                *capture_supervisor.process.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retained);
            }
            Ok(output)
        })();
        drop(scopes);
        let retained = match &output {
            Ok(Ok(output)) => output.stdout.capacity() + output.stderr.capacity(),
            Ok(Err(output)) => output.stdout.capacity() + output.stderr.capacity(),
            Err(_) => 0,
        };
        if let Err(error) = custody.substitute(retained as u64) {
            *failure.lock().expect("capture failure custody") = Some(Error::from(error));
        }
        (output, failure.into_inner().expect("capture failure custody"), custody)
    }, &invocation_work)?.await.map_err(|source| Error::NativeSupervisor { source, custody: None })?;
    let mut custody = custody;
    supervisor.record_capture(match &output {
        Ok(Ok(_)) => NativeCapturePhase::Drained,
        Ok(Err(failure)) if failure.cleanup_completed => NativeCapturePhase::Drained,
        _ => NativeCapturePhase::Uncertain,
    }).map_err(|error| Error::from(invocation_work.runtime_error(error)))?;
    if let Some(error) = original_failure {
        drop(output);
        custody.substitute(0)?;
        return Err(match error {
            Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody: Some(custody) },
            Error::NativeTimeout { source, custody: None } => Error::NativeTimeout { source, custody: Some(custody) },
            error => error,
        });
    }
    let result = match output {
        Ok(Ok(output)) => NativeProcessResult { success: output.status.success(), exit_code: output.status.code(), stdout: output.stdout, stderr: output.stderr, stop: None },
        Ok(Err(mut failure)) => {
            {
                failure.error = match invocation_work.runtime_error(failure.error) {
                    acyclic_fs::NativeTreeMaterializationError::Io(error)
                        if !error.get_ref().is_some_and(|cause|
                            cause.is::<acyclic_fs::DemandError>() || cause.is::<Error>()
                                || cause.is::<acyclic_fs::NativeTreeMaterializationError>()) => error,
                    cause => {
                        drop(failure.stdout);
                        drop(failure.stderr);
                        custody.substitute(0)?;
                        return Err(Error::NativeWork { cause, custody: Some(custody) });
                    }
                };

}
            let kind = match failure.error.kind() {
                std::io::ErrorKind::TimedOut => NativeProcessStopKind::Timeout,
                std::io::ErrorKind::FileTooLarge => NativeProcessStopKind::OutputLimit,
                std::io::ErrorKind::Interrupted => NativeProcessStopKind::ControlStopped,
                _ => NativeProcessStopKind::CaptureFailed,
            };
            let message = {
                let (message, guard) = invocation_work.display(&failure.error)?;
                let mut guard = guard;
                let mut message = message;
                let mut truncate = None;
                for (index, (offset, _)) in message.char_indices().enumerate() {
                    invocation_work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                    if index == 128 { truncate = Some(offset); break; }
                }
                if let Some(offset) = truncate { message.truncate(offset); }
                custody.absorb(&mut guard)?;
                message

};
            NativeProcessResult { success: false, exit_code: failure.status.and_then(|status| status.code()),
                stdout: failure.stdout, stderr: failure.stderr,
                stop: Some(NativeProcessStop { kind, message, cleanup_completed: failure.cleanup_completed }) }
        }
        Err(error) => return Err(Error::NativeWork { cause: invocation_work.runtime_error(error), custody: Some(custody) }),
    };
    Ok((result, custody))
}

#[cfg(test)]
mod supervisor_tests {
    use super::*;

    #[test]
    fn missing_owned_tree_cannot_promote_uncertain_collection_to_drained() {
        let supervisor = NativeSupervisorControl::new();
        let attempt = crate::EffectAttemptId::from_bytes([41; 16]);
        supervisor.begin(attempt).unwrap();
        supervisor.record_capture(NativeCapturePhase::Uncertain).unwrap();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
            ..acyclic_fs::WorkBudget::UNCHARGED
        }, &acyclic_fs::CancellationToken::new()).unwrap();
        let before = work.receipt();
        supervisor.settle_process(&work, &work).unwrap();
        assert!(matches!(supervisor.snapshot(attempt).unwrap().1, NativeCapturePhase::Uncertain));
        assert_eq!(work.receipt(), before);
    }
}
