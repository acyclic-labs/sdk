use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::filesystem::native_execution::{NativeProcessNamespaceScopes, NativeViewPublicationError};
use std::process::{Command, Stdio};

#[inline(never)]
pub(super) async fn execute<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>, view: &dyn NativeProcessView<P>,
    authority: &NativeProcessAuthority, request: &Arc<NativeProcessRequest>,
    dispatch: &EffectDispatch, schema: &serde_json::Value,
    supervisor: &Arc<NativeSupervisorControl>, original: &NativeMaterializationWork,
) -> Result<(NativeExecution, NativeAllocation)> {
    finite_binding::require_view_family(view, original)?;
        let mut custody = Some(original.retain_owned(0)?);
        let mut publication_slot = None;
        let result: Result<NativeExecution> = async {
            owner.verify_with_native_work(false, original).await?;
            grants(owner, &request.view, original)?;
            {
                view.operation_cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                authority.results.supports_native_stage(original)?;
                if !authority.result_content.supports_native_read(authority.results.volume(), original)? {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                        reason: "native result reader has no original finite SDK engine" });
                }
                publication_slot = Some(crate::filesystem::native_execution::reserve_box::<NativeViewPublicationError>(original)?);
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
            owner.verify_with_native_work(false, original).await?;
            let scopes = view.retain_native_namespace_scopes(original)?.await?;
            let captured = capture(owner, request, supervisor, view.operation_cancellation(), original, scopes).await?;
            let (captured, capture_custody) = captured;
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
                let publication = async {
                    owner.verify_with_native_work(false, original).await?;
                    grants(owner, &request.view, original)?;
                    view.publish(owner, original)?.await
                }.await.unwrap_or_else(|cause| Err(NativeViewPublicationError {
                    cause, accounting_failure: None, work: original.receipt(), destination: None,
                    completed: Vec::new(), published: Vec::new(), diagnostic: None, custody: None,
                }));
                let publication = match publication {
                    Ok(receipt) => {
                        let checked = async {
                            if view.manifest() != &request.view {
                                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                                    reason: "native publication view changed after publication" });
                            }
                            owner.verify_with_native_work(false, original).await
                        }.await;
                        match checked {
                            Ok(()) => {
                                let mut publication_custody = receipt.custody;
                                drop(receipt.completed);
                                custody.as_mut().expect("original execution custody")
                                    .absorb(&mut publication_custody).expect("same original prepaid publication ledger");
                                published = receipt.published;
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
                };
                if let Err(mut failure) = publication {
                    failure.diagnostic = Some(result);
                    let mut result_custody = custody.take().expect("original result custody");
                    if let Some(held) = failure.custody.as_mut() {
                        held.absorb(&mut result_custody).expect("same original admitted ledger");
                    } else { failure.custody = Some(result_custody); }
                    let (slot, custody) = publication_slot.take().expect("preadmitted failure slot");
                    return Err(Error::NativePublication { failure: Box::write(slot, failure), custody: Some(custody) });
                }
            }
            Ok(if cleanup_completed {
                NativeExecution { status: EffectStatus::Succeeded { result }, diagnostic: None,
                    published }
            } else {
                NativeExecution { status: EffectStatus::Indeterminate, diagnostic: Some(result),
                    published }
            })
        }.await;
        match result {
            Ok(execution) => Ok((execution, custody.expect("successful execution retains its original payload"))),
            Err(Error::NativeWork { cause, custody: None }) => Err(Error::NativeWork { cause, custody }),
            Err(error) => Err(error),
        }
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
    scopes: NativeProcessNamespaceScopes,
) -> Result<(NativeProcessResult, NativeAllocation)> {
    owner.verify_with_native_work(false, original).await?;
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
    let capture = move || {
        let (buffers, scopes, custody) = inputs;
        let mut custody = custody;
        let failure = Mutex::new(None);
        let root_failure = |error: Error| {
            *failure.lock().expect("capture failure custody") = Some(error);
            std::io::Error::from(std::io::ErrorKind::Other)
        };
        let check = || -> std::io::Result<()> {
            let result = runtime.block_on(tokio::time::timeout(
                Duration::from_millis(u64::from(invocation.control_timeout_ms)), async {
                    cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from)?;
                    check_owner.verify_with_native_work(false, &original).await
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
            capture_supervisor.record_quiescence(false)?;
            let process_custody = original.retain_owned(0).map_err(|error| root_failure(Error::from(error)))?;
            let mut process = None;
            let spawned = {acyclic_native_runtime::spawn_process_tree_with_meter_custody(
                    &mut command, original.runtime_meter(), &mut process,
                )
};
            if let Err(error) = spawned {
                if let Some(process) = process {
                    let retained = NativeRetainedProcess { process,
                        _scopes: scopes,
                        _custody: process_custody };
                    *capture_supervisor.process.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retained);
                }
                return Err(error);
            }
            let mut process = process.expect("actual canonical spawn installed custody");
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
                let output = {process.wait_with_exchange_observed_with_meter(timeout, maximum,
                        request.maximum_bytes as usize, &first, buffers, original.runtime_meter(), poll, observe)
};
                drop(exchange); drop(first); drop(response_custody); drop(exchange_custody);
                output
            } else if let Some(input) = &invocation.standard_input {
                {process.wait_with_input_observed_with_meter(timeout, maximum, input,
                        buffers, original.runtime_meter(), poll)
}
            } else {
                {process.wait_with_output_observed_with_meter(timeout, maximum,
                        buffers, original.runtime_meter(), poll)
}
            };
            if !process.is_reaped()
                || output.as_ref().is_err_and(|failure| !failure.cleanup_completed)
            {
                let retained = NativeRetainedProcess { process,
                    _scopes: scopes,
                    _custody: process_custody };
                *capture_supervisor.process.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retained);
            }
            Ok(output)
        })();
        let retained = match &output {
            Ok(Ok(output)) => output.stdout.capacity() + output.stderr.capacity(),
            Ok(Err(output)) => output.stdout.capacity() + output.stderr.capacity(),
            Err(_) => 0,
        };
        if let Err(error) = custody.substitute(retained as u64) {
            *failure.lock().expect("capture failure custody") = Some(Error::from(error));
        }
        (output, failure.into_inner().expect("capture failure custody"), custody)
    };
    let mut task = (capture, invocation_work.retain_owned(0)?);
    let bytes = std::mem::size_of_val(&task) as u64;
    task.1.grow(bytes)?;
    let (output, original_failure, custody) = tokio::task::spawn_blocking(move || {
        let (capture, _custody) = task;
        capture()
    }).await.map_err(|source| Error::NativeSupervisor { source, custody: None })?;
    let mut custody = custody;
    supervisor.record_quiescence(match &output {
        Ok(Ok(_)) => true,
        Ok(Err(failure)) => failure.cleanup_completed,
        Err(_) => false,
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
    fn missing_owned_tree_cannot_report_quiescence() {
        let supervisor = NativeSupervisorControl::new();
        let attempt = crate::EffectAttemptId::from_bytes([41; 16]);
        supervisor.begin(attempt).unwrap();
        supervisor.record_quiescence(false).unwrap();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
            ..Default::default()
        }, &acyclic_fs::CancellationToken::new()).unwrap();
        let before = work.receipt();
        supervisor.settle_process(&work, &work).unwrap();
        assert!(!supervisor.snapshot(attempt).unwrap().1);
        assert_eq!(work.receipt(), before);
    }
}
