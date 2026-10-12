use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::filesystem::native_execution::NativeViewPublicationError;
use std::process::{Command, Stdio};

pub(super) async fn execute<P: StreamProvider>(
    owner: &Arc<TaskJournalOwner<P>>, invocation: &Arc<Invocation<P>>,
    authority: &NativeProcessAuthority, attempt: crate::EffectAttemptId,
    supervisor: &Arc<NativeSupervisorControl>, original: &NativeMaterializationWork,
) -> Result<((EffectStatus, Option<crate::conversation::FileRef>, Vec<NativeProcessPublishedVolume>), NativeAllocation)> {
    let Invocation { view, request, result_schema: schema, .. } = invocation.as_ref();
    finite_binding::require_view_family(view.as_ref(), original)?;
    let mut custody = Some(original.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?);
    let result: Result<_> = async {
        owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(original)?)).await?;
        grants(owner, &request.view, original)?;
        view.original_native_work().cancellation().check().map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
        authority.results.supports_stage(Some(&crate::filesystem::PreparationIo::from_native_work(original)?))?;
        if !authority.result_content.supports_read(authority.results.volume(), Some(&crate::filesystem::PreparationIo::from_native_work(original)?))? {
            return Err(boundary(crate::wire::ErrorCode::Unsupported, "native result reader has no original finite SDK engine"));
        }
        let publication_slot = crate::filesystem::native_execution::reserve_box::<NativeViewPublicationError>(original)?;
        original.admit(WorkCounters { bytes_encoded: u64::from(request.maximum_result_bytes),
            bytes_hashed: u64::from(request.maximum_result_bytes),
            bytes_copied: u64::from(request.maximum_output_bytes),
            items_examined: u64::from(request.maximum_output_bytes).saturating_add(16),
            ..WorkCounters::default() })?;
        view.validate_working_directory(&request.cwd, original)?;
        view.validate_for_dispatch(original)?.await?;
        if view.manifest() != &request.view {
            return Err(boundary(crate::wire::ErrorCode::Conflict, "native dispatch view changed after observation"));
        }
        owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(original)?)).await?;
        let scopes = view.retain_native_namespace_scopes(original)?.await?;
        let captured = capture(owner, invocation, supervisor, view.original_native_work().cancellation(), original, scopes).await?;
        let (captured, capture_custody) = captured;
        let publish = captured.stop.is_none();
        let cleanup_completed = captured.stop.as_ref().is_none_or(|stop| stop.cleanup_completed);
        let bytes = {
            let io = crate::filesystem::PreparationIo::from_native_work(original)?;
            let value = io.value_json(&captured, u64::from(request.maximum_result_bytes))?;
            crate::executor::with_history_io(Some(&io), |work| crate::contract::validate_json_schema_value(schema, &value, "native process result", work))?;
            io.encode(&value, u64::from(request.maximum_result_bytes), true)?
        };
        drop(captured);
        drop(capture_custody);
        let result = {
            let io = crate::filesystem::PreparationIo::from_native_work(original)?;
            let (result, guard) = authority.results.stage_once(
                OperationId::from_bytes(attempt.into_bytes()), "native-process/result.json",
                &bytes, "application/json", "Native process result", Some(&io))?.await?;
            let guard = guard.ok_or(boundary(crate::wire::ErrorCode::Storage, "native result publisher omitted admitted custody"))?;
            let retained = guard.bytes();
            guard.into_retained();
            let mut guard = io.take_native_custody(retained)?;
            custody.as_mut().expect("original custody").absorb(&mut guard)?;
            original.charge(WorkCounters { items_examined: 1,
                ..WorkCounters::default() })?;
            result.descriptor().verify(&bytes, &mut Some(&mut &io))?;
            if result.descriptor().media_type() != "application/json" {
                return Err(boundary(crate::wire::ErrorCode::Invalid, "native effect result must be JSON content"));
            }
            authority.result_content.verify(
                &result, u64::from(request.maximum_result_bytes), Some(&io),
            )?.await?;
            result
        };
        drop(bytes);
        if result.volume() != authority.results.volume() {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized, "native result publisher returned another owner volume"));
        }
        let mut published = Vec::new();
        if publish {
            let publication = async {
                owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(original)?)).await?;
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
                            return Err(boundary(crate::wire::ErrorCode::Conflict, "native publication view changed after publication"));
                        }
                        owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(original)?)).await
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
                let (slot, custody) = publication_slot;
                return Err(Error::NativePublication { failure: Box::write(slot, failure), custody: Some(custody) });
            }
        }
        let (status, diagnostic) = if cleanup_completed {
            (EffectStatus::Succeeded { result }, None)
        } else {
            (EffectStatus::Indeterminate, Some(result))
        };
        Ok((status, diagnostic, published))
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
        owner.require_volume_grant(&mapping.volume, crate::conversation::VolumeOperation::Read, &mut Some(&mut crate::store::history::native_work_io(original)?))?;
        if mapping.writable {
            owner.require_volume_grant(&mapping.volume, crate::conversation::VolumeOperation::Write, &mut Some(&mut crate::store::history::native_work_io(original)?))?;
        }
    }
    Ok(())
}

async fn capture<P: StreamProvider>(
    owner: &Arc<TaskJournalOwner<P>>, invocation: &Arc<Invocation<P>>,
    supervisor: &Arc<NativeSupervisorControl>, cancellation: &acyclic_fs::CancellationToken,
    original: &NativeMaterializationWork,
    scopes: (Vec<acyclic_fs::native_mount::NativeMountWorkScope>, acyclic_fs::NativeAllocation),
) -> Result<(NativeProcessResult, NativeAllocation)> {
    owner.verify(false, &mut Some(&mut crate::store::history::native_work_io(original)?)).await?;
    let mut custody = original.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let request = &invocation.request;
    let maximum = request.maximum_output_bytes as usize;
    let input = request.mcp_stdio.as_ref().map_or_else(
        || request.standard_input.as_ref().map_or(0, Vec::len),
        |request| request.maximum_bytes as usize,
    );
    let mut buffers = [Vec::new(), Vec::new(), Vec::new()];
    for (buffer, capacity) in buffers.iter_mut().zip([maximum, maximum, input]) {
        if capacity != 0 {
            custody.absorb(&mut original.claim(capacity as u64, u64::from((capacity as u64) != 0), acyclic_fs::WorkCounters::UNCHARGED)
                ?)?;
        }
        buffer.try_reserve_exact(capacity).map_err(|error| Error::from(
            acyclic_fs::NativeTreeMaterializationError::AllocationFailed(error)))?;
        if buffer.capacity() > capacity {
            custody.absorb(
                &mut original.claim((buffer.capacity() - capacity) as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?,
            )?;
        }
    }
    let check_owner = Arc::clone(owner);
    let capture_invocation = Arc::clone(invocation);
    let capture_supervisor = Arc::clone(supervisor);
    let capture_work = original.clone();
    let cancellation = cancellation.clone();
    let runtime = tokio::runtime::Handle::current();
    let inputs = (buffers, scopes, custody);
    let capture = move || {
        let invocation = &capture_invocation.request;
        let (buffers, scopes, custody) = inputs;
        let mut custody = custody;
        let failure = std::cell::RefCell::new(None);
        let root_failure = |error: Error, kind: std::io::ErrorKind| {
            *failure.borrow_mut() = Some(error);
            std::io::Error::from(kind)
        };
        let check = || -> std::io::Result<()> {
            let result = runtime.block_on(tokio::time::timeout(
                Duration::from_millis(u64::from(invocation.control_timeout_ms)),
                async {
                    cancellation
                        .check()
                        .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
                    check_owner
                        .verify(
                            false,
                            &mut Some(&mut crate::store::history::native_work_io(&capture_work)?),
                        )
                        .await
                },
            ));
            result
                .unwrap_or_else(|source| {
                    Err(Error::NativeTimeout {
                        source,
                        custody: None,
                    })
                })
                .map_err(|error| root_failure(error, std::io::ErrorKind::Interrupted))
        };
        let output = (|| -> std::io::Result<_> {
            check()?;
            let exchange = invocation
                .mcp_stdio
                .as_ref()
                .map(|request| request.exchange(Some(&capture_work)))
                .transpose()
                .map_err(|error| root_failure(error, std::io::ErrorKind::Other))?;
            let _command_custody = {
                let mut bytes = invocation
                    .executable
                    .as_os_str()
                    .len()
                    .checked_add(invocation.cwd.as_os_str().len())
                    .ok_or(std::io::ErrorKind::OutOfMemory)?;
                for argument in &invocation.argv {
                    bytes = bytes
                        .checked_add(argument.len())
                        .ok_or(std::io::ErrorKind::OutOfMemory)?;
                }
                for (key, value) in &invocation.environment {
                    bytes = bytes
                        .checked_add(key.len())
                        .and_then(|bytes| bytes.checked_add(value.len()))
                        .ok_or(std::io::ErrorKind::OutOfMemory)?;
                }
                capture_work
                    .charge(WorkCounters {
                        bytes_copied: bytes as u64,
                        items_examined: (invocation.argv.len() + invocation.environment.len())
                            as u64,
                        ..WorkCounters::default()
                    })
                    .map_err(|error| root_failure(Error::from(error), std::io::ErrorKind::Other))?;
                capture_work
                    .claim(
                        bytes as u64,
                        u64::from(bytes != 0),
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )
                    .map_err(|error| root_failure(Error::from(error), std::io::ErrorKind::Other))?
            };
            let mut command = Command::new(&invocation.executable);
            command
                .args(&invocation.argv)
                .current_dir(&invocation.cwd)
                .env_clear()
                .envs(&invocation.environment)
                .stdin(
                    if invocation.mcp_stdio.is_some() || invocation.standard_input.is_some() {
                        Stdio::piped()
                    } else {
                        Stdio::null()
                    },
                )
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            check()?;
            capture_supervisor.record_quiescence(false)?;
            let mut process = None;
            let spawned = acyclic_native_runtime::ProcessTree::spawn_controlled(
                &mut command,
                Some(capture_work.runtime_meter().as_ref()),
                &mut process,
            );
            if let Err(error) = spawned {
                if let Some(process) = process {
                    let retained = (process, scopes.0, scopes.1);
                    capture_supervisor
                        .active
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_mut()
                        .expect("original capture attempt")
                        .process = Some(retained);
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
                    let bytes = exchange.push(chunk, Some(&capture_work))
                        .map(|(bytes, guard)| { response_custody = guard; bytes })
                        .map_err(|error| root_failure(error, std::io::ErrorKind::Other))?;
                    Ok(acyclic_native_runtime::ProcessInput { bytes, complete: exchange.is_complete() })
                };
                let output = process.wait_with_output(timeout, maximum,
                    Some(acyclic_native_runtime::CaptureInput::Exchange { initial: &first, maximum: request.maximum_bytes as usize }),
                    poll, observe, Some(capture_work.runtime_meter().as_ref()), Some(buffers));
                drop(exchange); drop(first); drop(response_custody); drop(exchange_custody);
                output
            } else {
                let input = invocation.standard_input.as_deref().map(acyclic_native_runtime::CaptureInput::Literal);
                process.wait_with_output(timeout, maximum, input,
                    poll, |_| Ok(acyclic_native_runtime::ProcessInput::default()), Some(capture_work.runtime_meter().as_ref()), Some(buffers))
            };
            if !process.is_reaped()
                || output.as_ref().is_err_and(|failure| !failure.cleanup_completed)
            {
                let retained = (process, scopes.0, scopes.1);
                capture_supervisor.active.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_mut().expect("original capture attempt").process = Some(retained);
            }
            Ok(output)
        })();
        let retained = match &output {
            Ok(Ok(output)) => output.stdout.capacity() + output.stderr.capacity(),
            Ok(Err(output)) => output.stdout.capacity() + output.stderr.capacity(),
            Err(_) => 0,
        };
        if let Err(error) = custody.bytes().checked_sub(retained as u64).ok_or(acyclic_fs::NativeTreeMaterializationError::from(acyclic_fs::WorkError::Overflow)).and_then(|bytes| custody.release(bytes)) {
            *failure.borrow_mut() = Some(Error::from(error));
        }
        (output, failure.into_inner(), custody)
    };
    let mut task = (capture, original.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?);
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
    }).map_err(|error| Error::from(original.runtime_error(error)))?;
    if let Some(error) = original_failure {
        drop(output);
        custody.release(custody.bytes())?;
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
                failure.error = match original.runtime_error(failure.error) {
                    acyclic_fs::NativeTreeMaterializationError::Io(error)
                        if !error.get_ref().is_some_and(|cause|
                            cause.is::<acyclic_fs::DemandError>() || cause.is::<Error>()
                                || cause.is::<acyclic_fs::NativeTreeMaterializationError>()) => error,
                    cause => {
                        drop(failure.stdout);
                        drop(failure.stderr);
                        custody.release(custody.bytes())?;
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
                let (message, guard) = original.display(&failure.error)?;
                let mut guard = guard;
                let mut message = message;
                let mut truncate = None;
                for (index, (offset, _)) in message.char_indices().enumerate() {
                    original.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
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
        Err(error) => return Err(Error::NativeWork { cause: original.runtime_error(error), custody: Some(custody) }),
    };
    Ok((result, custody))
}

#[cfg(test)]
mod supervisor_tests {
    use super::*;

    #[test]
    fn missing_owned_tree_cannot_report_quiescence() {
        let supervisor = NativeSupervisorControl { active: Mutex::new(None), completion: tokio::sync::Notify::new() };
        let attempt = crate::EffectAttemptId::from_bytes([41; 16]);
        supervisor.begin(attempt).unwrap();
        supervisor.record_quiescence(false).unwrap();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
            ..Default::default()
        }, &acyclic_fs::CancellationToken::new()).unwrap();
        supervisor.settle_process(&work, &work).unwrap();
        assert!(!supervisor.snapshot(attempt).unwrap().1);
    }
}
