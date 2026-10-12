use super::*;
use acyclic_native_runtime::{CaptureInput, ProcessInput};
use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

pub(super) async fn capture<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>,
    request: &NativeProcessRequest,
    work: &NativeMaterializationWork,
    active: &Arc<Mutex<Option<acyclic_native_runtime::ProcessTree>>>,
) -> Result<(NativeProcessResult, NativeAllocation)> {
    let io = PreparationIo::from_native_work(work)?;
    owner.verify(false, &mut Some(&mut io.history_io())).await?;
    let bytes = io.encode(request, owner.input_limits().file_bytes, true)?;
    let request = retain_decoded(io.decode(
        std::marker::PhantomData::<NativeProcessRequest>,
        &bytes,
        crate::wire::ErrorCode::Invalid,
    )?);
    let retained = io.owned_bytes()?;
    let mut custody = io.into_native_custody(retained)?;
    let maximum = request.maximum_output_bytes as usize;
    let input = request.mcp_stdio.as_ref().map_or_else(
        || request.standard_input.as_ref().map_or(0, Vec::len),
        |mcp| mcp.maximum_bytes as usize,
    );
    let mut buffers = [Vec::new(), Vec::new(), Vec::new()];
    for (buffer, capacity) in buffers.iter_mut().zip([maximum, maximum, input]) {
        custody.absorb(&mut work.claim(
            capacity as u64,
            u64::from(capacity != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?)?;
        buffer
            .try_reserve_exact(capacity)
            .map_err(acyclic_fs::NativeTreeMaterializationError::AllocationFailed)?;
        custody.absorb(&mut work.claim(
            (buffer.capacity() - capacity) as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?)?;
    }
    let owner = Arc::clone(owner);
    let active = Arc::clone(active);
    let original = work.clone();
    let runtime = tokio::runtime::Handle::current();
    let capture = move || {
        let failure = std::cell::RefCell::new(None);
        let fail = |cause: Error| {
            *failure.borrow_mut() = Some(cause);
            std::io::Error::from(std::io::ErrorKind::Interrupted)
        };
        let check = || {
            runtime
                .block_on(async {
                    original
                        .cancellation()
                        .check()
                        .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
                    let mut observer = crate::store::history::native_work_io(&original)?;
                    tokio::time::timeout(
                        Duration::from_millis(u64::from(request.control_timeout_ms)),
                        owner.verify(false, &mut Some(&mut observer)),
                    )
                    .await
                    .map_err(|source| Error::NativeTimeout {
                        source,
                        custody: None,
                    })?
                })
                .map_err(&fail)
        };
        let result = (|| {
            check()?;
            let mut exchange = request
                .mcp_stdio
                .as_ref()
                .map(|mcp| mcp.exchange(Some(&original)))
                .transpose()
                .map_err(&fail)?;
            let command_bytes = request.executable.as_os_str().len()
                + request.cwd.as_os_str().len()
                + request.argv.iter().map(String::len).sum::<usize>()
                + request
                    .environment
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>();
            let _command_custody = original
                .claim(
                    command_bytes as u64,
                    1,
                    acyclic_fs::WorkCounters {
                        bytes_copied: command_bytes as u64,
                        ..Default::default()
                    },
                )
                .map_err(|cause| fail(cause.into()))?;
            let mut command = Command::new(&request.executable);
            command
                .args(&request.argv)
                .current_dir(&request.cwd)
                .env_clear()
                .envs(&request.environment)
                .stdin(if exchange.is_some() || request.standard_input.is_some() {
                    Stdio::piped()
                } else {
                    Stdio::null()
                })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut process = None;
            if let Err(error) = acyclic_native_runtime::spawn_process_tree_with_meter_custody(
                &mut command,
                original.runtime_meter().as_ref(),
                &mut process,
            ) {
                *active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = process;
                return Err(error);
            }
            let mut process = process.expect("SDK successful spawn retains its tree");
            let mut next = Instant::now();
            let poll = || {
                if Instant::now() >= next {
                    check()?;
                    next = Instant::now()
                        + Duration::from_millis(u64::from(request.cancellation_poll_ms));
                }
                Ok(())
            };
            let mut response = None;
            let (mut state, first, _exchange_custody) = match exchange.take() {
                Some((state, first, custody)) => (Some(state), first, custody),
                None => (None, Vec::new(), None),
            };
            let input = if state.is_some() {
                Some(CaptureInput::Exchange {
                    initial: &first,
                    maximum: input,
                })
            } else {
                request.standard_input.as_deref().map(CaptureInput::Literal)
            };
            let output = process.wait_with_output(
                Duration::from_millis(u64::from(request.timeout_ms)),
                maximum,
                input,
                poll,
                |chunk| {
                    response.take();
                    match state.as_mut() {
                        Some(state) => {
                            let (bytes, guard) =
                                state.push(chunk, Some(&original)).map_err(&fail)?;
                            response = guard;
                            Ok(ProcessInput {
                                bytes,
                                complete: state.is_complete(),
                            })
                        }
                        None => Ok(ProcessInput::default()),
                    }
                },
                Some(original.runtime_meter().as_ref()),
                Some(buffers),
            );
            if !process.is_reaped()
                || output
                    .as_ref()
                    .is_err_and(|failure| !failure.cleanup_completed)
            {
                *active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(process);
            }
            Ok(output)
        })();
        (result, failure.into_inner(), custody)
    };
    let frame = work.claim(
        std::mem::size_of_val(&capture) as u64,
        1,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let (output, failure, mut custody) = tokio::task::spawn_blocking(move || {
        let _frame = frame;
        capture()
    })
    .await
    .map_err(|source| Error::NativeSupervisor {
        source,
        custody: None,
    })?;
    if let Some(cause) = failure {
        return Err(cause);
    }
    let output = output.map_err(|error| Error::from(work.runtime_error(error)))?;
    let result = match output {
        Ok(output) => NativeProcessResult {
            success: output.status.success(),
            exit_code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
            stop: None,
        },
        Err(failure) => {
            let (message, mut message_custody) = work.display(&failure.error)?;
            custody.absorb(&mut message_custody)?;
            NativeProcessResult {
                success: false,
                exit_code: failure.status.and_then(|status| status.code()),
                stdout: failure.stdout,
                stderr: failure.stderr,
                stop: Some(NativeProcessStop {
                    kind: match failure.error.kind() {
                        std::io::ErrorKind::TimedOut => NativeProcessStopKind::Timeout,
                        std::io::ErrorKind::FileTooLarge => NativeProcessStopKind::OutputLimit,
                        std::io::ErrorKind::Interrupted => NativeProcessStopKind::ControlStopped,
                        _ => NativeProcessStopKind::CaptureFailed,
                    },
                    message,
                    cleanup_completed: failure.cleanup_completed,
                }),
            }
        }
    };
    Ok((result, custody))
}
