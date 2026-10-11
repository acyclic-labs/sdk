use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use crate::filesystem::native_execution::{NativeProcessNamespaceScopes, NativeViewPublicationError};
use std::process::{Command, Stdio};

type ExecutionResult = Result<(NativeExecution, Option<NativeAllocation>)>;

pub(super) enum NativeProcessFuture<'a, T> {
    Ordinary(BoxProviderFuture<'a, Result<T>>),
    Original(crate::NativeFutureBundle<'a, T>),
}

impl<T> Future for NativeProcessFuture<'_, T> {
    type Output = Result<T>;

    fn poll(self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Ordinary(future) => future.as_mut().poll(context),
            Self::Original(future) => std::pin::Pin::new(future).poll(context),
        }
    }
}

fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

fn native_io(error: std::io::Error, original: Option<&NativeMaterializationWork>) -> Error {
    let cause = match original {
        Some(work) => work.runtime_error(error),
        None => acyclic_fs::NativeTreeMaterializationError::from_runtime_error(error),
    };
    native_error(cause)
}

struct NativePublicationSlot {
    slot: Box<std::mem::MaybeUninit<NativeViewPublicationError>>,
    failure_custody: Option<NativeAllocation>,
    custody: NativeAllocation,
}

impl NativePublicationSlot {
    fn new(work: &NativeMaterializationWork) -> Result<Self> {
        let custody = work.claim(std::mem::size_of::<NativeViewPublicationError>() as u64)
            .map_err(native_error)?;
        let failure_custody = Some(work.retain_owned(0).map_err(native_error)?);
        Ok(Self { slot: Box::new_uninit(), failure_custody, custody })
    }

    fn original_failure(&mut self, cause: Error) -> NativeViewPublicationError {
        NativeViewPublicationError {
            cause, destination: None, completed: Vec::new(), diagnostic: None,
            rendering_failure: None,
            custody: self.failure_custody.take().expect("preadmitted original failure alias"),
        }
    }

    fn retain(self, failure: NativeViewPublicationError) -> Error {
        Error::NativePublication { failure: Box::write(self.slot, failure),
            custody: Some(self.custody) }
    }
}

struct NativeCaptureBuffers {
    buffers: Option<[Vec<u8>; 3]>,
    scopes: Option<NativeProcessNamespaceScopes>,
    custody: Option<NativeAllocation>,
}

impl NativeCaptureBuffers {
    fn into_parts(self) -> (Option<[Vec<u8>; 3]>, Option<NativeProcessNamespaceScopes>, Option<NativeAllocation>) {
        (self.buffers, self.scopes, self.custody)
    }
}

struct NativeCaptureTask<F> {
    callback: F,
    _custody: Option<NativeAllocation>,
}

impl<F> NativeCaptureTask<F> {
    fn run<T>(self) -> T where F: FnOnce() -> T {
        let Self { callback, _custody } = self;
        callback()
    }
}

#[inline(never)]
fn spawn_capture<F, T>(
    callback: F, original: Option<&NativeMaterializationWork>,
) -> Result<tokio::task::JoinHandle<T>>
where F: FnOnce() -> T + Send + 'static, T: Send + 'static {
    let custody = original.map(|work| work.claim(std::mem::size_of::<NativeCaptureTask<F>>() as u64))
        .transpose().map_err(native_error)?;
    let task = NativeCaptureTask { callback, _custody: custody };
    Ok(tokio::task::spawn_blocking(move || task.run()))
}

// This is deliberately a synchronous, non-inlined construction boundary. The
// installed ordinary path and finite path both poll this same boxed engine.
#[inline(never)]
pub(super) fn execute<'a, P: StreamProvider + 'static>(
    owner: &'a Arc<TaskJournalOwner<P>>, view: &'a dyn NativeProcessView<P>,
    authority: &'a NativeProcessAuthority, request: &'a Arc<NativeProcessRequest>,
    dispatch: &'a EffectDispatch, schema: &'a serde_json::Value,
    supervisor: &'a Arc<NativeSupervisorControl>, original: Option<&'a NativeMaterializationWork>,
) -> Result<NativeProcessFuture<'a, (NativeExecution, Option<NativeAllocation>)>> {
    if let Some(original) = original {
        let prepared = view.original_native_work().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native process view has no original finite preparation family",
        })?;
        if !prepared.shares_ledger(original) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native process view belongs to another original work family" });
        }
        return crate::NativeFutureBundle::new(original, || execute_in_heap(owner, view, authority, request,
            dispatch, schema, supervisor, Some(original), WorkCounters::default(),
            acyclic_fs::WorkBudget::UNCHARGED)).map(NativeProcessFuture::Original);
    }
    let budget = match request.view.validation_budget() {
        Ok(budget) => budget,
        Err(error) => return Ok(NativeProcessFuture::Ordinary(Box::pin(std::future::ready(Ok((invocation_failure(error, WorkCounters::default()), None)))))),
    };
    match box_execution(|work| execute_in_heap(owner, view, authority, request, dispatch,
        schema, supervisor, None, work, budget), budget, view.operation_cancellation()) {
        Ok(future) => Ok(NativeProcessFuture::Ordinary(future)),
        Err(failure) => Ok(NativeProcessFuture::Ordinary(Box::pin(std::future::ready(Ok((failure, None)))))),
    }
}

#[inline(never)]
fn verify<'a, P: StreamProvider>(
    owner: &'a TaskJournalOwner<P>, original: Option<&'a NativeMaterializationWork>,
) -> Result<NativeProcessFuture<'a, ()>> {
    match original {
        Some(work) => crate::NativeFutureBundle::new(work, || owner.verify_with_native_work(false, work))
            .map(NativeProcessFuture::Original),
        None => Ok(NativeProcessFuture::Ordinary(Box::pin(owner.verify(false)))),
    }
}

fn grants<P: StreamProvider>(owner: &TaskJournalOwner<P>, manifest: &NativeViewManifest,
    original: Option<&NativeMaterializationWork>) -> Result<()> {
    if let Some(work) = original {
        for mapping in &manifest.volumes {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }).map_err(native_error)?;
            owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Read, work)?;
            if mapping.writable {
                owner.require_volume_grant_with_native_work(&mapping.volume, crate::conversation::VolumeOperation::Write, work)?;
            }
        }
        Ok(())
    } else { require_view_grants(owner, manifest) }
}

async fn execute_in_heap<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>, view: &dyn NativeProcessView<P>,
    authority: &NativeProcessAuthority, request: &Arc<NativeProcessRequest>,
    dispatch: &EffectDispatch, schema: &serde_json::Value,
    supervisor: &Arc<NativeSupervisorControl>, original: Option<&NativeMaterializationWork>,
    mut spent: WorkCounters, budget: acyclic_fs::WorkBudget,
) -> ExecutionResult {
    let mut custody = match original {
        Some(work) => Some(work.retain_owned(0).map_err(native_error)?),
        None => None,
    };
    let mut publication_slot = None;
    let frame_bytes = spent.peak_allocation_bytes;
    let result: Result<NativeExecution> = async {
        verify(owner, original)?.await?;
        grants(owner, &request.view, original)?;
        if let Some(work) = original {
            view.operation_cancellation().check().map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(native_error)?;
            authority.results.supports_native_stage(work)?;
            if !authority.result_content.supports_native_read(authority.results.volume(), work)? {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                    reason: "native result reader has no original finite SDK engine" });
            }
            publication_slot = Some(NativePublicationSlot::new(work)?);
            work.admit(WorkCounters { bytes_encoded: u64::from(request.maximum_result_bytes),
                bytes_hashed: u64::from(request.maximum_result_bytes),
                bytes_copied: u64::from(request.maximum_output_bytes),
                items_examined: u64::from(request.maximum_output_bytes).saturating_add(16),
                ..WorkCounters::default() }).map_err(native_error)?;
            view.validate_working_directory_with_native_work(&request.cwd, work)?.await?;
            view.validate_for_dispatch_with_native_work(work)?.await?;
        } else {
            let available = execution_remaining(spent, budget, frame_bytes)?;
            let cwd = match view.validate_working_directory(&request.cwd, available) {
                Ok(future) => {
                    let meter = future.work().clone();
                    future.await.unwrap_or_else(|error| Err((error, meter.receipt())))
                }
                Err(failure) => Err(failure),
            };
            retain_validation_result(cwd, &mut spent, frame_bytes)?;
            let available = execution_remaining(spent, budget, frame_bytes)?;
            let validated = match view.validate_for_dispatch(available) {
                Ok(future) => {
                    let meter = future.work().clone();
                    future.await.unwrap_or_else(|error| Err((error, meter.receipt())))
                }
                Err(failure) => Err(failure),
            };
            retain_validation_result(validated, &mut spent, frame_bytes)?;
            spent.verify(budget).map_err(native_work_error)?;
        }
        if view.manifest() != &request.view {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native dispatch view changed after observation" });
        }
        verify(owner, original)?.await?;
        let scopes = match original {
            Some(work) => Some(view.retain_native_namespace_scopes(work)?.await?),
            None => None,
        };
        let captured = match original {
            Some(work) => crate::NativeFutureBundle::new(work,
                || capture(owner, request, supervisor, view.operation_cancellation(), original, scopes),
            )?.await?,
            None => Box::pin(capture(owner, request, supervisor, view.operation_cancellation(), None, None)).await?,
        };
        let capture_custody = captured.1;
        let captured = captured.0;
        let Some(captured) = captured else {
            let message = "native capture did not retain an OS outcome";
            let invocation_failure = if let Some(work) = original {
                let (message, guard) = work.display(message).map_err(native_error)?;
                let mut guard = guard;
                let message = message;
                custody.as_mut().expect("original custody").absorb(&mut guard).map_err(native_error)?;
                message
            } else { message.into() };
            return Ok(NativeExecution { status: EffectStatus::Indeterminate, diagnostic: None,
                publication_failure: None, invocation_failure: Some(invocation_failure),
                work: original.map_or(spent, NativeMaterializationWork::receipt) });
        };
        let publish = captured.stop.is_none();
        let cleanup_completed = captured.stop.as_ref().is_none_or(|stop| stop.cleanup_completed);
        let (bytes, bytes_custody) = if let Some(work) = original {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let bytes = io.encode_json(&captured, u64::from(request.maximum_result_bytes), true)?;
            let decoded = io.decode_preparation_with_code::<serde_json::Value>(
                &bytes, crate::wire::ErrorCode::Invalid)?;
            crate::contract::validate_json_schema_value_with_io(schema, &decoded.value, "native process result", Some(&io))?;
            drop(decoded);
            let guard = io.into_native_custody(bytes.capacity() as u64)?;
            (bytes, Some(guard))
        } else {
            let bytes = crate::contract::canonical_json_bytes(&captured)?;
            if bytes.len() > request.maximum_result_bytes as usize {
                return Ok(invocation_failure(Error::Invalid("native result exceeded its admitted byte allowance".into()), spent));
            }
            crate::contract::validate_json_schema_value(schema,
                &serde_json::to_value(&captured).map_err(|error| Error::Invalid(error.to_string()))?, "native process result")?;
            (bytes, None)
        };
        let bytes_custody = bytes_custody;
        let bytes = bytes;
        drop(captured);
        drop(capture_custody);
        let result = if let Some(work) = original {
            let (result, guard) = authority.results.stage_once_with_native_work(
                OperationId::from_bytes(dispatch.attempt_id.into_bytes()), "native-process/result.json",
                &bytes, "application/json", "Native process result", work)?.await?;
            let mut guard = guard;
            let result = result;
            custody.as_mut().expect("original custody").absorb(&mut guard).map_err(native_error)?;
            work.charge(WorkCounters { items_examined: 1,
                ..WorkCounters::default() }).map_err(native_error)?;
            result.descriptor().verify_with_native_work(&bytes, work)?;
            if result.descriptor().media_type() != "application/json" {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native effect result must be JSON content" });
            }
            authority.result_content.verify_with_native_work(
                &result, u64::from(request.maximum_result_bytes), work,
            )?.await?;
            result
        } else {
            let result = authority.results.stage(OperationId::from_bytes(dispatch.attempt_id.into_bytes()),
                "native-process/result.json", &bytes, "application/json", "Native process result").await?;
            crate::effects::validate_result_bytes(schema, &result, &bytes)?;
            result
        };
        drop(bytes);
        drop(bytes_custody);
        if result.volume() != authority.results.volume() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native result publisher returned another owner volume" });
        }
        if publish {
            if let Some(work) = original {
                let admitted = async {
                    verify(owner, original)?.await?;
                    grants(owner, &request.view, original)?;
                    view.publish_with_native_work(NativeViewOwner::new(owner), work)
                }.await;
                let publication = match admitted {
                    Ok(future) => match future.await.unwrap_or_else(|cause| Err(
                        publication_slot.as_mut().expect("preadmitted failure slot").original_failure(cause))) {
                        Ok(receipt) => {
                            let checked = async {
                                if view.manifest() != &request.view {
                                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                                        reason: "native publication view changed after publication" });
                                }
                                verify(owner, original)?.await
                            }.await;
                            match checked {
                                Ok(()) => { drop(receipt); Ok(()) },
                                Err(cause) => Err(NativeViewPublicationError {
                                    cause, destination: None, completed: receipt.completed,
                                    diagnostic: None, rendering_failure: None, custody: receipt.custody,
                                }),
                            }
                        }
                        Err(failure) => Err(failure),
                    },
                    Err(cause) => Err(publication_slot.as_mut().expect("preadmitted failure slot").original_failure(cause)),
                };
                if let Err(mut failure) = publication {
                    failure.diagnostic = Some(result);
                    let mut result_custody = custody.take().expect("original result custody");
                    failure.custody.absorb(&mut result_custody)
                        .expect("same original admitted ledger");
                    return Err(publication_slot.take().expect("preadmitted failure slot").retain(failure));
                }
            } else {
                match publish_view(owner, view, &request.view).await {
                    Ok(work) => merge_execution_work(&mut spent, work, frame_bytes)?,
                    Err(mut failure) => {
                        merge_execution_work(&mut spent, failure.work, frame_bytes)?;
                        failure.work = spent;
                        return Ok(NativeExecution { status: EffectStatus::Indeterminate, diagnostic: Some(result),
                            work: spent, publication_failure: Some(failure), invocation_failure: None });
                    }
                }
            }
        }
        let work = original.map_or(spent, NativeMaterializationWork::receipt);
        Ok(if cleanup_completed {
            NativeExecution { status: EffectStatus::Succeeded { result }, diagnostic: None,
                publication_failure: None, invocation_failure: None, work }
        } else {
            NativeExecution { status: EffectStatus::Indeterminate, diagnostic: Some(result),
                publication_failure: None, invocation_failure: None, work }
        })
    }.await;
    match result {
        Ok(execution) => Ok((execution, custody)),
        Err(Error::NativeWork { cause, custody: None }) if original.is_some() => Err(Error::NativeWork { cause, custody }),
        Err(error) if original.is_some() => Err(error),
        Err(error) => Ok((invocation_failure(error, spent), None)),
    }
}

async fn capture<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>, request: &Arc<NativeProcessRequest>,
    supervisor: &Arc<NativeSupervisorControl>, cancellation: &acyclic_fs::CancellationToken,
    original: Option<&NativeMaterializationWork>,
    scopes: Option<NativeProcessNamespaceScopes>,
) -> Result<(Option<NativeProcessResult>, Option<NativeAllocation>)> {
    verify(owner, original)?.await?;
    let mut custody = match original { Some(work) => Some(work.retain_owned(0).map_err(native_error)?), None => None };
    let mut buffers = None;
    if let Some(work) = original {
        let maximum = request.maximum_output_bytes as usize;
        let input = request.mcp_stdio.as_ref().map_or_else(
            || request.standard_input.as_ref().map_or(0, Vec::len),
            |request| request.maximum_bytes as usize,
        );
        let mut allocated = [Vec::new(), Vec::new(), Vec::new()];
        for (buffer, capacity) in allocated.iter_mut().zip([maximum, maximum, input]) {
            if capacity != 0 {
                custody.as_mut().expect("original custody").absorb(&mut work.claim(capacity as u64)
                    .map_err(native_error)?).map_err(native_error)?;
            }
            buffer.try_reserve_exact(capacity).map_err(|error| native_error(
                acyclic_fs::NativeTreeMaterializationError::AllocationFailed(error)))?;
            if buffer.capacity() > capacity {
                custody.as_mut().expect("original custody").absorb(
                    &mut work.retain_owned((buffer.capacity() - capacity) as u64).map_err(native_error)?,
                ).map_err(native_error)?;
            }
        }
        buffers = Some(allocated);
    }
    let check_owner = Arc::clone(owner);
    let invocation = Arc::clone(request);
    let capture_supervisor = Arc::clone(supervisor);
    let invocation_work = original.cloned();
    let original = original.cloned();
    let cancellation = cancellation.clone();
    let runtime = tokio::runtime::Handle::current();
    let inputs = NativeCaptureBuffers { buffers, scopes, custody };
    let (output, original_failure, custody) = spawn_capture(move || {
        let (buffers, scopes, custody) = inputs.into_parts();
        let mut custody = custody;
        let mut scopes = scopes;
        let mut buffers = buffers;
        let failure = Mutex::new(None);
        let root_failure = |error: Error| {
            *failure.lock().expect("capture failure custody") = Some(error);
            std::io::Error::from(std::io::ErrorKind::Other)
        };
        let check = || -> std::io::Result<()> {
            let result = runtime.block_on(tokio::time::timeout(
                Duration::from_millis(u64::from(invocation.control_timeout_ms)), async {
                    cancellation.check().map_err(acyclic_fs::NativeTreeMaterializationError::from).map_err(native_error)?;
                    verify(check_owner.as_ref(), original.as_ref())?.await
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
            let exchange = invocation.mcp_stdio.as_ref().map(|request| {
                match original.as_ref() {
                    Some(work) => request.exchange_with_native_work(work)
                        .map(|(exchange, first, guard)| (exchange, first, Some(guard))),
                    None => request.exchange().map(|(exchange, first)| (exchange, first, None)),
                }
            }).transpose().map_err(|error| match original {
                Some(_) => root_failure(error),
                None => std::io::Error::other(error),
            })?;
            let _command_custody = if let Some(work) = original.as_ref() {
                let mut bytes = invocation.executable.as_os_str().len().checked_add(invocation.cwd.as_os_str().len()).ok_or(std::io::ErrorKind::OutOfMemory)?;
                for argument in &invocation.argv { bytes = bytes.checked_add(argument.len()).ok_or(std::io::ErrorKind::OutOfMemory)?; }
                for (key, value) in &invocation.environment { bytes = bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or(std::io::ErrorKind::OutOfMemory)?; }
                work.charge(WorkCounters { bytes_copied: bytes as u64,
                    items_examined: (invocation.argv.len() + invocation.environment.len()) as u64, ..WorkCounters::default() })
                    .map_err(|error| root_failure(native_error(error)))?;
                Some(work.claim(bytes as u64).map_err(|error| root_failure(native_error(error)))?)
            } else { None };
            let mut command = Command::new(&invocation.executable);
            command.args(&invocation.argv).current_dir(&invocation.cwd).env_clear().envs(&invocation.environment)
                .stdin(if invocation.mcp_stdio.is_some() || invocation.standard_input.is_some() { Stdio::piped() } else { Stdio::null() })
                .stdout(Stdio::piped()).stderr(Stdio::piped());
            check()?;
            capture_supervisor.record_capture(NativeCapturePhase::Spawning)?;
            let process_custody = original.as_ref().map(|work| work.retain_owned(0))
                .transpose().map_err(|error| root_failure(native_error(error)))?;
            let mut process = None;
            let spawned = match original.as_ref() {
                Some(work) => acyclic_native_runtime::spawn_process_tree_with_meter_custody(
                    &mut command, work.runtime_meter(), &mut process,
                ),
                None => acyclic_native_runtime::spawn_process_tree(&mut command).map(|tree| process = Some(tree)),
            };
            if let Err(error) = spawned {
                if let Some(process) = process {
                    let retained = NativeRetainedProcess { process,
                        _scopes: scopes.take(),
                        _custody: process_custody.expect("preadmitted original process alias") };
                    *capture_supervisor.process.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retained);
                }
                return Err(error);
            }
            let mut process = process.expect("actual canonical spawn installed custody");
            if let Err(error) = capture_supervisor.record_capture(NativeCapturePhase::Running) {
                if original.is_some() {
                    let retained = NativeRetainedProcess { process,
                        _scopes: scopes.take(),
                        _custody: process_custody.expect("preadmitted original process alias") };
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
                let exchange_custody = exchange_custody;
                let mut exchange = exchange;
                let first = first;
                let request = invocation.mcp_stdio.as_ref().expect("original prepared exchange");
                let mut response_custody = None;
                let observe = |chunk: &[u8]| {
                    response_custody.take();
                    let bytes = match original.as_ref() {
                        Some(work) => exchange.push_with_native_work(chunk, work).map(|(bytes, guard)| { response_custody = Some(guard); bytes }),
                        None => exchange.push(chunk),
                    }.map_err(|error| match original {
                        Some(_) => root_failure(error),
                        None => std::io::Error::other(error),
                    })?;
                    Ok(acyclic_native_runtime::ProcessInput { bytes, complete: exchange.is_complete() })
                };
                let output = match original.as_ref() {
                    Some(work) => process.wait_with_exchange_observed_with_meter(timeout, maximum,
                        request.maximum_bytes as usize, &first, buffers.take().expect("admitted capture buffers"), work.runtime_meter(), poll, observe),
                    None => process.wait_with_exchange_observed(timeout, maximum, request.maximum_bytes as usize, &first, poll, observe),
                };
                drop(exchange); drop(first); drop(response_custody); drop(exchange_custody);
                output
            } else if let Some(input) = &invocation.standard_input {
                match original.as_ref() {
                    Some(work) => process.wait_with_input_observed_with_meter(timeout, maximum, input,
                        buffers.take().expect("admitted capture buffers"), work.runtime_meter(), poll),
                    None => process.wait_with_input_observed(timeout, maximum, input, poll),
                }
            } else {
                match original.as_ref() {
                    Some(work) => process.wait_with_output_observed_with_meter(timeout, maximum,
                        buffers.take().expect("admitted capture buffers"), work.runtime_meter(), poll),
                    None => process.wait_with_output_observed(timeout, maximum, poll),
                }
            };
            if original.is_some() && !process.is_reaped() {
                let retained = NativeRetainedProcess { process,
                    _scopes: scopes.take(),
                    _custody: process_custody.expect("preadmitted original process alias") };
                *capture_supervisor.process.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retained);
            }
            Ok(output)
        })();
        // All unused buffers have physically dropped before releasing their quota.
        drop(buffers);
        drop(scopes);
        if let Some(guard) = custody.as_mut() {
            let retained = match &output {
                Ok(Ok(output)) => output.stdout.capacity() + output.stderr.capacity(),
                Ok(Err(output)) => output.stdout.capacity() + output.stderr.capacity(),
                Err(_) => 0,
            };
            if let Err(error) = guard.substitute(retained as u64) {
                *failure.lock().expect("capture failure custody") = Some(native_error(error));
            }
        }
        (output, failure.into_inner().expect("capture failure custody"), custody)
    }, invocation_work.as_ref())?.await.map_err(|source| Error::NativeSupervisor { source, custody: None })?;
    let mut custody = custody;
    let output = output;
    supervisor.record_capture(match &output {
        Ok(Ok(_)) => NativeCapturePhase::Drained,
        Ok(Err(failure)) if failure.cleanup_completed => NativeCapturePhase::Drained,
        _ => NativeCapturePhase::Uncertain,
    }).map_err(|error| native_io(error, invocation_work.as_ref()))?;
    if let Some(error) = original_failure {
        drop(output);
        if let Some(guard) = custody.as_mut() { guard.substitute(0).map_err(native_error)?; }
        return Err(match error {
            Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody },
            Error::NativeTimeout { source, custody: None } => Error::NativeTimeout { source, custody },
            error => error,
        });
    }
    let result = match output {
        Ok(Ok(output)) => NativeProcessResult { success: output.status.success(), exit_code: output.status.code(), stdout: output.stdout, stderr: output.stderr, stop: None },
        Ok(Err(mut failure)) => {
            if let Some(work) = invocation_work.as_ref() {
                failure.error = match work.runtime_error(failure.error) {
                    acyclic_fs::NativeTreeMaterializationError::Io(error)
                        if !error.get_ref().is_some_and(|cause|
                            cause.is::<acyclic_fs::DemandError>() || cause.is::<Error>()
                                || cause.is::<acyclic_fs::NativeTreeMaterializationError>()) => error,
                    cause => {
                        drop(failure.stdout);
                        drop(failure.stderr);
                        if let Some(guard) = custody.as_mut() { guard.substitute(0).map_err(native_error)?; }
                        return Err(Error::NativeWork { cause, custody });
                    }
                };
            }
            let kind = match failure.error.kind() {
                std::io::ErrorKind::TimedOut => NativeProcessStopKind::Timeout,
                std::io::ErrorKind::FileTooLarge => NativeProcessStopKind::OutputLimit,
                std::io::ErrorKind::Interrupted => NativeProcessStopKind::ControlStopped,
                _ => NativeProcessStopKind::CaptureFailed,
            };
            let message = if let Some(work) = invocation_work.as_ref() {
                let (message, guard) = work.display(&failure.error).map_err(native_error)?;
                let mut guard = guard;
                let mut message = message;
                let mut truncate = None;
                for (index, (offset, _)) in message.char_indices().enumerate() {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                        .map_err(native_error)?;
                    if index == 128 { truncate = Some(offset); break; }
                }
                if let Some(offset) = truncate { message.truncate(offset); }
                custody.as_mut().expect("original capture custody").absorb(&mut guard).map_err(native_error)?;
                message
            } else { failure.error.to_string().chars().take(128).collect() };
            NativeProcessResult { success: false, exit_code: failure.status.and_then(|status| status.code()),
                stdout: failure.stdout, stderr: failure.stderr,
                stop: Some(NativeProcessStop { kind, message, cleanup_completed: failure.cleanup_completed }) }
        }
        Err(error) if invocation_work.is_some() => return Err(Error::NativeWork {
            cause: invocation_work.as_ref().expect("original capture family").runtime_error(error), custody }),
        Err(_) => return Ok((None, custody)),
    };
    Ok((Some(result), custody))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn work_with_allocations(allocations: u64, extra_bytes: u64) -> NativeMaterializationWork {
        NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: allocations,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + extra_bytes,
            ..acyclic_fs::WorkBudget::UNCHARGED
        }, &acyclic_fs::CancellationToken::new()).unwrap()
    }

    #[test]
    fn actual_capture_frame_denial_precedes_scheduler_and_callback() {
        let work = work_with_allocations(1, 0);
        let called = Arc::new(AtomicBool::new(false));
        let callback_called = Arc::clone(&called);
        let before = work.receipt();
        let error = spawn_capture(move || callback_called.store(true, Ordering::SeqCst), Some(&work))
            .err().expect("the actual blocking frame needs admission");
        assert!(matches!(error, Error::NativeWork { .. }));
        assert!(!called.load(Ordering::SeqCst));
        assert_eq!(work.receipt(), before);
    }

    #[test]
    fn actual_runtime_denial_moves_original_cause_without_error_box() {
        let work = work_with_allocations(1, 0);
        let before = work.receipt();
        let error = work.runtime_meter().reserve_operations(
            acyclic_native_runtime::NativeOperationKind::Read, 1,
        ).expect_err("the original ledger admits no read operation");
        assert!(error.get_ref().is_none());
        let projected = native_io(error, Some(&work));
        assert!(matches!(projected, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(_), custody: None,
        }));
        assert_eq!(work.receipt(), before);
    }

    #[test]
    fn actual_publication_failure_slot_denial_keeps_original_prefix() {
        let work = work_with_allocations(1, 0);
        let before = work.receipt();
        assert!(matches!(NativePublicationSlot::new(&work),
            Err(Error::NativeWork { custody: None, .. })));
        assert_eq!(work.receipt(), before);
    }

    #[test]
    fn preadmitted_publication_failure_retains_both_guards_after_cancellation() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            allocation_operations: 2,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes()
                + std::mem::size_of::<NativeViewPublicationError>() as u64,
            ..acyclic_fs::WorkBudget::UNCHARGED
        }, &cancellation).unwrap();
        let mut slot = NativePublicationSlot::new(&work).unwrap();
        let before = work.receipt();
        cancellation.cancel();
        let mut failure = slot.original_failure(
            native_error(acyclic_fs::NativeTreeMaterializationError::InvalidLayout));
        failure.rendering_failure = Some(native_error(acyclic_fs::NativeTreeMaterializationError::Cancelled));
        let error = slot.retain(failure);
        let Error::NativePublication { failure, custody: Some(box_custody) } = error else {
            panic!("the original preallocated publication slot must be retained");
        };
        assert!(failure.custody.shares_ledger(&box_custody));
        assert_eq!(box_custody.bytes(), std::mem::size_of::<NativeViewPublicationError>() as u64);
        assert_eq!(work.receipt(), before);
        drop(failure);
        drop(box_custody);
    }
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
        supervisor.settle_process_with_native_work(&work).unwrap();
        assert!(matches!(supervisor.snapshot(attempt).unwrap().1, NativeCapturePhase::Uncertain));
        assert_eq!(work.receipt(), before);
    }
}
