use super::*;

pub(in crate::demand) struct SourceRequestStop {
        pub(in crate::demand) stopped: AtomicBool,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

pub(in crate::demand) struct StopOriginalRequestOnDrop(Arc<SourceRequestStop>);

impl Drop for StopOriginalRequestOnDrop {
        fn drop(&mut self) { self.0.stopped.store(true, Ordering::Release); }
    }

pub(in crate::demand) async fn await_source_job<T, F, C>(
        spawned: std::io::Result<F>, cancelled: &mut C, on_cancel: impl FnOnce(),
    ) -> (Result<T, DemandError>, bool)
    where F: Future<Output = std::io::Result<Result<T, DemandError>>> + Unpin,
        C: Future<Output = ()> + Unpin {
        let mut selected_cancellation = false;
        let result = match spawned {
            Ok(mut queued) => tokio::select! {
                result = &mut queued => result.map_err(native_demand_error).and_then(std::convert::identity),
                () = cancelled => {
                    selected_cancellation = true;
                    on_cancel();
                    queued.await.map_err(native_demand_error).and_then(std::convert::identity)
                },
            },
            Err(error) => Err(native_demand_error(error)),
        };
        (result, selected_cancellation)
    }

impl NativeDemandSource {
    #[cfg(test)]
                pub(in crate::demand) async fn occupy_all_requests(
                    &self,
                ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
                    self.inner.requests.clone().acquire_many_owned(64).await
                }

    #[cfg(test)]
                pub(in crate::demand) async fn wait_for_cancelled_worker(
                    &self,
                    started: tokio::sync::oneshot::Sender<()>,
                    stopped: tokio::sync::oneshot::Sender<()>,
                ) {
                    let cancellation = CancellationToken::new();
                    let _ = self
                        .request(&NamespacePath::new(Vec::new(), self.inner.limits).expect("empty root path"), WorkBudget::UNBOUNDED, &cancellation, move |_, _, work| {
                            let _ = started.send(());
                            while work.check().is_ok() {
                                std::thread::yield_now();
                            }
                            let _ = stopped.send(());
                            Ok(())
                        })
                        .await;
                }

    #[cfg(test)]
                pub(in crate::demand) fn cancel_during_page(
                    &self,
                    source: SourceReference,
                    directory: &NamespacePath,
                    cursor: SourceCursor,
                    cancellation: &CancellationToken,
                ) -> DemandResult<()> {
                    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, cancellation);
                    let result = (|| {
                        let metadata = self.metadata(directory, &mut work)?.ok_or(DemandError::Absent)?;
                        let lease =
                            self.cursor_lease(source, directory, version(&metadata, &mut work)?, Some(cursor), 4096, &mut work)?;
                        let mut visited = 0;
                        let result = self.page_entries(&mut *lease.state()?, 4096, &mut work, || {
                            visited += 1;
                            if visited == 2 {
                                cancellation.cancel();
                            }
                        });
                        work.record(publication_cleanup())?;
                        drop(lease);
                        match result {
                            Err(DemandError::Cancelled) => Ok(()),
                            Err(error) => Err(error),
                            Ok(_) => Err(DemandError::InvalidRequest),
                        }
                    })();
                    work.finish(result)
                }

    /// Opens a native root using the supplied accounting context.
                pub async fn open<W: crate::facade::FsWork + Send>(
                    path: std::borrow::Cow<'_, Path>, profile: FilesystemProfile, limits: VolumeLimits,
                    reference: Option<SourceReference>, observer: Option<Arc<dyn DemandDirectoryObserver>>,
                    work: W, cancellation: &CancellationToken,
                ) -> W::Output<Self, DemandError> {
                    let mut accounting = work.accounting(cancellation);
                    let result = async {
                        cancellation.check().map_err(|_| DemandError::Cancelled)?;
                        let reference = reference.unwrap_or_else(|| SourceReference {
                            identity: *uuid::Uuid::new_v4().as_bytes(), epoch: 0,
                        });
                        if let Some(original) = accounting.native_work() {
                            let mut work = NativeSourceWork::from_native_materialization_work(original);
                            work.check()?;
                            work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
                            let input = SourceRootInput::copy(path.as_ref(), &mut work)?;
                            acyclic_native_runtime::initialize_native_io_services()?;
                            let cancellation = work.materialization_work()?;
                            let mut cancelled = cancellation.cancellation().prepare_cancelled_with_native_work(&cancellation)
                                .map_err(cancellation_error)?;
                            let worker = work.owned();
                            work.check()?;
                            let task_meter = cancellation.runtime_meter();
                            let spawned = acyclic_native_runtime::run_blocking_io(
                                crate::obs::in_context(crate::obs::caller_context(), move || {
                                    let mut work = NativeSourceWork::from_native_materialization_work(&worker);
                                    Self::open_blocking(input, profile, limits, reference, observer, &mut work)
                                }), Some((task_meter, false)),
                            );
                            let (result, selected_cancellation) = await_source_job(spawned, &mut cancelled, || {}).await;
                            cancelled.retire_waker();
                            drop(cancelled);
                            let native_failure = work.with_original_leaf(DemandWork::take_native_io_failure);
                            return native_failure.map_or(result, Err).and_then(|value| {
                                if selected_cancellation { return Err(DemandError::Cancelled); }
                                work.check().map(|()| value)
                            });
                        }
                        let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
                        cancellation.check().map_err(|_| DemandError::Cancelled)?;
                        WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }.verify(budget)?;
                        acyclic_native_runtime::initialize_native_io_services()?;
                        let mut work = DemandWork::new(budget, cancellation);
                        let input = (|| {
                            let path = match path {
                                std::borrow::Cow::Owned(path) => { work.adopt(path.capacity() as u64)?; path }
                                std::borrow::Cow::Borrowed(path) => crate::native_host::clone_path_with_work(path, &mut work)?.0,
                            };
                            work.allocate(std::mem::size_of::<NativeDemandReadMeter>() as u64)?;
                            Ok(path)
                        })();
                        let path = match input {
                            Ok(path) => path,
                            Err(error) => return accept_source_result(work.finish(Err(error)), &mut accounting),
                        };
                        let shared = Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(work)));
                        let mut prepared = {
                            let mut work = shared.lock();
                            match work.prepare_cancelled(cancellation) {
                                Ok(prepared) => {
                                    work.budget.items_examined -= prepared.cleanup_items();
                                    prepared
                                }
                                Err(error) => {
                                    drop(work);
                                    return accept_source_result(crate::NativeMaterializationWork::from_meter(shared)
                                        .finish_source(Err(cancellation_error(error))), &mut accounting);
                                }
                            }
                        };
                        let worker = shared.clone();
                        let task_meter: Arc<dyn acyclic_native_runtime::NativeReadMeter> = shared.clone();
                        let task = acyclic_native_runtime::run_blocking_io(
                            crate::obs::in_context(crate::obs::caller_context(), move || {
                                let mut work = NativeSourceWork::ordinary(&worker);
                                let input = SourceRootInput::from_admitted(path, &mut work)?;
                                Self::open_blocking(input, profile, limits, reference, observer, &mut work)
                            }), Some((task_meter, false)),
                        );
                        let (result, _) = await_source_job(task, &mut prepared, || {}).await;
                        prepared.retire_waker();
                        let original = crate::NativeMaterializationWork::from_meter(shared);
                        let closed = original.source_leaf(|work| {
                            work.restore_budget(budget);
                            let cleanup = work.finish_cancelled(prepared).map_err(cancellation_error);
                            (cleanup, work.take_native_io_failure(), work.check())
                        });
                        let result = match closed {
                            Ok((cleanup, failure, checked)) =>
                                cleanup.and(failure.map_or(result, Err)).and_then(|value| checked.map(|()| value)),
                            Err(error) => Err(error),
                        };
                        accept_source_result(original.finish_source(result), &mut accounting)
                    }.await;
                    work.finish(result, &accounting)
                }

    pub(in crate::demand) fn open_blocking<W: SourceDemandReadWork>(
                    input: SourceRootInput,
                    profile: FilesystemProfile,
                    limits: VolumeLimits,
                    reference: SourceReference,
                    observer: Option<Arc<dyn DemandDirectoryObserver>>,
                    work: &mut W,
                ) -> Result<Self, DemandError> {
                    work.check()?;
                    let input = input.normalize(work)?;
                    #[cfg(unix)]
                    let root = HostRoot::open(&input.path, work)?;
                    #[cfg(windows)]
                    let root = HostRoot::open_root_wide_measured(&input.path, work)?;
                    let inline = root.is_local_with_work(work)?;
                    let requests_ownership = SourcePayloadOwnership::new(work)?;
                    let requests_bytes = (std::mem::size_of::<Semaphore>() + 2 * std::mem::size_of::<usize>()) as u64;
                    work.allocate(requests_bytes)?;
                    let requests = Arc::new(Semaphore::new(MAXIMUM_NATIVE_REQUESTS));
                    requests_ownership.retain(requests_bytes, work)?;
                    retain_root_control(&requests, &requests_ownership, work)?;
                    let inner_ownership = SourcePayloadOwnership::new(work)?;
                    let inner_bytes = (std::mem::size_of::<NativeDemandInner>() + 2 * std::mem::size_of::<usize>()) as u64;
                    work.allocate(inner_bytes)?;
                    let inner = Arc::new(NativeDemandInner {
                        path: input.path,
                        inline,
                        root,
                        identity: reference.identity,
                        epoch: AtomicU64::new(reference.epoch),
                        profile,
                        limits,
                        cursors: Mutex::new(CursorSlots::default()),
                        next_cursor: AtomicU64::new(0),
                        requests,
                        observer,
                        #[cfg(any(target_os = "linux", windows))]
                        watches: std::sync::RwLock::new(NativeSourceWatches::default()),
                        root_custody: input.custody,
                    });
                    inner_ownership.retain(inner_bytes, work)?;
                    retain_root_control(&inner, &inner_ownership, work)?;
                    Ok(Self {
                        inner,
                        #[cfg(test)]
                        watchable: true,
                    })
                }

    #[cfg(all(test, unix, feature = "native-mount"))]
                pub(crate) fn unwatched(self) -> Self {
                    Self {
                        watchable: false,
                        ..self
                    }
                }

    #[cfg(test)]
                pub(crate) fn remote(mut self) -> Self {
                    if let Some(inner) = Arc::get_mut(&mut self.inner) {
                        inner.inline = false;
                    }
                    self
                }

    pub(in crate::demand) async fn queue_request<T: Send + 'static>(
                    &self,
                    path: &NamespacePath,
                    mut work: NativeSourceWork<'_>,
                    job: impl FnOnce(Self, &NamespacePath, &mut NativeSourceWork<'_>) -> Result<T, DemandError> + Send + 'static,
                ) -> Result<T, DemandError> {
                    work.check()?;
                    let path_ownership = SourcePayloadOwnership::new(&mut work)?;
                    work.charge(WorkCounters {
                        source_path_components: path.depth() as u64, ..WorkCounters::UNCHARGED
                    })?;
                    let path = clone_namespace(path, &mut work)?;
                    path_ownership.retain(path.retained_bytes(), &mut work)?;
                    let path = (path, path_ownership);
                    let stop_ownership = SourcePayloadOwnership::new(&mut work)?;
                    let stop_bytes = (std::mem::size_of::<SourceRequestStop>() + 2 * std::mem::size_of::<usize>()) as u64;
                    work.allocate(stop_bytes)?;
                    let stop = Arc::new(SourceRequestStop { stopped: AtomicBool::new(false), ownership: stop_ownership });
                    stop.ownership.retain(stop_bytes, &mut work)?;
                    let stop_on_drop = StopOriginalRequestOnDrop(Arc::clone(&stop));
                    let original = work.owned();
                    let cancellation = work.materialization_work()?;
                    let mut cancelled = cancellation.cancellation().prepare_cancelled_with_native_work(&cancellation)
                        .map_err(cancellation_error)?;
                    let permit = tokio::select! {
                        acquired = self.inner.requests.clone().acquire_owned() =>
                            acquired.map_err(|_| DemandError::WorkerUnavailable),
                        () = &mut cancelled => Err(DemandError::Cancelled),
                    };
                    let permit = match permit.and_then(|permit| work.check().map(|()| permit)) {
                        Ok(permit) => permit,
                        Err(error) => { cancelled.retire_waker(); drop(cancelled); return Err(error); }
                    };
                    let worker_work = original.clone();
                    let native_original = work.native_original;
                    let task_meter = work.materialization_work()?.runtime_meter();
                    let source = self.clone();
                    let context = crate::obs::caller_context();
                    let spawned = acyclic_native_runtime::run_blocking_io(
                        crate::obs::in_context(context, move || {
                            let _permit = permit;
                            let mut worker = NativeSourceWork::from_native_materialization_work(&worker_work);
                            worker.native_original = native_original;
                            worker.operation_stop = Some(&stop.stopped);
                            job(source, &path.0, &mut worker)
                        }), Some((task_meter, false)),
                    );
                    let (result, selected_cancellation) = await_source_job(spawned, &mut cancelled, || {
                        stop_on_drop.0.stopped.store(true, Ordering::Release);
                    }).await;
                    cancelled.retire_waker();
                    drop(cancelled);
                    let native_failure = work.with_original_leaf(DemandWork::take_native_io_failure);
                    drop(stop_on_drop);
                    if let Some(error) = native_failure { return Err(error); }
                    if selected_cancellation { return Err(DemandError::Cancelled); }
                    result.and_then(|value| work.check().map(|()| value))
                }

    pub(in crate::demand) async fn request<T: Send + 'static, W: crate::facade::FsWork + Send>(
                    &self, path: &NamespacePath, work: W, cancellation: &CancellationToken,
                    job: impl FnOnce(Self, &NamespacePath, &mut NativeSourceWork<'_>) -> Result<T, DemandError> + Send + 'static,
                ) -> W::Output<T, DemandError> {
                    let mut accounting = work.accounting(cancellation);
                    let result = async {
                        if let Some(original) = accounting.native_work() {
                            return self.queue_request(path, NativeSourceWork::from_native_materialization_work(original), job).await;
                        }
                        let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
                        let mut work = DemandWork::new(budget, cancellation);
                        if let Err(error) = work.admit(WorkCounters {
                            source_path_components: path.depth() as u64, ..WorkCounters::UNCHARGED
                        }).and_then(|()| work.allocate(std::mem::size_of::<NativeDemandReadMeter>() as u64)) {
                            return accept_source_result(work.finish(Err(error)), &mut accounting);
                        }
                        let original = Arc::new(NativeDemandReadMeter(Mutex::new(work)));
                        let result = self.queue_request(path, NativeSourceWork::ordinary(&original), job).await;
                        accept_source_result(crate::NativeMaterializationWork::from_meter(original).finish_source(result), &mut accounting)
                    }.await;
                    work.finish(result, &accounting)
                }
}
