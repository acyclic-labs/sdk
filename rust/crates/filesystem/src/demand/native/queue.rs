use super::*;

// A request crosses several executor layers. Carrying CallbackWork by value
// makes each layer reserve space for its largest (ordinary) variant, even for
// a native invocation. The worker and its completion share this admitted owner.
struct RequestContext {
    value: Box<CallbackWork>,
    path: Option<NamespacePath>,
    ordinary_path_bytes: u64,
    allocation: Option<crate::NativeAllocation>,
}

impl RequestContext {
    #[inline(never)]
    fn new(mut value: CallbackWork) -> Result<Self, DemandError> {
        let bytes = std::mem::size_of::<CallbackWork>() as u64;
        let allocation = match &mut value {
            CallbackWork::Original(work) => Some(
                crate::NativeMaterializationWork::claim(work, bytes, 1, WorkCounters::UNCHARGED)
                    .map_err(crate::FsError::from)?,
            ),
            CallbackWork::Ordinary(work) => {
                work.claim_allocation(bytes, 1, WorkCounters::UNCHARGED)?;
                None
            }
        };
        Ok(Self {
            value: Box::new(value),
            path: None,
            ordinary_path_bytes: 0,
            allocation,
        })
    }

    // Both payloads precede their native guard in destruction order.
    // Dropping a pending future or queued worker also retires its copied path.
    fn copy_path(&mut self, path: &NamespacePath) -> Result<(), DemandError> {
        self.value.check()?;
        let bytes = path.retained_bytes();
        let copying = WorkCounters {
            bytes_copied: bytes,
            ..WorkCounters::UNCHARGED
        };
        self.value.admit(copying)?;
        if let Some(allocation) = &mut self.allocation {
            allocation.grow(bytes).map_err(crate::FsError::from)?;
        } else {
            self.ordinary_path_bytes = self.value.claim(bytes)?;
        }
        self.path = Some(path.clone());
        self.value.record(copying)
    }

    #[inline(never)]
    fn finish<T>(
        self,
        result: Result<T, DemandError>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<T, DemandError> {
        let Self {
            value,
            path,
            ordinary_path_bytes,
            allocation,
        } = self;
        drop(path);
        // Moving out frees the box before its charge is released. The large
        // value exists only in this terminal frame, after executor submission.
        let mut value = *value;
        drop(allocation);
        let released = match &mut value {
            CallbackWork::Original(_) => Ok(()),
            CallbackWork::Ordinary(work) => {
                work.release(ordinary_path_bytes)?;
                work.release(std::mem::size_of::<CallbackWork>() as u64)
            }
        };
        finish_context(
            result.and_then(|value| released.map(|()| value)),
            value,
            accounting,
        )
    }
}

impl NativeDemandSource {
    pub(super) async fn request<W: FsWork + Send, T: Send + 'static>(
        &self,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
        admission: Result<WorkCounters, DemandError>,
        job: impl FnOnce(Self, &NamespacePath, &mut CallbackWork) -> Result<T, DemandError>
        + Send
        + 'static,
    ) -> W::Output<T, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            // Reject known semantic work before allocating request transport.
            // The worker still records it at the original effect boundary.
            accounting.check().map_err(DemandError::from)?;
            accounting.admit(admission?).map_err(DemandError::from)?;
            let mut context = RequestContext::new(callback_work(&mut accounting)?)?;
            if let Err(error) = context.copy_path(path) {
                return context.finish(Err(error), &mut accounting);
            }
            let cancellation = cancellation.clone();
            let prepared = async {
                let permit = tokio::select! {
                    acquired = self.inner.requests.clone().acquire_owned() => acquired.map_err(|_| DemandError::WorkerUnavailable)?,
                    () = cancellation.cancelled() => return Err(DemandError::Cancelled),
                };
                let stop = CancelWorkerOnDrop(match context.value.as_mut() {
                    CallbackWork::Original(original) => {
                        let (invocation, stop) = original.invocation_view().map_err(crate::native_name::original_name_error)?;
                        *original = invocation;
                        stop
                    }
                    CallbackWork::Ordinary(work) => {
                        let stop = CancellationToken::new();
                        work.cancellation = stop.clone();
                        stop
                    }
                });
                Ok::<_, DemandError>((permit, stop))
            }.await;
            let (permit, stop) = match prepared {
                Ok(prepared) => prepared,
                Err(error) => return context.finish(Err(error), &mut accounting),
            };
            let source = self.clone();
            let worker_stop = stop.0.clone();
            let worker = spawn_blocking(crate::obs::in_context(crate::obs::caller_context(), move || {
                let _permit = permit;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    worker_stop.check().map_err(|_| DemandError::Cancelled)?;
                    job(source, context.path.as_ref().ok_or(DemandError::InvalidRequest)?, &mut context.value)
                })).unwrap_or_else(|_| {
                    context.value.with_original_leaf(|work| work.poison_native_original_work());
                    Err(DemandError::OriginalWorkPoisoned)
                });
                (result, context)
            }));
            tokio::pin!(worker);
            let answer = tokio::select! {
                answer = &mut worker => answer,
                () = cancellation.cancelled() => { stop.0.cancel(); worker.await }
            }.map_err(|_| DemandError::WorkerUnavailable)?;
            answer.1.finish(answer.0, &mut accounting)
        }.await;
        work.finish(result, &accounting)
    }
}

#[cfg(test)]
mod context_custody_tests {
    use super::*;

    #[test]
    fn original_request_context_is_funded_until_its_last_owner_drops() {
        let stop = CancellationToken::new();
        let bytes = std::mem::size_of::<CallbackWork>() as u64;
        let baseline = crate::NativeMaterializationWork::accounting_state_bytes();
        for available in [bytes - 1, bytes] {
            let work = crate::NativeMaterializationWork::new(
                WorkBudget {
                    peak_allocation_bytes: baseline + available,
                    ..WorkBudget::UNBOUNDED
                },
                &stop,
            )
            .map_err(|(error, _)| error)
            .unwrap();
            let context = RequestContext::new(CallbackWork::Original(work.clone()));
            if available < bytes {
                assert!(matches!(
                    context,
                    Err(DemandError::Work(crate::WorkError::BudgetExceeded { .. }))
                ));
                assert_eq!(work.live_allocation_bytes(), baseline);
            } else {
                let context = context.unwrap();
                assert_eq!(work.live_allocation_bytes(), baseline + bytes);
                drop(context);
                assert_eq!(work.live_allocation_bytes(), baseline);
            }
        }
    }

    #[test]
    fn ordinary_request_context_counts_and_retires_its_actual_backing() {
        let stop = CancellationToken::new();
        let bytes = std::mem::size_of::<CallbackWork>() as u64;
        let budget = WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: bytes,
            ..WorkBudget::UNBOUNDED
        };
        let context =
            RequestContext::new(CallbackWork::Ordinary(DemandWork::new(budget, &stop))).unwrap();
        let CallbackWork::Ordinary(work) = context.value.as_ref() else {
            panic!("ordinary owner")
        };
        assert_eq!(work.live_allocation_bytes(), bytes);
        let mut accounting = crate::kernel::KernelAccounting::standalone(budget, &stop);
        context.finish(Ok(()), &mut accounting).unwrap();
        let (allocations, work, _, _) = accounting.standalone_parts().unwrap();
        assert_eq!(allocations.live_bytes(), 0);
        assert_eq!(work.allocation_operations, 1);
        assert_eq!(work.peak_allocation_bytes, bytes);
    }
    #[tokio::test]
    async fn submission_denial_retires_and_reports_the_same_copied_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let directory = tempfile::tempdir()?;
        let token = CancellationToken::new();
        let source = NativeDemandSource::open(
            std::borrow::Cow::Owned(directory.path().to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &token,
        )
        .await?
        .value;
        // A closed queue refuses worker ownership after the path is copied.
        source.inner.requests.close();
        let limits = VolumeLimits::default();
        let path = NamespacePath::new(
            vec![crate::kernel::LogicalName::new(
                crate::kernel::NameEncoding::Utf8,
                b"child".to_vec(),
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        let budget = crate::performance::work_budget(100_000_000, 100_000_000);
        let failure = source
            .list_page(source.reference(), &path, None, 1, budget, &token)
            .await
            .err()
            .ok_or("closed queue admitted a request")?;
        assert!(matches!(failure.error, DemandError::WorkerUnavailable));
        assert_eq!(failure.work.allocation_operations, 2);
        assert_eq!(failure.work.bytes_copied, path.retained_bytes());
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.backend_write_operations, 0);
        failure.work.verify(budget)?;
        let work =
            crate::NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        let failure = source
            .list_page(source.reference(), &path, None, 1, &work, &token)
            .await
            .err()
            .ok_or("closed queue admitted a native request")?;
        assert!(matches!(failure, DemandError::WorkerUnavailable));
        assert_eq!(work.live_allocation_bytes(), live);
        assert_eq!(
            work.receipt().allocation_operations - before.allocation_operations,
            2
        );
        assert_eq!(
            work.receipt().bytes_copied - before.bytes_copied,
            path.retained_bytes()
        );
        assert_eq!(
            work.receipt().backend_read_operations,
            before.backend_read_operations
        );
        assert_eq!(
            work.receipt().backend_write_operations,
            before.backend_write_operations
        );
        Ok(())
    }

    #[tokio::test]
    async fn dropping_a_request_waiting_for_admission_retires_its_copied_path()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let directory = tempfile::tempdir()?;
        let token = CancellationToken::new();
        let source = NativeDemandSource::open(
            std::borrow::Cow::Owned(directory.path().to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &token,
        )
        .await?
        .value;
        let held = source
            .inner
            .requests
            .clone()
            .acquire_many_owned(u32::try_from(source.inner.requests.available_permits())?)
            .await?;
        let limits = VolumeLimits::default();
        let path = NamespacePath::new(
            vec![crate::kernel::LogicalName::new(
                crate::kernel::NameEncoding::Utf8,
                b"child".to_vec(),
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        let work = crate::NativeMaterializationWork::new(
            crate::performance::work_budget(100_000_000, 100_000_000),
            &token,
        )
        .map_err(|(error, _)| error)?;
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        let mut request =
            Box::pin(source.list_page(source.reference(), &path, None, 1, &work, &token));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(request.as_mut(), &mut context).is_pending());
        assert_eq!(
            work.live_allocation_bytes(),
            live + std::mem::size_of::<CallbackWork>() as u64 + path.retained_bytes()
        );
        drop(request);
        assert_eq!(work.live_allocation_bytes(), live);
        assert_eq!(
            work.receipt().allocation_operations - before.allocation_operations,
            2
        );
        assert_eq!(
            work.receipt().bytes_copied - before.bytes_copied,
            path.retained_bytes()
        );
        assert_eq!(
            work.receipt().backend_read_operations,
            before.backend_read_operations
        );
        assert_eq!(
            work.receipt().backend_write_operations,
            before.backend_write_operations
        );
        drop(held);
        Ok(())
    }
}
