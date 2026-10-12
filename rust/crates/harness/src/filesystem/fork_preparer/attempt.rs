//! One bounded preparation attempt and its capture journal.
use super::*;
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkPreparer<A, O> {
    pub(super) fn attempt<'a>(
        &'a self,
        task: Option<&'a Arc<TaskResourcePreparation>>,
    ) -> Result<PreparationAttempt<'a, A, O>> {
        let mut io = None;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((work, _)) = &self.native {
            io = Some(PreparationIo::from_native_work(work)?);
        }
        if io.is_none() {
            io = task
                .map(|task| {
                    let cancellation =
                        task.cancellation().ok_or_else(|| {
                            Error::Unauthorized(
                    "task-bound preparation has no original invocation cancellation token".into())
                        })?;
                    PreparationIo::new(task.request(), cancellation)
                })
                .transpose()?;
        }
        if let (Some(io), Some(task)) = (&io, task) {
            io.encode(task.request(), task.request().maximum_bytes, false)?;
        }
        Ok(PreparationAttempt {
            preparer: self,
            io,
            task_preparation: task,
        })
    }

    /// Prepares captures under the authenticated preparation task.
    pub async fn prepare_for_task(
        &self,
        request: ForkRequest,
        preparation: &Arc<TaskResourcePreparation>,
    ) -> Result<ForkReport> {
        if preparation.request().provider != self.host.provider
            || preparation.request().creation_grant != capability::FORK_PUBLISH
        {
            return Err(Error::Unauthorized(
                "fork preparation task belongs to another provider or creation authority".into(),
            ));
        }
        let attempt = self.attempt(Some(preparation))?;
        self.check_request(&request, attempt.io.as_ref(), Some(preparation.as_ref()))?;
        attempt.prepare(request).await
    }

    /// Borrows the exact admitted request and genuine current preparation owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prepare_for_task_with_native_work<'a>(
        &'a self,
        preparation: &'a Arc<TaskResourcePreparation>,
        request: &'a ForkRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ForkReport, acyclic_fs::NativeAllocation)>> {
        self.require_native_projection_work(work)?;
        let attempt = self.attempt(Some(preparation))?;
        self.check_request(request, attempt.io.as_ref(), Some(preparation.as_ref()))?;
        crate::NativeFutureBundle::new(work, || async move {
            preparation.verify_creation_with_native_work(work)?.await?;
            let io = attempt
                .io
                .as_ref()
                .expect("original native preparer owns its IO");
            io.retain_owned(std::mem::size_of::<ForkReport>() as u64)?;
            let owned = request.copy(&mut Some(&mut io.history_io()))?.0;
            let report = attempt.prepare(owned).await?;
            let retained = io.owned_bytes()?;
            let custody = io.take_native_custody(retained)?;
            Ok((report, custody))
        })
    }

    /// Reconciles the retained report under its settlement owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn reconcile_for_task_with_native_work<'a>(
        &'a self,
        preparation: &'a Arc<TaskResourcePreparation>,
        request: &'a ForkRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ForkReport>, acyclic_fs::NativeAllocation)>>
    {
        self.require_native_projection_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            preparation
                .verify_reconciliation_with_native_work(work)?
                .await?;
            let attempt = self.attempt(Some(preparation))?;
            let io = attempt
                .io
                .as_ref()
                .expect("original native preparer owns its IO");
            self.check_request(request, Some(io), Some(preparation.as_ref()))?;
            let report = attempt.reconcile(request).await?;
            preparation
                .verify_reconciliation_with_native_work(work)?
                .await?;
            let retained = io.owned_bytes()?;
            let custody = io.take_native_custody(retained)?;
            Ok((report, custody))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> PreparationAttempt<'_, A, O> {
    pub(super) fn journal_limit(&self, default: u64) -> u64 {
        self.task_preparation
            .as_ref()
            .map_or(default, |task| default.min(task.request().maximum_bytes))
    }

    pub(super) fn clone_authority(&self, value: &Authority) -> Result<Authority> {
        use crate::store::history::HistoryIo as _;
        self.io.as_ref().map_or_else(
            || Ok(value.clone()),
            |io| io.history_io().clone_authority(value),
        )
    }

    pub(super) fn clone_paths(&self, paths: &[String]) -> Result<Vec<String>> {
        let mut values = bounded_io::vector(paths.len(), self.io.as_ref())?;
        for path in paths {
            values.push(bounded_io::clone_text(path, self.io.as_ref())?);
        }
        Ok(values)
    }

    pub(super) fn clone_origin(
        &self,
        origin: &ResourceCreationOrigin,
    ) -> Result<ResourceCreationOrigin> {
        crate::executor::with_history_io(self.io.as_ref(), |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let origin = ResourceCreationOrigin {
                owner: crate::core::copy_authority(&origin.owner, &mut quota)?,
                parent_task: origin.parent_task,
                preparation_task: origin.preparation_task,
                intended_child: origin.intended_child,
                admission_digest: origin.admission_digest,
                source_grants: crate::core::copy_capabilities(&origin.source_grants, &mut quota)?,
                request_digest: origin.request_digest,
                creation_fence: crate::scheduler::LeaseFence {
                    reservation_id: crate::core::copy_text(
                        &origin.creation_fence.reservation_id,
                        &mut quota,
                    )?,
                    placement: crate::core::copy_text(
                        &origin.creation_fence.placement,
                        &mut quota,
                    )?,
                },
            };
            quota.transfer();
            Ok(origin)
        })
    }

    pub(super) fn read_grant(&self, volume: &VolumeRef) -> Result<ContentGrant> {
        crate::executor::with_history_io(self.io.as_ref(), |work| {
            ContentGrant::verify(
                &self.preparer.verifier,
                &self.preparer.scope,
                volume,
                VolumeOperation::Read,
                MAX_CAPTURE_BYTES,
                work,
            )
        })
    }

    pub(super) async fn verify_task_lease(&self, settlement: bool) -> Result<()> {
        if let Some(preparation) = &self.task_preparation {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = self
                .io
                .as_ref()
                .and_then(PreparationIo::original_native_work)
            {
                return if settlement {
                    preparation
                        .verify_reconciliation_with_native_work(work)?
                        .await
                } else {
                    preparation.verify_creation_with_native_work(work)?.await
                };
            }
            if settlement {
                preparation.verify_reconciliation().await?;
            } else {
                preparation.verify_creation().await?;
            }
        }
        Ok(())
    }

    pub(super) async fn verify_task_allocations(&self, request: &ForkRequest) -> Result<()> {
        let Some(preparation) = &self.task_preparation else {
            return Ok(());
        };
        let encoded_hold;
        let encoded = encode(
            request,
            self.journal_limit(MAX_REQUEST_BYTES),
            self.io.as_ref(),
        )?;
        encoded_hold = self
            .io
            .as_ref()
            .map(|io| io.guard_bytes(&encoded))
            .transpose()?
            .flatten();
        let digest = *bounded_io::digest(&encoded, self.io.as_ref())?.as_bytes();
        drop(encoded);
        drop(encoded_hold);
        for volume in creation_volumes(request) {
            let journal = allocation_ref(&self.preparer.host.provider, volume, self.io.as_ref())?;
            let retained = read_record::<A, O, AllocationClaim>(
                &self.preparer.host,
                &journal,
                "/claim.json",
                self.journal_limit(MAX_REQUEST_BYTES),
                self.io.as_ref(),
            )
            .await?
            .ok_or_else(|| {
                bounded_io::failure(
                    self.io.as_ref(),
                    crate::wire::ErrorCode::Unauthorized,
                    "task-bound allocation is missing",
                )
            })?;
            if retained.operation_id != request.operation_id
                || retained.preparation_digest != digest
                || retained.parent != request.parent
                || retained.child != request.child
                || &retained.volume != volume
                || !same_task_origin(retained.task_origin.as_ref(), Some(preparation.origin()))
            {
                return Err(bounded_io::failure(
                    self.io.as_ref(),
                    crate::wire::ErrorCode::Unauthorized,
                    "allocation belongs to another preparation task",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn reconcile(&self, request: &ForkRequest) -> Result<Option<ForkReport>> {
        let journal = preparation_journal(
            &self.preparer.host.provider,
            request.operation_id,
            self.io.as_ref(),
        )?;
        match bounded_io::resolve(&self.preparer.host, &journal, self.io.as_ref()).await {
            Ok(_) => {}
            Err(error) if journal_absent(&error) => return Ok(None),
            Err(error) => return Err(error),
        }
        if !read_claim(
            &self.preparer.host,
            &journal,
            request,
            self.journal_limit(MAX_REQUEST_BYTES),
            self.io.as_ref(),
        )
        .await?
        {
            return Ok(None);
        }
        let report = read_report(
            &self.preparer.host,
            &journal,
            request,
            self.journal_limit(MAX_REPORT_BYTES),
            self.io.as_ref(),
        )
        .await?;
        if report.is_some() {
            self.verify_task_allocations(request).await?;
        }
        Ok(report)
    }

    pub(super) async fn claim(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        let journal = preparation_journal(
            &self.preparer.host.provider,
            request.operation_id,
            self.io.as_ref(),
        )?;
        let name = std::str::from_utf8(journal.as_resource().key()).map_err(|_| {
            bounded_io::failure(
                self.io.as_ref(),
                crate::wire::ErrorCode::Invalid,
                "fork journal name is not UTF-8",
            )
        })?;
        let creation_key = IdempotencyKey::new(crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!("fork:{}:journal", request.operation_id), work))?)?;
        bounded_io::create(
            &self.preparer.host,
            name,
            &creation_key,
            self.io.as_ref(),
            self.task_preparation,
        )
        .await?;
        if read_claim(
            &self.preparer.host,
            &journal,
            request,
            self.journal_limit(MAX_REQUEST_BYTES),
            self.io.as_ref(),
        )
        .await?
        {
            return Ok(journal);
        }
        let key = IdempotencyKey::new(crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!("fork:{}:claim", request.operation_id), work))?)?;
        write_record(
            &self.preparer.host,
            &journal,
            "/request.json",
            request,
            self.journal_limit(MAX_REQUEST_BYTES),
            &key,
            self.io.as_ref(),
            self.task_preparation,
        )
        .await?;
        if !read_claim(
            &self.preparer.host,
            &journal,
            request,
            self.journal_limit(MAX_REQUEST_BYTES),
            self.io.as_ref(),
        )
        .await?
        {
            return Err(Error::Indeterminate(request.operation_id));
        }
        Ok(journal)
    }

    pub(super) async fn commit_report(
        &self,
        journal: &WorkspaceRef,
        report: &ForkReport,
    ) -> Result<ForkReport> {
        let key = IdempotencyKey::new(crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!("fork:{}:report", report.request.operation_id), work))?)?;
        write_record(
            &self.preparer.host,
            journal,
            "/report.json",
            report,
            self.journal_limit(MAX_REPORT_BYTES),
            &key,
            self.io.as_ref(),
            self.task_preparation,
        )
        .await?;
        match read_report(
            &self.preparer.host,
            journal,
            &report.request,
            self.journal_limit(MAX_REPORT_BYTES),
            self.io.as_ref(),
        )
        .await?
        {
            Some(committed) if committed == *report => Ok(committed),
            Some(_) => Err(bounded_io::failure(
                self.io.as_ref(),
                crate::wire::ErrorCode::Conflict,
                "fork operation has a different committed report",
            )),
            None => Err(Error::Indeterminate(report.request.operation_id)),
        }
    }

    pub(super) async fn capture_result(
        &self,
        journal: &WorkspaceRef,
        index: usize,
        selection: &ForkSelection,
    ) -> Result<Option<Capture>> {
        let path = crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!("/capture-{index}-result.json"), work))?;
        let Some(capture) = read_record::<A, O, Capture>(
            &self.preparer.host,
            journal,
            &path,
            self.journal_limit(MAX_CAPTURE_BYTES),
            self.io.as_ref(),
        )
        .await?
        else {
            return Ok(None);
        };
        let attempt_path = crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!("/capture-{index}-attempt.json"), work))?;
        read_record::<A, O, Uuid>(
            &self.preparer.host,
            journal,
            &attempt_path,
            self.journal_limit(MAX_CAPTURE_BYTES),
            self.io.as_ref(),
        )
        .await?
        .ok_or_else(|| {
            bounded_io::failure(
                self.io.as_ref(),
                crate::wire::ErrorCode::Storage,
                "fork capture result has no durable start",
            )
        })?;
        validate_capture(selection, &capture)?;
        Ok(Some(capture))
    }

    /// Returns true only to the writer that durably won this attempt.
    pub(super) async fn start_capture(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
    ) -> Result<bool> {
        let path = crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!("/capture-{index}-attempt.json"), work))?;
        if read_record::<A, O, Uuid>(
            &self.preparer.host,
            journal,
            &path,
            self.journal_limit(MAX_CAPTURE_BYTES),
            self.io.as_ref(),
        )
        .await?
        .is_some()
        {
            return Ok(false);
        }
        let attempt = Uuid::new_v4();
        let key = IdempotencyKey::new(crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!(
                "fork:{}:capture-attempt:{index}:{attempt}",
                request.operation_id
            ), work))?)?;
        write_record(
            &self.preparer.host,
            journal,
            &path,
            &attempt,
            self.journal_limit(MAX_CAPTURE_BYTES),
            &key,
            self.io.as_ref(),
            self.task_preparation,
        )
        .await?;
        match read_record::<A, O, Uuid>(
            &self.preparer.host,
            journal,
            &path,
            self.journal_limit(MAX_CAPTURE_BYTES),
            self.io.as_ref(),
        )
        .await?
        {
            Some(prior) => Ok(prior == attempt),
            None => Err(Error::Indeterminate(request.operation_id)),
        }
    }

    pub(super) async fn commit_capture_result(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
        capture: Capture,
    ) -> Result<Capture> {
        validate_capture(selection, &capture)?;
        if let Some(prior) = self.capture_result(journal, index, selection).await? {
            return if prior == capture {
                Ok(prior)
            } else {
                Err(bounded_io::failure(
                    self.io.as_ref(),
                    crate::wire::ErrorCode::Conflict,
                    "fork capture has another committed result",
                ))
            };
        }
        let path = crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!("/capture-{index}-result.json"), work))?;
        let key = IdempotencyKey::new(crate::executor::with_history_io(self.io.as_ref(), |work| crate::contract::preparation_format(format_args!("fork:{}:capture-result:{index}", request.operation_id), work))?)?;
        write_record(
            &self.preparer.host,
            journal,
            &path,
            &capture,
            self.journal_limit(MAX_CAPTURE_BYTES),
            &key,
            self.io.as_ref(),
            self.task_preparation,
        )
        .await?;
        match self.capture_result(journal, index, selection).await? {
            Some(committed) if committed == capture => Ok(committed),
            Some(_) => Err(bounded_io::failure(
                self.io.as_ref(),
                crate::wire::ErrorCode::Conflict,
                "fork capture has another committed result",
            )),
            None => Err(Error::Indeterminate(request.operation_id)),
        }
    }
}
