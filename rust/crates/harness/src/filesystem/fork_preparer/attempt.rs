//! Preparation ownership and the main fork journal protocol.
use super::*;
use crate::executor::{stock_boundary, stock_string_copy, with_history_io};

impl acyclic_fs::AuthorityGuard for &TaskResourcePreparation {
    type Error = Error;

    async fn seal(
        &mut self,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> std::result::Result<(), acyclic_fs::AuthorityError<Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = match work.original {
            Some(original) => PreparationIo::from_native_work(original),
            None => PreparationIo::from_budget(work.remaining()?, work.cancellation),
        };
        #[cfg(target_arch = "wasm32")]
        let io = PreparationIo::from_budget(work.remaining()?, work.cancellation);
        let io = io.map_err(acyclic_fs::AuthorityError::Owner)?;
        let result = match self.verify(false, Some(&io)) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        if io.original_native_work().is_none() {
            work.work = work
                .work
                .checked_add(io.receipt())
                .map_err(acyclic_fs::AuthorityStoreError::from)?;
        }
        result.map_err(acyclic_fs::AuthorityError::Owner)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn native_commit<P: StreamProvider>(
        &mut self,
        provider: &P,
        request: acyclic_stream::CommitRequest,
        deadline: Option<u64>,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> acyclic_stream::NativeStreamResult<
        acyclic_stream::CommitOutcome,
        acyclic_fs::AuthorityError<Error>,
    > {
        let owner = Arc::clone(&self.owner);
        let expected = self
            .native_custody
            .as_ref()
            .map(|allocation| allocation.work().clone());
        acyclic_fs::NativeAuthorityGuard(move |work: &acyclic_fs::NativeMaterializationWork| {
            let work = work.clone();
            let owner = Arc::clone(&owner);
            let expected = expected.clone();
            async move {
                if expected.is_none_or(|expected| !expected.shares_allocation_context(&work)) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "preparation lacks original native custody",
                    });
                }
                let io = PreparationIo::from_native_work(&work)?;
                owner.verify_lease(false, Some(&io))?.await
            }
        })
        .native_commit(provider, request, deadline, work)
        .await
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkPreparer<A, O> {
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    pub(crate) async fn project_creation_operation(
        &self,
        request: &ForkRequest,
        preparation: &Arc<TaskResourcePreparation>,
        io: &PreparationIo,
    ) -> Result<acyclic_fs::OperationId> {
        let attempt = self.task_attempt(preparation, request, Some(io))?;
        attempt.verify_task_lease(false).await?;
        let digest = blake3::Hash::from(record_digest(
            request,
            attempt.journal_limit(MAX_REQUEST_BYTES),
            Some(io),
        )?);
        let text = with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!("fork:{}:{digest}:project", request.operation_id),
                work,
            )
        })?;
        let key = IdempotencyKey::new(text)?;
        Ok(acyclic_fs::OperationId::from_bytes(
            super::bounded_io::operation_key(&key, Some(io))?.into_bytes(),
        ))
    }

    pub(super) fn preparation_io(
        &self,
        task: Option<&Arc<TaskResourcePreparation>>,
    ) -> Result<Option<PreparationIo>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(allocation) = &self.native {
            let io = PreparationIo::from_native_work(allocation.work())?;
            if let Some(task) = task {
                task.request().validate(&mut Some(&mut io.history_io()))?;
            }
            return Ok(Some(io));
        }
        task.map(|task| {
            let cancellation = task.cancellation().ok_or_else(|| {
                Error::Unauthorized(
                    "task-bound preparation has no original invocation cancellation token".into(),
                )
            })?;
            PreparationIo::new(task.request(), cancellation)
        })
        .transpose()
    }

    fn task_attempt<'a>(
        &'a self,
        task: &'a Arc<TaskResourcePreparation>,
        request: &ForkRequest,
        io: Option<&'a PreparationIo>,
    ) -> Result<PreparationAttempt<'a, A, O>> {
        if task.request().provider != self.host.provider
            || task.request().creation_grant != capability::FORK_PUBLISH
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "fork preparation task belongs to another provider or creation authority",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            self.require_native_projection_work(work)?;
        }
        self.check_request(request, io, Some(task))?;
        Ok(PreparationAttempt {
            preparer: self,
            io,
            task_preparation: Some(task),
        })
    }

    /// Prepares captures under the authenticated task and caller's work context.
    pub async fn prepare_for_task(
        &self,
        request: &ForkRequest,
        preparation: &Arc<TaskResourcePreparation>,
        io: Option<&PreparationIo>,
    ) -> Result<ForkReport> {
        let owned = if io.is_none() {
            self.preparation_io(Some(preparation))?
        } else {
            None
        };
        let io = io.or(owned.as_ref());
        let attempt = self.task_attempt(preparation, request, io)?;
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            io.retain_owned(std::mem::size_of::<ForkReport>() as u64)?;
        }
        let request = with_history_io(io, |work| request.copy(work).map(|copy| copy.0))?;
        attempt.prepare(request).await
    }

    /// Reconciles captures under their original settlement owner.
    pub async fn reconcile_for_task(
        &self,
        request: &ForkRequest,
        preparation: &Arc<TaskResourcePreparation>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<ForkReport>> {
        let owned = if io.is_none() {
            self.preparation_io(Some(preparation))?
        } else {
            None
        };
        let io = io.or(owned.as_ref());
        let attempt = self.task_attempt(preparation, request, io)?;
        attempt.verify_task_lease(true).await?;
        let report = attempt.reconcile(request).await?;
        attempt.verify_task_lease(true).await?;
        Ok(report)
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> PreparationAttempt<'_, A, O> {
    pub(super) fn journal_limit(&self, default: u64) -> u64 {
        self.task_preparation
            .map_or(default, |task| default.min(task.request().maximum_bytes))
    }
    pub(super) fn clone_authority(&self, value: &Authority) -> Result<Authority> {
        use crate::store::history::HistoryIo;
        self.io.map_or_else(
            || Ok(value.clone()),
            |io| io.history_io().clone_authority(value),
        )
    }
    pub(super) fn clone_paths(&self, paths: &[String]) -> Result<Vec<String>> {
        let mut values = crate::model::vec_with_capacity(paths.len(), self.io)?;
        for path in paths {
            values.push(stock_string_copy(path, self.io)?);
        }
        Ok(values)
    }
    pub(super) fn clone_origin(
        &self,
        origin: &ResourceCreationOrigin,
    ) -> Result<ResourceCreationOrigin> {
        with_history_io(self.io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let origin = ResourceCreationOrigin {
                owner: crate::core::copy_authority(&origin.owner, &mut quota)?,
                parent_task: origin.parent_task,
                preparation_task: origin.preparation_task,
                intended_child: origin.intended_child,
                admission_digest: origin.admission_digest,
                source_grants: crate::core::copy(
                    &origin.source_grants,
                    &mut quota,
                    |value: &crate::Capabilities, _| value.logical_owned_bytes(),
                )?,
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
        with_history_io(self.io, |work| {
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
        if self.preparer.parent.authority().kind == crate::core::AggregateKind::Task
            && self.task_preparation.is_none()
        {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Unauthorized,
                "task fork requires its original preparation lease",
            ));
        }
        if let Some(task) = self.task_preparation {
            task.verify(settlement, self.io)?.await?;
        }
        Ok(())
    }
    pub(super) async fn verify_task_allocations(&self, request: &ForkRequest) -> Result<()> {
        let Some(task) = self.task_preparation else {
            return Ok(());
        };
        let maximum = self.journal_limit(MAX_REQUEST_BYTES);
        let digest = record_digest(request, maximum, self.io)?;
        for volume in creation_volumes(request) {
            let journal = allocation_ref(&self.preparer.host.provider, volume, self.io)?;
            let claim: AllocationClaim = read_record(
                &self.preparer.host,
                &journal,
                "/claim.json",
                maximum,
                self.io,
            )
            .await?
            .ok_or_else(|| {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Unauthorized,
                    "task-bound allocation is missing",
                )
            })?;
            if claim.operation_id != request.operation_id
                || claim.preparation_digest != digest
                || claim.parent != request.parent
                || claim.child != request.child
                || &claim.volume != volume
                || !same_task_origin(claim.task_origin.as_ref(), Some(task.origin()))
            {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Unauthorized,
                    "allocation belongs to another preparation task",
                ));
            }
        }
        Ok(())
    }
    pub(super) async fn reconcile(&self, request: &ForkRequest) -> Result<Option<ForkReport>> {
        let host = &self.preparer.host;
        let journal = preparation_journal(&host.provider, request.operation_id, self.io)?;
        match (host).resolve(&journal, self.io).await {
            Ok(_) => {}
            Err(error) if journal_absent(&error) => return Ok(None),
            Err(error) => return Err(error),
        }
        if !read_claim(
            host,
            &journal,
            request,
            self.journal_limit(MAX_REQUEST_BYTES),
            self.io,
        )
        .await?
        {
            return Ok(None);
        }
        let report = read_report(
            host,
            &journal,
            request,
            self.journal_limit(MAX_REPORT_BYTES),
            self.io,
        )
        .await?;
        if report.is_some() {
            self.verify_task_allocations(request).await?;
        }
        Ok(report)
    }
    pub(super) async fn claim(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        let host = &self.preparer.host;
        let journal = preparation_journal(&host.provider, request.operation_id, self.io)?;
        let name = std::str::from_utf8(journal.as_resource().key()).map_err(|_| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "fork journal name is not UTF-8",
            )
        })?;
        let creation = self.journal_key(request.operation_id, "journal")?;
        bounded_io::create(host, name, &creation, self.io, self.task_preparation).await?;
        let key = self.journal_key(request.operation_id, "claim")?;
        let prior = write_once(
            host,
            &journal,
            "/request.json",
            request,
            self.journal_limit(MAX_REQUEST_BYTES),
            &key,
            request.operation_id,
            self.io,
            self.task_preparation,
        )
        .await?;
        if prior != *request {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "fork operation is claimed by another request",
            ));
        }
        Ok(journal)
    }
    pub(super) async fn commit_report(
        &self,
        journal: &WorkspaceRef,
        report: &ForkReport,
    ) -> Result<ForkReport> {
        let key = self.journal_key(report.request.operation_id, "report")?;
        let prior = write_once(
            &self.preparer.host,
            journal,
            "/report.json",
            report,
            self.journal_limit(MAX_REPORT_BYTES),
            &key,
            report.request.operation_id,
            self.io,
            self.task_preparation,
        )
        .await?;
        if prior != *report {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "fork operation has a different committed report",
            ));
        }
        Ok(prior)
    }
    fn journal_key(&self, operation: OperationId, suffix: &str) -> Result<IdempotencyKey> {
        IdempotencyKey::new(with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("fork:{operation}:{suffix}"), work)
        })?)
    }
}
