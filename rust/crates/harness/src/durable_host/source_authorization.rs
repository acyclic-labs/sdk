use super::resources::Verification;
use super::*;
use crate::source_authorization::{ProjectSourceAuthorization, ProjectSourceReadHandle};

impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Restores an authorization retained by the original caller.
    pub async fn restore_project_source_authorization(
        self: &Arc<Self>,
        context: &crate::runtime::TaskContext,
        authorization: &ProjectSourceAuthorization,
    ) -> Result<ProjectSourceReadHandle> {
        self.restore_source(context, authorization, None).await
    }

    /// Restores an authorization on its original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn restore_project_source_authorization_with_native_work<'a>(
        self: &'a Arc<Self>,
        context: &'a crate::runtime::TaskContext,
        authorization: &'a ProjectSourceAuthorization,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (ProjectSourceReadHandle, acyclic_fs::NativeAllocation)>,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let handle = self
                .restore_source(context, authorization, Some(&io))
                .await?;
            let held = io.owned_bytes()?;
            Ok((handle, io.into_native_custody(held)?))
        })
    }

    async fn restore_source(
        &self,
        context: &crate::runtime::TaskContext,
        authorization: &ProjectSourceAuthorization,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<ProjectSourceReadHandle> {
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            let binding = context.owned_task_binding_with_native_work(native)?;
            self.require_mail_scope_with_native_work(context.scope(), native)?;
            if binding != (self.task_id, &self.fence) {
                return Err(guarded_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "project source restoration lacks its original owner",
                ));
            }
        }
        if !context.original_task_binding_matches(self.task_id, &self.fence)
            || context.scope().grants() != &self.input_grants
            || authorization.caller_admission_digest != self.original_admission_digest
        {
            return Err(guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "project source restoration lacks its original owner",
            ));
        }
        let cancellation = self.invocation_cancellation.as_ref();
        if io.is_some() && cancellation.is_none() {
            return Err(guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "project source restoration requires original cancellation",
            ));
        }
        let check = || match cancellation {
            Some(token) if token.is_cancelled() => {
                #[cfg(not(target_arch = "wasm32"))]
                if io.is_some() {
                    return Err(Error::from(
                        acyclic_fs::NativeTreeMaterializationError::Cancelled,
                    ));
                }
                Err(Error::Unauthorized(
                    "project source invocation is cancelled".into(),
                ))
            }
            _ => Ok(()),
        };
        check()?;
        self.verify(false, &mut work).await?;
        let handle = self
            .host
            .seal_project_source_authorization(authorization, self.task_id, io)
            .await?;
        self.verify(false, &mut work).await?;
        check()?;
        Ok(handle)
    }

    pub(crate) async fn verify_project_source_handle(
        &self,
        context: &crate::runtime::TaskContext,
        handle: &ProjectSourceReadHandle,
        native: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = native {
            work.remaining()?;
            if !context.original_task_binding_matches(self.task_id, &self.fence)
                || context.scope().grants() != &self.input_grants
                || context.scope().limits() != self.input_limits
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "selected project read lacks its current original owner",
                });
            }
            return self.verify_project_source_read(handle, native).await;
        }
        if context.durable_task_id() != Some(self.task_id)
            || context.owned_task_fence()? != self.fence
            || context.scope().grants() != &self.input_grants
        {
            return Err(Error::Unauthorized(
                "selected project read lacks its current original owner".into(),
            ));
        }
        self.verify_project_source_read(handle, native).await
    }

    /// Revalidates the sealed reader captured by the original owner.
    pub(crate) async fn verify_project_source_read(
        &self,
        handle: &ProjectSourceReadHandle,
        native: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let mut io = native
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut work = io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(target_arch = "wasm32")]
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = None;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = native {
            work.remaining()?;
        }
        let cancellation = self.invocation_cancellation.as_ref().ok_or_else(|| {
            guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "selected project read requires actual invocation cancellation",
            )
        })?;
        let check = || {
            #[cfg(not(target_arch = "wasm32"))]
            if native.is_some() {
                return cancellation.check().map_err(|_| {
                    Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled)
                });
            }
            cancellation
                .check()
                .map_err(|_| Error::Unauthorized("selected project invocation cancelled".into()))
        };
        check()?;
        if handle.authorization().caller_admission_digest != self.original_admission_digest {
            return Err(guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "selected project read differs from its original admission",
            ));
        }
        self.verify(false, &mut work).await?;
        self.host
            .verify_project_source_authorization(handle.authorization(), self.task_id, &mut work)
            .await?;
        self.verify(false, &mut work).await?;
        check()
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn source_graph_root(
        &self,
        task: TaskId,
        budget: &mut Verification,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskId> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let mut operation = OperationId::from_bytes(task.into_bytes());
        let mut anchor = operation;
        let mut run = 1_u64;
        let mut steps = 0_u64;
        loop {
            budget.charge(1, 0, work)?;
            let state = coordinator.observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation,
                work,
            )?;
            if state.cancellation_requested {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "project source graph is cancelled",
                ));
            }
            match &state.spec.parent {
                Some(parent) => {
                    operation = parent.operation_id;
                    if let Some(io) = work.as_deref_mut() {
                        io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                    }
                    if operation == anchor {
                        return Err(guarded_boundary(
                            work,
                            crate::wire::ErrorCode::Unauthorized,
                            "project source graph is cyclic",
                        ));
                    }
                    steps += 1;
                    if steps == run {
                        anchor = operation;
                        run = run.checked_mul(2).ok_or_else(|| {
                            guarded_boundary(
                                work,
                                crate::wire::ErrorCode::Invalid,
                                "project source graph depth overflows",
                            )
                        })?;
                        steps = 0;
                    }
                }
                None => {
                    coordinator
                        .scheduler()
                        .session_limits(operation, work)?
                        .validate(work)?;
                    return Ok(TaskId::from_bytes(operation.into_bytes()));
                }
            }
        }
    }

    pub(super) async fn source_created_project(
        &self,
        source: &TaskAdmissionRecord,
        project: &crate::conversation::VolumeRef,
        budget: &mut Verification,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        if source.parent.is_none()
            && source.resource_proofs.is_empty()
            && let Some(original) = work
                .as_deref()
                .and_then(|io| io.original_native_work())
                .cloned()
        {
            budget.charge(1, 0, work)?;
            let access = self
                .verify_root_bootstrap_access_with_native_work(source, &original)
                .await?;
            let read = project.capability(crate::conversation::VolumeOperation::Read, work)?;
            let allowed = project.class() == crate::conversation::VolumeClass::Project
                && access.as_ref().is_some_and(|access| {
                    access.grants.contains(&read)
                        && access
                            .content
                            .iter()
                            .any(|binding| &binding.volume == project)
                });
            let retained = read.capacity() as u64;
            drop(read);
            work.as_deref_mut()
                .expect("native observer")
                .release_owned(retained)?;
            return Ok(allowed);
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let access = self.created_receipts(source, budget, quota.work()).await;
        let path = std::mem::take(&mut budget.path);
        quota.adopt((path.capacity() * std::mem::size_of::<OperationId>()) as u64)?;
        drop(path);
        let access = access?
            .map(|(mut receipts, origin)| {
                crate::resource_preparation::CreatedResourceAccess::consume(
                    source,
                    &mut receipts,
                    origin,
                    quota.work(),
                )
                .map(|(access, _)| access)
            })
            .transpose()?;
        let selected = access.as_ref().is_some_and(|access| {
            access
                .content
                .iter()
                .any(|binding| &binding.volume == project)
        });
        drop(access);
        Ok(selected)
    }

    pub(super) async fn seal_prepared_project_source(
        &self,
        request: &crate::resource_preparation::ResourcePreparationRequest,
        parent: TaskId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Option<ProjectSourceReadHandle>> {
        let Some(record) = &request.read_authorization else {
            return Ok(None);
        };
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        if request.child.parent != Some(parent)
            || request.maximum_work > record.maximum_work
            || request.maximum_bytes > record.maximum_bytes
            || request
                .command
                .as_ref()
                .is_some_and(|command| command.operation_id != record.operation_id)
        {
            return Err(guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "project source differs from original preparation caller or bounds",
            ));
        }
        let payload_matches = match io {
            #[cfg(not(target_arch = "wasm32"))]
            Some(io) => {
                let comparison = crate::filesystem::PreparationIo::from_native_work(
                    io.original_native_work().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "project source preparation requires original work",
                    })?,
                )?;
                let operation =
                    comparison.value_json(&record.operation_id, request.maximum_bytes)?;
                let authorization = comparison.value_json(record, request.maximum_bytes)?;
                request.payload.get("operation_id") == Some(&operation)
                    && request.payload.get("authorized_source") == Some(&authorization)
            }
            _ => {
                request.payload.get("operation_id")
                    == Some(
                        &serde_json::to_value(record.operation_id)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    )
                    && request.payload.get("authorized_source")
                        == Some(
                            &serde_json::to_value(record)
                                .map_err(|error| Error::Invalid(error.to_string()))?,
                        )
            }
        };
        if !payload_matches {
            return Err(guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "project source differs from the retained preparation payload",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<ProjectSourceReadHandle>() as u64)?;
        }
        self.seal_project_source_authorization(record, parent, io)
            .await
            .map(Some)
    }

    async fn seal_project_source_authorization(
        &self,
        record: &ProjectSourceAuthorization,
        parent: TaskId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<ProjectSourceReadHandle> {
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        self.verify_project_source_authorization(record, parent, &mut work)
            .await?;
        let copied = record.clone_with_work_and_custody(&mut work)?.0;
        ProjectSourceReadHandle::seal(copied, &self.verifier, &mut work)
    }

    async fn verify_project_source_authorization(
        &self,
        record: &ProjectSourceAuthorization,
        parent: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        record.verify_scope_with_work(&self.verifier, quota.work())?;
        if &record.owner != self.verifier.audience() || record.caller_task != parent {
            return Err(guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source differs from original caller or owning coordinator",
            ));
        }
        let mut budget = Verification {
            remaining: Some((record.maximum_work, record.maximum_bytes)),
            path: Vec::new(),
        };
        let (caller, retained) = self
            .source_admission(parent, &mut budget, quota.work())
            .await?;
        quota.adopt(retained)?;
        if crate::contract::canonical_json_digest_with_work(
            &caller,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            quota.work(),
        )? != record.caller_admission_digest
            || caller
                .run_limits
                .deadline_epoch_ms
                .is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
        {
            return Err(guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source original caller changed or expired",
            ));
        }
        if self
            .source_graph_root(parent, &mut budget, quota.work())
            .await?
            != record.graph_root
            || self
                .source_graph_root(record.source_task, &mut budget, quota.work())
                .await?
                != record.graph_root
        {
            return Err(guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source retained graph changed",
            ));
        }
        let (source, retained) = self
            .source_admission(record.source_task, &mut budget, quota.work())
            .await?;
        quota.adopt(retained)?;
        if !self
            .source_created_project(&source, &record.project, &mut budget, quota.work())
            .await?
            && record.source_task != record.graph_root
        {
            return Err(guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source retained creator evidence changed",
            ));
        }
        Ok(())
    }
}
