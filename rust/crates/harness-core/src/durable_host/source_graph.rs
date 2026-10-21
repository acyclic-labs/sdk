//! Bounded source graph and retained authorization checks.
use super::*;
use crate::source_authorization::{ProjectSourceAuthorization, ProjectSourceReadHandle};
pub(super) struct SourceBudget {
    pub(super) work: u64,
    pub(super) bytes: u64,
}
impl SourceBudget {
    pub(super) fn charge(
        &mut self,
        units: u64,
        bytes: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        self.work = self.work.checked_sub(units).ok_or_else(|| {
            crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "project source verification exceeds work bound",
            )
        })?;
        self.bytes = self.bytes.checked_sub(bytes).ok_or_else(|| {
            crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "project source verification exceeds byte bound",
            )
        })?;
        Ok(())
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn source_admission(
        &self,
        task: TaskId,
        budget: &mut SourceBudget,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(TaskAdmissionRecord, u64)> {
        budget.charge(1, 0, work)?;
        let operation = OperationId::from_bytes(task.into_bytes());
        let spec = self
            .retained_declaration(operation, work)
            .await?
            .ok_or_else(|| {
                crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::NotFound,
                    "project source admission is missing",
                )
            })?;
        let held = if work.is_some() {
            spec.logical_owned_bytes()?
        } else {
            0
        };
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, held);
        budget.charge(0, spec.state.descriptor().byte_length(), quota.work())?;
        drop(spec);
        quota.relinquish(held)?;
        let record = self.admission(operation, quota.work()).await?;
        let retained = if quota.work().is_some() {
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + record.logical_owned_bytes()?
        } else {
            0
        };
        Ok((record, retained))
    }

    pub(super) async fn source_graph_root(
        &self,
        task: TaskId,
        budget: &mut SourceBudget,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
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
                return Err(crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "project source graph is cancelled",
                ));
            }
            if let Some(parent) = &state.spec.parent {
                operation = parent.operation_id;
                if let Some(io) = work.as_deref_mut() {
                    io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if operation == anchor {
                    return Err(crate::store::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Unauthorized,
                        "project source graph is cyclic",
                    ));
                }
                steps += 1;
                if steps == run {
                    anchor = operation;
                    run = run.checked_mul(2).ok_or_else(|| {
                        crate::store::guarded_boundary(
                            work,
                            crate::wire::ErrorCode::Invalid,
                            "project source graph depth overflows",
                        )
                    })?;
                    steps = 0;
                }
            } else {
                coordinator
                    .scheduler()
                    .session_limits(operation, work)?
                    .validate(work)?;
                return Ok(TaskId::from_bytes(operation.into_bytes()));
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "The bounded creator traversal retains its proof custody and scan credit in one future."
    )]
    pub(super) async fn source_created_project(
        &self,
        source: &TaskAdmissionRecord,
        project: &crate::conversation::VolumeRef,
        budget: &mut SourceBudget,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, 0);
        #[cfg(not(target_arch = "wasm32"))]
        if source.parent.is_none()
            && source.resource_proofs.is_empty()
            && let Some(original) = quota
                .work()
                .as_deref()
                .and_then(|io| io.original_native_work())
                .cloned()
        {
            budget.charge(1, 0, quota.work())?;
            let read =
                project.capability(crate::conversation::VolumeOperation::Read, quota.work())?;
            quota.adopt(read.capacity() as u64)?;
            let digest = source.canonical_digest_with_work(quota.work())?;
            let mut allowed = false;
            for provider in &self.resource_creation {
                budget.charge(1, 0, quota.work())?;
                for (snapshot, binding) in provider
                    .root_bootstrap_sources(source.operation_id)
                    .into_iter()
                    .flatten()
                {
                    if &snapshot.volume != project || !source.grants.contains(&read) {
                        continue;
                    }
                    if binding.original_task() != source.task_id()
                        || binding.admission_digest() != digest
                    {
                        return Err(Error::Unauthorized(
                            "ROOT source differs from its retained admission".into(),
                        ));
                    }
                    let context =
                        crate::resource_preparation::RootBootstrapContentContext::authenticated(
                            source,
                            self.verifier.audience(),
                            binding,
                            digest,
                        );
                    let access = provider
                        .verify_root_bootstrap_with_native_work(context, snapshot, &original)?
                        .await?;
                    allowed |= access.operation == source.operation_id
                        && access.admission_digest == digest
                        && access.grants.contains(&read)
                        && access
                            .content
                            .iter()
                            .any(|binding| &binding.volume == project);
                }
            }
            drop(read);
            return Ok(allowed);
        }
        use std::borrow::Cow;
        let mut pending = Vec::new();
        let mut current = Some((Cow::Borrowed(source), Vec::new(), 0));
        while let Some((admission, mut path, held)) = current.take().or_else(|| pending.pop()) {
            budget.charge(1, 0, quota.work())?;
            if !admission.resource_proofs.is_empty() {
                if Self::proof_contains(&path, &admission.operation_id, quota.work())? {
                    return Err(crate::store::guarded_boundary(
                        quota.work(),
                        crate::wire::ErrorCode::Unauthorized,
                        "project creator proof is cyclic",
                    ));
                }
                let slots = Self::reserve_proof_slots(&mut path, quota.work())?;
                quota.adopt(slots)?;
                path.push(admission.operation_id);
                let parent = admission.parent.ok_or_else(|| {
                    crate::store::guarded_boundary(
                        quota.work(),
                        crate::wire::ErrorCode::Unauthorized,
                        "project creator has no original parent",
                    )
                })?;
                let (_, bytes) = self.source_admission(parent, budget, quota.work()).await?;
                if let Some(io) = quota.work().as_deref_mut() {
                    io.release_owned(bytes)?;
                }
                for proof in &admission.resource_proofs {
                    budget.charge(1, 0, quota.work())?;
                    let (preparation, retained) = self
                        .source_admission(proof.preparation, budget, quota.work())
                        .await?;
                    quota.adopt(retained)?;
                    {
                        let mut request_quota =
                            crate::wire_codec::CodecQuota::new(quota.work(), 0)?;
                        let (request, bytes) =
                            crate::resource_preparation::ResourcePreparationRequest::decode(
                                &preparation.input,
                                request_quota.work(),
                            )?;
                        request_quota.adopt(bytes)?;
                        request.validate(request_quota.work())?;
                        if request.maximum_work > budget.work
                            || request.maximum_bytes > budget.bytes
                        {
                            return Err(crate::store::guarded_boundary(
                                request_quota.work(),
                                crate::wire::ErrorCode::Invalid,
                                "creator verification exceeds selected source bounds",
                            ));
                        }
                        budget.charge(
                            request.maximum_work,
                            request.maximum_bytes,
                            request_quota.work(),
                        )?;
                        let original_parent = preparation.parent.ok_or_else(|| {
                            crate::store::guarded_boundary(
                                request_quota.work(),
                                crate::wire::ErrorCode::Unauthorized,
                                "project preparation has no original parent",
                            )
                        })?;
                        let (_, bytes) = self
                            .source_admission(original_parent, budget, request_quota.work())
                            .await?;
                        if let Some(io) = request_quota.work().as_deref_mut() {
                            io.release_owned(bytes)?;
                        }
                        if request.child != *admission {
                            let (_, bytes) = self
                                .source_admission(
                                    TaskId::from_bytes(request.child.operation_id.into_bytes()),
                                    budget,
                                    request_quota.work(),
                                )
                                .await?;
                            if let Some(io) = request_quota.work().as_deref_mut() {
                                io.release_owned(bytes)?;
                            }
                            self.source_graph_root(parent, budget, request_quota.work())
                                .await?;
                        }
                    }
                    let next_path = crate::core::copy_copy_vector(&path, &mut quota)?;
                    if quota.work().is_none() {
                        quota.adopt(
                            (next_path.capacity() * std::mem::size_of::<OperationId>()) as u64,
                        )?;
                    }
                    let slots = Self::reserve_proof_slots(&mut pending, quota.work())?;
                    quota.adopt(slots)?;
                    pending.push((Cow::Owned(preparation), next_path, retained));
                }
            }
            let path_bytes = (path.capacity() * std::mem::size_of::<OperationId>()) as u64;
            drop(path);
            drop(admission);
            quota.relinquish(held)?;
            quota.relinquish(path_bytes)?;
        }
        let mut visited = Vec::new();
        let access = self
            .created_access(source, &mut visited, quota.work())
            .await;
        let retained = (visited.capacity() * std::mem::size_of::<OperationId>()) as u64;
        quota.adopt(retained)?;
        let access = access?;
        let selected = access.as_ref().is_some_and(|access| {
            access
                .content
                .iter()
                .any(|binding| &binding.volume == project)
        });
        drop(access);
        drop(visited);
        drop(pending);
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
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        if request.child.parent != Some(parent)
            || request.maximum_work > record.maximum_work
            || request.maximum_bytes > record.maximum_bytes
            || request
                .command
                .as_ref()
                .is_some_and(|command| command.operation_id != record.operation_id)
        {
            return Err(crate::store::guarded_boundary(
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
            return Err(crate::store::guarded_boundary(
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

    pub(super) async fn seal_project_source_authorization(
        &self,
        record: &ProjectSourceAuthorization,
        parent: TaskId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<ProjectSourceReadHandle> {
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        self.verify_project_source_authorization(record, parent, &mut work)
            .await?;
        let copied = record.copy(&mut work)?.0;
        ProjectSourceReadHandle::seal(copied, &self.verifier, &mut work)
    }

    pub(super) async fn verify_project_source_authorization(
        &self,
        record: &ProjectSourceAuthorization,
        parent: TaskId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        record.verify_scope(&self.verifier, quota.work())?;
        if &record.owner != self.verifier.audience() || record.caller_task != parent {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source differs from original caller or owning coordinator",
            ));
        }
        let mut budget = SourceBudget {
            work: record.maximum_work,
            bytes: record.maximum_bytes,
        };
        let (caller, retained) = self
            .source_admission(parent, &mut budget, quota.work())
            .await?;
        quota.adopt(retained)?;
        if crate::executor::json::canonical_json_digest(
            &caller,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            quota.work(),
        )? != record.caller_admission_digest
            || caller
                .run_limits
                .deadline_epoch_ms
                .is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
        {
            return Err(crate::store::guarded_boundary(
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
            return Err(crate::store::guarded_boundary(
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
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source retained creator evidence changed",
            ));
        }
        Ok(())
    }
}
