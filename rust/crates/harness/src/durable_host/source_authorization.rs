use super::*;
use crate::core::AuthorityIssuer;
use crate::source_authorization::{
    ProjectSourceAuthorization, ProjectSourceReadHandle, ProjectSourceSnapshot,
    RootProjectSourceContext, RootProjectSourceProvider,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::store::history::HistoryIo;

struct SourceBudget { work: u64, bytes: u64 }
impl SourceBudget {
    fn charge(&mut self, work: u64, bytes: u64) -> Result<()> {
        self.work = self.work.checked_sub(work)
            .ok_or_else(|| Error::Invalid("project source verification exceeds work bound".into()))?;
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or_else(|| Error::Invalid("project source verification exceeds byte bound".into()))?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn charge_with_native_work(
        &mut self, units: u64, bytes: u64, original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        original.remaining()?;
        self.work = self.work.checked_sub(units).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "project source verification exceeds its original work bound",
        })?;
        self.bytes = self.bytes.checked_sub(bytes).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "project source verification exceeds its original byte bound",
        })?;
        Ok(())
    }
}

impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Authorizes one project selection under the original current owner and an explicitly configured RootService policy.
    #[allow(clippy::too_many_arguments)]
    pub async fn authorize_project_source(
        self: &Arc<Self>, context: &crate::runtime::TaskContext,
        issuer: &AuthorityIssuer, provider: &dyn RootProjectSourceProvider,
        source_task: TaskId, operation_id: OperationId, snapshot: ProjectSourceSnapshot,
        maximum_work: u64, maximum_bytes: u64,
    ) -> Result<ProjectSourceReadHandle> {
        if context.durable_task_id() != Some(self.task_id)
            || context.owned_task_fence()? != self.fence
            || context.scope().grants() != &self.input_grants
            || issuer.verifier() != self.host.verifier
            || maximum_work == 0 || maximum_work == u64::MAX
            || maximum_bytes == 0 || maximum_bytes == u64::MAX
            || operation_id == self.operation_id()
        {
            return Err(Error::Unauthorized("project source issuer, bounds or original owner differs".into()));
        }
        let cancellation = self.invocation_cancellation.as_ref().ok_or_else(|| {
            Error::Unauthorized("project source policy requires actual invocation cancellation".into())
        })?;
        if cancellation.is_cancelled() {
            return Err(Error::Unauthorized("project source invocation is cancelled".into()));
        }
        self.verify(false, &mut None).await?;
        let snapshot_bytes = crate::contract::json_byte_length(&snapshot, maximum_bytes)?;
        snapshot.project.validate()?;
        snapshot.generation.validate()?;
        if snapshot.project.class() != crate::conversation::VolumeClass::Project
            || snapshot.project.provider() != snapshot.generation.as_resource().provider()
        {
            return Err(Error::Unauthorized("source selection is not one provider's project generation".into()));
        }
        let mut budget = SourceBudget { work: maximum_work, bytes: maximum_bytes };
        budget.charge(1, snapshot_bytes)?;
        let caller = self.host.source_admission(self.task_id, &mut budget).await?;
        if crate::contract::canonical_json_digest(&caller)? != self.original_admission_digest {
            return Err(Error::Unauthorized("original project source caller admission changed".into()));
        }
        let source = self.host.source_admission(source_task, &mut budget).await?;
        let graph_root = self.host.source_graph_root(self.task_id, &mut budget).await?;
        if self.host.source_graph_root(source_task, &mut budget).await? != graph_root {
            return Err(Error::Unauthorized("project source belongs to another actual session graph".into()));
        }
        let created_project_authenticated = self.host.source_created_project(
            &source, &snapshot.project, &mut budget,
        ).await?;
        if !created_project_authenticated && source_task != graph_root {
            return Err(Error::Unauthorized("selected source has no authenticated created project".into()));
        }
        if budget.work == 0 || budget.bytes == 0 {
            return Err(Error::Invalid("project source snapshot verifier has no remaining work or bytes".into()));
        }
        provider.verify(RootProjectSourceContext {
            owner: &self.host.owner, caller: &caller, source: &source, graph_root,
            operation_id, maximum_work: budget.work, maximum_bytes: budget.bytes,
            created_project_authenticated,
            cancellation,
        }, &snapshot).await?;
        self.verify(false, &mut None).await?;
        if cancellation.is_cancelled() {
            return Err(Error::Unauthorized("project source invocation cancelled during snapshot verification".into()));
        }
        let mut authorization = ProjectSourceAuthorization {
            owner: self.host.owner.clone(), caller_task: self.task_id,
            caller_admission_digest: self.original_admission_digest, caller_fence: self.fence.clone(),
            operation_id, source_task, project: snapshot.project, generation: snapshot.generation,
            native_snapshot: snapshot.native_snapshot, graph_root, maximum_work, maximum_bytes,
            policy_scope: Scope::from_wire(String::new(), Capabilities::default(), String::new(), None, None, [0;32]),
        };
        let label = authorization.scope_label()?;
        authorization.policy_scope = issuer.root(label.clone(), Capabilities::new([label]));
        ProjectSourceReadHandle::seal(authorization, &self.host.verifier)
    }

    /// Restores immutable issued evidence only through the genuine current original owner.
    pub async fn restore_project_source(
        self: &Arc<Self>, context: &crate::runtime::TaskContext,
        request: &crate::resource_preparation::ResourcePreparationRequest,
    ) -> Result<Option<ProjectSourceReadHandle>> {
        if context.durable_task_id() != Some(self.task_id)
            || context.owned_task_fence()? != self.fence
            || context.scope().grants() != &self.input_grants
            || self.invocation_cancellation.as_ref().is_some_and(acyclic_fs::CancellationToken::is_cancelled)
        {
            return Err(Error::Unauthorized("project source restoration lacks its current original owner".into()));
        }
        self.verify(false, &mut None).await?;
        request.validate()?;
        let handle = self.host.seal_prepared_project_source(request, self.task_id).await?;
        self.verify(false, &mut None).await?;
        if self.invocation_cancellation.as_ref().is_some_and(acyclic_fs::CancellationToken::is_cancelled) {
            return Err(Error::Unauthorized("project source restoration invocation is cancelled".into()));
        }
        Ok(handle)
    }

    /// Restores the issued authorization retained in an original source intent before staging effects or constructing an ordinary preparation request.
    pub async fn restore_project_source_authorization(
        self: &Arc<Self>, context: &crate::runtime::TaskContext,
        authorization: &ProjectSourceAuthorization,
    ) -> Result<ProjectSourceReadHandle> {
        if context.durable_task_id() != Some(self.task_id)
            || context.owned_task_fence()? != self.fence
            || context.scope().grants() != &self.input_grants
            || authorization.caller_admission_digest != self.original_admission_digest
            || self.invocation_cancellation.as_ref().is_some_and(acyclic_fs::CancellationToken::is_cancelled)
        {
            return Err(Error::Unauthorized("project source intent restoration lacks its current original owner".into()));
        }
        self.verify(false, &mut None).await?;
        let handle = self.host.seal_project_source_authorization(authorization, self.task_id).await?;
        self.verify(false, &mut None).await?;
        if self.invocation_cancellation.as_ref().is_some_and(acyclic_fs::CancellationToken::is_cancelled) {
            return Err(Error::Unauthorized("project source intent restoration invocation is cancelled".into()));
        }
        Ok(handle)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn authorize_project_source_with_native_work<'a>(
        self: &'a Arc<Self>, context: &'a crate::runtime::TaskContext,
        issuer: &'a AuthorityIssuer, provider: &'a dyn RootProjectSourceProvider,
        source_task: TaskId, operation_id: OperationId, snapshot: &'a ProjectSourceSnapshot,
        maximum_work: u64, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ProjectSourceReadHandle, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let binding = context.owned_task_binding_with_native_work(work)?;
            self.require_mail_scope_with_native_work(context.scope(), work)?;
            if binding != (self.task_id, &self.fence)
                || issuer.audience() != self.host.verifier.audience()
                || maximum_work == 0 || maximum_work == u64::MAX
                || maximum_bytes == 0 || maximum_bytes == u64::MAX
                || operation_id == self.operation_id()
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source issuer, bounds or original owner differs" });
            }
            let cancellation = self.invocation_cancellation.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "project source requires original invocation cancellation",
            })?;
            cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
            self.verify_with_native_work(false, work).await?;
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            let verifier = issuer.verifier_with_work(&mut observer)?;
            if verifier != self.host.verifier {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source signer differs from installed owner" });
            }
            let verifier_bytes = verifier.logical_owned_bytes()?;
            drop(verifier);
            observer.as_deref_mut().expect("original source observer").release_owned(verifier_bytes)?;
            snapshot.project.validate_with_work(&mut observer)?;
            snapshot.generation.validate_with_work(&mut observer)?;
            if snapshot.project.class() != crate::conversation::VolumeClass::Project
                || snapshot.project.provider() != snapshot.generation.as_resource().provider()
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source selection has another namespace or provider" });
            }
            let snapshot_bytes = crate::contract::count_trusted_json_bytes_with_work(
                snapshot, maximum_bytes, &mut observer,
            )?;
            let mut budget = SourceBudget { work: maximum_work, bytes: maximum_bytes };
            budget.charge_with_native_work(1, snapshot_bytes, work)?;
            let caller = self.host.source_admission_with_native_work(self.task_id, &mut budget, work).await?;
            if crate::contract::canonical_json_digest_with_work(
                &caller.0, acyclic_stream::MAX_RECORD_BYTES as u64, &mut observer,
            )? != self.original_admission_digest {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source original caller admission changed" });
            }
            let source = self.host.source_admission_with_native_work(source_task, &mut budget, work).await?;
            let graph_root = self.host.source_graph_root_with_native_work(self.task_id, &mut budget, work).await?;
            if self.host.source_graph_root_with_native_work(source_task, &mut budget, work).await? != graph_root {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source belongs to another original graph" });
            }
            let created_project_authenticated = self.host.source_created_project_with_native_work(
                &source.0, &snapshot.project, &mut budget, work,
            ).await?;
            if (!created_project_authenticated && source_task != graph_root)
                || budget.work == 0 || budget.bytes == 0 {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source has no bounded authenticated creator" });
            }
            provider.verify_with_native_work(RootProjectSourceContext {
                owner: &self.host.owner, caller: &caller.0, source: &source.0, graph_root,
                operation_id, maximum_work: budget.work, maximum_bytes: budget.bytes,
                created_project_authenticated, cancellation,
            }, snapshot, work)?.await?;
            self.verify_with_native_work(false, work).await?;
            cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
            drop(observer);
            drop(history);
            let selected = {
                let mut history = io.history_io();
                let mut observer: Option<&mut dyn HistoryIo> = Some(&mut history);
                let project = snapshot.project.clone_with_work(&mut observer)?;
                let generation = super::headless::copy_generation(&snapshot.generation, &mut observer)?;
                let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
                let native_snapshot = snapshot.native_snapshot.as_ref()
                    .map(|value| crate::core::copy_value(value, &mut quota)).transpose()?;
                quota.transfer();
                ProjectSourceSnapshot { project, generation, native_snapshot }
            };
            let owner = {
                let mut history = io.history_io();
                history.clone_authority(&self.host.owner)?
            };
            let caller_fence = {
                let mut history = io.history_io();
                self.fence.clone_with_work(&mut Some(&mut history))?
            };
            let mut authorization = ProjectSourceAuthorization {
                owner, caller_task: self.task_id, caller_admission_digest: self.original_admission_digest,
                caller_fence, operation_id, source_task, project: selected.project,
                generation: selected.generation, native_snapshot: selected.native_snapshot,
                graph_root, maximum_work, maximum_bytes,
                policy_scope: Scope::from_wire(String::new(), Capabilities::default(), String::new(), None, None, [0; 32]),
            };
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            let label = authorization.scope_label_with_work(&mut observer)?;
            observer.as_deref_mut().expect("original source observer")
                .admit_owned(std::mem::size_of::<String>() as u64)?;
            let capability = observer.as_deref_mut().expect("original source observer").clone_text(&label)?;
            let capabilities = Capabilities::new([capability]);
            observer.as_deref_mut().expect("original source observer")
                .retain_owned(std::mem::size_of::<String>() as u64)?;
            authorization.policy_scope = issuer.root_with_work(&label, &capabilities, &mut observer)?;
            let capabilities_bytes = capabilities.logical_owned_bytes()?;
            drop(capabilities);
            observer.as_deref_mut().expect("original source observer").release_owned(capabilities_bytes)?;
            let label_bytes = label.capacity() as u64;
            drop(label);
            observer.as_deref_mut().expect("original source observer").release_owned(label_bytes)?;
            let handle = ProjectSourceReadHandle::seal_with_work(authorization, &self.host.verifier, &mut observer)?;
            drop(observer);
            drop(history);
            let retained = io.owned_bytes()?;
            Ok((handle, io.into_native_custody(retained)?))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn restore_project_source_authorization_with_native_work<'a>(
        self: &'a Arc<Self>, context: &'a crate::runtime::TaskContext,
        authorization: &'a ProjectSourceAuthorization,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ProjectSourceReadHandle, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let binding = context.owned_task_binding_with_native_work(work)?;
            self.require_mail_scope_with_native_work(context.scope(), work)?;
            if binding != (self.task_id, &self.fence)
                || authorization.caller_admission_digest != self.original_admission_digest {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source restoration lacks its original owner" });
            }
            let cancellation = self.invocation_cancellation.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "project source restoration requires original cancellation",
            })?;
            cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
            self.verify_with_native_work(false, work).await?;
            self.host.verify_project_source_authorization_with_native_work(authorization, self.task_id, work).await?;
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let copied = {
                let mut history = io.history_io();
                let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
                authorization.clone_with_work_and_custody(&mut observer)?.0
            };
            let handle = {
                let mut history = io.history_io();
                let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
                ProjectSourceReadHandle::seal_with_work(copied, &self.host.verifier, &mut observer)?
            };
            self.verify_with_native_work(false, work).await?;
            cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
            let retained = io.owned_bytes()?;
            Ok((handle, io.into_native_custody(retained)?))
        })
    }

    pub(crate) async fn verify_project_source_handle(
        self: &Arc<Self>, context: &crate::runtime::TaskContext,
        handle: &ProjectSourceReadHandle,
    ) -> Result<()> {
        let cancellation = self.invocation_cancellation.as_ref().ok_or_else(|| {
            Error::Unauthorized("selected project read requires actual invocation cancellation".into())
        })?;
        if context.durable_task_id() != Some(self.task_id)
            || context.owned_task_fence()? != self.fence
            || context.scope().grants() != &self.input_grants
            || handle.authorization().caller_admission_digest != self.original_admission_digest
            || cancellation.is_cancelled()
        {
            return Err(Error::Unauthorized("selected project read lacks its current original owner".into()));
        }
        self.verify(false, &mut None).await?;
        self.host.verify_project_source_authorization(handle.authorization(), self.task_id).await?;
        self.verify(false, &mut None).await?;
        cancellation.check().map_err(|_| Error::Unauthorized("selected project invocation cancelled".into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn verify_project_source_handle_with_native_work(
        self: &Arc<Self>, context: &crate::runtime::TaskContext,
        handle: &ProjectSourceReadHandle, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let cancellation = self.invocation_cancellation.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "selected project read requires actual invocation cancellation",
        })?;
        work.remaining()?;
        if cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        if !context.original_task_binding_matches(self.task_id, &self.fence)
            || context.scope().grants() != &self.input_grants
            || context.scope().limits() != self.input_limits
            || handle.authorization().caller_admission_digest != self.original_admission_digest
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "selected project read lacks its current original owner",
            });
        }
        self.verify_project_source_read_with_native_work(handle, work).await
    }

    /// Revalidates the sealed reader already captured by the original owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn verify_project_source_read_with_native_work(
        &self, handle: &ProjectSourceReadHandle, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        let cancellation = self.invocation_cancellation.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "selected project read requires actual invocation cancellation",
        })?;
        if cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        if handle.authorization().caller_admission_digest != self.original_admission_digest {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "selected project read differs from its original admission",
            });
        }
        self.verify_with_native_work(false, work).await?;
        self.host.verify_project_source_authorization_with_native_work(
            handle.authorization(), self.task_id, work,
        ).await?;
        self.verify_with_native_work(false, work).await?;
        if cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        Ok(())
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    async fn source_admission(&self, task: TaskId, budget: &mut SourceBudget) -> Result<TaskAdmissionRecord> {
        budget.charge(1, 0)?;
        let operation = OperationId::from_bytes(task.into_bytes());
        let spec = self.retained_declaration_with_work(operation, &mut None).await?
            .ok_or_else(|| Error::NotFound("project source admission".into()))?;
        budget.charge(0, spec.state.descriptor().byte_length())?;
        let admission = self.read_retained_admission_counted(operation, &mut None).await.map(|(record, _)| record)?;
        Ok(admission)
    }

    async fn source_graph_root(&self, task: TaskId, budget: &mut SourceBudget) -> Result<TaskId> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let mut operation = OperationId::from_bytes(task.into_bytes());
        let mut visited = BTreeSet::new();
        loop {
            budget.charge(1, 0)?;
            if !visited.insert(operation) {
                return Err(Error::Unauthorized("project source graph is cyclic".into()));
            }
            let state = coordinator.observe_operation(&self.owner, &self.owner_scope, &self.verifier, operation)?;
            if state.cancellation_requested {
                return Err(Error::Unauthorized("project source graph is cancelled".into()));
            }
            match state.spec.parent {
                Some(parent) => operation = parent.operation_id,
                None => {
                    coordinator.scheduler().session_limits(operation, &mut None)?.validate()?;
                    return Ok(TaskId::from_bytes(operation.into_bytes()));
                }
            }
        }
    }

    async fn source_created_project(
        &self, source: &TaskAdmissionRecord, project: &crate::conversation::VolumeRef,
        budget: &mut SourceBudget,
    ) -> Result<bool> {
        let mut pending = vec![(source.clone(), BTreeSet::new())];
        while let Some((admission, mut path)) = pending.pop() {
            budget.charge(1, 0)?;
            if admission.resource_proofs.is_empty() { continue; }
            if !path.insert(admission.operation_id) {
                return Err(Error::Unauthorized("project creator proof is cyclic".into()));
            }
            let parent = admission.parent.ok_or_else(|| Error::Unauthorized("project creator has no parent".into()))?;
            self.source_admission(parent, budget).await?;
            for proof in &admission.resource_proofs {
                budget.charge(1, 0)?;
                let preparation = self.source_admission(proof.preparation, budget).await?;
                let request: crate::resource_preparation::ResourcePreparationRequest =
                    serde_json::from_value(preparation.input.clone()).map_err(|error| Error::Invalid(error.to_string()))?;
                request.validate()?;
                if request.maximum_work > budget.work || request.maximum_bytes > budget.bytes {
                    return Err(Error::Invalid("creator verification exceeds selected source bounds".into()));
                }
                budget.charge(request.maximum_work, request.maximum_bytes)?;
                let original_parent = preparation.parent.ok_or_else(|| Error::Unauthorized("project preparation has no parent".into()))?;
                self.source_admission(original_parent, budget).await?;
                if request.child != admission {
                    self.source_admission(TaskId::from_bytes(request.child.operation_id.into_bytes()), budget).await?;
                    self.source_graph_root(parent, budget).await?;
                }
                pending.push((preparation, path.clone()));
            }
        }
        let access = self.verify_created_access_with_work(source, &mut Vec::new(), &mut None).await?;
        Ok(access.as_ref().is_some_and(|access| access.content.iter().any(|binding| &binding.volume == project)))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn source_admission_with_native_work(
        &self, task: TaskId, budget: &mut SourceBudget,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        budget.charge_with_native_work(1, 0, original)?;
        let io = crate::filesystem::PreparationIo::from_native_work(original)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let operation = OperationId::from_bytes(task.into_bytes());
        let spec = self.retained_declaration_with_work(operation, &mut observer).await?
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::NotFound,
                reason: "project source admission is missing" })?;
        budget.charge_with_native_work(0, spec.state.descriptor().byte_length(), original)?;
        let spec_bytes = spec.logical_owned_bytes()?;
        let admitted = self.read_declared_admission_counted(&spec, &mut observer).await;
        drop(spec);
        let released = observer.as_deref_mut().expect("original source observer").release_owned(spec_bytes);
        let (record, retained) = admitted?;
        released?;
        drop(observer);
        drop(history);
        Ok((record, io.into_native_custody(retained)?))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn source_graph_root_with_native_work(
        &self, task: TaskId, budget: &mut SourceBudget,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskId> {
        let io = crate::filesystem::PreparationIo::from_native_work(original)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh_with_work(&mut observer).await?;
        let mut operation = OperationId::from_bytes(task.into_bytes());
        let mut visited = Vec::new();
        loop {
            budget.charge_with_native_work(1, 0, original)?;
            if Self::proof_contains(&visited, &operation, &mut observer)? {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source graph is cyclic" });
            }
            Self::reserve_proof_slots(&mut visited, &mut observer)?;
            visited.push(operation);
            let state = coordinator.observe_operation_ref_with_work(
                &self.owner, &self.owner_scope, &self.verifier, operation, &mut observer,
            )?;
            if state.cancellation_requested {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source graph is cancelled" });
            }
            match &state.spec.parent {
                Some(parent) => operation = parent.operation_id,
                None => {
                    coordinator.scheduler().session_limits(operation, &mut observer)?
                        .validate_with_work(&mut observer)?;
                    return Ok(TaskId::from_bytes(operation.into_bytes()));
                }
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn source_created_project_with_native_work(
        &self, source: &TaskAdmissionRecord, project: &crate::conversation::VolumeRef,
        budget: &mut SourceBudget, original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        if source.parent.is_none() && source.resource_proofs.is_empty() {
            budget.charge_with_native_work(1, 0, original)?;
            let access = self.verify_root_bootstrap_access_with_native_work(source, original).await?;
            let read = project.capability_with_native_work(crate::conversation::VolumeOperation::Read, original)?;
            let selected = project.class() == crate::conversation::VolumeClass::Project
                && access.as_ref().is_some_and(|access|
                    access.grants.contains(&read.0)
                        && access.content.iter().any(|binding| &binding.volume == project));
            drop(access);
            drop(read);
            return Ok(selected);
        }
        use std::borrow::Cow;
        let io = crate::filesystem::PreparationIo::from_native_work(original)?;
        let mut pending = Vec::new();
        let mut current = Some((Cow::Borrowed(source), Vec::new(), None));
        while let Some(mut item) = current.take().or_else(|| pending.pop()) {
            let admission = &item.0;
            let path = &mut item.1;
            budget.charge_with_native_work(1, 0, original)?;
            if admission.resource_proofs.is_empty() { continue; }
            {
                let mut history = io.history_io();
                let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
                if Self::proof_contains(path, &admission.operation_id, &mut observer)? {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "project creator proof is cyclic" });
                }
                Self::reserve_proof_slots(path, &mut observer)?;
                path.push(admission.operation_id);
            }
            let parent = admission.parent.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "project creator has no original parent",
            })?;
            self.source_admission_with_native_work(parent, budget, original).await?;
            for proof in &admission.resource_proofs {
                budget.charge_with_native_work(1, 0, original)?;
                let preparation = self.source_admission_with_native_work(
                    proof.preparation, budget, original,
                ).await?;
                {
                    let mut history = io.history_io();
                    let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
                    let mut request_custody = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
                    let (request, retained) =
                        crate::resource_preparation::ResourcePreparationRequest::from_borrowed_value_with_work(
                            &preparation.0.input, request_custody.work(),
                        )?;
                    request_custody.adopt(retained)?;
                    request.validate_with_work(request_custody.work())?;
                    if request.maximum_work > budget.work || request.maximum_bytes > budget.bytes {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                            reason: "creator verification exceeds original source bounds" });
                    }
                    budget.charge_with_native_work(request.maximum_work, request.maximum_bytes, original)?;
                    let original_parent = preparation.0.parent.ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "project preparation has no original parent",
                    })?;
                    self.source_admission_with_native_work(original_parent, budget, original).await?;
                    if request.child != **admission {
                        self.source_admission_with_native_work(
                            TaskId::from_bytes(request.child.operation_id.into_bytes()), budget, original,
                        ).await?;
                        self.source_graph_root_with_native_work(parent, budget, original).await?;
                    }
                }
                let mut next_path = Vec::new();
                {
                    let mut history = io.history_io();
                    let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
                    for operation in path.iter() {
                        Self::reserve_proof_slots(&mut next_path, &mut observer)?;
                        observer.as_deref_mut().expect("original source observer").admit_copy(16)?;
                        next_path.push(*operation);
                        observer.as_deref_mut().expect("original source observer").copy_bytes(16)?;
                    }
                    Self::reserve_proof_slots(&mut pending, &mut observer)?;
                }
                pending.push((Cow::Owned(preparation.0), next_path, Some(preparation.1)));
            }
        }
        let mut visited = Vec::new();
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let access = self.verify_created_access_with_work(source, &mut visited, &mut observer).await?;
        let selected = access.as_ref().is_some_and(|access|
            access.content.iter().any(|binding| &binding.volume == project));
        drop(access);
        drop(visited);
        drop(pending);
        Ok(selected)
    }

    pub(super) async fn seal_prepared_project_source(
        &self, request: &crate::resource_preparation::ResourcePreparationRequest, parent: TaskId,
    ) -> Result<Option<ProjectSourceReadHandle>> {
        let Some(record) = &request.read_authorization else { return Ok(None); };
        if request.child.parent != Some(parent)
            || request.maximum_work > record.maximum_work || request.maximum_bytes > record.maximum_bytes
            || request.command.as_ref().is_some_and(|command| command.operation_id != record.operation_id)
            || request.payload.get("operation_id") != Some(&serde_json::to_value(record.operation_id)
                .map_err(|error| Error::Invalid(error.to_string()))?)
            || request.payload.get("authorized_source") != Some(&serde_json::to_value(record)
                .map_err(|error| Error::Invalid(error.to_string()))?)
        {
            return Err(Error::Unauthorized("project source differs from original preparation caller or bounds".into()));
        }
        self.seal_project_source_authorization(record, parent).await.map(Some)
    }

    /// Seals the exact retained preparation selection on its original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn seal_prepared_project_source_with_native_work(
        &self, request: &crate::resource_preparation::ResourcePreparationRequest,
        parent: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<ProjectSourceReadHandle>, acyclic_fs::NativeAllocation)> {
        let Some(record) = &request.read_authorization else {
            return work.retain_owned(0)
                .map(|custody| (None, custody))
                .map_err(Error::from);
        };
        if request.child.parent != Some(parent)
            || request.maximum_work > record.maximum_work
            || request.maximum_bytes > record.maximum_bytes
            || request.command.as_ref().is_some_and(|command| command.operation_id != record.operation_id)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "project source differs from original preparation caller or bounds",
            });
        }
        {
            let comparison = crate::filesystem::PreparationIo::from_native_work(work)?;
            let operation = comparison.value_json(&record.operation_id, request.maximum_bytes)?;
            let authorization = comparison.value_json(record, request.maximum_bytes)?;
            if request.payload.get("operation_id") != Some(&operation)
                || request.payload.get("authorized_source") != Some(&authorization)
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source differs from the retained preparation payload",
                });
            }
        }
        self.verify_project_source_authorization_with_native_work(record, parent, work).await?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        io.retain_owned(std::mem::size_of::<ProjectSourceReadHandle>() as u64)?;
        let handle = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            let copied = record.clone_with_work_and_custody(&mut observer)?.0;
            ProjectSourceReadHandle::seal_with_work(copied, &self.verifier, &mut observer)?
        };
        let retained = io.owned_bytes()?;
        Ok((Some(handle), io.into_native_custody(retained)?))
    }

    async fn seal_project_source_authorization(
        &self, record: &ProjectSourceAuthorization, parent: TaskId,
    ) -> Result<ProjectSourceReadHandle> {
        self.verify_project_source_authorization(record, parent).await?;
        ProjectSourceReadHandle::seal(record.clone(), &self.verifier)
    }

    async fn verify_project_source_authorization(
        &self, record: &ProjectSourceAuthorization, parent: TaskId,
    ) -> Result<()> {
        record.verify_scope_with_work(&self.verifier, &mut None)?;
        if record.owner != self.owner || record.caller_task != parent {
            return Err(Error::Unauthorized("project source differs from original caller or owning coordinator".into()));
        }
        let mut budget = SourceBudget { work: record.maximum_work, bytes: record.maximum_bytes };
        let caller = self.source_admission(parent, &mut budget).await?;
        if crate::contract::canonical_json_digest(&caller)? != record.caller_admission_digest {
            return Err(Error::Unauthorized("project source original caller admission changed".into()));
        }
        if caller.run_limits.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
            return Err(Error::Unauthorized("project source original caller deadline elapsed".into()));
        }
        if self.source_graph_root(parent, &mut budget).await? != record.graph_root
            || self.source_graph_root(record.source_task, &mut budget).await? != record.graph_root
        {
            return Err(Error::Unauthorized("project source retained graph changed".into()));
        }
        let source = self.source_admission(record.source_task, &mut budget).await?;
        let created = self.source_created_project(&source, &record.project, &mut budget).await?;
        if !created && record.source_task != record.graph_root {
            return Err(Error::Unauthorized("project source retained creator evidence changed".into()));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn verify_project_source_authorization_with_native_work(
        &self, record: &ProjectSourceAuthorization, parent: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        record.verify_scope_with_work(&self.verifier, &mut observer)?;
        if record.owner != self.owner || record.caller_task != parent {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "project source differs from original coordinator or caller" });
        }
        let mut budget = SourceBudget { work: record.maximum_work, bytes: record.maximum_bytes };
        let caller = self.source_admission_with_native_work(parent, &mut budget, work).await?;
        if crate::contract::canonical_json_digest_with_work(
            &caller.0, acyclic_stream::MAX_RECORD_BYTES as u64, &mut observer,
        )? != record.caller_admission_digest
            || caller.0.run_limits.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "project source original caller changed or expired" });
        }
        if self.source_graph_root_with_native_work(parent, &mut budget, work).await? != record.graph_root
            || self.source_graph_root_with_native_work(record.source_task, &mut budget, work).await? != record.graph_root {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "project source retained graph changed" });
        }
        let source = self.source_admission_with_native_work(record.source_task, &mut budget, work).await?;
        if !self.source_created_project_with_native_work(&source.0, &record.project, &mut budget, work).await?
            && record.source_task != record.graph_root {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "project source original creator evidence changed" });
        }
        Ok(())
    }
}
