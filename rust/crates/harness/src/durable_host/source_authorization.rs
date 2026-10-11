use super::*;
use crate::core::AuthorityIssuer;
use crate::source_authorization::{
    ProjectSourceAuthorization, ProjectSourceReadHandle, ProjectSourceSnapshot,
    RootProjectSourceContext, RootProjectSourceProvider,
};

struct SourceBudget { work: u64, bytes: u64 }
impl SourceBudget {
    fn charge(&mut self, work: u64, bytes: u64) -> Result<()> {
        self.work = self.work.checked_sub(work)
            .ok_or_else(|| Error::Invalid("project source verification exceeds work bound".into()))?;
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or_else(|| Error::Invalid("project source verification exceeds byte bound".into()))?;
        Ok(())
    }
}

impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Authorizes one project selection under the original current owner and an
    /// explicitly configured RootService policy. Does not alter original grants.
    #[allow(clippy::too_many_arguments, reason = "original owner, policy, snapshot and finite bounds are explicit")]
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
        self.verify(false).await?;
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
        // The callback may await a backend. Recheck the original lease before issuing.
        self.verify(false).await?;
        if cancellation.is_cancelled() {
            return Err(Error::Unauthorized("project source invocation cancelled during snapshot verification".into()));
        }
        let mut authorization = ProjectSourceAuthorization {
            owner: self.host.owner.clone(), caller_task: self.task_id,
            caller_admission_digest: self.original_admission_digest, caller_fence: self.fence.clone(),
            operation_id, source_task, project: snapshot.project, generation: snapshot.generation,
            native_snapshot: snapshot.native_snapshot, graph_root, maximum_work, maximum_bytes,
            // Unissued local placeholder; it cannot verify and never escapes.
            policy_scope: Scope::from_wire(String::new(), Capabilities::default(), String::new(), None, None, [0;32]),
        };
        let label = authorization.scope_label()?;
        authorization.policy_scope = issuer.root(label.clone(), Capabilities::new([label]));
        ProjectSourceReadHandle::seal(authorization, &self.host.verifier)
    }

    /// Restores immutable issued evidence only through the genuine current
    /// original owner. A recovered lease does not rewrite the admitted request.
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
        self.verify(false).await?;
        request.validate()?;
        let handle = self.host.seal_prepared_project_source(request, self.task_id).await?;
        self.verify(false).await?;
        if self.invocation_cancellation.as_ref().is_some_and(acyclic_fs::CancellationToken::is_cancelled) {
            return Err(Error::Unauthorized("project source restoration invocation is cancelled".into()));
        }
        Ok(handle)
    }

    /// Restores the issued authorization retained in an original source intent
    /// before staging effects or constructing an ordinary preparation request.
    /// This cannot reselect a newer generation or substitute an issuing fence.
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
        self.verify(false).await?;
        let handle = self.host.seal_project_source_authorization(authorization, self.task_id).await?;
        self.verify(false).await?;
        if self.invocation_cancellation.as_ref().is_some_and(acyclic_fs::CancellationToken::is_cancelled) {
            return Err(Error::Unauthorized("project source intent restoration invocation is cancelled".into()));
        }
        Ok(handle)
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
        self.verify(false).await?;
        self.host.verify_project_source_authorization(handle.authorization(), self.task_id).await?;
        self.verify(false).await?;
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
        let spec = self.retained_declaration(operation).await?
            .ok_or_else(|| Error::NotFound("project source admission".into()))?;
        // Reject oversized immutable admission content before provider IO.
        budget.charge(0, spec.state.descriptor().byte_length())?;
        let admission = self.read_retained_admission(operation).await?;
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
                    coordinator.scheduler().session_limits(operation)?.validate()?;
                    return Ok(TaskId::from_bytes(operation.into_bytes()));
                }
            }
        }
    }

    async fn source_created_project(
        &self, source: &TaskAdmissionRecord, project: &crate::conversation::VolumeRef,
        budget: &mut SourceBudget,
    ) -> Result<bool> {
        // Preflight the complete existing proof verifier's recursive admission
        // dependency tree, including repeated paths, before provider work begins.
        // The provider remains the sole allocation-record verifier.
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
                // Charge provider-declared maximums before trusting callback IO.
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
        let access = self.verify_created_access(source, &mut BTreeSet::new()).await?;
        Ok(access.as_ref().is_some_and(|access| access.content.iter().any(|binding| &binding.volume == project)))
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

    async fn seal_project_source_authorization(
        &self, record: &ProjectSourceAuthorization, parent: TaskId,
    ) -> Result<ProjectSourceReadHandle> {
        self.verify_project_source_authorization(record, parent).await?;
        ProjectSourceReadHandle::seal(record.clone(), &self.verifier)
    }

    async fn verify_project_source_authorization(
        &self, record: &ProjectSourceAuthorization, parent: TaskId,
    ) -> Result<()> {
        record.verify_scope(&self.verifier)?;
        if record.owner != self.owner || record.caller_task != parent {
            return Err(Error::Unauthorized("project source differs from original caller or owning coordinator".into()));
        }
        let mut budget = SourceBudget { work: record.maximum_work, bytes: record.maximum_bytes };
        let caller = self.source_admission(parent, &mut budget).await?;
        if crate::contract::canonical_json_digest(&caller)? != record.caller_admission_digest {
            return Err(Error::Unauthorized("project source original caller admission changed".into()));
        }
        // Issuing-fence provenance survives an ordinary owner reload. Current
        // creation is fenced by the preparation owner, not this historic fence.
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
        // For bootstrap roots, the original issuer MAC attests the mandatory
        // provider's protected allocation verification. No raw root scope does.
        Ok(())
    }
}
