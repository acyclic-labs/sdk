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
    fn charge(
        &mut self, units: u64, bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        self.work = self.work.checked_sub(units).ok_or_else(||
            admission_boundary(
                work, crate::wire::ErrorCode::Invalid, "project source verification exceeds work bound"))?;
        self.bytes = self.bytes.checked_sub(bytes).ok_or_else(||
            admission_boundary(
                work, crate::wire::ErrorCode::Invalid, "project source verification exceeds byte bound"))?;
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
            || issuer.verifier(&mut None).expect("verifier copy") != self.host.verifier
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
        snapshot.project.validate(&mut None)?;
        snapshot.generation.validate(&mut None)?;
        if snapshot.project.class() != crate::conversation::VolumeClass::Project
            || snapshot.project.provider() != snapshot.generation.as_resource().provider()
        {
            return Err(Error::Unauthorized("source selection is not one provider's project generation".into()));
        }
        let mut budget = SourceBudget { work: maximum_work, bytes: maximum_bytes };
        budget.charge(1, snapshot_bytes, &mut None)?;
        let caller = self.host.source_admission(self.task_id, &mut budget, &mut None).await?.0;
        if crate::contract::canonical_json_digest(&caller)? != self.original_admission_digest {
            return Err(Error::Unauthorized("original project source caller admission changed".into()));
        }
        let source = self.host.source_admission(source_task, &mut budget, &mut None).await?.0;
        let graph_root = self.host.source_graph_root(self.task_id, &mut budget, &mut None).await?;
        if self.host.source_graph_root(source_task, &mut budget, &mut None).await? != graph_root {
            return Err(Error::Unauthorized("project source belongs to another actual session graph".into()));
        }
        let created_project_authenticated = self.host.source_created_project(&source, &snapshot.project, &mut budget, &mut None).await?;
        if !created_project_authenticated && source_task != graph_root {
            return Err(Error::Unauthorized("selected source has no authenticated created project".into()));
        }
        if budget.work == 0 || budget.bytes == 0 {
            return Err(Error::Invalid("project source snapshot verifier has no remaining work or bytes".into()));
        }
        provider.verify(RootProjectSourceContext {
            owner: self.host.verifier.audience(), caller: &caller, source: &source, graph_root,
            operation_id, maximum_work: budget.work, maximum_bytes: budget.bytes,
            created_project_authenticated,
            cancellation,
        }, &snapshot).await?;
        self.verify(false, &mut None).await?;
        if cancellation.is_cancelled() {
            return Err(Error::Unauthorized("project source invocation cancelled during snapshot verification".into()));
        }
        let mut authorization = ProjectSourceAuthorization {
            owner: self.host.verifier.audience().clone(), caller_task: self.task_id,
            caller_admission_digest: self.original_admission_digest, caller_fence: self.fence.clone(),
            operation_id, source_task, project: snapshot.project, generation: snapshot.generation,
            native_snapshot: snapshot.native_snapshot, graph_root, maximum_work, maximum_bytes,
            policy_scope: Scope::from_wire(String::new(), Capabilities::default(), String::new(), None, None, [0;32]),
        };
        let label = authorization.scope_label(&mut None)?;
        authorization.policy_scope = issuer.issue(&label, &Capabilities::new([label.as_str()]), None, None, &mut None)?;
        ProjectSourceReadHandle::seal(authorization, &self.host.verifier, &mut None)
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
        let handle = self.host.seal_project_source_authorization(authorization, self.task_id, None).await?;
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
            let verifier = issuer.verifier(&mut observer)?;
            if verifier != self.host.verifier {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source signer differs from installed owner" });
            }
            let verifier_bytes = verifier.logical_owned_bytes()?;
            drop(verifier);
            observer.as_deref_mut().expect("original source observer").release_owned(verifier_bytes)?;
            snapshot.project.validate(&mut observer)?;
            snapshot.generation.validate(&mut observer)?;
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
            budget.charge(1, snapshot_bytes, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
            let (caller, retained) = self.host.source_admission(self.task_id, &mut budget, &mut observer).await?;
            let caller = (caller, io.take_native_custody(retained)?);
            if crate::contract::canonical_json_digest_with_work(
                &caller.0, acyclic_stream::MAX_RECORD_BYTES as u64, &mut observer,
            )? != self.original_admission_digest {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source original caller admission changed" });
            }
            let (source, retained) = self.host.source_admission(source_task, &mut budget, &mut observer).await?;
            let source = (source, io.take_native_custody(retained)?);
            let graph_root = self.host.source_graph_root(self.task_id, &mut budget, &mut observer).await?;
            if self.host.source_graph_root(source_task, &mut budget, &mut observer).await? != graph_root {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source belongs to another original graph" });
            }
            let created_project_authenticated = self.host.source_created_project(&source.0, &snapshot.project, &mut budget, &mut observer).await?;
            if (!created_project_authenticated && source_task != graph_root)
                || budget.work == 0 || budget.bytes == 0 {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source has no bounded authenticated creator" });
            }
            provider.verify_with_native_work(RootProjectSourceContext {
                owner: self.host.verifier.audience(), caller: &caller.0, source: &source.0, graph_root,
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
                let project = snapshot.project.copy(&mut observer)?;
                let generation = super::headless::copy_generation(&snapshot.generation, &mut observer)?;
                let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
                let native_snapshot = snapshot.native_snapshot.as_ref()
                    .map(|value| crate::core::copy_value(value, &mut quota)).transpose()?;
                quota.transfer();
                ProjectSourceSnapshot { project, generation, native_snapshot }
            };
            let owner = {
                let mut history = io.history_io();
                history.clone_authority(self.host.verifier.audience())?
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
            let label = authorization.scope_label(&mut observer)?;
            observer.as_deref_mut().expect("original source observer")
                .admit_owned(std::mem::size_of::<String>() as u64)?;
            let capability = observer.as_deref_mut().expect("original source observer").clone_text(&label)?;
            let capabilities = Capabilities::new([capability]);
            observer.as_deref_mut().expect("original source observer")
                .retain_owned(std::mem::size_of::<String>() as u64)?;
            authorization.policy_scope = issuer.issue(&label, &capabilities, None, None, &mut observer)?;
            let capabilities_bytes = capabilities.logical_owned_bytes()?;
            drop(capabilities);
            observer.as_deref_mut().expect("original source observer").release_owned(capabilities_bytes)?;
            let label_bytes = label.capacity() as u64;
            drop(label);
            observer.as_deref_mut().expect("original source observer").release_owned(label_bytes)?;
            let handle = ProjectSourceReadHandle::seal(authorization, &self.host.verifier, &mut observer)?;
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
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let handle = self.host.seal_project_source_authorization(authorization, self.task_id, Some(&io)).await?;
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
        self.host.verify_project_source_authorization(handle.authorization(), self.task_id, &mut None).await?;
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
        self.host.verify_project_source_authorization(handle.authorization(), self.task_id, &mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
        self.verify_with_native_work(false, work).await?;
        if cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        Ok(())
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    async fn source_admission(
        &self, task: TaskId, budget: &mut SourceBudget,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(TaskAdmissionRecord, u64)> {
        budget.charge(1, 0, work)?;
        let operation = OperationId::from_bytes(task.into_bytes());
        let spec = self.retained_declaration_with_work(operation, work).await?
            .ok_or_else(|| admission_boundary(work, crate::wire::ErrorCode::NotFound,
                "project source admission is missing"))?;
        let held = if work.is_some() { spec.logical_owned_bytes()? } else { 0 };
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, held);
        budget.charge(0, spec.state.descriptor().byte_length(), quota.work())?;
        let admitted = self.read_declared_admission_counted(&spec, quota.work()).await;
        drop(spec);
        let released = quota.relinquish(held);
        let record = admitted?;
        released?;
        Ok(record)
    }

    async fn source_graph_root(
        &self, task: TaskId, budget: &mut SourceBudget,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskId> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(quota.work()).await?;
        let mut operation = OperationId::from_bytes(task.into_bytes());
        let mut visited = Vec::new();
        loop {
            budget.charge(1, 0, quota.work())?;
            if Self::proof_contains(&visited, &operation, quota.work())? {
                return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized,
                    "project source graph is cyclic"));
            }
            let retained = Self::reserve_proof_slots(&mut visited, quota.work())?;
            quota.adopt(retained)?;
            visited.push(operation);
            let state = coordinator.observe_operation(
                self.verifier.audience(), &self.owner_scope, &self.verifier, operation, quota.work())?;
            if state.cancellation_requested {
                return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized,
                    "project source graph is cancelled"));
            }
            match &state.spec.parent {
                Some(parent) => operation = parent.operation_id,
                None => {
                    coordinator.scheduler().session_limits(operation, quota.work())?.validate(quota.work())?;
                    return Ok(TaskId::from_bytes(operation.into_bytes()));
                }
            }
        }
    }

    async fn source_created_project(
        &self, source: &TaskAdmissionRecord, project: &crate::conversation::VolumeRef,
        budget: &mut SourceBudget, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        if source.parent.is_none() && source.resource_proofs.is_empty()
            && let Some(original) = work.as_deref().and_then(|io| io.original_native_work()).cloned()
        {
            budget.charge(1, 0, work)?;
            let access = self.verify_root_bootstrap_access_with_native_work(source, &original).await?;
            let read = project.capability(crate::conversation::VolumeOperation::Read, work)?;
            let allowed = project.class() == crate::conversation::VolumeClass::Project
                && access.as_ref().is_some_and(|access|
                    access.grants.contains(&read)
                        && access.content.iter().any(|binding| &binding.volume == project));
            let retained = read.capacity() as u64;
            drop(read);
            work.as_deref_mut().expect("native observer").release_owned(retained)?;
            return Ok(allowed);
        }
        use std::borrow::Cow;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut pending = Vec::new();
        let mut current = Some((Cow::Borrowed(source), Vec::new(), 0));
        while let Some((admission, mut path, held)) = current.take().or_else(|| pending.pop()) {
            budget.charge(1, 0, quota.work())?;
            if !admission.resource_proofs.is_empty() {
                if Self::proof_contains(&path, &admission.operation_id, quota.work())? {
                    return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized,
                        "project creator proof is cyclic"));
                }
                let slots = Self::reserve_proof_slots(&mut path, quota.work())?;
                quota.adopt(slots)?;
                path.push(admission.operation_id);
                let parent = admission.parent.ok_or_else(|| admission_boundary(
                    quota.work(), crate::wire::ErrorCode::Unauthorized, "project creator has no original parent"))?;
                let (_, bytes) = self.source_admission(parent, budget, quota.work()).await?;
                if let Some(io) = quota.work().as_deref_mut() { io.release_owned(bytes)?; }
                for proof in &admission.resource_proofs {
                    budget.charge(1, 0, quota.work())?;
                    let (preparation, retained) = self.source_admission(proof.preparation, budget, quota.work()).await?;
                    quota.adopt(retained)?;
                    {
                        let mut request_quota = crate::wire_codec::CodecQuota::new(quota.work(), 0)?;
                        let (request, bytes) = crate::resource_preparation::ResourcePreparationRequest::from_borrowed_value_with_work(
                            &preparation.input, request_quota.work())?;
                        request_quota.adopt(bytes)?;
                        request.validate(request_quota.work())?;
                        if request.maximum_work > budget.work || request.maximum_bytes > budget.bytes {
                            return Err(admission_boundary(request_quota.work(), crate::wire::ErrorCode::Invalid,
                                "creator verification exceeds selected source bounds"));
                        }
                        budget.charge(request.maximum_work, request.maximum_bytes, request_quota.work())?;
                        let original_parent = preparation.parent.ok_or_else(|| admission_boundary(
                            request_quota.work(), crate::wire::ErrorCode::Unauthorized, "project preparation has no original parent"))?;
                        let (_, bytes) = self.source_admission(original_parent, budget, request_quota.work()).await?;
                        if let Some(io) = request_quota.work().as_deref_mut() { io.release_owned(bytes)?; }
                        if request.child != *admission {
                            let (_, bytes) = self.source_admission(TaskId::from_bytes(request.child.operation_id.into_bytes()),
                                budget, request_quota.work()).await?;
                            if let Some(io) = request_quota.work().as_deref_mut() { io.release_owned(bytes)?; }
                            self.source_graph_root(parent, budget, request_quota.work()).await?;
                        }
                    }
                    let next_path = crate::core::copy_copy_vector(&path, &mut quota)?;
                    if quota.work().is_none() {
                        quota.adopt((next_path.capacity() * std::mem::size_of::<OperationId>()) as u64)?;
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
        let access = self.verify_created_access_with_work(source, &mut visited, quota.work()).await;
        let retained = (visited.capacity() * std::mem::size_of::<OperationId>()) as u64;
        quota.adopt(retained)?;
        let access = access?;
        let selected = access.as_ref().is_some_and(|access|
            access.content.iter().any(|binding| &binding.volume == project));
        drop(access);
        drop(visited);
        drop(pending);
        Ok(selected)
    }

    pub(super) async fn seal_prepared_project_source(
        &self, request: &crate::resource_preparation::ResourcePreparationRequest, parent: TaskId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Option<ProjectSourceReadHandle>> {
        let Some(record) = &request.read_authorization else { return Ok(None); };
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let work = history.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        if request.child.parent != Some(parent)
            || request.maximum_work > record.maximum_work || request.maximum_bytes > record.maximum_bytes
            || request.command.as_ref().is_some_and(|command| command.operation_id != record.operation_id)
        {
            return Err(admission_boundary(&work, crate::wire::ErrorCode::Unauthorized,
                "project source differs from original preparation caller or bounds"));
        }
        let payload_matches = match io {
            #[cfg(not(target_arch = "wasm32"))]
            Some(io) => {
                let comparison = crate::filesystem::PreparationIo::from_native_work(
                    io.original_native_work().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "project source preparation requires original work",
                    })?)?;
                let operation = comparison.value_json(&record.operation_id, request.maximum_bytes)?;
                let authorization = comparison.value_json(record, request.maximum_bytes)?;
                request.payload.get("operation_id") == Some(&operation)
                    && request.payload.get("authorized_source") == Some(&authorization)
            }
            _ => request.payload.get("operation_id") == Some(&serde_json::to_value(record.operation_id)
                .map_err(|error| Error::Invalid(error.to_string()))?)
                && request.payload.get("authorized_source") == Some(&serde_json::to_value(record)
                    .map_err(|error| Error::Invalid(error.to_string()))?),
        };
        if !payload_matches {
            return Err(admission_boundary(&work, crate::wire::ErrorCode::Unauthorized,
                "project source differs from the retained preparation payload"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io { io.retain_owned(std::mem::size_of::<ProjectSourceReadHandle>() as u64)?; }
        self.seal_project_source_authorization(record, parent, io).await.map(Some)
    }

    async fn seal_project_source_authorization(
        &self, record: &ProjectSourceAuthorization, parent: TaskId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<ProjectSourceReadHandle> {
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        self.verify_project_source_authorization(record, parent, &mut work).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if io.is_some() {
            let copied = record.clone_with_work_and_custody(&mut work)?.0;
            return ProjectSourceReadHandle::seal(copied, &self.verifier, &mut work);
        }
        ProjectSourceReadHandle::seal(record.clone(), &self.verifier, &mut None)
    }

    async fn verify_project_source_authorization(
        &self, record: &ProjectSourceAuthorization, parent: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        record.verify_scope_with_work(&self.verifier, quota.work())?;
        if &record.owner != self.verifier.audience() || record.caller_task != parent {
            return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized,
                "project source differs from original caller or owning coordinator"));
        }
        let mut budget = SourceBudget { work: record.maximum_work, bytes: record.maximum_bytes };
        let (caller, retained) = self.source_admission(parent, &mut budget, quota.work()).await?;
        quota.adopt(retained)?;
        if crate::contract::canonical_json_digest_with_work(
            &caller, acyclic_stream::MAX_RECORD_BYTES as u64, quota.work())? != record.caller_admission_digest
            || caller.run_limits.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
        {
            return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized,
                "project source original caller changed or expired"));
        }
        if self.source_graph_root(parent, &mut budget, quota.work()).await? != record.graph_root
            || self.source_graph_root(record.source_task, &mut budget, quota.work()).await? != record.graph_root
        {
            return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized,
                "project source retained graph changed"));
        }
        let (source, retained) = self.source_admission(record.source_task, &mut budget, quota.work()).await?;
        quota.adopt(retained)?;
        if !self.source_created_project(&source, &record.project, &mut budget, quota.work()).await?
            && record.source_task != record.graph_root
        {
            return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized,
                "project source retained creator evidence changed"));
        }
        Ok(())
    }

}
