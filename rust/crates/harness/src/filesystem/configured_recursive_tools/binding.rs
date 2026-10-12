use super::*;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn authenticate_caller<'a>(&'a self, context: &ToolContext, require_project_read: bool) -> Result<&'a ConfiguredRecursiveCaller> {
        let task = context.durable_task_id().ok_or_else(|| Error::Unauthorized("configured tool has no admitted caller".into()))?;
        let caller = self.caller(task, None)?;
        let runtime = self.runtime(None)?;
        let admission = runtime.task_host().observe_admission(task).await?;
        if admission.parent.is_some() {
            let inspection = runtime.inspect_source(task, task).await?;
            if inspection.project()?.volume != caller.project
                || context.scope().current_created_authority(&mut None)? != Some(caller.owner.authority())
                || context.scope().current_created_private_volume(&mut None)? != Some(&caller.private_volume)
                || inspection.volumes().iter().filter(|snapshot| snapshot.volume == caller.private_volume).count() != 1
                || caller.result_volume.as_ref().is_some_and(|result|
                    inspection.volumes().iter().filter(|snapshot| &snapshot.volume == result).count() != 1) {
                return Err(Error::Unauthorized("configured descendant is not its actual admitted source allocation".into()));
            }
        }
        let verifier = verifier(&caller.owner)?;
        for volume in [&caller.project, &caller.private_volume] {
            if volume == &caller.project && !require_project_read { continue; }
            let read = volume.capability(VolumeOperation::Read, &mut None)?;
            if !context.scope().grants().contains(&read) {
                return Err(Error::Unauthorized("configured caller lacks its exact selected source read".into()));
            }
            ContentGrant::verify(&verifier, &caller.owner.scope, volume, VolumeOperation::Read, u64::MAX, &mut None)?;
        }
        Ok(caller)
    }
    pub(super) fn bind_aggregate(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, scope: &Scope,
        aggregate: StreamAggregate<P>, content: Option<Arc<dyn crate::conversation::ContentResidencyVerifier>>,
        _io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<StreamAggregate<P>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = _io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native aggregate binding requires original work",
            })?;
            let verifier = {
                let mut history = io.history_io();
                binding.issuer().verifier(&mut Some(&mut history))?
            };
            let content = match content {
                Some(content) => content,
                None => FilesystemContentVerifier::new(Arc::clone(&self.filesystem), &verifier, scope, self.limits.file_bytes, Some(&io))?,
            };
            let filesystem: Arc<dyn crate::fork::ForkSeedVerifier> = FilesystemForkVerifier::new(Arc::clone(&self.filesystem), self.limits.file_bytes, None, Some((&verifier, scope, &crate::filesystem::PreparationIo::from_native_work(work)?)))?;
            let history: Arc<dyn crate::fork::ForkSeedVerifier> = StreamHistoryForkVerifier::new(&self.config.stream_provider, &mut Some(&mut io.history_io()))?;
            let forks = CompositeForkVerifier::new(&[filesystem, history], &mut Some(&mut io.history_io()))?;
            let merge = FilesystemProjectMergeVerifier::new(
                Arc::clone(&self.filesystem), Some((&verifier, scope, &io)),
            )?;
            let mut aggregate = aggregate.with_content_verifier(content).with_fork_verifier(forks)
                .with_merge_verifier(merge);
            aggregate.set_limits(self.limits)?;
            return Ok(aggregate);
        }
        let content = match content {
            Some(content) => content,
            None => FilesystemContentVerifier::new(self.filesystem.clone(), &(verifier(binding)?), &scope, self.limits.file_bytes, None)?,
        };
        let forks = CompositeForkVerifier::new(&[
            FilesystemForkVerifier::new(self.filesystem.clone(), self.limits.file_bytes, None, None)?,
            StreamHistoryForkVerifier::new(&self.config.stream_provider, &mut None)?,
        ], &mut None)?;
        let mut aggregate = aggregate.with_content_verifier(content).with_fork_verifier(forks)
            .with_merge_verifier(FilesystemProjectMergeVerifier::new(self.filesystem.clone(), None)?);
        aggregate.set_limits(self.limits)?;
        Ok(aggregate)
    }
    pub(super) async fn aggregate(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, scope: &Scope,
        content: Option<Arc<dyn crate::conversation::ContentResidencyVerifier>>,
        cancellation: &CancellationToken,
    ) -> Result<StreamAggregate<P>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let aggregate = StreamAggregate::open(&self.stream, binding.authority().clone(),
            verifier(binding)?, SchemaRegistry::default(), None, self.config.history_limits, crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        self.bind_aggregate(binding, scope, aggregate, content, None)
    }
    pub(super) async fn ordinary_resolver(
        &self, context: &TaskContext, cancellation: &CancellationToken,
    ) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        let task = context.durable_task_id().ok_or_else(||
            Error::Unauthorized("recursive source has no original admitted reader".into()))?;
        let owner = Arc::new(self.runtime(None)?.task_host()
            .journal_owner(task, context.owned_task_fence()?).await?
            .with_invocation_cancellation(cancellation));
        crate::runtime::content_residency_verifier(Arc::new(context.clone()), owner, None)
    }
    pub(super) async fn restore_intent_source(&self, context: &TaskContext, request: &ForkRequest,
        cancellation: &CancellationToken) -> Result<Option<ProjectSourceReadHandle>> {
        let Some(authorization) = &request.authorized_source else { return Ok(None); };
        let caller = context.durable_task_id().ok_or_else(|| Error::Unauthorized("source intent has no original caller".into()))?;
        let runtime = self.runtime(None)?;
        let owner = Arc::new(runtime.task_host().journal_owner(caller, context.owned_task_fence()?).await?
            .with_invocation_cancellation(cancellation));
        Ok(Some(owner.restore_project_source_authorization(context, authorization).await?))
    }
}
use super::*;

#[cfg(not(target_arch = "wasm32"))]
impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) fn require_native_policy(
        &self, custody: Option<&acyclic_fs::NativeAllocation>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        let custody = custody.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native Project policy requires originally funded installed metadata",
        })?;
        if !custody.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native Project policy belongs to another original ledger" });
        }
        custody.work().remaining()?;
        Ok(())
    }
    pub(super) async fn native_source_owner(&self, context: &TaskContext,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<Arc<crate::durable_host::TaskJournalOwner<P>>> {
        let (task, fence) = context.owned_task_binding_with_native_work(work)?;
        let owner = self.runtime(Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?.task_host()
            .journal_owner_with_native_work(task, fence, work).await?
            .with_invocation_cancellation(cancellation);
        owner.require_mail_scope_with_native_work(context.scope(), work)?;
        owner.into_native_shared(work)
    }
    pub(super) async fn authenticate_native_caller<'a>(&'a self, context: &TaskContext,
        owner: &crate::durable_host::TaskJournalOwner<P>, require_project_read: bool,
        work: &acyclic_fs::NativeMaterializationWork) -> Result<&'a ConfiguredRecursiveCaller> {
        let (task, fence) = context.owned_task_binding_with_native_work(work)?;
        owner.require_mail_scope_with_native_work(context.scope(), work)?;
        if owner.task_binding() != (task, fence) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source owner differs from original context" });
        }
        let caller = self.caller(task, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        if owner.authenticated_binding_agent(&mut Some(&mut crate::store::history::native_work_io(work)?))? != caller.owner.scope.agent() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source actor differs from admitted current creator" });
        }
        let runtime = self.runtime(Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        let admission = runtime.task_host().observe_admission_with_native_work(task, work).await?;
        if admission.0.parent.is_some() {
            let snapshots = runtime.task_host().observe_volume_snapshots_with_native_work(task, work).await?;
            let mut project = 0_usize;
            let mut scratch = 0_usize;
            let mut result = 0_usize;
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            for snapshot in &snapshots.0 {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }, true)?;
                project += usize::from(snapshot.volume == caller.project);
                scratch += usize::from(snapshot.volume == caller.private_volume);
                result += usize::from(caller.result_volume.as_ref() == Some(&snapshot.volume));
            }
            if project != 1 || scratch != 1 || (caller.result_volume.is_some() && result != 1)
                || context.scope().current_created_authority(&mut None)? != Some(caller.owner.authority())
                || context.scope().current_created_private_volume(&mut None)? != Some(&caller.private_volume) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native source binding differs from authenticated current allocation" });
            }
        }
        owner.require_volume_grant(&caller.private_volume, VolumeOperation::Read, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
        if require_project_read {
            owner.require_volume_grant(&caller.project, VolumeOperation::Read, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        let verifier = caller.owner.issuer().verifier(&mut observer)?;
        verifier.verify_scope_for_with_work(caller.owner.authority(), &caller.owner.scope, &mut observer)?;
        Ok(caller)
    }
    pub(super) async fn native_open_aggregate(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, cut: Option<u64>,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(StreamAggregate<P>, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let aggregate = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            let verifier = binding.issuer().verifier(&mut observer)?;
            verifier.verify_scope_for_with_work(binding.authority(), &binding.scope, &mut observer)?;
            let authority = observer.as_deref_mut().expect("native aggregate observer").clone_authority(binding.authority())?;
            StreamAggregate::open(&self.stream, authority, verifier, SchemaRegistry::new(), None,
                self.config.history_limits, crate::store::DEFAULT_TERMINAL_EFFECTS, cut, &mut observer).await?
        };
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let retained = io.owned_bytes()?;
        Ok((aggregate, io.into_native_custody(retained)?))
    }
    pub(super) fn require_native_effective_volume(
        &self, context: &ToolContext, volume: &VolumeRef, operation: VolumeOperation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let capability = volume.capability(operation, &mut Some(&mut io.history_io()))?;
        let hold = io.guard_owned(capability.capacity() as u64)?;
        let capability = (capability, hold);
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }, true)?;
        if !context.effective_scope().grants().contains(&capability.0) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native recursive tool effective scope denies its selected volume operation" });
        }
        Ok(())
    }
    pub(super) fn native_resolver(
        &self, context: &ToolContext, owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        let context = context.native_task().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native recursive source requires its original native task context",
        })?;
        crate::runtime::content_residency_verifier(Arc::clone(context), owner,
            Some(&crate::filesystem::PreparationIo::from_native_work(work)?))
    }
}
