use super::*;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn merge_binding(&self, context: &ToolContext, arguments: &MergeToolArguments,
        original: Option<&RecursiveMergeToolPlan>, cancellation: &CancellationToken) -> Result<RecursiveMergeToolBinding<P, A, O>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context, true).await?;
        let source_owner = self.caller(arguments.source, None)?;
        let allocation = self.config.merges.iter().find(|merge| merge.caller == caller.task && merge.source == arguments.source)
            .ok_or_else(|| Error::Unauthorized("configured merge has no explicit notice binding".into()))?;
        let runtime = self.runtime(None)?;
        let inspected = runtime.inspect_source(caller.task, arguments.source).await?;
        if inspected.project()?.volume != source_owner.project
            || inspected.volumes().iter().filter(|snapshot| snapshot.volume == source_owner.private_volume).count() != 1
            || source_owner.result_volume.as_ref().is_some_and(|result|
                inspected.volumes().iter().filter(|snapshot| &snapshot.volume == result).count() != 1) {
            return Err(Error::Unauthorized("configured merge source is not its admitted project".into()));
        }
        let (project, generation, notice) = if let Some(original) = original {
            if original.caller != caller.task || original.arguments != *arguments || &original.destination != caller.owner.authority()
                || &original.source != source_owner.owner.authority() || original.target_project != caller.project || original.source_project != source_owner.project {
                return Err(Error::Unauthorized("configured merge retry changed original owners".into()));
            }
            (original.source_project.clone(), original.source_generation.clone(), original.notice.clone())
        } else { (inspected.project()?.volume.clone(), inspected.project()?.generation.clone(), allocation.notice.clone()) };
        let source_read = if let Some(authorization) = original.and_then(|plan| plan.authorized_source.as_ref()) {
            let owner = Arc::new(runtime.task_host().journal_owner(caller.task, context.task().owned_task_fence()?).await?
                .with_invocation_cancellation(cancellation));
            let handle = owner.restore_project_source_authorization(context.task(), authorization).await?;
            handle.require_selection(context.operation_id(), arguments.source, &project, &generation,
                source_owner.native_snapshot.as_ref(), None)?;
            Some(handle)
        } else if original.is_none() && arguments.source != caller.task
            && !context.scope().grants().contains(&project.capability(VolumeOperation::Read, &mut None)?) {
            let BoundProjectSourcePolicy { issuer, provider, .. } = self.project_policy.get().ok_or_else(||
                Error::Unauthorized("foreign merge lacks an explicit RootService Project policy".into()))?;
            let owner = Arc::new(runtime.task_host().journal_owner(caller.task, context.task().owned_task_fence()?).await?
                .with_invocation_cancellation(cancellation));
            Some(owner.authorize_project_source(context.task(), issuer, provider.as_ref(), arguments.source,
                context.operation_id(), ProjectSourceSnapshot { project: project.clone(), generation: generation.clone(),
                    native_snapshot: source_owner.native_snapshot.clone() },
                self.config.content_budget.items_examined, self.config.history_limits.maximum_bytes).await?)
        } else { None };
        context.read_file_measured(&notice.content, self.config.content_budget, cancellation)
            .await.map_err(|failure| failure.error)?;
        Ok(RecursiveMergeToolBinding { filesystem: self.filesystem.clone(), destination: self.aggregate(
            &caller.owner, &caller.owner.scope, Some(self.ordinary_resolver(context.task(), cancellation).await?),
            cancellation).await?, verifier: verifier(&caller.owner)?,
            scope: caller.owner.scope.clone(), project: caller.project.clone(), source: source_owner.owner.authority().clone(), source_project: project,
            source_generation: generation, source_read, notice })
    }
}
use super::*;

#[cfg(not(target_arch = "wasm32"))]
impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn native_merge_binding(
        &self, context: &ToolContext, arguments: &MergeToolArguments,
        original: Option<&RecursiveMergeToolPlan>, cancellation: &CancellationToken,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(RecursiveMergeToolBinding<P, A, O>, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let owner = self.native_source_owner(context.task(), cancellation, work).await?;
        let caller = self.authenticate_native_caller(context.task(), owner.as_ref(), true, work).await?;
        self.require_native_effective_volume(context, &caller.project, VolumeOperation::Read, work)?;
        self.require_native_effective_volume(context, &caller.project, VolumeOperation::Write, work)?;
        let source_owner = self.caller(arguments.source, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let allocation = {
            let mut found = None;
            for merge in &self.config.merges {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 32, items: 1 }, true)?;
                if merge.caller == caller.task && merge.source == arguments.source {
                    found = Some(merge);
                    break;
                }
            }
            found.ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native merge has no configured original notice binding" })?
        };
        let runtime = self.runtime(Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        let snapshots = runtime.task_host().observe_volume_snapshots_with_native_work(arguments.source, work).await?;
        let selected = {
            let mut found = None;
            for snapshot in &snapshots.0 {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }, true)?;
                if snapshot.volume == source_owner.project {
                    if found.is_some() {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                            reason: "native merge repeats its authenticated Project selection" });
                    }
                    found = Some(snapshot);
                }
            }
            found.ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native merge selected source has no authenticated created Project" })?
        };
        if let Some(original) = original {
            if original.caller != caller.task || original.arguments != *arguments
                || &original.destination != caller.owner.authority()
                || &original.source != source_owner.owner.authority()
                || original.target_project != caller.project || original.source_project != selected.volume {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native merge retry changed original source or destination" });
            }
        }
        let generation = original.map_or(&selected.generation, |original| &original.source_generation).clone_with_io(&io)?;
        let mut observer = io.history_io();
        let mut history_work = Some(&mut observer as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut history_work, 0)?;
        let notice = original.map_or(&allocation.notice, |original| &original.notice).clone_in_quota(&mut quota)?;
        quota.transfer();
        drop(quota);
        let source_read = if let Some(authorization) = original.and_then(|plan| plan.authorized_source.as_ref()) {
            Some(owner.restore_project_source_authorization_with_native_work(
                context.task(), authorization, work,
            )?.await?)
        } else {
            let capability = selected.volume.capability(VolumeOperation::Read, &mut Some(&mut io.history_io()))?;
            let hold = io.guard_owned(capability.capacity() as u64)?;
            let capability = (capability, hold);
            if context.effective_scope().grants().contains(&capability.0) { None }
            else if original.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "retained ambient merge no longer has original Project read authority" });
            } else {
                let BoundProjectSourcePolicy { issuer, provider: policy, custody } = self.project_policy.get().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "foreign native merge has no original RootService Project policy",
                })?;
                self.require_native_policy(custody.as_ref(), work)?;
                let snapshot = ProjectSourceSnapshot {
                    project: {
                        let mut history = io.history_io();
                        selected.volume.copy(&mut Some(&mut history))?
                    },
                    generation: generation.clone_with_io(&io)?,
                    native_snapshot: source_owner.native_snapshot.as_ref()
                        .map(|value| crate::model::clone_value(value, Some(&io))).transpose()?,
                };
                Some(owner.authorize_project_source_with_native_work(context.task(), issuer, policy.as_ref(),
                    arguments.source, context.operation_id(), &snapshot,
                    self.config.content_budget.items_examined, self.config.history_limits.maximum_bytes, work,
                )?.await?)
            }
        };
        if let Some(handle) = &source_read {
            let authorization = handle.0.authorization();
            handle.0.require_selection(context.operation_id(), arguments.source,
                &selected.volume, &generation, authorization.native_snapshot.as_ref(), Some(work))?;
        }
        context.read_file_with_native_work(&notice.content, work).await?;
        let destination = self.native_open_aggregate(&caller.owner, None, cancellation, work).await?;
        let (verifier, scope, project, source, source_project) = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            (caller.owner.issuer().verifier(&mut observer)?,
                caller.owner.scope.copy(&mut observer)?, caller.project.copy(&mut observer)?,
                observer.as_deref_mut().expect("native merge observer").clone_authority(source_owner.owner.authority())?,
                selected.volume.copy(&mut observer)?)
        };
        let aggregate = self.bind_aggregate(&caller.owner, &scope, destination.0,
            Some(self.native_resolver(context, Arc::clone(&owner), work)?), Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        io.absorb_native_custody(destination.1)?;
        let source_read = match source_read {
            Some((handle, guard)) => {
                io.absorb_native_custody(guard)?;
                Some(handle)
            }
            None => None,
        };
        let retained = io.owned_bytes()?;
        Ok((RecursiveMergeToolBinding { filesystem: Arc::clone(&self.filesystem), destination: aggregate,
            verifier, scope, project, source, source_project, source_generation: generation, source_read, notice,
        }, io.into_native_custody(retained)?))
    }
}
