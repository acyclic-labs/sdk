use super::*;
use crate::executor::with_history_io;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn authenticate<'a>(
        &'a self,
        context: &ToolContext,
        _native_owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        project_read: bool,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<&'a ConfiguredRecursiveCaller> {
        check_cancellation(cancellation, io)?;
        let denied = |reason| failure(io, ErrorCode::Unauthorized, reason);
        let scratch = PreparationIo::temporary(io)?;
        let io = scratch.as_ref().or(io);
        let task = context
            .durable_task_id()
            .ok_or_else(|| denied("configured tool has no admitted caller"))?;
        let caller = self.caller(task, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
            let retained;
            let owner = if let Some(owner) = _native_owner {
                owner
            } else {
                retained = self.source_owner(context.task(), cancellation, io).await?;
                &retained
            };
            let binding = context.task().owned_task_binding(io)?;
            owner.require_mail_scope(context.scope(), io)?;
            if owner.task_binding() != binding
                || with_history_io(io, |work| owner.authenticated_binding_agent(work))?
                    != caller.owner.scope.agent()
            {
                return Err(denied(
                    "source owner differs from its admitted current creator",
                ));
            }
        }
        if self.check_caller_allocation(caller, io).await?
            && !with_history_io(io, |work| {
                Ok::<_, Error>(context.scope().current_created_content(work)?.is_some_and(
                    |binding| {
                        binding.authority.as_ref() == Some(caller.owner.issuer.audience())
                            && binding.volume == caller.private_volume
                    },
                ))
            })?
        {
            return Err(denied(
                "configured descendant is not its admitted allocation",
            ));
        }
        let verifier = with_history_io(io, |work| caller.owner.issuer.verifier(work))?;
        for volume in
            std::iter::once(&caller.private_volume).chain(project_read.then_some(&caller.project))
        {
            let granted = match io {
                Some(io) => {
                    crate::model::charge_items(Some(io), 1)?;
                    io.contains_capability(
                        context.scope().grants(),
                        volume,
                        acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
                    )? && io.contains_capability(
                        context.effective_scope().grants(),
                        volume,
                        acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
                    )?
                }
                None => context
                    .scope()
                    .grants()
                    .contains(&volume.capability(VolumeOperation::Read, &mut None)?),
            };
            if !granted {
                return Err(denied("configured caller lacks its selected source read"));
            }
            with_history_io(io, |work| {
                ContentGrant::authorize_volume(
                    &verifier,
                    &caller.owner.scope,
                    volume,
                    VolumeOperation::Read,
                    u64::MAX,
                    work,
                )
            })?;
        }
        Ok(caller)
    }

    pub(super) async fn snapshots<'a>(
        &self,
        task: TaskId,
        io: Option<&'a PreparationIo>,
    ) -> Result<(
        Vec<crate::durable_host::TaskVolumeSnapshot>,
        Option<super::super::DecodeReservation<'a>>,
    )> {
        let mut history = io.map(PreparationIo::history_io);
        let (snapshots, retained) = self
            .runtime(io)?
            .task_host()
            .observe_volume_snapshots(
                task,
                &mut history.as_mut().map(|work| work as &mut dyn HistoryIo),
            )
            .await?;
        let hold = io.map(|io| io.guard_owned(retained)).transpose()?;
        Ok((snapshots, hold))
    }

    pub(super) async fn check_caller_allocation(
        &self,
        caller: &ConfiguredRecursiveCaller,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let scratch = PreparationIo::temporary(io)?;
        let io = scratch.as_ref().or(io);
        if self
            .runtime(io)?
            .admission_observed(caller.task, io)
            .await?
            .parent
            .is_none()
        {
            return Ok(false);
        }
        let (snapshots, _hold) = self.snapshots(caller.task, io).await?;
        let (mut projects, mut project, mut private, mut result) = (0, 0, 0, 0);
        for snapshot in &snapshots {
            crate::model::charge_items(io, 1)?;
            projects +=
                usize::from(snapshot.volume.class() == crate::conversation::VolumeClass::Project);
            project += usize::from(snapshot.volume == caller.project);
            private += usize::from(snapshot.volume == caller.private_volume);
            result += usize::from(caller.result_volume.as_ref() == Some(&snapshot.volume));
        }
        if projects != 1
            || project != 1
            || private != 1
            || (caller.result_volume.is_some() && result != 1)
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "configured source differs from its admitted allocation",
            ));
        }
        Ok(true)
    }

    pub(super) fn bind_aggregate(
        &self,
        binding: &ConfiguredRecursiveAuthorityBinding,
        scope: &Scope,
        aggregate: StreamAggregate<P>,
        content: Option<Arc<dyn crate::conversation::ContentResidencyVerifier>>,
        io: Option<&PreparationIo>,
    ) -> Result<StreamAggregate<P>> {
        native_io(io)?;
        let verifier = if io.is_some() || content.is_none() {
            Some(with_history_io(io, |work| binding.issuer.verifier(work))?)
        } else {
            None
        };
        let content = match content {
            Some(content) => content,
            None => FilesystemContentVerifier::new(
                Arc::clone(&self.filesystem),
                verifier.as_ref().ok_or_else(|| {
                    failure(
                        io,
                        ErrorCode::Invalid,
                        "configured content verifier is missing",
                    )
                })?,
                scope,
                self.limits.file_bytes,
                io,
            )?,
        };
        let original = io
            .map(|io| {
                verifier
                    .as_ref()
                    .map(|verifier| (verifier, scope, io))
                    .ok_or_else(|| io.invalid("configured native verifier is missing"))
            })
            .transpose()?;
        let filesystem: Arc<dyn crate::fork::ForkSeedVerifier> = FilesystemForkVerifier::new(
            Arc::clone(&self.filesystem),
            self.limits.file_bytes,
            None,
            original,
        )?;
        let history = with_history_io(io, |work| {
            StreamHistoryForkVerifier::new(&self.config.stream_provider, work)
        })?;
        let forks = with_history_io(io, |work| {
            CompositeForkVerifier::new(&[filesystem, history], work)
        })?;
        let mut aggregate = aggregate
            .with_content_verifier(content)
            .with_fork_verifier(forks)
            .with_merge_verifier(FilesystemProjectMergeVerifier::new(
                Arc::clone(&self.filesystem),
                original,
            )?);
        aggregate.set_limits(self.limits)?;
        Ok(aggregate)
    }

    pub(super) async fn source_owner(
        &self,
        context: &TaskContext,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<Arc<crate::durable_host::TaskJournalOwner<P>>> {
        let task = context.durable_task_id().ok_or_else(|| {
            failure(
                io,
                ErrorCode::Unauthorized,
                "recursive source has no original admitted reader",
            )
        })?;
        let fence = context.owned_task_binding(io)?.1;
        let owner = self
            .runtime(io)?
            .task_host()
            .journal_owner(task, fence, io)
            .await?
            .with_invocation_cancellation(cancellation);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            owner.require_mail_scope(context.scope(), io)?;
            return owner.into_native_shared(work);
        }
        Ok(Arc::new(owner))
    }

    pub(super) fn resolver(
        &self,
        context: &ToolContext,
        owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        _native_io: Option<&PreparationIo>,
    ) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        #[cfg(not(target_arch = "wasm32"))]
        if _native_io
            .and_then(PreparationIo::original_native_work)
            .is_some()
        {
            let context = context.native_task().ok_or_else(|| {
                failure(
                    _native_io,
                    ErrorCode::Unauthorized,
                    "native source requires its original shared task context",
                )
            })?;
            return crate::runtime::content_residency_verifier(
                Arc::clone(context),
                owner,
                _native_io,
            );
        }
        crate::runtime::content_residency_verifier(Arc::new(context.task().clone()), owner, None)
    }
}
