use super::*;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn authenticate_caller<'a>(
        &'a self,
        context: &TaskContext,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        project_read: bool,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<&'a ConfiguredRecursiveCaller> {
        use crate::executor::stock_boundary as failure;
        use crate::wire::ErrorCode;
        #[cfg(not(target_arch = "wasm32"))]
        let scratch = io
            .and_then(crate::filesystem::PreparationIo::original_native_work)
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = scratch.as_ref().or(io);
        let task = context.durable_task_id().ok_or_else(|| {
            failure(
                io,
                ErrorCode::Unauthorized,
                "configured tool has no admitted caller",
            )
        })?;
        let caller = self.caller(task, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            let binding = context.owned_task_binding_with_native_work(work)?;
            let owner = owner.ok_or_else(|| {
                failure(
                    io,
                    ErrorCode::Unauthorized,
                    "native source has no original owner",
                )
            })?;
            owner.require_mail_scope_with_native_work(context.scope(), work)?;
            if owner.task_binding() != binding
                || owner.authenticated_binding_agent(&mut Some(
                    &mut crate::store::HistoryWork::new(
                        crate::filesystem::PreparationIo::from_native_work(work)?,
                    ),
                ))? != caller.owner.scope.agent()
            {
                return Err(failure(
                    io,
                    ErrorCode::Unauthorized,
                    "source owner differs from its admitted current creator",
                ));
            }
        }
        if self.check_caller_allocation(caller, io).await?
            && !crate::executor::with_history_io(io, |work| {
                Ok::<_, Error>(context.scope().current_created_content(work)?.is_some_and(
                    |binding| {
                        binding.authority.as_ref() == Some(caller.owner.authority())
                            && binding.volume == caller.private_volume
                    },
                ))
            })?
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "configured descendant is not its admitted allocation",
            ));
        }
        let verifier = crate::executor::with_history_io(io, |work| {
            let verifier = caller.owner.issuer().verifier(work)?;
            verifier.verify_scope_for_with_work(
                caller.owner.authority(),
                &caller.owner.scope,
                work,
            )?;
            Ok::<_, Error>(verifier)
        })?;
        for volume in
            std::iter::once(&caller.private_volume).chain(project_read.then_some(&caller.project))
        {
            #[cfg(not(target_arch = "wasm32"))]
            if io
                .and_then(crate::filesystem::PreparationIo::original_native_work)
                .is_some()
            {
                crate::executor::with_history_io(io, |work| {
                    owner.expect("native source owner").require_volume_grant(
                        volume,
                        VolumeOperation::Read,
                        work,
                    )
                })?;
                continue;
            }
            let read = volume.capability(VolumeOperation::Read, &mut None)?;
            if !context.scope().grants().contains(&read) {
                return Err(failure(
                    io,
                    ErrorCode::Unauthorized,
                    "configured caller lacks its selected source read",
                ));
            }
            ContentGrant::verify(
                &verifier,
                &caller.owner.scope,
                volume,
                VolumeOperation::Read,
                u64::MAX,
                &mut None,
            )?;
        }
        let _ = owner;
        Ok(caller)
    }
    pub(super) async fn check_caller_allocation(
        &self,
        caller: &ConfiguredRecursiveCaller,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        let scratch = io
            .and_then(crate::filesystem::PreparationIo::original_native_work)
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = scratch.as_ref().or(io);
        let runtime = self.runtime(io)?;
        let admitted = runtime.admission_observed(caller.task, io).await?;
        if admitted.parent.is_none() {
            return Ok(false);
        }
        let ordinary;
        #[cfg(not(target_arch = "wasm32"))]
        let native;
        let volumes = 'volumes: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work)
            {
                native = runtime
                    .task_host()
                    .observe_volume_snapshots_with_native_work(caller.task, work)
                    .await?;
                break 'volumes native.0.as_slice();
            }
            ordinary = runtime.inspect_source(caller.task, caller.task).await?;
            if ordinary.project()?.volume != caller.project {
                return Err(crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "configured source is not its admitted Project",
                ));
            }
            ordinary.volumes()
        };
        let (mut project, mut private, mut result) = (0, 0, 0);
        for snapshot in volumes {
            if let Some(io) = io {
                io.codec_charge(
                    crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 },
                    true,
                )?;
            }
            project += usize::from(snapshot.volume == caller.project);
            private += usize::from(snapshot.volume == caller.private_volume);
            result += usize::from(caller.result_volume.as_ref() == Some(&snapshot.volume));
        }
        if project != 1 || private != 1 || (caller.result_volume.is_some() && result != 1) {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
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
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<StreamAggregate<P>> {
        #[cfg(not(target_arch = "wasm32"))]
        io.map(|io| {
            io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native aggregate binding requires original work",
            })
        })
        .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io = None;
        let verifier = if io.is_some() || content.is_none() {
            Some(crate::executor::with_history_io(io, |work| {
                binding.issuer().verifier(work)
            })?)
        } else {
            None
        };
        let content = match content {
            Some(content) => content,
            None => FilesystemContentVerifier::new(
                Arc::clone(&self.filesystem),
                verifier.as_ref().expect("content verifier"),
                scope,
                self.limits.file_bytes,
                io,
            )?,
        };
        let original = io.map(|io| (verifier.as_ref().expect("native verifier"), scope, io));
        let filesystem: Arc<dyn crate::fork::ForkSeedVerifier> = FilesystemForkVerifier::new(
            Arc::clone(&self.filesystem),
            self.limits.file_bytes,
            None,
            original,
        )?;
        let history = crate::executor::with_history_io(io, |work| {
            StreamHistoryForkVerifier::new(&self.config.stream_provider, work)
        })?;
        let forks = crate::executor::with_history_io(io, |work| {
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
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Arc<crate::durable_host::TaskJournalOwner<P>>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            let (task, fence) = context.owned_task_binding_with_native_work(work)?;
            let owner = self
                .runtime(io)?
                .task_host()
                .journal_owner(task, fence, Some(&crate::executor::PreparationIo::from_native_work(work)?))
                .await?
                .with_invocation_cancellation(cancellation);
            owner.require_mail_scope_with_native_work(context.scope(), work)?;
            return owner.into_native_shared(work);
        }
        let task = context.durable_task_id().ok_or_else(|| {
            Error::Unauthorized("recursive source has no original admitted reader".into())
        })?;
        Ok(Arc::new(
            self.runtime(io)?
                .task_host()
                .journal_owner(task, &(context.owned_task_fence()?), None)
                .await?
                .with_invocation_cancellation(cancellation),
        ))
    }
    pub(super) fn resolver(
        &self,
        context: &ToolContext,
        owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        #[cfg(not(target_arch = "wasm32"))]
        if io
            .and_then(crate::filesystem::PreparationIo::original_native_work)
            .is_some()
        {
            let context = context.native_task().ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "native source requires its original shared task context",
                )
            })?;
            return crate::runtime::content_residency_verifier(Arc::clone(context), owner, io);
        }
        crate::runtime::content_residency_verifier(Arc::new(context.task().clone()), owner, None)
    }
}
