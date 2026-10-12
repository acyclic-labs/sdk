use super::*;
use std::collections::BTreeSet;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn child_claims(
        &self,
        context: &ToolContext,
        allocation: &ConfiguredRecursiveFork,
        arguments: &ForkToolArguments,
        source: &RetainedSourceBinding<A, O>,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<(TaskAdmissionRecord, Scope)> {
        use crate::executor::{stock_string_copy as clone_text, stock_boundary as failure};
        let mut child = 'child: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                let work = io
                    .original_native_work()
                    .ok_or_else(|| io.invalid("child claims require original work"))?;
                let (child, custody) = allocation.admission.clone_with_native_work(work)?;
                io.absorb_native_custody(custody)?;
                break 'child child;
            }
            allocation.admission.clone()
        };
        #[cfg(not(target_arch = "wasm32"))]
        let reference_io = io
            .map(|io| {
                crate::filesystem::PreparationIo::from_native_work(
                    io.original_native_work()
                        .expect("child claims verified original work"),
                )
            })
            .transpose()?;
        let references = 'references: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(observer) = &reference_io {
                break 'references crate::conversation::BoundedContentFuture::new(Some(observer), || {
                        source
                            .preparer
                            .required_reference_grants(&source.request, Some(observer))
                    })?
                    .await?;
            }
            source
                .preparer
                .required_reference_grants(&source.request, None)
                .await?
        };
        let mut grants = BTreeSet::new();
        let mut scoped = BTreeSet::new();
        let append = |values: &mut BTreeSet<String>, grant: &str| -> Result<()> {
            crate::model::charge_items(io, 1)?;
            if !values.contains(grant) {
                if let Some(io) = io {
                    io.retain_owned(std::mem::size_of::<String>() as u64)?;
                    io.allocation_attempt(std::mem::size_of::<String>() as u64)?;
                }
                values.insert(clone_text(grant, io)?);
            }
            Ok(())
        };
        for grant in child.grants.iter() {
            append(&mut grants, grant)?;
        }
        for grant in allocation.child.scope.capabilities().iter() {
            append(&mut scoped, grant)?;
        }
        for volume in [&allocation.project, &allocation.private_volume]
            .into_iter()
            .chain(allocation.result_volume.as_ref())
        {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                let grant = crate::executor::with_history_io(io, |work| {
                    volume.capability(operation, work)
                })?;
                let hold = io
                    .map(|io| io.guard_owned(grant.capacity() as u64))
                    .transpose()?;
                append(&mut grants, &grant)?;
                append(&mut scoped, &grant)?;
                drop((grant, hold));
            }
        }
        for file in &allocation.input_files {
            self.delegate_file(context, file, io).await?;
            let grant = match io { Some(io) => io.capability(file, "file:read:"), None => file.read_capability(&mut None) }?;
            let hold = io
                .map(|io| io.guard_owned(grant.capacity() as u64))
                .transpose()?;
            append(&mut grants, &grant)?;
            append(&mut scoped, &grant)?;
            drop((grant, hold));
        }
        for reference in &references {
            if reference.reader != arguments.id {
                return Err(failure(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "inherited reference targets another child reader",
                ));
            }
            for file in
                std::iter::once(&reference.file).chain(reference.attachment_manifest.as_ref())
            {
                self.delegate_file(context, file, io).await?;
            }
            let grant = match io { Some(io) => io.capability(&reference.file, "file:read:"), None => reference.file.read_capability(&mut None) }?;
            let hold = io
                .map(|io| io.guard_owned(grant.capacity() as u64))
                .transpose()?;
            append(&mut grants, &grant)?;
            append(&mut scoped, &grant)?;
            drop((grant, hold));
        }
        let old_bytes = if io.is_some() {
            child.grants.logical_owned_bytes()?
        } else {
            0
        };
        let old = std::mem::replace(&mut child.grants, Capabilities(grants));
        drop(old);
        if let Some(io) = io {
            io.release(old_bytes)?;
        }
        let scoped = Capabilities(scoped);
        let scope = crate::executor::with_history_io(io, |work| {
            allocation.child.issuer().issue(
                allocation.child.scope.id(),
                &scoped,
                Some(arguments.id),
                None,
                work,
            )
        })?;
        let temporary = if io.is_some() {
            scoped.logical_owned_bytes()?
        } else {
            0
        };
        drop(scoped);
        if let Some(io) = io {
            io.release(temporary)?;
        }
        crate::executor::with_history_io(io, |work| child.validate(work))?;
        Ok((child, scope))
    }
    async fn delegate_file(
        &self,
        context: &ToolContext,
        file: &crate::conversation::FileRef,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        if !crate::executor::with_history_io(io, |work| {
            crate::runtime::read_granted_with_work(context.scope().grants(), file, work)
        })? {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "current tool scope cannot delegate this file",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            drop(
                context
                    .task()
                    .read_file(file, Some(&crate::executor::PreparationIo::from_native_work(work)?))
                    .await?,
            );
            return Ok(());
        }
        drop(context.task().read_file(file, None).await?);
        Ok(())
    }
    pub(super) async fn select_source(
        &self,
        context: &ToolContext,
        arguments: &ForkToolArguments,
        cancellation: &CancellationToken,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<RecursiveForkSourceIntent> {
        use crate::filesystem::fork_preparer_io;
        use crate::executor::stock_boundary as failure;
        use crate::wire::ErrorCode;
        let caller = self
            .authenticate(
                context,
                arguments
                    .source
                    .is_none_or(|source| Some(source) == context.durable_task_id()),
                cancellation,
                io,
            )
            .await?;
        let allocation = self.allocation(caller.task, arguments, io)?;
        let parent = self.open(&caller.owner, None, io).await?;
        let project = self.project_generation(&caller.project, io).await?;
        let private = self.project_generation(&caller.private_volume, io).await?;
        let selected = arguments.source.unwrap_or(caller.task);
        let source_read = if selected != caller.task {
            let (issuer, provider) = self.project_policy.get().ok_or_else(|| {
                failure(
                    io,
                    ErrorCode::Unauthorized,
                    "foreign selection has no original RootService policy",
                )
            })?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work)
            {
                work.remaining()?;
                return Err(failure(
                    io,
                    ErrorCode::Unsupported,
                    "native Project policy requires originally funded installed metadata",
                ));
            }
            let chosen = self.caller(selected, io)?;
            if let Some(value) = &chosen.native_snapshot {
                crate::contract::json_byte_length(
                    value,
                    context.scope().limits().file_bytes,
                    &mut None,
                )
                .map(|_| ())?;
            }
            let generation = self.project_generation(&chosen.project, io).await?;
            cancellation
                .check()
                .map_err(|error| Error::Storage(error.to_string()))?;
            let owner = self
                .source_owner(context.task(), cancellation, None)
                .await?;
            Some(
                owner
                    .authorize_project_source(
                        context.task(),
                        issuer,
                        provider.as_ref(),
                        selected,
                        context.operation_id(),
                        ProjectSourceSnapshot {
                            project: chosen.project.clone(),
                            generation,
                            native_snapshot: chosen.native_snapshot.clone(),
                        },
                        allocation.limits.work,
                        allocation.limits.bytes,
                    )
                    .await?,
            )
        } else {
            None
        };
        let limits = if io.is_some() {
            context.effective_scope().limits()
        } else {
            context.scope().limits()
        };
        crate::executor::with_history_io(io, |work| {
            limits.validate(work)?;
            arguments.source_task(caller.task, limits, work).map(|_| ())
        })?;
        if parent
            .reducer()
            .conversation()
            .and_then(|conversation| conversation.agent)
            != caller.owner.scope.agent()
            || !context.scope().grants().contains("fork:publish")
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "fork source is not the admitted caller agent",
            ));
        }
        // Authentication already checked Private and the current caller's Project.
        if io.is_none() && selected != caller.task {
            let read = caller
                .project
                .capability(VolumeOperation::Read, &mut None)?;
            if !context.scope().grants().contains(&read) {
                return Err(failure(
                    None,
                    ErrorCode::Unauthorized,
                    "caller lacks its source read",
                ));
            }
            ContentGrant::verify(
                &verifier(&caller.owner)?,
                &caller.owner.scope,
                &caller.project,
                VolumeOperation::Read,
                u64::MAX,
                &mut None,
            )?;
        }
        let config = crate::executor::with_history_io(io, |work| {
            Ok::<_, Error>(RecursiveForkConfig {
                child: match work.as_deref_mut() {
                    Some(work) => work.clone_authority(allocation.child.authority())?,
                    None => allocation.child.authority().clone(),
                },
                child_project: allocation.project.copy(work)?,
                child_private: allocation.private_volume.copy(work)?,
                child_result_volume: allocation
                    .result_volume
                    .as_ref()
                    .map(|volume| volume.copy(work))
                    .transpose()?,
                limits,
                inherited_messages: self.config.inherited_messages,
                inherited_bytes: limits.file_bytes,
                inherited_references: self.config.inherited_references,
                history_limits: self.config.history_limits,
            })
        })?;
        if let Some(source_read) = &source_read {
            let authorization = source_read.authorization();
            if authorization.caller_task != caller.task {
                return Err(failure(
                    io,
                    ErrorCode::Unauthorized,
                    "selected source approval belongs to another caller",
                ));
            }
            source_read.require_selection(
                context.operation_id(),
                selected,
                &authorization.project,
                &authorization.generation,
                authorization.native_snapshot.as_ref(),
                None,
            )?;
        }
        check_cancellation(cancellation, io)?;
        let authorization = source_read
            .as_ref()
            .map(ProjectSourceReadHandle::authorization);
        let authorized_source = authorization
            .map(|authorization| {
                crate::executor::with_history_io(io, |work| {
                    authorization
                        .clone_with_work_and_custody(work)
                        .map(|(authorization, _)| authorization)
                })
            })
            .transpose()?;
        let request = crate::filesystem::recursive::fork_request(
            parent.reducer(),
            context.operation_id(),
            arguments,
            (
                caller.task,
                authorization.map_or(&caller.project, |authorization| &authorization.project),
                authorization.map_or(&project, |authorization| &authorization.generation),
                &caller.private_volume,
                &private,
            ),
            config,
            &self.config.stream_provider,
            authorized_source,
            io,
        )?;
        let source = self
            .source(
                context,
                caller,
                parent.into_reducer(),
                &request,
                cancellation,
                io,
            )
            .await?;
        let (mut child, scope) = self
            .child_claims(context, allocation, arguments, &source, io)
            .await?;
        Ok(RecursiveForkSourceIntent {
            arguments: ForkToolArguments {
                id: arguments.id,
                msg: crate::executor::stock_string_copy(&arguments.msg, io)?,
                source: arguments.source,
            },
            caller: caller.task,
            source_task: selected,
            child_operation_id: child.operation_id,
            allocation_digest: allocation_digest(allocation, &mut child, &scope, io)?,
            request: source.request,
        })
    }
}
