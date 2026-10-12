use super::*;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn child_claims(
        &self,
        context: &ToolContext,
        caller: &ConfiguredRecursiveCaller,
        allocation: &ConfiguredRecursiveFork,
        arguments: &ForkToolArguments,
        source: &RetainedSourceBinding<A, O>,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<(TaskAdmissionRecord, Scope)> {
        use crate::filesystem::fork_preparer_io::{clone_text, failure, reference_key, vector};
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
            break 'child allocation.admission.clone();
        };
        if child.parent != Some(caller.task) || allocation.child.scope.agent() != Some(arguments.id)
        {
            return Err(failure(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "child allocation differs from original caller or agent",
            ));
        }
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
            if let Some(reference_io) = &reference_io {
                let work = reference_io
                    .original_native_work()
                    .expect("child claims verified original work");
                break 'references crate::NativeFutureBundle::new(work, || {
                    source
                        .preparer
                        .required_reference_grants(&source.request, Some(reference_io))
                })?
                .await?;
            }
            break 'references source
                .preparer
                .required_reference_grants(&source.request, None)
                .await?;
        };
        let extra = (2 + usize::from(allocation.result_volume.is_some()))
            .checked_mul(2)
            .and_then(|count| count.checked_add(allocation.input_files.len()))
            .and_then(|count| count.checked_add(references.len()))
            .ok_or_else(|| {
                failure(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "child claim slots overflow",
                )
            })?;
        let capacity = |count: usize| {
            count.checked_add(extra).ok_or_else(|| {
                failure(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "child claim slots overflow",
                )
            })
        };
        let mut grants = vector(capacity(child.grants.iter().count())?, io)?;
        let mut scoped = vector(
            capacity(allocation.child.scope.capabilities().iter().count())?,
            io,
        )?;
        let append = |values: &mut Vec<String>, value: &str| -> Result<()> {
            values.push(clone_text(value, io)?);
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
                let hold;
                let grant = crate::executor::with_history_io(io, |work| {
                    volume.capability(operation, work)
                })?;
                hold = io
                    .map(|io| io.guard_owned(grant.capacity() as u64))
                    .transpose()?;
                let grant = (grant, hold);
                append(&mut grants, &grant.0)?;
                append(&mut scoped, &grant.0)?;
            }
        }
        for file in &allocation.input_files {
            self.delegate_file(context, file, io).await?;
            let hold;
            let grant = reference_key(file, io)?;
            hold = io
                .map(|io| io.guard_owned(grant.capacity() as u64))
                .transpose()?;
            let grant = (grant, hold);
            append(&mut grants, &grant.0)?;
            append(&mut scoped, &grant.0)?;
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
            let hold;
            let grant = reference_key(&reference.file, io)?;
            hold = io
                .map(|io| io.guard_owned(grant.capacity() as u64))
                .transpose()?;
            let grant = (grant, hold);
            append(&mut grants, &grant.0)?;
            append(&mut scoped, &grant.0)?;
        }
        let capabilities = |values: Vec<String>| -> Result<Capabilities> {
            let slots = values.capacity() as u64 * std::mem::size_of::<String>() as u64;
            if let Some(io) = io {
                let bytes = values.len() as u64 * std::mem::size_of::<String>() as u64;
                io.retain_owned(bytes)?;
                if bytes != 0 {
                    io.allocation_attempt(bytes)?;
                }
            }
            let capabilities = Capabilities::new(values);
            if let Some(io) = io {
                io.release(slots)?;
            }
            Ok(capabilities)
        };
        let old_grants_bytes = if io.is_some() {
            child.grants.logical_owned_bytes()?
        } else {
            0
        };
        let old = std::mem::replace(&mut child.grants, capabilities(grants)?);
        drop(old);
        if let Some(io) = io {
            io.release(old_grants_bytes)?;
        }
        let scoped = capabilities(scoped)?;
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
            return Err(crate::filesystem::fork_preparer_io::failure(
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
                    .read_file_with_native_work(file, work)
                    .await?,
            );
            return Ok(());
        }
        drop(context.task().read_file(file).await?);
        Ok(())
    }
    pub(super) async fn select_source(
        &self,
        context: &ToolContext,
        arguments: &ForkToolArguments,
        cancellation: &CancellationToken,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<RecursiveForkSourceIntent> {
        use crate::filesystem::fork_preparer_io::{self, failure};
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
                crate::contract::validate_json_byte_bound(
                    value,
                    context.scope().limits().file_bytes,
                )?;
            }
            let generation = self.project_generation(&chosen.project, io).await?;
            cancellation
                .check()
                .map_err(|error| Error::Storage(error.to_string()))?;
            let owner = Arc::new(
                self.runtime(io)?
                    .task_host()
                    .journal_owner(caller.task, context.task().owned_task_fence()?)
                    .await?
                    .with_invocation_cancellation(cancellation),
            );
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
        let verifier =
            crate::executor::with_history_io(io, |work| caller.owner.issuer().verifier(work))?;
        if parent.reducer().authority() != caller.owner.authority()
            || parent
                .reducer()
                .conversation()
                .and_then(|conversation| conversation.agent)
                != caller.owner.scope.agent()
            || caller.owner.scope.agent()
                != match caller.private_volume.owner() {
                    VolumeOwner::Agent(agent) => Some(*agent),
                    _ => None,
                }
            || !context.scope().grants().contains("fork:publish")
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "fork source is not the admitted caller agent",
            ));
        }
        if io.is_none() {
            for volume in [&caller.project, &caller.private_volume] {
                let read = volume.capability(VolumeOperation::Read, &mut None)?;
                if !context.scope().grants().contains(&read) {
                    return Err(failure(
                        None,
                        ErrorCode::Unauthorized,
                        "caller lacks its selected source read",
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
        }
        let config = crate::executor::with_history_io(io, |work| {
            Ok(RecursiveForkConfig {
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
            .child_claims(context, caller, allocation, arguments, &source, io)
            .await?;
        Ok(RecursiveForkSourceIntent {
            arguments: ForkToolArguments {
                id: arguments.id,
                msg: fork_preparer_io::clone_text(&arguments.msg, io)?,
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
