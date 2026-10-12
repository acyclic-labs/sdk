use super::*;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn child_claims(
        &self, context: &ToolContext, caller: &ConfiguredRecursiveCaller,
        allocation: &ConfiguredRecursiveFork, arguments: &ForkToolArguments,
        source: &RetainedSourceBinding<A, O>, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<(TaskAdmissionRecord, Scope)> {
                let mut child = 'child: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                let work = io.original_native_work().ok_or_else(|| io.invalid("child claims require original work"))?;
                let (child, custody) = allocation.admission.clone_with_native_work(work)?;
                io.absorb_native_custody(custody)?;
                break 'child child;
            }
            break 'child allocation.admission.clone();
        };
        if child.parent != Some(caller.task) || allocation.child.scope.agent() != Some(arguments.id) {
            return Err(stock_boundary(io, crate::wire::ErrorCode::Unauthorized, "child allocation differs from original caller or agent"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let reference_io = io.map(|io| crate::filesystem::PreparationIo::from_native_work(
            io.original_native_work().expect("child claims verified original work"))).transpose()?;
        let references = 'references: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(reference_io) = &reference_io {
                let work = reference_io.original_native_work().expect("child claims verified original work");
                break 'references crate::NativeFutureBundle::new(work, ||
                    source.preparer.required_reference_grants(&source.request, Some(reference_io)))?.await?;
            }
            break 'references source.preparer.required_reference_grants(&source.request, None).await?;
        };
        let extra = (2 + usize::from(allocation.result_volume.is_some())).checked_mul(2)
            .and_then(|count| count.checked_add(allocation.input_files.len()))
            .and_then(|count| count.checked_add(references.len()))
            .ok_or_else(|| stock_boundary(io, crate::wire::ErrorCode::Invalid, "child claim slots overflow"))?;
        let capacity = |count: usize| count.checked_add(extra)
            .ok_or_else(|| stock_boundary(io, crate::wire::ErrorCode::Invalid, "child claim slots overflow"));
        let mut grants = crate::model::vec_with_capacity(capacity(child.grants.iter().count())?, io)?;
        let mut scoped = crate::model::vec_with_capacity(capacity(allocation.child.scope.capabilities().iter().count())?, io)?;
        let append = |values: &mut Vec<String>, value: &str| -> Result<()> {
            values.push(stock_string_copy(value, io)?);
            Ok(())
        };
        for grant in child.grants.iter() { append(&mut grants, grant)?; }
        for grant in allocation.child.scope.capabilities().iter() { append(&mut scoped, grant)?; }
        for volume in [&allocation.project, &allocation.private_volume].into_iter().chain(allocation.result_volume.as_ref()) {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                let hold;
                let grant = crate::executor::with_history_io(io, |work| volume.capability(operation, work))?;
                hold = io.map(|io| io.guard_owned(grant.capacity() as u64)).transpose()?;
                let grant = (grant, hold);
                append(&mut grants, &grant.0)?; append(&mut scoped, &grant.0)?;
            }
        }
        for file in &allocation.input_files {
            self.delegate_file(context, file, io).await?;
            let hold;
            let grant = match io { Some(io) => io.capability(file, "file:read:"), None => file.read_capability() }?;
            hold = io.map(|io| io.guard_owned(grant.capacity() as u64)).transpose()?;
            let grant = (grant, hold);
            append(&mut grants, &grant.0)?; append(&mut scoped, &grant.0)?;
        }
        for reference in &references {
            if reference.reader != arguments.id {
                return Err(stock_boundary(io, crate::wire::ErrorCode::Unauthorized, "inherited reference targets another child reader"));
            }
            for file in std::iter::once(&reference.file).chain(reference.attachment_manifest.as_ref()) {
                self.delegate_file(context, file, io).await?;
            }
            let hold;
            let grant = match io { Some(io) => io.capability(&reference.file, "file:read:"), None => &reference.file.read_capability() }?;
            hold = io.map(|io| io.guard_owned(grant.capacity() as u64)).transpose()?;
            let grant = (grant, hold);
            append(&mut grants, &grant.0)?; append(&mut scoped, &grant.0)?;
        }
        let capabilities = |values: Vec<String>| -> Result<Capabilities> {
            let slots = values.capacity() as u64 * std::mem::size_of::<String>() as u64;
            if let Some(io) = io {
                let bytes = values.len() as u64 * std::mem::size_of::<String>() as u64;
                io.retain_owned(bytes)?;
                if bytes != 0 { io.allocation_attempt(bytes)?; }
            }
            let capabilities = Capabilities::new(values);
            if let Some(io) = io { io.release(slots)?; }
            Ok(capabilities)
        };
        let old_grants_bytes = if io.is_some() { child.grants.logical_owned_bytes()? } else { 0 };
        let old = std::mem::replace(&mut child.grants, capabilities(grants)?);
        drop(old);
        if let Some(io) = io { io.release(old_grants_bytes)?; }
        let scoped = capabilities(scoped)?;
        let scope = crate::executor::with_history_io(io, |work|
            allocation.child.issuer().issue(allocation.child.scope.id(), &scoped, Some(arguments.id), None, work))?;
        let temporary = if io.is_some() { scoped.logical_owned_bytes()? } else { 0 };
        drop(scoped);
        if let Some(io) = io { io.release(temporary)?; }
        crate::executor::with_history_io(io, |work| child.validate(work))?;
        Ok((child, scope))
    }
    async fn delegate_file(&self, context: &ToolContext, file: &crate::conversation::FileRef,
        io: Option<&crate::filesystem::PreparationIo>) -> Result<()> {
        if !crate::executor::with_history_io(io, |work|
            crate::runtime::read_granted_with_work(context.scope().grants(), file, work))? {
            return Err(stock_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "current tool scope cannot delegate this file"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            drop(context.task().read_file_with_native_work(file, work).await?);
            return Ok(());
        }
        drop(context.task().read_file(file).await?);
        Ok(())
    }
    pub(super) async fn select_source(&self, context: &ToolContext, arguments: &ForkToolArguments,
        cancellation: &CancellationToken) -> Result<RecursiveForkSourceIntent> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context,
            arguments.source.is_none_or(|source| Some(source) == context.durable_task_id())).await?;
        let allocation = self.allocation(caller.task, arguments, None)?;
        let runtime = self.runtime(None)?;
        let parent = self.aggregate(&caller.owner, &caller.owner.scope, None, cancellation).await?;
        let project = self.filesystem.resolve(&workspace_ref(caller.project.provider().clone(), &caller.project.storage_name(&mut None)?)?).await?.generation;
        let private = self.filesystem.resolve(&workspace_ref(caller.private_volume.provider().clone(), &caller.private_volume.storage_name(&mut None)?)?).await?.generation;
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let selected = arguments.source.unwrap_or(caller.task);
        let source_read = if selected != caller.task {
            let BoundProjectSourcePolicy { issuer, provider, .. } = self.project_policy.get().ok_or_else(|| Error::Unauthorized(
                "foreign project selection lacks an explicitly configured RootService approval policy".into()))?;
            let chosen = self.caller(selected, None)?;
            if let Some(native) = &chosen.native_snapshot {
                crate::contract::json_byte_length(native, context.scope().limits().file_bytes, &mut None).map(|_| ())?;
            }
            let generation = self.filesystem.resolve(&workspace_ref(chosen.project.provider().clone(), &chosen.project.storage_name(&mut None)?)?).await?.generation;
            cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
            let original_owner = Arc::new(runtime.task_host().journal_owner(caller.task, context.task().owned_task_fence()?).await?
                .with_invocation_cancellation(cancellation));
            Some(original_owner.authorize_project_source(context.task(), issuer, provider.as_ref(), selected,
                context.operation_id(), ProjectSourceSnapshot { project: chosen.project.clone(), generation,
                    native_snapshot: chosen.native_snapshot.clone() }, allocation.limits.work, allocation.limits.bytes).await?)
        } else { None };
        let source = recursive_fork_source(self.filesystem.clone(), &self.stream, &parent,
            verifier(&caller.owner)?, caller.owner.scope.clone(), context.task(), RecursiveSourceSnapshot {
                task: caller.task, project: caller.project.clone(), project_generation: project,
                private_volume: caller.private_volume.clone(), private_generation: private,
            }, self.config.stream_provider.clone(), context.operation_id(), arguments, RecursiveForkConfig {
                child: allocation.child.authority().clone(), child_project: allocation.project.clone(), child_private: allocation.private_volume.clone(),
                child_result_volume: allocation.result_volume.clone(),
                limits: context.scope().limits(), inherited_messages: self.config.inherited_messages,
                inherited_bytes: context.scope().limits().file_bytes, inherited_references: self.config.inherited_references,
                history_limits: self.config.history_limits,
            }, source_read, self.ordinary_resolver(context.task(), cancellation).await?, cancellation).await?;
        let (child, scope) = self.child_claims(context, caller, allocation, arguments, &source, None).await?;
        Ok(RecursiveForkSourceIntent {
            arguments: arguments.clone(), caller: caller.task, source_task: arguments.source.unwrap_or(caller.task),
            child_operation_id: child.operation_id, allocation_digest: allocation_digest(allocation, child.clone(), &scope, None)?,
            request: source.request,
        })
    }
}
use super::*;

#[cfg(not(target_arch = "wasm32"))]
impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {

    pub(super) async fn native_select_source(
        &self, context: &ToolContext, arguments: &ForkToolArguments,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(RecursiveForkSourceIntent, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let owner = self.native_source_owner(context.task(), cancellation, work).await?;
        let caller = self.authenticate_native_caller(context.task(), owner.as_ref(),
            arguments.source.is_none_or(|task| Some(task) == context.durable_task_id()), work).await?;
        self.require_native_effective_volume(context, &caller.private_volume, VolumeOperation::Read, work)?;
        if arguments.source.is_none_or(|source| source == caller.task) {
            self.require_native_effective_volume(context, &caller.project, VolumeOperation::Read, work)?;
        }
        let allocation = self.allocation(caller.task, arguments, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        let parent = self.native_open_aggregate(&caller.owner, None, cancellation, work).await?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let current_project = crate::filesystem::fork_preparer::bounded_io::content_workspace(
            caller.project.provider(), &caller.project, Some(&io),
        )?;
        let current_private = crate::filesystem::fork_preparer::bounded_io::content_workspace(
            caller.private_volume.provider(), &caller.private_volume, Some(&io),
        )?;
        let project = crate::filesystem::fork_preparer::bounded_io::resolve(&self.filesystem, &current_project, Some(&io)).await?;
        let private = crate::filesystem::fork_preparer::bounded_io::resolve(&self.filesystem, &current_private, Some(&io)).await?;
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let selected_task = arguments.source.unwrap_or(caller.task);
        let selected = if selected_task != caller.task {
            let BoundProjectSourcePolicy { issuer, provider: policy, custody } = self.project_policy.get().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "foreign native Project selection has no configured original Root policy",
            })?;
            self.require_native_policy(custody.as_ref(), work)?;
            let chosen = self.caller(selected_task, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
            let chosen_workspace = crate::filesystem::fork_preparer::bounded_io::content_workspace(
                chosen.project.provider(), &chosen.project, Some(&io),
            )?;
            let generation = crate::filesystem::fork_preparer::bounded_io::resolve(
                &self.filesystem, &chosen_workspace, Some(&io),
            ).await?;
            let snapshot = ProjectSourceSnapshot {
                project: {
                    let mut history = io.history_io();
                    chosen.project.copy(&mut Some(&mut history))?
                },
                generation: generation.generation.clone_with_io(&io)?,
                native_snapshot: match &chosen.native_snapshot {
                    Some(value) => Some(crate::model::clone_value(value, Some(&io))?),
                    None => None,
                },
            };
            Some(owner.authorize_project_source_with_native_work(context.task(), issuer, policy.as_ref(),
                selected_task, context.operation_id(), &snapshot, allocation.limits.work, allocation.limits.bytes, work,
            )?.await?)
        } else { None };
        let request = {
            let parent = parent.0.reducer();
            if parent.authority() != caller.owner.authority()
                || parent.conversation().and_then(|conversation| conversation.agent) != caller.owner.scope.agent()
                || caller.owner.scope.agent() != match caller.private_volume.owner() {
                    VolumeOwner::Agent(agent) => Some(*agent), _ => None,
                } {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native fork source is not the admitted current caller agent" });
            }
            let output = crate::filesystem::PreparationIo::from_native_work(work)?;
            let source_read = selected.as_ref().map(|selected| &selected.0);
            let selected_task = arguments.source_task(caller.task, context.effective_scope().limits(),
                &mut Some(&mut output.history_io()))?;
            if selected_task == caller.task && source_read.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "current fork cannot substitute a foreign reader" });
            }
            let authorized_source = match source_read {
                Some(handle) => {
                    let authorization = handle.authorization();
                    if authorization.caller_task != caller.task {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                            reason: "native source approval belongs to another caller" });
                    }
                    handle.require_selection(context.operation_id(), selected_task, &authorization.project,
                        &authorization.generation, authorization.native_snapshot.as_ref(), Some(work))?;
                    Some(authorization.clone_with_work_and_custody(&mut Some(&mut output.history_io()))?.0)
                }
                None if selected_task != caller.task => return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized, reason: "foreign fork has no operation-sealed Project reader" }),
                None => None,
            };
            let config = crate::executor::with_history_io(Some(&output), |observer| Ok::<_, Error>(RecursiveForkConfig {
                child: observer.as_deref_mut().expect("native fork observer").clone_authority(allocation.child.authority())?,
                child_project: allocation.project.copy(observer)?, child_private: allocation.private_volume.copy(observer)?,
                child_result_volume: allocation.result_volume.as_ref().map(|volume| volume.copy(observer)).transpose()?,
                limits: context.effective_scope().limits(), inherited_messages: self.config.inherited_messages,
                inherited_bytes: context.effective_scope().limits().file_bytes, inherited_references: self.config.inherited_references,
                history_limits: self.config.history_limits,
            }))?;
            let project_volume = source_read.map_or(&caller.project, |handle| &handle.authorization().project);
            let generation = source_read.map_or(&project.generation, |handle| &handle.authorization().generation);
            let request = crate::filesystem::recursive::fork_request(parent, context.operation_id(), arguments,
                (caller.task, project_volume, generation, &caller.private_volume, &private.generation),
                config, &self.config.stream_provider, authorized_source, Some(&output))?;
            let retained = output.owned_bytes()?;
            (request, output.into_native_custody(retained)?)
        };
        let resolver = self.native_resolver(context, Arc::clone(&owner), work)?;
        let verifier = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            caller.owner.issuer().verifier(&mut observer)?
        };
        let source = crate::filesystem::recursive::restore_recursive_fork_source_with_native_work(
            Arc::clone(&self.filesystem), &self.stream, parent.0.into_reducer(), &verifier, &caller.owner.scope,
            &caller.project, &self.config.stream_provider, request, self.config.history_limits,
            selected.as_ref().map(|selected| &selected.0), resolver, cancellation, work,
        ).await?;
        let _parent_custody = parent.1;
        let claims = {
            let output = crate::filesystem::PreparationIo::from_native_work(work)?;
            let claims = self.child_claims(context, caller, allocation, arguments, &source.0, Some(&output)).await?;
            let retained = output.owned_bytes()?;
            (claims, output.into_native_custody(retained)?)
        };
        let allocation_digest = {
            let (admission, _admission_hold) = claims.0.0.clone_with_native_work(work)?;
            allocation_digest(allocation, admission, &claims.0.1, Some(&io))?
        };
        let retained_arguments = ForkToolArguments { id: arguments.id, msg: stock_string_copy(&arguments.msg, Some(&io))?, source: arguments.source };
        let history = io.history_io();
        history.absorb_native_custody(source.1)?;
        drop(history);
        let retained = io.owned_bytes()?;
        Ok((RecursiveForkSourceIntent {
            arguments: retained_arguments, caller: caller.task, source_task: selected_task,
            child_operation_id: claims.0.0.operation_id, allocation_digest, request: source.0.request,
        }, io.into_native_custody(retained)?))
    }
}

use crate::executor::{stock_boundary, stock_string_copy};
