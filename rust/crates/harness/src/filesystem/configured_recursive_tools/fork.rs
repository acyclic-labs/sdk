use super::*;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn fork_binding(&self, context: &ToolContext, arguments: &ForkToolArguments,
        intent: &RecursiveForkSourceIntent, original: Option<&RecursiveForkToolPlan>,
        cancellation: &CancellationToken) -> Result<RecursiveForkToolBinding<P, A, O>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        let caller = self.authenticate_caller(context,
            arguments.source.is_none_or(|source| Some(source) == context.durable_task_id())).await?;
        let allocation = self.allocation(caller.task, arguments, None)?;
        if intent.arguments != *arguments || intent.caller != caller.task
            || intent.source_task != arguments.source.unwrap_or(caller.task)
            || intent.child_operation_id != allocation.admission.operation_id
            || intent.request.operation_id != context.operation_id() || &intent.request.parent != caller.owner.authority()
            || &intent.request.child != allocation.child.authority() || intent.request.child_agent != arguments.id
            || intent.request.preparation.child_project_volume != allocation.project
            || intent.request.preparation.child_private_volume != allocation.private_volume
            || intent.request.preparation.child_result_volume != allocation.result_volume {
            return Err(Error::Unauthorized("configured fork changed its committed original source/allocation".into()));
        }
        if let Some(original) = original {
            let request: ForkRequest = serde_json::from_value(original.request.payload.clone()).map_err(invalid_json)?;
            if original.caller != caller.task || original.arguments != *arguments || request != intent.request
                || original.request.child.operation_id != intent.child_operation_id
                || original.request.input_files != allocation.input_files
                || original.child_message_pointer != allocation.child_message_pointer
                || original.child_message_file_pointer != allocation.child_message_file_pointer {
                return Err(Error::Unauthorized("configured fork retry changed its retained original plan".into()));
            }
        }
        let authority = caller.owner.authority().clone();
        let authority_verifier = verifier(&caller.owner)?;
        let mut open_work = crate::store::HistoryWork::new(self.config.content_budget, cancellation);
        let pinned = async {
            open_work.preflight()?;
            StreamAggregate::open(&self.stream, authority, authority_verifier, SchemaRegistry::default(), None, self.config.history_limits, crate::store::DEFAULT_TERMINAL_EFFECTS, Some(intent.request.parent_revision), &mut Some(&mut open_work)).await
        }.await;
        let pinned = open_work.finish(pinned).map_err(|failure| failure.error)?;
        let mut content_work = pinned.work;
        let pinned = self.bind_aggregate(&caller.owner, &caller.owner.scope, pinned.value, None, None)?;
        let source_read = self.restore_intent_source(context.task(), &intent.request, cancellation).await?;
        let pinned_source = restore_recursive_fork_source(self.filesystem.clone(), &self.stream, pinned.reducer(),
            verifier(&caller.owner)?, caller.owner.scope.clone(), caller.project.clone(), self.config.stream_provider.clone(),
            intent.request.clone(), self.config.history_limits, source_read.clone(),
            self.ordinary_resolver(context.task(), cancellation).await?, cancellation).await?;
        let (child, child_scope) = self.child_claims(context, caller, allocation, arguments, &pinned_source, None).await?;
        if allocation_digest(allocation, child.clone(), &child_scope, None)? != intent.allocation_digest {
            return Err(Error::Unauthorized("configured producer claims changed after source selection".into()));
        }
        let checkpoint = if let Some(original) = original {
            Some(original.request.source_checkpoint.as_ref().ok_or_else(||
                Error::Unauthorized("original fork has no source checkpoint".into()))?.clone())
        } else {
            let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
            let observed = context.observe_staged_file_once_measured(checkpoint_operation(context.operation_id()),
                remaining, cancellation).await.map_err(|failure| failure.error)?;
            content_work = content_work.checked_add(observed.work).map_err(work_error)?;
            observed.value
        };
        let source_checkpoint = if let Some(checkpoint) = checkpoint { checkpoint } else {
            let capture = SourceCheckpoint { snapshot: pinned.reducer().snapshot(1, &mut None).map(|(snapshot, _)| snapshot)?,
                request: intent.request.clone(), caller: caller.task, arguments: arguments.clone() };
            let bytes = serde_json::to_vec(&capture).map_err(invalid_json)?;
            let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
            let published = context.stage_file_once_measured(checkpoint_operation(context.operation_id()),
                &format!("recursive-tools/{}/source-checkpoint.json", context.operation_id()), &bytes,
                "application/json", "source-checkpoint.json", remaining, cancellation).await.map_err(|failure| failure.error)?;
            content_work = content_work.checked_add(published.work).map_err(work_error)?;
            published.value
        };
        let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
        let retained = context.read_file_measured(&source_checkpoint, remaining, cancellation).await.map_err(|failure| failure.error)?;
        content_work = content_work.checked_add(retained.work).map_err(work_error)?;
        let capture: SourceCheckpoint = serde_json::from_slice(&retained.value).map_err(invalid_json)?;
        if capture.caller != caller.task || capture.arguments != *arguments || capture.request != intent.request {
            return Err(Error::Unauthorized("published checkpoint differs from the committed original source intent".into()));
        }
        let reducer = self.restore_snapshot(&caller.owner, &capture.request, capture.snapshot, cancellation, None).await?;
        let source = restore_recursive_fork_source(self.filesystem.clone(), &self.stream, &reducer,
            verifier(&caller.owner)?, caller.owner.scope.clone(), caller.project.clone(), self.config.stream_provider.clone(),
            capture.request, self.config.history_limits, source_read,
            self.ordinary_resolver(context.task(), cancellation).await?, cancellation).await?;
        for file in &allocation.input_files {
            let remaining = content_work.remaining(self.config.content_budget).map_err(work_error)?;
            let read = context.read_file_measured(file, remaining, cancellation).await.map_err(|failure| failure.error)?;
            content_work = content_work.checked_add(read.work).map_err(work_error)?;
        }
        Ok(RecursiveForkToolBinding { source, source_checkpoint, child, child_message_pointer: allocation.child_message_pointer.clone(),
            child_message_file_pointer: allocation.child_message_file_pointer.clone(), input_files: allocation.input_files.clone(),
            parent: self.aggregate(&caller.owner, &caller.owner.scope,
                Some(self.ordinary_resolver(context.task(), cancellation).await?), cancellation).await?,
            child_conversation: self.aggregate(&allocation.child, &child_scope, None, cancellation).await?,
            parent_scope: caller.owner.scope.clone(), child_scope, limits: allocation.limits })
    }
}
use super::*;

#[cfg(not(target_arch = "wasm32"))]
impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn native_fork_binding(
        &self, context: &ToolContext, arguments: &ForkToolArguments,
        intent: &RecursiveForkSourceIntent, original: Option<&RecursiveForkToolPlan>,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(RecursiveForkToolBinding<P, A, O>, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let owner = self.native_source_owner(context.task(), cancellation, work).await?;
        let caller = self.authenticate_native_caller(context.task(), owner.as_ref(),
            arguments.source.is_none_or(|task| Some(task) == context.durable_task_id()), work).await?;
        self.require_native_effective_volume(context, &caller.private_volume, VolumeOperation::Read, work)?;
        if arguments.source.is_none_or(|source| source == caller.task) {
            self.require_native_effective_volume(context, &caller.project, VolumeOperation::Read, work)?;
        }
        let allocation = self.allocation(caller.task, arguments, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        if intent.arguments != *arguments || intent.caller != caller.task
            || intent.source_task != arguments.source.unwrap_or(caller.task)
            || intent.child_operation_id != allocation.admission.operation_id
            || intent.request.operation_id != context.operation_id() || &intent.request.parent != caller.owner.authority()
            || &intent.request.child != allocation.child.authority() || intent.request.child_agent != arguments.id
            || intent.request.preparation.child_project_volume != allocation.project
            || intent.request.preparation.child_private_volume != allocation.private_volume
            || intent.request.preparation.child_result_volume != allocation.result_volume {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native fork changed committed source or original child allocation" });
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        if let Some(original) = original {
            let retained = io.value_decode_preparation::<ForkRequest>(&original.request.payload, crate::wire::ErrorCode::Storage)?;
            if original.caller != caller.task || original.arguments != *arguments || retained.0 != intent.request
                || original.request.child.operation_id != intent.child_operation_id
                || original.request.input_files != allocation.input_files
                || original.child_message_pointer != allocation.child_message_pointer
                || original.child_message_file_pointer != allocation.child_message_file_pointer {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native fork retry changed retained original plan" });
            }
        }
        let retained_request = {
            let mut history = io.history_io();
            intent.request.copy(&mut Some(&mut history))?
        };
        let retained_guard = io.take_native_custody(retained_request.1)?;
        let pinned_source = self.native_restore_source(context, caller, Arc::clone(&owner),
            (retained_request.0, retained_guard), None, cancellation, work).await?;
        let claims = {
            let output = crate::filesystem::PreparationIo::from_native_work(work)?;
            let claims = self.child_claims(context, caller, allocation, arguments, &pinned_source.0, Some(&output)).await?;
            let retained = output.owned_bytes()?;
            (claims, output.into_native_custody(retained)?)
        };
        let digest = {
            let (admission, _admission_hold) = claims.0.0.clone_with_native_work(work)?;
            allocation_digest(allocation, admission, &claims.0.1, Some(&io))?
        };
        if digest != intent.allocation_digest {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native child claims changed after immutable source choice" });
        }
        let checkpoint = match original {
            Some(original) => Some(crate::executor::with_history_io(Some(&io), |work| {
                original.request.source_checkpoint.as_ref().ok_or_else(||
                    stock_boundary(Some(&io), crate::wire::ErrorCode::Unauthorized,
                        "original fork has no source checkpoint"))?.copy(work)
            })?),
            None => {
                let observed = context.observe_staged_file_once_with_native_work(
                    checkpoint_operation(context.operation_id()), work,
                ).await?;
                io.absorb_native_custody(observed.1)?;
                observed.0
            }
        };
        let source_checkpoint = match checkpoint {
            Some(checkpoint) => checkpoint,
            None => {
                let pinned = self.native_open_aggregate(&caller.owner, Some(intent.request.parent_revision), cancellation, work).await?;
                let request = {
                    let mut history = io.history_io();
                    intent.request.copy(&mut Some(&mut history))?.0
                };
                let capture = SourceCheckpoint {
                    snapshot: pinned.0.reducer().snapshot(1, &mut Some(&mut io.history_io()))?.0,
                    request, caller: caller.task, arguments: ForkToolArguments { id: arguments.id, msg: stock_string_copy(&arguments.msg, Some(&io))?, source: arguments.source },
                };
                let bytes_guard;
                let bytes = io.encode(&capture, context.effective_scope().limits().file_bytes, false)?;
                bytes_guard = io.guard_bytes(&bytes)?;
                let path_hold;
                let path = crate::contract::preparation_format(
                    format_args!("recursive-tools/{}/source-checkpoint.json", context.operation_id()), Some(&io))?;
                path_hold = io.guard_owned(path.capacity() as u64)?;
                let path = (path, path_hold);
                let staged = context.stage_file_once_with_native_work(checkpoint_operation(context.operation_id()),
                    &path.0, &bytes, "application/json", "source-checkpoint.json", work).await?;
                io.absorb_native_custody(staged.1)?;
                drop(bytes);
                drop(bytes_guard);
                staged.0
            }
        };
        let retained = context.read_file_with_native_work(&source_checkpoint, work).await?;
        let capture = crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<SourceCheckpoint>, retained.as_ref(), crate::wire::ErrorCode::Storage)?);
        if capture.caller != caller.task || capture.arguments != *arguments || capture.request != intent.request {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native published checkpoint differs from immutable source intent" });
        }
        let snapshot_bytes = capture.snapshot.predecoded_native_owned_bytes()?;
        let snapshot_guard = io.take_native_custody(snapshot_bytes)?;
        let restored = self.restore_snapshot(&caller.owner, &capture.request,
            capture.snapshot.adopt_predecoded_native_custody(snapshot_guard)?, cancellation, Some(&io)).await?;
        let copied_request = {
            let mut history = io.history_io();
            capture.request.copy(&mut Some(&mut history))?
        };
        let request_guard = io.take_native_custody(copied_request.1)?;
        let source = self.native_restore_source(context, caller, Arc::clone(&owner),
            (copied_request.0, request_guard), Some(restored), cancellation, work).await?;
        for file in &allocation.input_files { context.read_file_with_native_work(file, work).await?; }
        let parent = self.native_open_aggregate(&caller.owner, None, cancellation, work).await?;
        let child = self.native_open_aggregate(&allocation.child, None, cancellation, work).await?;
        let scope = {
            let mut history = io.history_io();
            caller.owner.scope.copy(&mut Some(&mut history))?
        };
        let parent_aggregate = self.bind_aggregate(&caller.owner, &scope, parent.0,
            Some(self.native_resolver(context, owner, work)?), Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        let child_aggregate = self.bind_aggregate(&allocation.child, &claims.0.1, child.0, None, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        io.absorb_native_custody(parent.1)?;
        io.absorb_native_custody(child.1)?;
        io.absorb_native_custody(claims.1)?;
        io.absorb_native_custody(source.1)?;
        let child_message_pointer = stock_string_copy(&allocation.child_message_pointer, Some(&io))?;
        let child_message_file_pointer = allocation.child_message_file_pointer.as_deref().map(|value| stock_string_copy(value, Some(&io))).transpose()?;
        let mut input_files = Vec::new();
        for file in &allocation.input_files {
            let file = crate::executor::with_history_io(Some(&io), |work| file.copy(work))?;
            crate::executor::stock_vec_push(&mut input_files, file, Some(&io))?;
        }
        let retained = io.owned_bytes()?;
        Ok((RecursiveForkToolBinding {
            source: source.0, source_checkpoint, child: claims.0.0, child_message_pointer,
            child_message_file_pointer, input_files, parent: parent_aggregate, child_conversation: child_aggregate,
            parent_scope: scope, child_scope: claims.0.1, limits: allocation.limits,
        }, io.into_native_custody(retained)?))
    }
}

use crate::executor::{stock_boundary, stock_string_copy};
