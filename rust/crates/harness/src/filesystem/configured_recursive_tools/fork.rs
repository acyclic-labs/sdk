use super::*;
use crate::filesystem::{
    PreparationIo,
    fork_preparer_io::{self, failure},
};
use crate::wire::ErrorCode;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn fork_binding(
        &self,
        context: &ToolContext,
        arguments: &ForkToolArguments,
        intent: &RecursiveForkSourceIntent,
        original: Option<&RecursiveForkToolPlan>,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<RecursiveForkToolBinding<P, A, O>> {
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
        if intent.arguments != *arguments
            || intent.caller != caller.task
            || intent.source_task != arguments.source.unwrap_or(caller.task)
            || intent.child_operation_id != allocation.admission.operation_id
            || intent.request.operation_id != context.operation_id()
            || &intent.request.parent != caller.owner.authority()
            || &intent.request.child != allocation.child.authority()
            || intent.request.child_agent != arguments.id
            || intent.request.preparation.child_project_volume != allocation.project
            || intent.request.preparation.child_private_volume != allocation.private_volume
            || intent.request.preparation.child_result_volume != allocation.result_volume
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "fork changed its committed source or allocation",
            ));
        }
        if let Some(original) = original {
            let request: ForkRequest = match io {
                Some(io) => crate::filesystem::retain_decoded(
                    io.value_decode_preparation(&original.request.payload, ErrorCode::Storage)?,
                ),
                None => serde_json::from_value(original.request.payload.clone())
                    .map_err(invalid_json)?,
            };
            if original.caller != caller.task
                || original.arguments != *arguments
                || request != intent.request
                || original.request.child.operation_id != intent.child_operation_id
                || original.request.input_files != allocation.input_files
                || original.child_message_pointer != allocation.child_message_pointer
                || original.child_message_file_pointer != allocation.child_message_file_pointer
            {
                return Err(failure(
                    io,
                    ErrorCode::Unauthorized,
                    "fork retry changed its retained original plan",
                ));
            }
        }
        let ordinary;
        let observer = match io {
            Some(io) => io,
            None => {
                ordinary = PreparationIo::from_budget(self.config.content_budget, cancellation)?;
                &ordinary
            }
        };
        let parent = self
            .open(
                &caller.owner,
                Some(intent.request.parent_revision),
                Some(observer),
            )
            .await?;
        let source = self
            .source(
                context,
                caller,
                parent.into_reducer(),
                &intent.request,
                cancellation,
                io,
            )
            .await?;
        let (mut child, child_scope) = self
            .child_claims(context, allocation, arguments, &source, io)
            .await?;
        if allocation_digest(allocation, &mut child, &child_scope, io)? != intent.allocation_digest
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "child claims changed after immutable source selection",
            ));
        }
        let operation = checkpoint_operation(context.operation_id());
        let source_checkpoint = match original {
            Some(original) => crate::executor::with_history_io(io, |work| {
                original
                    .request
                    .source_checkpoint
                    .as_ref()
                    .ok_or_else(|| {
                        failure(
                            io,
                            ErrorCode::Unauthorized,
                            "original fork has no source checkpoint",
                        )
                    })?
                    .copy(work)
            })?,
            None => {
                let bytes = observer.encode(
                    intent,
                    context.effective_scope().limits().file_bytes,
                    false,
                )?;
                let bytes_hold = observer.guard_bytes(&bytes)?;
                let path = crate::executor::with_history_io(io, |work| {
                    crate::contract::preparation_format(
                        format_args!(
                            "recursive-tools/{}/source-checkpoint.json",
                            context.operation_id()
                        ),
                        work,
                    )
                })?;
                let path_hold = io
                    .map(|io| io.guard_owned(path.capacity() as u64))
                    .transpose()?;
                let file = 'staged: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = observer.original_native_work() {
                        let (file, custody) = context
                            .stage_file_once_with_native_work(
                                operation,
                                &path,
                                &bytes,
                                "application/json",
                                "source-checkpoint.json",
                                work,
                            )
                            .await?;
                        observer.absorb_native_custody(custody)?;
                        break 'staged file;
                    }
                    observer.receive_content(
                        context
                            .stage_file_once_measured(
                                operation,
                                &path,
                                &bytes,
                                "application/json",
                                "source-checkpoint.json",
                                observer.remaining()?,
                                observer.cancellation(),
                            )
                            .await,
                    )?
                };
                drop((path, path_hold));
                drop(bytes);
                drop(bytes_hold);
                file
            }
        };
        let bytes = self.read(context, &source_checkpoint, observer).await?;
        let record: RecursiveForkSourceIntent = match io {
            #[cfg(not(target_arch = "wasm32"))]
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData,
                &bytes,
                ErrorCode::Storage,
            )?),
            _ => serde_json::from_slice(&bytes).map_err(invalid_json)?,
        };
        if record != *intent {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "published checkpoint differs from immutable source intent",
            ));
        }
        for file in &allocation.input_files {
            drop(self.read(context, file, observer).await?);
        }
        let parent = self.open(&caller.owner, None, io).await?;
        let child_conversation = self.open(&allocation.child, None, io).await?;
        let parent_scope =
            crate::executor::with_history_io(io, |work| caller.owner.scope.copy(work))?;
        let owner = self.source_owner(context.task(), cancellation, io).await?;
        let parent = self.bind_aggregate(
            &caller.owner,
            &parent_scope,
            parent,
            Some(self.resolver(context, owner, io)?),
            io,
        )?;
        let child_conversation = self.bind_aggregate(
            &allocation.child,
            &child_scope,
            child_conversation,
            None,
            io,
        )?;
        let mut input_files = fork_preparer_io::vector(allocation.input_files.len(), io)?;
        for file in &allocation.input_files {
            input_files.push(crate::executor::with_history_io(io, |work| {
                file.copy(work)
            })?);
        }
        Ok(RecursiveForkToolBinding {
            source,
            source_checkpoint,
            child,
            parent,
            child_conversation,
            parent_scope,
            child_scope,
            child_message_pointer: fork_preparer_io::clone_text(
                &allocation.child_message_pointer,
                io,
            )?,
            child_message_file_pointer: allocation
                .child_message_file_pointer
                .as_deref()
                .map(|value| fork_preparer_io::clone_text(value, io))
                .transpose()?,
            input_files,
            limits: allocation.limits,
        })
    }
}
