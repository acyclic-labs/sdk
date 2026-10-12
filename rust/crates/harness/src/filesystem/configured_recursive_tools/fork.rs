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
        let checkpoint_io = match io {
            Some(io) => io,
            None => {
                ordinary = PreparationIo::from_budget(self.config.content_budget, cancellation)?;
                &ordinary
            }
        };
        let pinned = self
            .open(
                &caller.owner,
                Some(intent.request.parent_revision),
                Some(checkpoint_io),
            )
            .await?;
        let operation = checkpoint_operation(context.operation_id());
        let checkpoint = match original {
            Some(original) => Some(crate::executor::with_history_io(io, |work| {
                original.source_checkpoint.copy(work)
            })?),
            None => 'observed: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = checkpoint_io.original_native_work() {
                    let (file, custody) = context
                        .observe_staged_file_once_with_native_work(operation, work)
                        .await?;
                    checkpoint_io.absorb_native_custody(custody)?;
                    break 'observed file;
                }
                checkpoint_io.receive(
                    context
                        .observe_staged_file_once_measured(
                            operation,
                            checkpoint_io.remaining()?,
                            checkpoint_io.cancellation(),
                        )
                        .await,
                )?
            }
        };
        let snapshot = if checkpoint.is_none() {
            Some(crate::executor::with_history_io(io, |work| {
                pinned
                    .reducer()
                    .snapshot(1, work)
                    .map(|(snapshot, _)| snapshot)
            })?)
        } else {
            None
        };
        let source = self
            .source(
                context,
                caller,
                pinned.into_reducer(),
                &intent.request,
                cancellation,
                io,
            )
            .await?;
        let (mut child, child_scope) = self
            .child_claims(context, caller, allocation, arguments, &source, io)
            .await?;
        if allocation_digest(allocation, &mut child, &child_scope, io)? != intent.allocation_digest
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "child claims changed after immutable source selection",
            ));
        }
        let source_checkpoint = match checkpoint {
            Some(file) => file,
            None => {
                let request = crate::executor::with_history_io(io, |work| {
                    intent.request.copy(work).map(|(request, _)| request)
                })?;
                let arguments = ForkToolArguments {
                    id: arguments.id,
                    msg: fork_preparer_io::clone_text(&arguments.msg, io)?,
                    source: arguments.source,
                };
                let capture = SourceCheckpoint {
                    snapshot: snapshot.expect("new checkpoint needs its pinned snapshot"),
                    request,
                    caller: caller.task,
                    arguments,
                };
                let bytes_hold;
                let bytes = if io.is_some() {
                    checkpoint_io.encode(
                        &capture,
                        context.effective_scope().limits().file_bytes,
                        false,
                    )?
                } else {
                    serde_json::to_vec(&capture).map_err(invalid_json)?.into()
                };
                bytes_hold = if io.is_some() {
                    Some(checkpoint_io.guard_bytes(&bytes)?)
                } else {
                    None
                };
                let path_hold;
                let path = crate::contract::preparation_format(
                    format_args!(
                        "recursive-tools/{}/source-checkpoint.json",
                        context.operation_id()
                    ),
                    io,
                )?;
                path_hold = io
                    .map(|io| io.guard_owned(path.capacity() as u64))
                    .transpose()?;
                let file = 'staged: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = checkpoint_io.original_native_work() {
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
                        checkpoint_io.absorb_native_custody(custody)?;
                        break 'staged file;
                    }
                    checkpoint_io.receive(
                        context
                            .stage_file_once_measured(
                                operation,
                                &path,
                                &bytes,
                                "application/json",
                                "source-checkpoint.json",
                                checkpoint_io.remaining()?,
                                checkpoint_io.cancellation(),
                            )
                            .await,
                    )?
                };
                drop(path);
                drop(path_hold);
                drop(bytes);
                drop(bytes_hold);
                file
            }
        };
        let bytes = self
            .read(context, &source_checkpoint, checkpoint_io)
            .await?;
        let capture: SourceCheckpoint = match io {
            #[cfg(not(target_arch = "wasm32"))]
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData,
                &bytes,
                ErrorCode::Storage,
            )?),
            _ => serde_json::from_slice(&bytes).map_err(invalid_json)?,
        };
        if capture.caller != caller.task
            || capture.arguments != *arguments
            || capture.request != intent.request
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "published checkpoint differs from immutable source intent",
            ));
        }
        let snapshot = capture.snapshot;
        #[cfg(not(target_arch = "wasm32"))]
        let snapshot = if let Some(io) = io {
            let bytes = snapshot.predecoded_native_owned_bytes()?;
            snapshot.adopt_predecoded_native_custody(io.take_native_custody(bytes)?)?
        } else {
            snapshot
        };
        drop(
            self.restore_snapshot(&caller.owner, &capture.request, snapshot, cancellation, io)
                .await?,
        );
        for file in &allocation.input_files {
            drop(self.read(context, file, checkpoint_io).await?);
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
