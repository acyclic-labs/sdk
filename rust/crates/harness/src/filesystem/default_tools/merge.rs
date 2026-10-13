//! Applies and publishes the original inspected project import.
use super::*;
use crate::filesystem::PreparationIo;

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum MergeResult<'a> {
    Applied {
        receipt: &'a ProjectMergeReceipt,
    },
    NoChanges {
        generation: &'a GenerationRef,
    },
    StaleTarget {
        generation: &'a GenerationRef,
    },
    Conflicted {
        #[serde(serialize_with = "recursive_conflict_keys")]
        conflicts: &'a [acyclic_fs::MergeConflict],
        truncated: bool,
    },
    Fenced,
    IdempotencyConflict,
}

impl<P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    RecursiveExecutor<P, A, O>
{
    pub(super) async fn apply_merge(
        &self,
        context: &ToolContext,
        owner: &(
            Arc<FilesystemTaskRuntime<P, A, O>>,
            Arc<FilesystemWorkflowJournal<P, A, O>>,
        ),
        merge: &RecursiveMergeToolPlan,
        cancellation: &CancellationToken,
        output_io: Option<&PreparationIo>,
    ) -> Result<Value> {
        let (runtime, workflow) = owner;
        let owner = workflow.owner.as_ref().expect("validated tool owner");
        let temporary = recursive_temporary_io(output_io)?;
        let io = temporary.as_ref();
        let binding = self
            .source
            .as_ref()
            .expect("authorized source")
            .merge(context, &merge.arguments, Some(merge), cancellation, io)?
            .await?;
        validate_merge_binding(context, merge.arguments.source, &binding, io)?;
        let controller = ParentProjectController::new(
            binding.filesystem.as_ref(),
            binding.destination.reducer(),
            &binding.verifier,
            &binding.scope,
            &binding.project,
            io,
        )?;
        let inspected = match &binding.source_read {
            Some(source) => {
                controller
                    .prepare_project_import_from_selected_source(
                        &merge.source_project,
                        &merge.source_generation,
                        Some(&merge.expected_target_generation),
                        source,
                        owner,
                        context.task(),
                        context.operation_id(),
                    )
                    .await?
            }
            None => {
                controller
                    .prepare_project_import(
                        &merge.source_project,
                        &merge.source_generation,
                        Some(&merge.expected_target_generation),
                    )
                    .await?
            }
        };
        cancellation
            .check()
            .map_err(|error| Error::Storage(error.to_string()))?;
        runtime
            .owner_observed(merge.caller, owner.task_binding().1, false, io)
            .await?;
        let mut sides = BTreeMap::new();
        for selection in &merge.arguments.selections {
            let conflict =
                crate::filesystem::project_workspaces::decode_conflict(&selection.conflict, io)?;
            reject_if(
                sides.contains_key(&conflict),
                io,
                crate::wire::ErrorCode::Invalid,
                "merge conflict selected twice",
            )?;
            if let Some(io) = io {
                let bytes = std::mem::size_of::<(acyclic_fs::MergeConflict, ConflictSide)>() as u64;
                io.retain_owned(bytes)?;
                io.allocation_attempt(bytes)?;
            }
            sides.insert(
                conflict,
                match selection.side {
                    MergeToolSide::Base => ConflictSide::Base,
                    MergeToolSide::Target => ConflictSide::Ours,
                    MergeToolSide::Source => ConflictSide::Theirs,
                },
            );
        }
        let (outcome, custody) = controller
            .apply_project_merge(&inspected, context.operation_id(), sides)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = custody {
            io.expect("native controller")
                .absorb_native_custody(custody)?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = custody;
        runtime
            .owner_observed(merge.caller, owner.task_binding().1, false, io)
            .await?;
        cancellation
            .check()
            .map_err(|error| Error::Storage(error.to_string()))?;
        match &outcome {
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) => {
                let (receipt, custody) = controller.merge_receipt(
                    &inspected,
                    &outcome,
                    &merge.source,
                    context.operation_id(),
                    &merge.notice,
                )?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(custody) = custody {
                    io.expect("native controller")
                        .absorb_native_custody(custody)?;
                }
                #[cfg(target_arch = "wasm32")]
                let _ = custody;
                encode(&MergeResult::Applied { receipt: &receipt }, output_io)
            }
            JoinOutcome::NoChanges(generation) | JoinOutcome::StaleTarget(generation) => {
                let generation = crate::filesystem::fork_preparer_io::generation_ref(
                    binding.project.provider(),
                    generation.id(),
                    io,
                )?;
                let result = if matches!(&outcome, JoinOutcome::NoChanges(_)) {
                    MergeResult::NoChanges {
                        generation: &generation,
                    }
                } else {
                    MergeResult::StaleTarget {
                        generation: &generation,
                    }
                };
                encode(&result, output_io)
            }
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => encode(
                &MergeResult::Conflicted {
                    conflicts,
                    truncated: *truncated,
                },
                output_io,
            ),
            JoinOutcome::Fenced => encode(&MergeResult::Fenced, output_io),
            JoinOutcome::IdempotencyConflict => {
                encode(&MergeResult::IdempotencyConflict, output_io)
            }
        }
    }

    pub(super) async fn publish_merge(
        &self,
        context: &ToolContext,
        owner: &(
            Arc<FilesystemTaskRuntime<P, A, O>>,
            Arc<FilesystemWorkflowJournal<P, A, O>>,
        ),
        merge: &RecursiveMergeToolPlan,
        receipt: Option<&ProjectMergeReceipt>,
        cancellation: &CancellationToken,
        output_io: Option<&PreparationIo>,
    ) -> Result<Value> {
        let temporary = recursive_temporary_io(output_io)?;
        let io = temporary.as_ref();
        let mut binding = self
            .source
            .as_ref()
            .expect("authorized source")
            .merge(context, &merge.arguments, Some(merge), cancellation, io)?
            .await?;
        validate_merge_binding(context, merge.arguments.source, &binding, io)?;
        let receipt = receipt.ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "merge publication has no receipt",
            )
        })?;
        validate_merge_receipt(merge, receipt, io)?;
        reject_if(
            receipt.operation_id != context.operation_id(),
            io,
            crate::wire::ErrorCode::Unauthorized,
            "receipt names another effect",
        )?;
        let copy =
            crate::executor::with_history_io(io, |work| receipt.clone_with_work_and_custody(work))?
                .0;
        let copy = recursive_box(copy, io)?;
        let key = crate::executor::with_history_io(io, |work| {
            crate::contract::preparation_format(
                format_args!("recursive-merge-publication:{}", context.operation_id()),
                work,
            )
        })?;
        let command = Command {
            operation_id: context.operation_id(),
            idempotency_key: IdempotencyKey(key),
            expected_revision: binding.destination.reducer().revision(),
            scope: binding.scope,
            causal_parent: None,
            action: Action::PublishProjectMerge { receipt: copy },
        };
        let owner = owner.1.owner.as_ref().expect("validated tool owner");
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        binding
            .destination
            .publish(
                command,
                Some(owner.as_ref()),
                crate::distributed::JournalWrite::Fresh,
                false,
                &mut work,
            )
            .await?;
        encode(receipt, output_io)
    }
}
