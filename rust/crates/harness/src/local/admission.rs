use super::*;
use crate::sync::owned::{Arc as StateArc, Mutex, lock_owned};
use crate::{
    conversation::ContentGrant,
    goal::{GoalBlockedReason, GoalContinuation, GoalStatus, GoalView},
    resources::GenerationRef,
    runtime::TaskStateProvider,
};

use crate::goal::request_head as record;
use record::Turn;
mod goal;
#[cfg(all(test, acyclic_harness_loom))]
mod model;
#[cfg(all(test, feature = "native-execution", not(acyclic_harness_loom)))]
mod poll_sweep;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod snapshot;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

const HEAD: &str = "inputs/current.json";

// The same serialization boundary and predicate are exercised by the Loom model.
pub(super) async fn serialized<T>(
    gate: &StateArc<Mutex<()>>,
    action: impl AsyncFnOnce() -> T,
) -> T {
    let _guard = lock_owned(StateArc::clone(gate)).await;
    action().await
}

fn can_admit(current: Option<OperationId>, expected: Option<OperationId>, terminal: bool) -> bool {
    terminal && expected.is_none_or(|expected| current == Some(expected))
}

impl LocalHarness {
    pub(super) fn read_grant(&self, volume: &VolumeRef) -> Result<ContentGrant> {
        ContentGrant::verify_directory_read(
            &self.verifier,
            &self.signed,
            volume,
            if volume == &self.journal {
                "inputs"
            } else {
                ""
            },
            &mut None,
        )
    }

    async fn current_turn(&self) -> Result<(Option<Turn>, GenerationRef)> {
        let generation = self
            .files
            .resolve(
                &crate::filesystem::workspace_ref(
                    self.journal.provider().clone(),
                    &self.journal.storage_name(&mut None)?,
                )?,
                None,
            )
            .await?
            .generation;
        match self
            .files
            .read_private_path(
                &self.journal,
                &self.read_grant(&self.journal)?,
                HEAD,
                Some(&generation),
                self.limits.file_bytes,
                None,
                None,
            )
            .await
        {
            Ok((_, bytes)) => Ok((Some(record::decode(&bytes).map_err(storage)?), generation)),
            Err(Error::NotFound(_)) => Ok((None, generation)),
            Err(error) => Err(error),
        }
    }

    async fn head_is_terminal(&self, head: Option<&Turn>) -> Result<bool> {
        let Some(head) = head else { return Ok(true) };
        let outcome = match self
            .runtime
            .task_host()
            .outcome(crate::runtime::task_id_for_admission(head.operation))
            .await
        {
            Ok(outcome) => outcome,
            Err(Error::NotFound(_)) => {
                // HEAD is a request, not proof of admission. A cancelled first
                // submission may leave only this file. Incomplete/uncertain
                // admissions still fail closed through reconciliation.
                return Ok(self
                    .runtime
                    .harness()
                    .reconcile_admission(head.operation)
                    .await?
                    .is_none());
            }
            Err(error) => return Err(error),
        };
        Ok(matches!(
            outcome,
            Some(
                crate::Outcome::Succeeded(_)
                    | crate::Outcome::Failed { .. }
                    | crate::Outcome::Cancelled
            )
        ))
    }

    async fn admit_retained(
        &self,
        turn: &Turn,
    ) -> Result<Admission<RuntimeTask<crate::executor::TurnOutput>>> {
        self.prepare_inheritance(crate::runtime::task_id_for_admission(turn.operation), None)
            .await?;
        let definition = self
            .runtime
            .harness()
            .task::<serde_json::Value, crate::executor::TurnOutput>("acyclic.stock_turn@1")?;
        self.runtime
            .harness()
            .admit(
                turn.operation,
                &definition,
                serde_json::json!({"receive": turn.receive, "model": turn.model}),
                None,
            )
            .await
    }

    async fn publish_turn(
        &self,
        turn: Turn,
        previous: Option<OperationId>,
    ) -> Result<Admission<RuntimeTask<crate::executor::TurnOutput>>> {
        self.publish_turn_with(turn, previous, async |turn| self.admit_retained(turn).await)
            .await
    }

    async fn publish_turn_with(
        &self,
        turn: Turn,
        previous: Option<OperationId>,
        admit: impl AsyncFnOnce(&Turn) -> Result<Admission<RuntimeTask<crate::executor::TurnOutput>>>,
    ) -> Result<Admission<RuntimeTask<crate::executor::TurnOutput>>> {
        let (latest, generation) = self.current_turn().await?;
        if latest.as_ref().map(|turn| turn.operation) != previous {
            return Err(Error::Conflict("local admission head changed".into()));
        }
        // Request publication precedes admission. Cancellation/lost acknowledgements
        // retain the exact input so resume can finish admission without re-reading GOAL.md.
        self.write_turn(
            &turn,
            &generation,
            &crate::IdempotencyKey::new(format!("local-turn:{}", turn.operation))?,
        )
        .await?;
        admit(&turn).await
    }

    async fn write_turn(
        &self,
        turn: &Turn,
        generation: &GenerationRef,
        key: &crate::IdempotencyKey,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(turn).map_err(storage)?;
        let grant = crate::conversation::ContentGrant::verify(
            &self.verifier,
            &self.signed,
            &self.journal,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        self.files
            .put_content(
                &self.journal,
                &grant,
                HEAD,
                &bytes,
                "application/json",
                "current.json",
                self.limits.file_bytes,
                key,
                Some(generation),
                None,
                None,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn recover_unadmitted_turn(
        &self,
        task: TaskId,
    ) -> Result<RuntimeTask<crate::executor::TurnOutput>> {
        serialized(&self.admission, async || {
            let (current, _) = self.current_turn().await?;
            let turn = current
                .filter(|turn| crate::runtime::task_id_for_admission(turn.operation) == task)
                .ok_or_else(|| Error::NotFound("current local request".into()))?;
            let admission = self.admit_retained(&turn).await?;
            match admission {
                Admission::Accepted(handle @ RuntimeTask::Durable { task_id, .. })
                    if task_id == task =>
                {
                    Ok(handle)
                }
                other => {
                    admitted_task(other)?;
                    Err(Error::Conflict(
                        "recovered admission belongs to another task".into(),
                    ))
                }
            }
        })
        .await
    }

    pub(super) async fn submit_turn(
        &self,
        operation: OperationId,
        prompt: &str,
    ) -> Result<Admission<RuntimeTask<crate::executor::TurnOutput>>> {
        serialized(&self.admission, async || {
            if let Some((task, _)) = self
                .runtime
                .harness()
                .reconcile_admission(operation)
                .await?
            {
                // Ordinary retries retain the same immutable request bytes too.
                self.stage_turn(operation, prompt).await?;
                let definition = self
                    .runtime
                    .harness()
                    .task::<serde_json::Value, crate::executor::TurnOutput>(
                        "acyclic.stock_turn@1",
                    )?;
                return Ok(Admission::Accepted(
                    self.runtime.harness().attach(task, &definition).await?,
                ));
            }
            let (current, _) = self.current_turn().await?;
            if let Some(turn) = &current
                && turn.operation == operation
            {
                return self.admit_retained(turn).await;
            }
            if !can_admit(
                current.as_ref().map(|turn| turn.operation),
                None,
                self.head_is_terminal(current.as_ref()).await?,
            ) {
                return Ok(Admission::Rejected {
                    reason: "local agent already has an admitted turn".into(),
                });
            }
            let (receive, model) = self.stage_turn(operation, prompt).await?;
            self.publish_turn(
                Turn {
                    operation,
                    root: operation,
                    automatic_turns: 0,
                    goal_revision: None,
                    receive,
                    model,
                },
                current.as_ref().map(|turn| turn.operation),
            )
            .await
        })
        .await
    }

    pub(super) async fn continue_goal(
        &self,
        task: TaskId,
        root: OperationId,
    ) -> Result<Option<TaskId>> {
        serialized(&self.admission, async || {
            let (current, generation) = self.current_turn().await?;
            let Some(current) = current else {
                return Ok(None);
            };
            let settled = OperationId::from_bytes(task.into_bytes());
            if current.operation != settled {
                // Replaying an older settle follows its already-published ordinary
                // request instead of consuming another continuation allowance.
                if current.root == root {
                    return admitted_task(self.admit_retained(&current).await?).map(Some);
                }
                return Ok(None);
            }
            let Some(policy) = self.goal_continuations else {
                return Ok(None);
            };
            if !can_admit(
                Some(current.operation),
                Some(settled),
                self.head_is_terminal(Some(&current)).await?,
            ) {
                return Ok(None);
            }
            let operation = crate::goal::continuation_operation(
                crate::runtime::task_id_for_admission(current.root),
                settled,
            );
            // A staged model input is already an immutable choice. Recover it
            // before reading a possibly edited, completed or removed GOAL.md.
            if let Some(inputs) = self.recover_turn_inputs(operation).await? {
                let count = current
                    .automatic_turns
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("goal continuation counter overflow".into()))?;
                return admitted_task(
                    self.publish_turn(
                        Turn {
                            operation,
                            root: current.root,
                            automatic_turns: count,
                            goal_revision: None,
                            receive: inputs.0,
                            model: inputs.1,
                        },
                        Some(current.operation),
                    )
                    .await?,
                )
                .map(Some);
            }
            let (source, bytes, publication) = match self
                .files
                .read_private_path_indexed(
                    &self.scratch,
                    &self.read_grant(&self.scratch)?,
                    "GOAL.md",
                    None,
                    self.limits.file_bytes,
                    None,
                    None,
                )
                .await
            {
                Ok(file) => file,
                Err(Error::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            let goal = GoalView::from_file(source, &bytes, self.limits.file_bytes)?;
            if goal.status == GoalStatus::Blocked {
                self.record_goal_revision(&current, &generation, &goal.source, publication)
                    .await?;
            }
            let unreachable = GoalBlockedReason::TaskViewUnreachable;
            if goal.status == GoalStatus::Blocked
                && goal.reason.as_deref() == Some(unreachable.to_string().as_str())
            {
                return Err(Error::GoalBlocked(unreachable));
            }
            match policy.on_settle(
                &goal,
                crate::runtime::task_id_for_admission(current.root),
                settled,
                current.automatic_turns,
            ) {
                None => Ok(None),
                Some(GoalContinuation::Continue(operation)) => {
                    let Some(path) = self.goal_task_path() else {
                        let contents = goal
                            .status_contents(GoalStatus::Blocked, Some(&unreachable.to_string()))?;
                        let blocked = self.block_goal(&goal, settled, &contents).await?;
                        self.record_goal_revision(
                            &current,
                            &generation,
                            &blocked.0,
                            Some(blocked.1),
                        )
                        .await?;
                        return Err(Error::GoalBlocked(unreachable));
                    };
                    let prompt =
                        format!("Continue the active goal in {path}.\n\n{}", goal.markdown);
                    let (receive, model) = self.stage_turn(operation, &prompt).await?;
                    let admission = self
                        .publish_turn(
                            Turn {
                                operation,
                                root: current.root,
                                automatic_turns: current.automatic_turns + 1,
                                goal_revision: publication,
                                receive,
                                model,
                            },
                            Some(current.operation),
                        )
                        .await?;
                    admitted_task(admission).map(Some)
                }
                Some(GoalContinuation::Block(contents)) => {
                    let blocked = self.block_goal(&goal, settled, &contents).await?;
                    self.record_goal_revision(&current, &generation, &blocked.0, Some(blocked.1))
                        .await?;
                    Ok(None)
                }
            }
        })
        .await
    }

    pub(super) async fn loop_root(&self, task: TaskId) -> Result<OperationId> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.signed,
            &self.journal,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        // Each head publication already retains its immutable generation under
        // this ordinary file-write identity, even after a successor replaces HEAD.
        let Some(file) = self
            .files
            .content_for_operation(
                &self.journal,
                &grant,
                &crate::IdempotencyKey::new(format!("local-turn:{operation}"))?,
                HEAD,
                self.limits.file_bytes,
                None,
                None,
            )
            .await?
        else {
            return Ok(operation);
        };
        let (_, bytes) = self
            .files
            .read_private_path(
                &self.journal,
                &self.read_grant(&self.journal)?,
                HEAD,
                Some(&self.files.file_generation(&file, None)?),
                self.limits.file_bytes,
                None,
                None,
            )
            .await?;
        let turn = record::decode(&bytes).map_err(storage)?;
        if turn.operation != operation {
            return Err(Error::Conflict(
                "retained turn belongs to another operation".into(),
            ));
        }
        Ok(turn.root)
    }
}

fn admitted_task(admission: Admission<RuntimeTask<crate::executor::TurnOutput>>) -> Result<TaskId> {
    match admission {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => Ok(task_id),
        Admission::Accepted(RuntimeTask::Live(_)) => {
            Err(Error::Invalid("local turn is not durable".into()))
        }
        Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
        Admission::Rejected { reason } => Err(Error::Conflict(reason)),
    }
}

impl LocalHarness {
    /// Closes quiescent native mounts before releasing their provider handles.
    /// An admitted unfinished turn must first settle or be recovered by its owner.
    pub async fn close(self) -> Result<()> {
        let (head, _) = self.current_turn().await?;
        if !self.head_is_terminal(head.as_ref()).await? {
            return Err(Error::Conflict(
                "cannot close an unfinished local turn".into(),
            ));
        }
        self.disconnect().await
    }

    /// Releases this host's native mounts while preserving every durable task,
    /// including unfinished admission. Consuming the host excludes live borrows;
    /// callers must first join active execution and drop client observations.
    pub async fn disconnect(self) -> Result<()> {
        #[cfg(feature = "native-execution")]
        if let Some(workspace) = &self.workspace {
            for (_, mount) in workspace.mounts.mounts() {
                if let Some(source) = &mount.view.source {
                    let native = source.native().await;
                    if let Some(native) = native.as_ref() {
                        native.unmount().await?;
                    }
                }
            }
        }
        Ok(())
    }
}
