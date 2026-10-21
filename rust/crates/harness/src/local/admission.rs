use super::*;
use crate::sync::owned::{Arc as StateArc, Mutex, lock_owned};
use crate::{
    conversation::ContentGrant,
    goal::{GoalBlockedReason, GoalContinuation, GoalStatus, GoalView},
    resources::GenerationRef,
};

use acyclic_native_runtime::spawn;

use crate::goal::request_head as record;
use record::Turn;

/// Where this thread's latest head publication stands: before its request
/// write, inside it (where the head becomes durable at an await the caller
/// cannot see), written but not yet admitted, or admitted. Cancellation
/// sweeps classify awaits by it.
#[cfg(test)]
pub(super) mod publication_phase {
    use std::cell::Cell;

    pub(in crate::local) const BEFORE: u8 = 0;
    pub(in crate::local) const WRITING: u8 = 1;
    pub(in crate::local) const WRITTEN: u8 = 2;
    pub(in crate::local) const ADMITTED: u8 = 3;

    thread_local! {
        static PHASE: Cell<u8> = const { Cell::new(BEFORE) };
    }

    pub(in crate::local) fn set(phase: u8) {
        PHASE.set(phase);
    }

    pub(in crate::local) fn get() -> u8 {
        PHASE.get()
    }
}
mod goal;
#[cfg(test)]
#[cfg(acyclic_harness_loom)]
mod model;
#[cfg(test)]
#[cfg(all(feature = "native-execution", not(acyclic_harness_loom)))]
mod poll_sweep;
#[cfg(test)]
pub(in crate::local) mod snapshot;
#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
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

// Readers hold admission or consume the host during teardown. The detached
// writer holds only publication, so admission -> publication is the nested order.
async fn wait_for_publication(publication: &StateArc<Mutex<()>>) {
    #[cfg(test)]
    if SKIP_PUBLICATION_WAIT.get() {
        return;
    }
    drop(lock_owned(StateArc::clone(publication)).await);
}

#[cfg(test)]
thread_local! {
    static SKIP_PUBLICATION_WAIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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
        // A cancelled publisher's HEAD write is settled before HEAD is read.
        wait_for_publication(&self.publication).await;
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
            )
            .await
        {
            Ok((_, bytes)) => Ok((Some(record::decode(&bytes).map_err(storage)?), generation)),
            Err(Error::NotFound(_)) => Ok((None, generation)),
            Err(error) => Err(error),
        }
    }

    async fn head_is_terminal(&self, head: Option<&Turn>) -> Result<bool> {
        self.head_ready(head, false).await
    }

    async fn head_ready(&self, head: Option<&Turn>, recover: bool) -> Result<bool> {
        let Some(head) = head else { return Ok(true) };
        let outcome = match self
            .runtime
            .task_host()
            .terminal_status(crate::runtime::task_id_for_admission(head.operation))
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
        let terminal = super::orphans::settled(outcome.as_ref());
        if recover
            && (!terminal || matches!(outcome, Some(crate::Outcome::Cancelled)))
            && self.drivers.orphaned(head.operation)
        {
            self.supersede(head.operation).await?;
            return Ok(true);
        }
        Ok(terminal)
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
        #[cfg(test)]
        publication_phase::set(publication_phase::WRITING);
        self.write_turn(
            &turn,
            &generation,
            &crate::IdempotencyKey::new(format!("local-turn:{}", turn.operation))?,
        )
        .await?;
        #[cfg(test)]
        publication_phase::set(publication_phase::WRITTEN);
        let admission = admit(&turn).await?;
        #[cfg(test)]
        if matches!(admission, Admission::Accepted(_)) {
            publication_phase::set(publication_phase::ADMITTED);
        }
        Ok(admission)
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
        let (files, journal, limit) = (
            Arc::clone(&self.files),
            self.journal.clone(),
            self.limits.file_bytes,
        );
        let (key, generation) = (key.clone(), generation.clone());
        // The write is owned apart from its publisher, holding `publication`
        // until it lands or fails. A publisher cancelled mid-write releases
        // the admission gate, and the next holder's HEAD read waits here
        // instead of racing the write it abandoned.
        let held = lock_owned(StateArc::clone(&self.publication)).await;
        spawn(async move {
            let _held = held;
            files
                .put_content(
                    &journal,
                    &grant,
                    HEAD,
                    &bytes,
                    "application/json",
                    "current.json",
                    limit,
                    &key,
                    Some(&generation),
                    None,
                )
                .await
                .map(drop)
        })
        .await
        .map_err(storage)?
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
    ) -> Result<(
        Admission<RuntimeTask<crate::executor::TurnOutput>>,
        Option<super::orphans::Driving<'_>>,
    )> {
        serialized(&self.admission, async || {
            // Every accepted admission of this operation, retries included,
            // hands this claim to its caller; refusal or cancellation drops it.
            let claim = self.drivers.drive(operation);
            let admission = self.admit_head(operation, prompt).await?;
            let accepted = matches!(admission, Admission::Accepted(_));
            Ok((admission, accepted.then_some(claim)))
        })
        .await
    }

    // The caller holds the admission gate and a claim on `operation`.
    async fn admit_head(
        &self,
        operation: OperationId,
        prompt: &str,
    ) -> Result<Admission<RuntimeTask<crate::executor::TurnOutput>>> {
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
                .task::<serde_json::Value, crate::executor::TurnOutput>("acyclic.stock_turn@1")?;
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
        let terminal = self.head_ready(current.as_ref(), true).await?;
        if !can_admit(current.as_ref().map(|turn| turn.operation), None, terminal) {
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
    }

    /// Publishes and admits the goal turn that follows a settled one, handing
    /// its caller a claim taken in the same hold of the admission gate.
    pub(super) async fn continue_goal(
        &self,
        task: TaskId,
        root: OperationId,
    ) -> Result<Option<(TaskId, super::orphans::Driving<'_>)>> {
        serialized(&self.admission, async || {
            let next = self.next_goal_turn(task, root).await?;
            Ok(next.map(|next| {
                (
                    next,
                    self.drivers
                        .drive(OperationId::from_bytes(next.into_bytes())),
                )
            }))
        })
        .await
    }

    // The caller holds the admission gate.
    async fn next_goal_turn(&self, task: TaskId, root: OperationId) -> Result<Option<TaskId>> {
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
        // Disabling automatic admissions must not make an unrelated turn
        // depend on a goal body outside the configured readable domain.
        // Valid blocked goals still determine the terminal result.
        let goal_generation = if self.goal_continuations.is_none() {
            let workspace = crate::filesystem::workspace_ref(
                self.scratch.provider().clone(),
                &self.scratch.storage_name(&mut None)?,
            )?;
            let generation = self.files.resolve(&workspace, None).await?.generation;
            match self
                .files
                .stat(&workspace, Some(&generation), "/GOAL.md")
                .await
            {
                Ok(stat)
                    if stat.kind == acyclic_fs::kernel::FileKind::Regular
                        && stat
                            .logical_bytes
                            .is_some_and(|bytes| bytes <= self.limits.file_bytes) => {}
                Ok(_) | Err(Error::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
            Some(generation)
        } else {
            None
        };
        let (source, bytes, publication) = match self
            .files
            .read_private_path_indexed(
                &self.scratch,
                &self.read_grant(&self.scratch)?,
                "GOAL.md",
                goal_generation.as_ref(),
                self.limits.file_bytes,
                None,
            )
            .await
        {
            Ok(file) => file,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let goal = match GoalView::from_file(source, &bytes, self.limits.file_bytes) {
            Ok(goal) => goal,
            Err(Error::Invalid(_)) if self.goal_continuations.is_none() => return Ok(None),
            Err(error) => return Err(error),
        };
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
        // A goal whose final state is blocked is the loop's result, so a
        // headless caller reports it instead of a completed turn.
        if goal.status == GoalStatus::Blocked {
            return Err(Error::GoalBlocked(GoalBlockedReason::Declared));
        }
        let Some(policy) = self.goal_continuations else {
            return Ok(None);
        };
        match policy.on_settle(
            &goal,
            crate::runtime::task_id_for_admission(current.root),
            settled,
            current.automatic_turns,
        ) {
            None => Ok(None),
            Some(GoalContinuation::Continue(operation)) => {
                let Some(path) = self.goal_task_path() else {
                    let contents =
                        goal.status_contents(GoalStatus::Blocked, Some(&unreachable.to_string()))?;
                    let blocked = self.block_goal(&goal, settled, &contents).await?;
                    self.record_goal_revision(&current, &generation, &blocked.0, Some(blocked.1))
                        .await?;
                    return Err(Error::GoalBlocked(unreachable));
                };
                let prompt = format!("Continue the active goal in {path}.\n\n{}", goal.markdown);
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
                Err(Error::GoalBlocked(GoalBlockedReason::Declared))
            }
        }
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

    /// Test teardown for a host that deliberately leaves its HEAD turn
    /// unfinished. It requires that turn to be unfinished, so each teardown is
    /// checked either way: `close` proves a settled HEAD, this proves an
    /// unsettled one, and a test cannot use either to hide the other.
    #[cfg(test)]
    pub(crate) async fn disconnect_unfinished(self) -> Result<()> {
        let (head, _) = self.current_turn().await?;
        if self.head_is_terminal(head.as_ref()).await? {
            return Err(Error::Conflict(
                "a settled local turn must close, not disconnect".into(),
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
            crate::local::workspace::volumes::retire(&workspace.mounts).await?;
        }
        Ok(())
    }
}
