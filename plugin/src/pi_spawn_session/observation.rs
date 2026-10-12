//! Observes the original preparation and child outcomes.
use super::*;

#[derive(Serialize)]
struct Child<'a> {
    id: &'a str,
    task: TaskId,
    native_session_id: &'a str,
    cwd: &'a Path,
    session_file: &'a Path,
    session_directory: Option<&'a Path>,
}
#[derive(Serialize)]
struct Preparation<'a> {
    task: TaskId,
    operation: OperationId,
    outcome: &'a Option<Outcome<Value>>,
}
#[derive(Serialize)]
struct Observation<'a> {
    operation: &'a str,
    state: &'static str,
    uncertain: bool,
    child: Child<'a>,
    preparation: Preparation<'a>,
    fork_operation: OperationId,
    native_operation: OperationId,
    native_effect: acyclic_harness::EffectId,
    outcome: &'a Option<Outcome<Value>>,
}
impl PiSession {
    pub(super) async fn spawn_observation(
        &self,
        commit: &RetainedSpawnCommit,
        operations: PiForkOperations,
        created: bool,
        work: &NativeMaterializationWork,
        maximum: u64,
    ) -> Result<(Value, NativeAllocation)> {
        let host = self.runtime.runtime.task_host();
        let preparation = acyclic_harness::runtime::task_id_for_admission(operations.preparation);
        let preparation_outcome = match host.outcome_with_native_work(preparation, work).await {
            Ok(outcome) => Some(outcome),
            Err(Error::NotFound(_)) if !created => None,
            Err(error) => return Err(error),
        };
        let outcome = if created {
            Some(
                host.outcome_with_native_work(commit.child.task_id, work)
                    .await?,
            )
        } else {
            None
        };
        let preparation_value = preparation_outcome
            .as_ref()
            .map(|entry| &entry.0)
            .unwrap_or(&None);
        let outcome_value = outcome.as_ref().map(|entry| &entry.0).unwrap_or(&None);
        let native_effect = acyclic_harness::effect_host::task_effect_id(
            commit.child.task_id,
            operations.native,
            &mut Some(&mut HistoryWork::new(PreparationIo::from_native_work(
                work,
            )?)),
        )?;
        let observation = Observation {
            operation: &commit.sdk_operation,
            state: if outcome_value.as_ref().is_some_and(Outcome::is_terminal) {
                "completed"
            } else if created {
                "pending"
            } else {
                "preparing"
            },
            uncertain: !created,
            child: Child {
                id: &commit.alias,
                task: commit.child.task_id,
                native_session_id: &commit.child.native_session_id,
                cwd: &commit.launch.invocation.cwd,
                session_file: &commit.launch.invocation.session_file,
                session_directory: commit.launch.invocation.session_file.parent(),
            },
            preparation: Preparation {
                task: preparation,
                operation: operations.preparation,
                outcome: preparation_value,
            },
            fork_operation: operations.fork,
            native_operation: operations.native,
            native_effect,
            outcome: outcome_value,
        };
        pi_root_provider::guarded_pi_value(&observation, maximum, work)
    }
}
