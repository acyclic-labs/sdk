//! The executor turn driver: one turn is one executor operation, resumed by identity.

use super::{Result, adopted, id, intent};
use acyclic_harness_core::{
    Error as CoreError, InteractionRejection, OperationId,
    agent::*,
    conversation::FileRef,
    executor::{
        ExecutionClaim, ExecutionEvent, ExecutionJournal, ExecutionProgress, ExecutionRecord,
        Executor, PreparationIo, TurnInput as ExecutorInput, TurnOutput,
    },
    interaction::{Interaction, InteractionOutcome},
};
use acyclic_stream::{BoxProviderFuture as BoxFuture, OwnedBytes};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

/// Journal the scripted executor never touches; every method refuses.
struct Unused;

fn unused<T>() -> acyclic_harness_core::Result<T> {
    Err(CoreError::Unsupported("unused test journal".into()))
}

impl ExecutionJournal for Unused {
    fn claim_execution<'a>(
        &'a self,
        _: OperationId,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<ExecutionClaim<'a>> {
        unused()
    }

    fn replay<'a>(
        &'a self,
        _: OperationId,
        _: u64,
        _: u32,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>,
    > {
        unused()
    }

    fn append<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: &'a ExecutionEvent,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, ()>,
    > {
        unused()
    }

    fn stage<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: &'a [u8],
        _: &'static str,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, FileRef>,
    > {
        unused()
    }

    fn load<'a>(
        &'a self,
        _: &'a FileRef,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, OwnedBytes>,
    > {
        unused()
    }

    fn open_interaction<'a>(
        &'a self,
        _: acyclic_harness_core::InteractionId,
        _: &'a Interaction,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, ()>,
    > {
        unused()
    }

    fn interaction_outcome<'a>(
        &'a self,
        _: acyclic_harness_core::InteractionId,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>,
    > {
        unused()
    }
}

/// Executor replaying a script of progress results, recording each operation.
#[derive(Default)]
struct Script {
    progress: Mutex<VecDeque<ExecutionProgress<TurnOutput>>>,
    operations: Mutex<Vec<OperationId>>,
}

impl Executor for Script {
    fn execute<'a>(
        &'a self,
        _: ExecutorInput,
        _: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<TurnOutput>> {
        Box::pin(async { unused() })
    }

    fn execute_progress<'a>(
        &'a self,
        input: ExecutorInput,
        _: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<ExecutionProgress<TurnOutput>>> {
        self.operations
            .lock()
            .expect("lock")
            .push(input.operation_id);
        let next = self.progress.lock().expect("lock").pop_front();
        Box::pin(async move { next.map_or_else(unused, Ok) })
    }
}

/// Host binding every turn to the script.
struct Host(Arc<Script>);

impl TurnHost for Host {
    fn open<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a acyclic_harness_core::model::ModelContent,
        _: Arc<acyclic_harness_core::agent::TurnStop>,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<OpenTurn>> {
        let open = OpenTurn {
            executor: self.0.clone(),
            journal: Arc::new(Unused),
            max_steps: 4,
            custody: None,
            selected: None,
            approvals: None,
        };
        Box::pin(async move { Ok(open) })
    }
}

fn driver(progress: Vec<ExecutionProgress<TurnOutput>>) -> (Arc<Script>, ExecutorDriver<Host>) {
    let script = Arc::new(Script {
        progress: Mutex::new(progress.into()),
        ..Script::default()
    });
    (script.clone(), ExecutorDriver::new(Host(script)))
}

/// A turn-starting input that carries no file: a child's approval request.
fn request(id: &str) -> Result<(InboxRecord, TurnInput)> {
    let child = AgentPath::root(&super::id(1)).child(&super::id(7))?;
    Ok((
        InboxRecord::ApprovalRequested(ApprovalRequested {
            child: child.clone(),
            id: intent(id),
        }),
        TurnInput::ApprovalRequested {
            child,
            id: intent(id),
        },
    ))
}

#[tokio::test]
async fn an_approval_pause_keeps_the_operation_and_ignores_other_approvals() -> Result {
    let path = AgentPath::root(&id(1));
    let state = AgentState::adopted(path.clone(), &adopted(id(1)));
    let approval = OperationId::from_bytes([9; 16]);
    let (script, driver) = driver(vec![ExecutionProgress::Pending(approval)]);
    let first = step(
        &driver,
        &path,
        Some(state),
        1,
        Wake::Inbox {
            sequence: 1,
            record: request("m1")?.0,
        },
    )
    .await?;
    let waiting = intent(&approval_intent(approval));
    assert_eq!(
        first.state.phase,
        Phase::WaitingApproval {
            id: waiting.clone()
        }
    );
    assert!(first.state.turn.is_some());
    // An unrelated approval does not resume the paused operation.
    let other = step(
        &driver,
        &path,
        Some(first.state.clone()),
        2,
        Wake::Inbox {
            sequence: 2,
            record: InboxRecord::Approval(Approval {
                id: intent("tool:other"),
            }),
        },
    )
    .await?;
    assert_eq!(script.operations.lock().expect("lock").len(), 1);
    assert_eq!(other.state.phase, first.state.phase);
    // The checkpoint holds the starting input, so a resume re-derives the same operation.
    assert_eq!(other.state.turn, first.state.turn);
    Ok(())
}

#[tokio::test]
async fn a_refused_approval_fails_the_agent() -> Result {
    let path = AgentPath::root(&id(1));
    let state = AgentState::adopted(path.clone(), &adopted(id(1)));
    let (_, driver) = driver(vec![ExecutionProgress::Rejected(
        InteractionRejection::Declined,
    )]);
    let failed = step(
        &driver,
        &path,
        Some(state),
        1,
        Wake::Inbox {
            sequence: 1,
            record: request("m1")?.0,
        },
    )
    .await?;
    assert_eq!(
        failed.state.phase,
        Phase::Done {
            outcome: AgentOutcome::Failed {
                message: "approval declined".into()
            }
        }
    );
    Ok(())
}

#[test]
fn distinct_inputs_start_distinct_turns() -> Result {
    let path = AgentPath::root(&id(1));
    let state = AgentState::adopted(path, &adopted(id(1)));
    let input = |id: &str| -> Result<TurnInput> { Ok(request(id)?.1) };
    assert_eq!(
        turn_id(&state, &input("a")?)?,
        turn_id(&state, &input("a")?)?
    );
    assert_ne!(
        turn_id(&state, &input("a")?)?,
        turn_id(&state, &input("b")?)?
    );
    Ok(())
}
