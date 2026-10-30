//! The executor turn driver: one turn is one executor operation, resumed by identity.

use super::{Result, adopted, id, intent};
use acyclic_harness_core::{
    Error as CoreError, OperationId,
    agent::*,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    executor::{
        ExecutionClaim, ExecutionEvent, ExecutionJournal, ExecutionProgress, ExecutionRecord,
        Executor, PreparationIo, TurnInput as ExecutorInput, TurnOutput,
    },
    goal::{GoalContinuationPolicy, GoalStatus, GoalView},
    interaction::{Interaction, InteractionOutcome},
};
use acyclic_stream::{BoxProviderFuture as BoxFuture, OwnedBytes};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

/// Journal the scripted executor never touches; every method but `stage`
/// refuses, and `stage` names one fixed output file.
struct Unused;

impl acyclic_harness_core::agent::TurnOutputs for Unused {
    fn stage<'a>(
        &'a self,
        _: OperationId,
        _: &'a [u8],
    ) -> BoxFuture<'a, acyclic_harness_core::Result<FileRef>> {
        Box::pin(async { file("output.json") })
    }
}

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
        let file = file("output.json")?;
        acyclic_harness_core::conversation::BoundedContentFuture::new(None, move || async move {
            Ok(file)
        })
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
    failure: Mutex<Option<CoreError>>,
    operations: Mutex<Vec<OperationId>>,
    effects: Mutex<Vec<TurnEffect>>,
}

impl TurnEffects for Script {
    fn record<'a>(
        &'a self,
        _: &'a str,
        _: &'a TurnEffect,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<TurnEffect>> {
        Box::pin(async { unused() })
    }

    fn recorded(&self) -> BoxFuture<'_, acyclic_harness_core::Result<Vec<TurnEffect>>> {
        Box::pin(async {
            self.effects
                .lock()
                .map(|effects| effects.clone())
                .map_err(|error| CoreError::Storage(error.to_string()))
        })
    }

    fn holds(&self, _: OperationId) -> BoxFuture<'_, acyclic_harness_core::Result<bool>> {
        Box::pin(async { Ok(false) })
    }
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
        let failure = self.failure.lock().expect("lock").take();
        Box::pin(async move {
            match failure {
                Some(error) => Err(error),
                None => next.map_or_else(unused, Ok),
            }
        })
    }
}

/// Host binding every turn to the script and, optionally, a goal.
struct Host(Arc<Script>, Option<Arc<Goal>>);

impl TurnHost for Host {
    fn open<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a acyclic_harness_core::model::ModelContent,
        _: acyclic_actors::guest::CancellationToken,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<OpenTurn>> {
        let open = OpenTurn {
            executor: self.0.clone(),
            journal: Arc::new(Unused),
            max_steps: 4,
            custody: None,
            selected: None,
            checkpoint: None,
            approvals: None,
            effects: Some(self.0.clone()),
            outputs: Arc::new(Unused),
            input: None,
            roster: None,
            goal: self
                .1
                .clone()
                .map(|goal| goal as Arc<dyn acyclic_harness_core::agent::TurnGoal>),
        };
        Box::pin(async move { Ok(open) })
    }
}

/// Host announcing its roster exactly when the agent's announced digest
/// differs, as every host does ([`Roster::announcement`]).
struct Announcing(Host, Arc<Mutex<Roster>>);

impl TurnHost for Announcing {
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a acyclic_harness_core::model::ModelContent,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<OpenTurn>> {
        Box::pin(async move {
            let mut open = self.0.open(agent, turn, input, stop).await?;
            let announced = self
                .1
                .lock()
                .expect("lock")
                .announcement(agent.roster)
                .map_err(|error| CoreError::Invalid(error.to_string()))?;
            open.roster = match announced {
                Some((_, digest)) => Some((file("targets.json")?, digest)),
                None => None,
            };
            Ok(open)
        })
    }
}

#[tokio::test]
async fn a_roster_enters_the_conversation_only_when_its_digest_changes() -> Result {
    let device = |os: &str| TargetRecord {
        name: "laptop".into(),
        owner: "ana".into(),
        class: TargetClass::Attached,
        os: os.into(),
        shell: "bash".into(),
        path_style: PathStyle::Posix,
        state: TargetState::Online,
    };
    let roster = |os| Roster::home(device(os)).expect("valid roster");
    let script = Arc::new(Script {
        progress: Mutex::new((0..3).map(|_| ready()).collect()),
        ..Script::default()
    });
    let current = Arc::new(Mutex::new(roster("linux")));
    let driver = ExecutorDriver::new(Announcing(Host(script, None), Arc::clone(&current)));
    let path = AgentPath::root(&id(1));
    let mut state = AgentState::adopted(path.clone(), &adopted(id(1))).expect("adopts");
    // The first turn announces; an unchanged roster is never announced
    // again; a changed one (the negative control) is.
    for (sequence, os, announced) in [(1, "linux", true), (2, "linux", false), (3, "macos", true)] {
        *current.lock().expect("lock") = roster(os);
        let record = InboxRecord::Message(Message {
            from: super::user(),
            id: intent(&format!("m{sequence}")),
            body: Box::new(file("prompt.md")?),
            attachments: None,
        });
        let settled = step(
            &driver,
            &path,
            Some(state.clone()),
            sequence,
            Wake::Inbox { sequence, record },
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
        let entries: Vec<_> = settled
            .outs
            .iter()
            .filter_map(|out| match out {
                Out::Conversation(bytes) => Some(ConversationEntry::decode(bytes)),
                _ => None,
            })
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].targets.is_some(), announced, "turn {sequence}");
        let digest = roster(os).canonical().map(|(_, digest)| digest).ok();
        assert_eq!(settled.state.roster, digest);
        state = settled.state;
    }
    Ok(())
}

fn driver(progress: Vec<ExecutionProgress<TurnOutput>>) -> (Arc<Script>, ExecutorDriver<Host>) {
    let script = Arc::new(Script {
        progress: Mutex::new(progress.into()),
        ..Script::default()
    });
    (script.clone(), ExecutorDriver::new(Host(script, None)))
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
async fn provider_failures_commit_terminal_status_while_uncertain_errors_propagate() -> Result {
    use acyclic_harness_core::ModelTransportFailure;
    let mut cases = vec![
        (
            CoreError::ModelUnavailable {
                failure: ModelTransportFailure::Status(500),
                excerpt: "exhausted".into(),
            },
            true,
        ),
        (
            CoreError::ModelUnavailable {
                failure: ModelTransportFailure::Connect,
                excerpt: String::new(),
            },
            true,
        ),
        (
            CoreError::ModelUnavailable {
                failure: ModelTransportFailure::Interrupted,
                excerpt: String::new(),
            },
            true,
        ),
        (CoreError::Storage("unreadable journal".into()), false),
        (
            CoreError::Indeterminate(OperationId::from_bytes([8; 16])),
            false,
        ),
        (CoreError::Cancelled, false),
        // A definitive failure settles the agent as failed; only an
        // undecided one retries the step.
        (
            CoreError::Invalid("unclassified executor error".into()),
            true,
        ),
    ];
    for (code, terminal) in [
        (acyclic_harness_core::wire::ErrorCode::Invalid, true),
        (acyclic_harness_core::wire::ErrorCode::Unauthorized, true),
        (acyclic_harness_core::wire::ErrorCode::Unavailable, true),
        (acyclic_harness_core::wire::ErrorCode::Storage, false),
        (acyclic_harness_core::wire::ErrorCode::Indeterminate, false),
        (acyclic_harness_core::wire::ErrorCode::Cancelled, false),
    ] {
        cases.push((
            CoreError::ModelDiagnostic {
                code,
                message: "host-protected model exchange failed".into(),
                custody: None,
            },
            terminal,
        ));
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    cases.push((
        CoreError::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Unavailable,
            reason: "native admission refused",
        },
        false,
    ));
    for (error, terminal) in cases.drain(..) {
        let diagnostic = error.to_string();
        let path = AgentPath::root(&id(1));
        let script = Arc::new(Script {
            failure: Mutex::new(Some(error)),
            ..Script::default()
        });
        let driver = ExecutorDriver::new(Host(script, None));
        let result = step(
            &driver,
            &path,
            Some(AgentState::adopted(path.clone(), &adopted(id(1))).expect("adopts")),
            1,
            Wake::Inbox {
                sequence: 1,
                record: request("failed")?.0,
            },
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await;
        if terminal {
            let settled = result?;
            assert_eq!(
                settled.state.phase,
                Phase::Done {
                    outcome: AgentOutcome::Failed {
                        message: diagnostic
                    }
                }
            );
            let status = settled.status.ok_or("missing terminal status")?;
            assert_eq!(status.phase, settled.state.phase);
            assert_eq!(status.cursor, 1);
            assert!(settled.state.turn.is_none());
        } else {
            assert!(
                matches!(result, Err(Error::Turn(_))),
                "uncertain failure was settled: {diagnostic}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn settled_turns_keep_actions_recorded_before_a_later_provider_failure() -> Result {
    for failed in [false, true] {
        let (script, driver) = driver(if failed { Vec::new() } else { vec![ready()] });
        if failed {
            *script.failure.lock().map_err(|error| error.to_string())? =
                Some(CoreError::ModelUnavailable {
                    failure: acyclic_harness_core::ModelTransportFailure::Status(500),
                    excerpt: "exhausted after a successful send".into(),
                });
        }
        let path = AgentPath::root(&id(1));
        let to = path.child(&id(2))?;
        let body = Box::new(file("sent-before-failure.md")?);
        script
            .effects
            .lock()
            .map_err(|error| error.to_string())?
            .push(TurnEffect::Send {
                to: to.clone(),
                id: intent("sent"),
                body: body.clone(),
            });
        let state = AgentState::adopted(path, &adopted(id(1))).expect("adopts");
        let input = Queued {
            inbox: 0,
            input: request("input")?.1,
        };
        let step = driver
            .step(
                &state,
                1,
                Some(input),
                acyclic_actors::guest::CancellationToken::new(),
            )
            .await?;
        assert!(
            matches!(step.outs.first(), Some(Out::Send { to: recipient, record: InboxRecord::Message(message) })
            if recipient == &to && message.id == intent("sent") && message.body == body)
        );
        assert_eq!(step.outs.len(), if failed { 1 } else { 2 });
        if failed {
            assert!(matches!(step.wait, Wait::Done(AgentOutcome::Failed { .. })));
        } else {
            assert!(matches!(step.outs.get(1), Some(Out::Conversation(_))));
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_approval_pause_keeps_the_operation_and_ignores_other_approvals() -> Result {
    let path = AgentPath::root(&id(1));
    let state = AgentState::adopted(path.clone(), &adopted(id(1))).expect("adopts");
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
        acyclic_actors::guest::CancellationToken::new(),
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
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert_eq!(script.operations.lock().expect("lock").len(), 1);
    assert_eq!(other.state.phase, first.state.phase);
    // The checkpoint holds the starting input, so a resume re-derives the same operation.
    assert_eq!(other.state.turn, first.state.turn);
    Ok(())
}

#[test]
fn distinct_inputs_start_distinct_turns() -> Result {
    let path = AgentPath::root(&id(1));
    let state = AgentState::adopted(path, &adopted(id(1))).expect("adopts");
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

/// A file on a private volume of agent 1.
pub(super) fn file(path: &str) -> acyclic_harness_core::Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            acyclic_harness_core::resources::ProviderRef::new(
                "test".into(),
                "filesystem".into(),
                "1".into(),
                &mut None,
            )?,
            "scratch".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes([1; 16])),
            &mut None,
        )?,
        path.into(),
        "version-1".into(),
        FileDescriptor::from_bytes(b"ship it", "text/markdown", &mut None)?,
        path.into(),
        &mut None,
    )
}

/// A GOAL.md whose status the test sets, recording every rewrite.
struct Goal {
    status: Mutex<GoalStatus>,
    rewrites: Mutex<Vec<String>>,
}

impl TurnGoal for Goal {
    fn policy(&self) -> GoalContinuationPolicy {
        GoalContinuationPolicy {
            max_continuations: 2,
        }
    }

    fn read(&self) -> BoxFuture<'_, acyclic_harness_core::Result<Option<GoalView>>> {
        let status = *self.status.lock().expect("lock");
        Box::pin(async move {
            Ok(Some(GoalView {
                source: file("GOAL.md")?,
                status,
                reason: None,
                markdown: "ship it
"
                .into(),
            }))
        })
    }

    fn rewrite<'a>(
        &'a self,
        _: &'a GoalView,
        contents: &'a str,
        _: &'a str,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<()>> {
        self.rewrites
            .lock()
            .expect("lock")
            .push(contents.to_owned());
        *self.status.lock().expect("lock") = GoalStatus::Blocked;
        Box::pin(async { Ok(()) })
    }
}

fn ready() -> ExecutionProgress<TurnOutput> {
    ExecutionProgress::Ready(TurnOutput {
        text: "worked".into(),
        attachments: Vec::new(),
        metadata: Default::default(),
        steps: 1,
    })
}

/// The goal continuation a step sent its own agent, if any.
fn continuation(step: &Step, path: &AgentPath) -> Option<Message> {
    let sends: Vec<_> = step
        .outs
        .iter()
        .filter_map(|out| match out {
            Out::Send {
                to,
                record: InboxRecord::Message(message),
            } if to == path => Some(message.clone()),
            _ => None,
        })
        .collect();
    assert!(sends.len() <= 1, "one continuation per settled turn");
    sends.into_iter().next()
}

#[tokio::test]
async fn an_active_goal_continues_through_the_own_inbox_until_its_allowance_blocks_it() -> Result {
    use acyclic_stream::{CommitOutcome, MemoryStream, StreamProviderExt as _};
    let provider = MemoryStream::default();
    let missing_completion = MemoryStream::default();
    let wrong_completion = MemoryStream::default();
    super::create(&provider, id(1)).await?;
    super::create(&missing_completion, id(1)).await?;
    super::create(&wrong_completion, id(1)).await?;
    let path = AgentPath::root(&id(1));
    let mut state = AgentState::adopted(path.clone(), &adopted(id(1))).expect("adopts");
    let goal = Arc::new(Goal {
        status: Mutex::new(GoalStatus::Active),
        rewrites: Mutex::new(Vec::new()),
    });
    let script = Arc::new(Script {
        progress: Mutex::new((0..5).map(|_| ready()).collect()),
        ..Script::default()
    });
    let driver = ExecutorDriver::new(Host(script, Some(Arc::clone(&goal))));
    let user = InboxRecord::Message(Message {
        from: super::user(),
        id: intent("m1"),
        body: Box::new(file("prompt.md")?),
        attachments: None,
    });
    let mut next = Some(user.clone());
    let mut sent = Vec::new();
    for sequence in 1..=3 {
        let record = next.take().expect("a continuation woke the agent");
        let settled = step(
            &driver,
            &path,
            Some(state.clone()),
            sequence,
            Wake::Inbox { sequence, record },
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
        assert_eq!(settled.state.phase, Phase::Idle);
        if let Some(message) = continuation(&settled, &path) {
            assert_eq!(message.from, Sender::Agent { path: path.clone() });
            assert_eq!(*message.body, file("GOAL.md")?);
            sent.push(message.id.as_str().to_owned());
            next = Some(InboxRecord::Message(message));
        }
        let commit = lower(
            &settled,
            sequence + 1,
            delivery_key(&settled.state, &format!("goal-{sequence}"))?,
        )?;
        assert!(matches!(
            provider.commit(commit.clone()).await?,
            CommitOutcome::Committed(_)
        ));
        assert!(
            matches!(provider.commit(commit).await?, CommitOutcome::Committed(_)),
            "delivery replay changes no result"
        );
        // The original lowering without its terminal join still commits every
        // conversation entry, but cannot answer the original goal request.
        let mut broken = lower(
            &settled,
            sequence + 1,
            delivery_key(&settled.state, &format!("goal-{sequence}"))?,
        )?;
        broken.mutations.retain(|mutation| !matches!(mutation,
            acyclic_stream::CommitMutation::Append { path, .. } if path.as_str().contains("/goals/")));
        assert!(matches!(
            missing_completion.commit(broken).await?,
            CommitOutcome::Committed(_)
        ));
        let mut wrong = Step {
            state: settled.state.clone(),
            outs: settled.outs.clone(),
            status: settled.status.clone(),
            alarm: settled.alarm,
        };
        if sequence == 3 {
            for out in &mut wrong.outs {
                if let Out::Conversation(bytes) = out {
                    let mut entry = ConversationEntry::decode(bytes)?;
                    entry.goal = Some(OperationId::from_bytes([99; 16]));
                    *bytes = entry.encode()?;
                }
            }
        }
        let wrong = lower(
            &wrong,
            sequence + 1,
            delivery_key(&settled.state, &format!("goal-{sequence}"))?,
        )?;
        assert!(matches!(
            wrong_completion.commit(wrong).await?,
            CommitOutcome::Committed(_)
        ));
        let answer = reply(&provider, &settled.state, sequence + 1, &intent("m1"), 1).await?;
        if sequence < 3 {
            assert_eq!(
                answer,
                Reply::Pending,
                "first settlement is not the goal's result"
            );
            let mut cancelled = settled.state.clone();
            cancelled.dropped_before = sequence + 1;
            assert_eq!(
                reply(&provider, &cancelled, sequence + 1, &intent("m1"), 1).await?,
                Reply::Dropped
            );
        } else {
            let Reply::Answered {
                sequence: result,
                entry,
                origin: Some(origin),
            } = answer
            else {
                return Err("goal did not return its terminal conversation entry".into());
            };
            assert_eq!((result, origin.inbox, entry.inbox), (2, 1, 3));
            assert_eq!(entry.goal, origin.goal);
            assert_eq!(
                reply(&missing_completion, &settled.state, 4, &intent("m1"), 1).await?,
                Reply::Pending
            );
            assert!(matches!(
                reply(&wrong_completion, &settled.state, 4, &intent("m1"), 1).await,
                Err(Error::Record("goal completion names another run"))
            ));
            // A stale checkpoint cannot expose the future terminal record.
            assert_eq!(
                reply(&provider, &state, 3, &intent("m1"), 1).await?,
                Reply::Pending
            );
        }
        state = settled.state;
    }
    // Two continuations of one loop, then the allowance blocks the goal.
    assert_eq!(sent.len(), 2);
    let run = sent[0]
        .strip_prefix(GOAL_INTENT)
        .and_then(|rest| rest.strip_suffix(":1"))
        .expect("first continuation of the loop");
    assert_eq!(sent[1], format!("{GOAL_INTENT}{run}:2"));
    let rewrites = goal.rewrites.lock().expect("lock").clone();
    assert_eq!(rewrites.len(), 1);
    assert!(rewrites[0].starts_with(
        "---
status: blocked
reason: \"Maximum goal continuations"
    ));
    assert!(rewrites[0].ends_with(
        "---
ship it
"
    ));
    // A blocked or done goal sends nothing and rewrites nothing.
    for status in [GoalStatus::Blocked, GoalStatus::Done] {
        *goal.status.lock().expect("lock") = status;
        let sequence = 4 + u64::from(status == GoalStatus::Done);
        let mut user = user.clone();
        let InboxRecord::Message(message) = &mut user else {
            return Err("original input is not a message".into());
        };
        message.id = intent(&format!("m{sequence}"));
        let settled = step(
            &driver,
            &path,
            Some(state),
            sequence,
            Wake::Inbox {
                sequence,
                record: user,
            },
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
        assert!(continuation(&settled, &path).is_none());
        let commit = lower(
            &settled,
            sequence + 1,
            delivery_key(&settled.state, &format!("goal-{sequence}"))?,
        )?;
        assert!(matches!(
            provider.commit(commit).await?,
            CommitOutcome::Committed(_)
        ));
        assert!(
            matches!(reply(&provider, &settled.state, sequence + 1, &intent(&format!("m{sequence}")), sequence).await?,
            Reply::Answered { origin: None, entry, .. } if entry.inbox == sequence)
        );
        assert!(
            matches!(reply(&provider, &settled.state, sequence + 1, &intent("m1"), 1).await?,
            Reply::Answered { sequence: 2, entry, origin: Some(origin) } if entry.inbox == 3 && origin.inbox == 1)
        );
        state = settled.state;
    }
    assert_eq!(goal.rewrites.lock().expect("lock").len(), 1);
    Ok(())
}
