//! Agent model commit shapes and step protocol against the in-memory Streams provider.

use acyclic_harness_core::agent::*;
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream, ReadRequest,
    StreamError, StreamPath, StreamProvider,
};
use futures::TryStreamExt;
use std::{collections::VecDeque, sync::Mutex};

#[path = "agent_model/driver.rs"]
mod driver;
#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "agent_model/hosting.rs"]
mod hosting;
#[path = "agent_model/runtime.rs"]
mod runtime;
#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "agent_model/wired.rs"]
mod wired;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn id(byte: u8) -> AgentId {
    AgentId::from_bytes([byte; 16])
}

fn intent(text: &str) -> IntentId {
    IntentId::try_from(text).expect("valid intent")
}

fn user() -> Sender {
    Sender::User {
        principal: "user-1".into(),
    }
}

fn adopted(agent: AgentId) -> ForkAdopted {
    ForkAdopted {
        agent,
        parent: None,
        forked_at: 0,
        conversation_tail: 0,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    }
}

fn cancel(id: &str) -> InboxRecord {
    InboxRecord::Cancel(Control {
        by: user(),
        id: intent(id),
    })
}

async fn values(provider: &MemoryStream, path: StreamPath) -> Result<Vec<Vec<u8>>> {
    let tail = match provider.tail(path.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    if tail == 0 {
        return Ok(Vec::new());
    }
    let records: Vec<_> = provider
        .read(ReadRequest {
            path,
            from: 0,
            limit: u32::try_from(tail)?,
        })
        .await?
        .try_collect()
        .await?;
    Ok(records
        .into_iter()
        .map(|record| record.value.to_vec())
        .collect())
}

async fn create(provider: &MemoryStream, agent: AgentId) -> Result<AgentPath> {
    let created = create_root(&intent("create"), adopted(agent))?;
    assert!(matches!(
        submit(provider, created.request).await?,
        Submitted::Committed(_)
    ));
    Ok(created.child)
}

#[test]
fn layout_round_trips_and_bounds_depth() -> Result {
    let root = AgentPath::root(&id(1));
    let child = root.child(&id(2))?;
    assert_eq!(child.parent().as_ref(), Some(&root));
    assert_eq!(child.id(), id(2));
    assert_eq!(AgentPath::try_from(child.as_str().to_owned())?, child);
    assert!(AgentPath::try_from(format!("{}/status", root.as_str())).is_err());
    assert!(AgentId::try_from("A".repeat(32)).is_err());
    let mut deep = root;
    for _ in 0..MAX_DEPTH {
        deep = deep.child(&id(3))?;
    }
    assert!(deep.child(&id(3)).is_err());
    assert!(AgentPath::try_from(format!("{deep}/children/{}", id(3))).is_err());
    Ok(())
}

#[test]
fn records_are_canonical_total_and_bounded() -> Result {
    let record = InboxRecord::ChildDone(ChildDone {
        child: AgentPath::root(&id(1)).child(&id(2))?,
        outcome: AgentOutcome::Failed {
            message: "tool failed".into(),
        },
    });
    let bytes = record.encode()?;
    assert_eq!(InboxRecord::decode(&bytes)?, record);
    assert_eq!(InboxRecord::decode(&bytes)?.encode()?, bytes);
    let mut extra: serde_json::Value = serde_json::from_slice(&bytes)?;
    extra["child_done"]["unknown"] = serde_json::Value::Bool(true);
    assert!(InboxRecord::decode(&serde_json::to_vec(&extra)?).is_err());
    let huge = InboxRecord::ChildDone(ChildDone {
        child: AgentPath::root(&id(1)),
        outcome: AgentOutcome::Failed {
            message: "x".repeat(MAX_MESSAGE_BYTES + 1),
        },
    });
    assert!(huge.encode().is_err());
    assert!(InboxRecord::decode(&vec![b' '; MAX_RECORD_BYTES + 1]).is_err());
    Ok(())
}

#[tokio::test]
async fn send_is_one_conditional_commit_refused_after_archive() -> Result {
    let provider = MemoryStream::default();
    let agent = create(&provider, id(1)).await?;
    let tail = inbox_tail(&provider, &agent).await?;
    assert_eq!(tail, 1);

    let request = send(&agent, tail, &user(), &cancel("c1"))?;
    let Submitted::Committed(first) = submit(&provider, request.clone()).await? else {
        panic!("send did not commit");
    };
    let Submitted::Committed(replay) = submit(&provider, request).await? else {
        panic!("exact replay did not commit");
    };
    assert_eq!(first.commit_id, replay.commit_id);
    assert_eq!(inbox_tail(&provider, &agent).await?, 2);

    let stale = send(&agent, 1, &user(), &cancel("c2"))?;
    assert!(matches!(
        submit(&provider, stale).await?,
        Submitted::InboxMoved(paths) if paths == vec![agent.inbox()?]
    ));

    let closing = archive(&agent, 2, &user(), &intent("a1"))?;
    assert!(matches!(
        submit(&provider, closing).await?,
        Submitted::Committed(_)
    ));
    let late = send(&agent, 3, &user(), &cancel("c3"))?;
    assert!(matches!(
        submit(&provider, late.clone()).await?,
        Submitted::RecipientClosed(paths) if paths == vec![agent.closed()?]
    ));
    assert_eq!(inbox_tail(&provider, &agent).await?, 3);

    // Negative control: without the close guard the same append lands.
    let mut unguarded = late;
    unguarded
        .conditions
        .retain(|condition| !matches!(condition, CommitCondition::Absent { .. }));
    assert!(matches!(
        provider.commit(unguarded).await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(inbox_tail(&provider, &agent).await?, 4);
    Ok(())
}

#[tokio::test]
async fn send_cannot_create_an_agent() -> Result {
    let provider = MemoryStream::default();
    let absent = AgentPath::root(&id(9));
    assert!(matches!(
        inbox_tail(&provider, &absent).await,
        Err(Error::NoSuchAgent)
    ));
    assert!(matches!(
        send(&absent, 0, &user(), &cancel("c1")),
        Err(Error::NoSuchAgent)
    ));
    // Negative control: `Tail{0}` matches absence, which is why it is refused.
    let mut forced = send(&absent, 1, &user(), &cancel("c1"))?;
    for condition in &mut forced.conditions {
        if let CommitCondition::Tail { expected, .. } = condition {
            *expected = 0;
        }
    }
    assert!(matches!(
        provider.commit(forced).await?,
        CommitOutcome::Committed(_)
    ));
    Ok(())
}

struct Seeded {
    provider: MemoryStream,
    parent: AgentPath,
    state: AgentState,
}

async fn seeded() -> Result<Seeded> {
    let provider = MemoryStream::default();
    let parent = create(&provider, id(1)).await?;
    let conversation = parent.conversation()?;
    let status = parent.status()?;
    let seeded = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: conversation.clone(),
                expected: 0,
            },
            CommitCondition::Tail {
                path: status.clone(),
                expected: 1,
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: conversation,
                records: vec![b"c1".to_vec().into(), b"c2".to_vec().into()],
            },
            CommitMutation::Append {
                path: status,
                records: vec![
                    StatusRecord {
                        phase: Phase::Running,
                        cursor: 1,
                    }
                    .encode()?
                    .into(),
                ],
            },
        ],
        idempotency_key: intent_key("seed", &[])?,
    };
    assert!(matches!(
        provider.commit(seeded).await?,
        CommitOutcome::Committed(_)
    ));
    let mut state = AgentState::adopted(parent.clone(), &adopted(id(1)));
    state.conversation_tail = 2;
    state.status_tail = 2;
    Ok(Seeded {
        provider,
        parent,
        state,
    })
}

/// Commits `name`'s spawn from `state`, optionally without the fork exclusion.
async fn spawn_child(
    provider: &MemoryStream,
    state: &AgentState,
    name: &str,
    exclude: bool,
) -> Result<Step> {
    let step = spawning(state.clone(), name);
    let mut request = lower(&step, 1, delivery_key(&step.state, name)?)?;
    for mutation in &mut request.mutations {
        if let CommitMutation::Fork { exclude: names, .. } = mutation
            && !exclude
        {
            names.clear();
        }
    }
    assert!(matches!(
        provider.commit(request).await?,
        CommitOutcome::Committed(_)
    ));
    Ok(step)
}

fn spawning(mut state: AgentState, name: &str) -> Step {
    state.conversation_tail += 1;
    state.status_tail += 1;
    Step {
        outs: vec![
            Out::Conversation(b"c3".to_vec()),
            Out::Spawn {
                intent: intent(name),
                brief: None,
                volumes: Vec::new(),
            },
        ],
        status: Some(StatusRecord {
            phase: Phase::Idle,
            cursor: 1,
        }),
        state,
        alarm: Alarm::Clear,
    }
}

#[tokio::test]
async fn spawn_forks_the_pre_step_subtree_in_one_commit() -> Result {
    let Seeded {
        provider,
        parent,
        state,
    } = seeded().await?;
    let step = spawning(state, "s1");
    let request = lower(&step, 1, delivery_key(&step.state, "t1")?)?;
    let Submitted::Committed(first) = submit(&provider, request.clone()).await? else {
        panic!("spawn did not commit");
    };
    let Submitted::Committed(replay) = submit(&provider, request).await? else {
        panic!("exact replay did not commit");
    };
    assert_eq!(first.commit_id, replay.commit_id);

    let child = parent.child(&AgentId::derive(&parent.id(), "s1"))?;
    let inbox = values(&provider, child.inbox()?).await?;
    assert_eq!(inbox.len(), 2);
    assert_eq!(inbox[0], values(&provider, parent.inbox()?).await?[0]);
    let InboxRecord::ForkAdopted(adopted) = InboxRecord::decode(&inbox[1])? else {
        panic!("child does not begin with ForkAdopted");
    };
    assert_eq!(adopted.agent, child.id());
    assert_eq!(adopted.parent.as_ref(), Some(&parent));
    assert_eq!((adopted.forked_at, adopted.conversation_tail), (1, 2));
    assert_eq!(
        values(&provider, child.conversation()?).await?,
        vec![b"c1".to_vec(), b"c2".to_vec()]
    );
    assert_eq!(values(&provider, parent.conversation()?).await?.len(), 3);
    // The child's first status rides with its membership: Idle at its ForkAdopted.
    let first = StatusRecord {
        phase: Phase::Idle,
        cursor: 1,
    };
    assert_eq!(
        values(&provider, child.status()?).await?,
        vec![first.encode()?]
    );
    assert_eq!(values(&provider, parent.status()?).await?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn spawn_is_all_or_none_and_exclusion_is_load_bearing() -> Result {
    let Seeded {
        provider,
        parent,
        mut state,
    } = seeded().await?;
    state.conversation_tail = 3;
    let stale = spawning(state.clone(), "s1");
    let request = lower(&stale, 1, delivery_key(&stale.state, "t1")?)?;
    assert!(matches!(
        submit(&provider, request).await?,
        Submitted::Conflict(_)
    ));
    let child = parent.child(&AgentId::derive(&parent.id(), "s1"))?;
    assert!(values(&provider, child.inbox()?).await?.is_empty());
    assert!(values(&provider, parent.status()?).await?.len() == 2);

    // Spawn s2 normally, then s3 from the advanced parent.
    state.conversation_tail = 2;
    let first = spawning(state, "s2");
    let request = lower(&first, 1, delivery_key(&first.state, "t2")?)?;
    assert!(matches!(
        submit(&provider, request).await?,
        Submitted::Committed(_)
    ));
    let s2 = AgentId::derive(&parent.id(), "s2");
    let s3 = spawn_child(&provider, &first.state, "s3", true).await?;
    let child = |name: &str| parent.child(&AgentId::derive(&parent.id(), name));
    assert!(
        values(&provider, child("s3")?.child(&s2)?.inbox()?)
            .await?
            .is_empty()
    );
    // Negative control: without the exclusion s4 inherits its siblings' subtrees.
    spawn_child(&provider, &s3.state, "s4", false).await?;
    assert!(
        !values(&provider, child("s4")?.child(&s2)?.inbox()?)
            .await?
            .is_empty()
    );
    Ok(())
}

#[test]
fn a_step_cannot_write_into_a_child_it_spawns() -> Result {
    let parent = AgentPath::root(&id(1));
    let state = AgentState::adopted(parent.clone(), &adopted(id(1)));
    let child = parent.child(&AgentId::derive(&parent.id(), "s1"))?;
    let step = Step {
        outs: vec![
            Out::Spawn {
                intent: intent("s1"),
                brief: None,
                volumes: Vec::new(),
            },
            Out::Send {
                to: child,
                record: cancel("c1"),
            },
        ],
        status: None,
        state,
        alarm: Alarm::Clear,
    };
    assert!(lower(&step, 1, delivery_key(&step.state, "t")?).is_err());
    Ok(())
}

struct Scripted(Mutex<VecDeque<TurnStep>>);

impl TurnDriver for Scripted {
    fn step<'a>(
        &'a self,
        _agent: &'a AgentState,
        _cursor: u64,
        _input: Option<TurnInput>,
    ) -> TurnFuture<'a> {
        let next = self.0.lock().expect("script lock").pop_front();
        Box::pin(async move { next.ok_or(Error::Record("script exhausted")) })
    }
}

fn script(steps: Vec<TurnStep>) -> Scripted {
    Scripted(Mutex::new(steps.into()))
}

fn child_of(parent: &AgentPath, byte: u8) -> Result<(AgentPath, AgentState)> {
    let path = parent.child(&id(byte))?;
    let mut adoption = adopted(id(byte));
    adoption.parent = Some(parent.clone());
    let state = AgentState::adopted(path.clone(), &adoption);
    Ok((path, state))
}

#[tokio::test]
async fn done_reports_to_the_parent_exactly_once() -> Result {
    let parent = AgentPath::root(&id(1));
    let (path, state) = child_of(&parent, 2)?;
    let driver = script(vec![TurnStep {
        reserved_children: 0,
        checkpoint: None,
        outs: vec![Out::Conversation(b"answer".to_vec())],
        wait: Wait::Done(AgentOutcome::Succeeded { result: None }),
    }]);
    let approval = InboxRecord::Approval(Approval { id: intent("q1") });
    let first = step(
        &driver,
        &path,
        Some(state),
        2,
        Wake::Inbox {
            sequence: 1,
            record: approval.clone(),
        },
    )
    .await?;
    let reports = |outs: &[Out]| {
        outs.iter()
            .filter(|out| matches!(out, Out::Send { to, record: InboxRecord::ChildDone(_) } if *to == parent))
            .count()
    };
    assert_eq!(reports(&first.outs), 1);
    assert_eq!(first.state.conversation_tail, 1);
    assert!(matches!(first.state.phase, Phase::Done { .. }));
    let again = step(
        &driver,
        &path,
        Some(first.state),
        3,
        Wake::Inbox {
            sequence: 2,
            record: approval,
        },
    )
    .await?;
    assert_eq!(reports(&again.outs), 0);
    assert!(again.status.is_none());
    Ok(())
}

#[tokio::test]
async fn a_state_too_large_to_checkpoint_fails_the_agent_instead_of_wedging_it() -> Result {
    // A turn checkpoint past the record limit would make every retry of this
    // step fail its commit; the step instead settles a visible failure.
    let parent = AgentPath::root(&id(1));
    let (path, state) = child_of(&parent, 2)?;
    let driver = script(vec![TurnStep {
        reserved_children: 0,
        checkpoint: Some(vec![7; MAX_CHECKPOINT_BYTES]),
        outs: vec![Out::Conversation(b"partial".to_vec())],
        wait: Wait::Continue,
    }]);
    let stepped = step(
        &driver,
        &path,
        Some(state),
        2,
        Wake::Inbox {
            sequence: 1,
            record: InboxRecord::Approval(Approval { id: intent("q1") }),
        },
    )
    .await?;
    let failed = AgentOutcome::Failed {
        message: OVERSIZED_STATE.into(),
    };
    assert_eq!(
        stepped.state.phase,
        Phase::Done {
            outcome: failed.clone()
        }
    );
    assert_eq!(stepped.state.turn, None);
    assert!(stepped.state.reported);
    assert!(stepped.outs.iter().any(|out| matches!(
        out,
        Out::Send { to, record: InboxRecord::ChildDone(done) } if *to == parent && done.outcome == failed
    )));
    // The turn's own effects still commit with the failure.
    assert!(
        stepped
            .outs
            .contains(&Out::Conversation(b"partial".to_vec()))
    );
    Ok(())
}

#[tokio::test]
async fn an_outcome_too_large_for_the_parents_record_settles_as_a_failure() -> Result {
    let parent = AgentPath::root(&id(1));
    let (path, state) = child_of(&parent, 2)?;
    let driver = script(vec![TurnStep {
        reserved_children: 0,
        checkpoint: None,
        outs: Vec::new(),
        wait: Wait::Done(AgentOutcome::Failed {
            message: "x".repeat(MAX_OUTCOME_BYTES),
        }),
    }]);
    let stepped = step(
        &driver,
        &path,
        Some(state),
        2,
        Wake::Inbox {
            sequence: 1,
            record: InboxRecord::Approval(Approval { id: intent("q1") }),
        },
    )
    .await?;
    let failed = AgentOutcome::Failed {
        message: OVERSIZED_OUTCOME.into(),
    };
    assert_eq!(
        stepped.state.phase,
        Phase::Done {
            outcome: failed.clone()
        }
    );
    // The report it sends encodes within the parent's inbox record limit.
    let Some(Out::Send { record, .. }) = stepped.outs.iter().find(|out| {
        matches!(
            out,
            Out::Send {
                record: InboxRecord::ChildDone(_),
                ..
            }
        )
    }) else {
        return Err("no ChildDone was sent".into());
    };
    assert!(record.encode().is_ok());
    Ok(())
}

#[tokio::test]
async fn a_full_window_overflows_into_the_inbox_and_never_drops_input() -> Result {
    let path = AgentPath::root(&id(1));
    let mut state = AgentState::adopted(path.clone(), &adopted(id(1)));
    state.phase = Phase::WaitingApproval { id: intent("q") };
    state.turn = Some(Vec::new());
    let request = |n: u64| -> Result<InboxRecord> {
        Ok(InboxRecord::ApprovalRequested(ApprovalRequested {
            child: path.child(&id(2))?,
            id: intent(&format!("r{n}")),
        }))
    };
    let held = |state: &AgentState| -> Vec<String> {
        state
            .queued
            .iter()
            .filter_map(|input| match input {
                TurnInput::ApprovalRequested { id, .. } => Some(id.as_str().to_owned()),
                _ => None,
            })
            .collect()
    };
    // Forty inputs arrive while the turn waits; the window holds the first 32.
    let idle = script(Vec::new());
    for sequence in 1..=40 {
        let wake = Wake::Inbox {
            sequence,
            record: request(sequence)?,
        };
        state = step(&idle, &path, Some(state), sequence + 1, wake)
            .await?
            .state;
    }
    assert_eq!(state.queued.len(), MAX_QUEUED);
    assert_eq!(state.overflow, Some(33));
    // The awaited response is taken out of order, and remembered as taken.
    let resumed = script(vec![TurnStep {
        reserved_children: 0,
        checkpoint: None,
        outs: Vec::new(),
        wait: Wait::Input,
    }]);
    let approval = InboxRecord::Approval(Approval { id: intent("q") });
    let wake = Wake::Inbox {
        sequence: 41,
        record: approval.clone(),
    };
    let after = step(&resumed, &path, Some(state), 42, wake).await?;
    state = after.state;
    assert_eq!(after.alarm, Alarm::At(CONTINUE_NOW));
    assert_eq!(state.taken, std::collections::BTreeSet::from([41]));
    assert_eq!(state.overflowed(42), Some((33, 1)));
    // As the window drains, read-back restores inbox order and skips the taken
    // response; every one of the forty inputs is still held exactly once.
    state.queued.drain(..8);
    let (from, limit) = state.overflowed(42).ok_or("nothing to read back")?;
    assert_eq!((from, limit), (33, 9));
    let mut inbox = (33..=40)
        .map(|n| Ok((n, request(n)?)))
        .collect::<Result<Vec<_>>>()?;
    inbox.push((41, approval));
    state.refill(inbox, 42);
    let expected: Vec<String> = (9..=40).map(|n| format!("r{n}")).collect();
    assert_eq!(held(&state), expected);
    assert_eq!((state.overflow, state.taken.len()), (None, 0));
    Ok(())
}

#[tokio::test]
async fn archive_cascades_in_bounded_batches_then_reports() -> Result {
    let root = AgentPath::root(&id(1));
    let (path, mut state) = child_of(&root, 2)?;
    for byte in 0..40 {
        state
            .open_children
            .insert(path.child(&AgentId::from_bytes([byte; 16]))?);
    }
    let driver = script(Vec::new());
    let archive_record = InboxRecord::Archive(Control {
        by: user(),
        id: intent("a1"),
    });
    let first = step(
        &driver,
        &path,
        Some(state),
        2,
        Wake::Inbox {
            sequence: 1,
            record: archive_record,
        },
    )
    .await?;
    let archives = |outs: &[Out]| {
        outs.iter()
            .filter(|out| matches!(out, Out::Archive { .. }))
            .count()
    };
    assert_eq!(archives(&first.outs), MAX_FANOUT);
    assert_eq!(first.alarm, Alarm::At(CONTINUE_NOW));
    assert!(!first.state.reported);
    let second = step(&driver, &path, Some(first.state), 2, Wake::Alarm).await?;
    assert_eq!(archives(&second.outs), 40 - MAX_FANOUT);
    assert_eq!(second.alarm, Alarm::Clear);
    assert!(second.state.reported);
    assert!(matches!(
        second.outs.last(),
        Some(Out::Send { to, record: InboxRecord::ChildDone(ChildDone { outcome: AgentOutcome::Archived, .. }) }) if *to == root
    ));
    let third = step(&driver, &path, Some(second.state), 2, Wake::Alarm).await?;
    assert!(third.outs.is_empty());
    Ok(())
}

#[tokio::test]
async fn first_input_must_adopt_this_agent() -> Result {
    let path = AgentPath::root(&id(1));
    let driver = script(Vec::new());
    let wrong = step(
        &driver,
        &path,
        None,
        1,
        Wake::Inbox {
            sequence: 0,
            record: InboxRecord::ForkAdopted(Box::new(adopted(id(2)))),
        },
    )
    .await;
    assert!(wrong.is_err());
    let not_adopted = step(
        &driver,
        &path,
        None,
        1,
        Wake::Inbox {
            sequence: 0,
            record: cancel("c1"),
        },
    )
    .await;
    assert!(not_adopted.is_err());
    let adopted_step = step(
        &driver,
        &path,
        None,
        1,
        Wake::Inbox {
            sequence: 0,
            record: InboxRecord::ForkAdopted(Box::new(adopted(id(1)))),
        },
    )
    .await?;
    assert_eq!(adopted_step.state.phase, Phase::Idle);
    assert!(adopted_step.outs.is_empty());
    Ok(())
}

#[tokio::test]
async fn ten_thousand_dormant_agents_cost_nothing_per_send() -> Result {
    const AGENTS: u32 = 10_000;
    let provider = MemoryStream::new(acyclic_stream::MemoryLimits::default());
    let mut paths = Vec::new();
    for index in 0..AGENTS {
        let mut bytes = [0; 16];
        bytes[..4].copy_from_slice(&index.to_le_bytes());
        paths.push(create(&provider, AgentId::from_bytes(bytes)).await?);
    }
    // A send names only its recipient: the same conditions, mutations and
    // committed facts whether the domain holds one agent or ten thousand, and
    // no other agent's streams are read or written.
    let mut shapes = Vec::new();
    for agent in [&paths[0], &paths[paths.len() / 2], &paths[paths.len() - 1]] {
        let tail = inbox_tail(&provider, agent).await?;
        let request = send(agent, tail, &user(), &cancel("c1"))?;
        let touched: Vec<_> = request
            .conditions
            .iter()
            .map(|condition| match condition {
                CommitCondition::Head { path, .. }
                | CommitCondition::Tail { path, .. }
                | CommitCondition::Absent { path } => path.clone(),
            })
            .collect();
        assert!(
            touched
                .iter()
                .all(|path| path.as_str().starts_with(agent.as_str()))
        );
        let Submitted::Committed(envelope) = submit(&provider, request.clone()).await? else {
            panic!("send to a dormant agent did not commit");
        };
        shapes.push((
            request.conditions.len(),
            request.mutations.len(),
            envelope.mutations.len(),
        ));
    }
    assert!(shapes.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(shapes[0], (2, 1, 1));
    for agent in [&paths[1], &paths[paths.len() - 2]] {
        assert_eq!(inbox_tail(&provider, agent).await?, 1);
    }
    Ok(())
}

#[test]
fn fuzz_corpus_seeds_decode_as_labelled() -> Result {
    let corpus =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/inbox_record");
    let mut seen = 0;
    for entry in std::fs::read_dir(corpus)? {
        let entry = entry?;
        let name = entry.file_name().into_string().map_err(|_| "corpus name")?;
        let data = std::fs::read(entry.path())?;
        let Some((selector, bytes)) = data.split_first() else {
            continue;
        };
        let accepted = match selector % 4 {
            0 => InboxRecord::decode(bytes)
                .map(|record| record.encode().map(drop))
                .is_ok(),
            1 => StatusRecord::decode(bytes).is_ok(),
            2 => Closed::decode(bytes).is_ok(),
            _ => AgentPath::try_from(String::from_utf8(bytes.to_vec())?).is_ok(),
        };
        let expected = !matches!(name.as_str(), "unknown-field" | "path-bad-id");
        assert_eq!(accepted, expected, "corpus seed {name}");
        seen += 1;
    }
    assert!(seen >= 13);
    Ok(())
}

/// Root at `MAX_OPEN_CHILDREN - 1` open children, idle.
fn one_below_the_child_limit() -> Result<(AgentPath, AgentState)> {
    let root = AgentPath::root(&id(1));
    let mut state = AgentState::adopted(root.clone(), &adopted(id(1)));
    for n in 0..MAX_OPEN_CHILDREN - 1 {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&(n as u64).to_le_bytes());
        state
            .open_children
            .insert(root.child(&AgentId::from_bytes(bytes))?);
    }
    Ok((root, state))
}

fn spawn_request(sequence: u64) -> Wake {
    Wake::Inbox {
        sequence,
        record: InboxRecord::Spawn(SpawnRequest {
            by: user(),
            id: intent(&format!("s{sequence}")),
            brief: "task".into(),
        }),
    }
}

fn spawns(outs: &[Out]) -> Vec<IntentId> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Spawn { intent, .. } => Some(intent.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn concurrent_spawn_requests_at_the_limit_spawn_one_and_refuse_the_other() -> Result {
    // Two clients' requests land at inbox 1 and 2; the parent's steps apply
    // them in order and each records its outcome in the committed state.
    let (root, state) = one_below_the_child_limit()?;
    let driver = script(Vec::new());
    let first = step(&driver, &root, Some(state), 2, spawn_request(1)).await?;
    let second = step(
        &driver,
        &root,
        Some(first.state.clone()),
        3,
        spawn_request(2),
    )
    .await?;
    let (one, child) = requested_child(&root, 1)?;
    let (two, _) = requested_child(&root, 2)?;
    assert_eq!(spawns(&first.outs), [one.clone()]);
    assert!(spawns(&second.outs).is_empty());
    assert_eq!(
        first.state.spawn_outcome(&one),
        SpawnOutcome::Spawned { child }
    );
    assert_eq!(
        second.state.spawn_outcome(&two),
        SpawnOutcome::Refused {
            reason: SpawnRefusal::Limit
        }
    );
    assert_eq!(second.state.open_children.len(), MAX_OPEN_CHILDREN);
    Ok(())
}

/// Spawns `spawn:tool` on its one turn, recording how many children it saw.
struct ToolSpawner(Mutex<Option<usize>>);

impl TurnDriver for ToolSpawner {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        _input: Option<TurnInput>,
    ) -> TurnFuture<'a> {
        *self.0.lock().expect("seen lock") = Some(agent.open_children.len());
        Box::pin(async move {
            Ok(TurnStep {
                reserved_children: 0,
                checkpoint: None,
                outs: vec![Out::Spawn {
                    intent: intent("spawn:tool"),
                    brief: None,
                    volumes: Vec::new(),
                }],
                wait: Wait::Input,
            })
        })
    }
}

#[tokio::test]
async fn a_client_spawn_counts_against_a_tool_spawn_in_the_same_step() -> Result {
    let (root, mut state) = one_below_the_child_limit()?;
    state
        .queued
        .push_back(TurnInput::Approval { id: intent("q") });
    let driver = ToolSpawner(Mutex::new(None));
    let stepped = step(&driver, &root, Some(state), 2, spawn_request(1)).await?;
    // The turn ran with the client's child already open, and the tool's
    // spawn past the limit is refused rather than failing every retry.
    assert_eq!(
        *driver.0.lock().expect("seen lock"),
        Some(MAX_OPEN_CHILDREN)
    );
    let (client, child) = requested_child(&root, 1)?;
    assert_eq!(spawns(&stepped.outs), [client.clone()]);
    assert_eq!(
        stepped.state.spawn_outcome(&client),
        SpawnOutcome::Spawned { child }
    );
    assert_eq!(
        stepped.state.spawn_outcome(&intent("spawn:tool")),
        SpawnOutcome::Refused {
            reason: SpawnRefusal::Limit
        }
    );
    assert_eq!(stepped.state.open_children.len(), MAX_OPEN_CHILDREN);
    Ok(())
}

#[tokio::test]
async fn a_paused_turns_recorded_spawn_keeps_its_slot_from_a_client_spawn() -> Result {
    // The turn's tool recorded a spawn into the last slot, then paused.
    let (root, mut state) = one_below_the_child_limit()?;
    state.turn = Some(Vec::new());
    state.phase = Phase::WaitingApproval { id: intent("q") };
    state.reserved_children = 1;
    let driver = script(vec![TurnStep {
        reserved_children: 0,
        checkpoint: None,
        outs: vec![Out::Spawn {
            intent: intent("spawn:tool"),
            brief: None,
            volumes: Vec::new(),
        }],
        wait: Wait::Input,
    }]);
    // A client's spawn meanwhile is refused rather than taking that slot.
    let requested = step(&driver, &root, Some(state), 2, spawn_request(1)).await?;
    let (client, _) = requested_child(&root, 1)?;
    assert!(spawns(&requested.outs).is_empty());
    assert_eq!(
        requested.state.spawn_outcome(&client),
        SpawnOutcome::Refused {
            reason: SpawnRefusal::Limit
        }
    );
    // The resumed turn's spawn opens the child its tool reported.
    let resumed = step(
        &driver,
        &root,
        Some(requested.state),
        3,
        Wake::Inbox {
            sequence: 2,
            record: InboxRecord::Approval(Approval { id: intent("q") }),
        },
    )
    .await?;
    assert_eq!(spawns(&resumed.outs), [intent("spawn:tool")]);
    assert_eq!(resumed.state.open_children.len(), MAX_OPEN_CHILDREN);
    assert_eq!(resumed.state.reserved_children, 0);
    Ok(())
}

#[tokio::test]
async fn a_driver_effect_no_record_can_carry_fails_the_turn_instead_of_wedging_it() -> Result {
    let parent = AgentPath::root(&id(1));
    let (path, state) = child_of(&parent, 2)?;
    let oversized = vec![b'x'; MAX_RECORD_BYTES + 1];
    let driver = script(vec![TurnStep {
        reserved_children: 0,
        checkpoint: None,
        outs: vec![
            Out::Conversation(oversized),
            Out::Conversation(b"fits".to_vec()),
        ],
        wait: Wait::Input,
    }]);
    let stepped = step(
        &driver,
        &path,
        Some(state),
        2,
        Wake::Inbox {
            sequence: 1,
            record: InboxRecord::Approval(Approval { id: intent("q1") }),
        },
    )
    .await?;
    let failed = AgentOutcome::Failed {
        message: INADMISSIBLE_EFFECT.into(),
    };
    assert_eq!(
        stepped.state.phase,
        Phase::Done {
            outcome: failed.clone()
        }
    );
    assert!(
        !stepped
            .outs
            .iter()
            .any(|out| matches!(out, Out::Conversation(_)))
    );
    assert!(stepped.outs.iter().any(|out| matches!(
        out,
        Out::Send { to, record: InboxRecord::ChildDone(done) } if *to == parent && done.outcome == failed
    )));
    Ok(())
}
