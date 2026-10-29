//! Agent model commit shapes and step protocol against the in-memory Streams provider.

use acyclic_harness_core::agent::*;
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream, ReadRequest,
    StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
};
use futures::TryStreamExt;
use std::{collections::VecDeque, sync::Mutex};

#[path = "agent_model/awaiting.rs"]
mod awaiting;
#[path = "agent_model/corpus.rs"]
mod corpus;
#[path = "agent_model/driver.rs"]
mod driver;
#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "agent_model/hosting.rs"]
mod hosting;
#[path = "agent_model/reply.rs"]
mod reply;
#[path = "agent_model/runtime.rs"]
mod runtime;
#[path = "agent_model/slot_schedules.rs"]
mod slot_schedules;
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
        checkpoint: None,
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
            key: None,
            path,
            from: 0,
            limit: u32::try_from(tail)?,
            at_least: Default::default(),
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
fn layout_round_trips_and_rejects_noncanonical_paths() -> Result {
    let root = AgentPath::root(&id(1));
    let child = root.child(&id(2))?;
    assert_eq!(child.parent().as_ref(), Some(&root));
    assert_eq!(child.id(), id(2));
    assert_eq!(AgentPath::try_from(child.as_str().to_owned())?, child);
    assert!(AgentPath::try_from(format!("{}/status", root.as_str())).is_err());
    assert!(AgentId::try_from("A".repeat(32)).is_err());
    Ok(())
}

#[test]
fn records_are_canonical_and_validate_their_structure() -> Result {
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
    Ok(())
}

#[tokio::test]
async fn send_is_one_conditional_commit_refused_after_archive() -> Result {
    let provider = MemoryStream::default();
    let agent = create(&provider, id(1)).await?;
    assert_eq!(inbox_tail(&provider, &agent).await?, 1);

    let request = send(&agent, &user(), &cancel("c1"))?;
    let Submitted::Committed(first) = submit(&provider, request.clone()).await? else {
        panic!("send did not commit");
    };
    let Submitted::Committed(replay) = submit(&provider, request).await? else {
        panic!("exact replay did not commit");
    };
    assert_eq!(first.commit_id, replay.commit_id);
    assert_eq!(inbox_tail(&provider, &agent).await?, 2);

    let closing = archive(&agent, &user(), &intent("a1"))?;
    assert!(matches!(
        submit(&provider, closing).await?,
        Submitted::Committed(_)
    ));
    let late = send(&agent, &user(), &cancel("c3"))?;
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
    let request = send(&absent, &user(), &cancel("c1"))?;
    assert!(matches!(
        submit(&provider, request.clone()).await?,
        Submitted::NoSuchAgent(paths) if paths == vec![absent.inbox()?]
    ));
    // Negative control: without `Present` the same send creates the inbox.
    let mut forced = request;
    forced
        .conditions
        .retain(|condition| !matches!(condition, CommitCondition::Present { .. }));
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
        deadline_unix_millis: None,
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
                        conversation: 2,
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
    let mut state = AgentState::adopted(parent.clone(), &adopted(id(1))).expect("adopts");
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
            conversation: 3,
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
        conversation: 2,
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
    let state = AgentState::adopted(parent.clone(), &adopted(id(1))).expect("adopts");
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
        _input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
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
    let state = AgentState::adopted(path.clone(), &adoption).expect("adopts");
    Ok((path, state))
}

#[tokio::test]
async fn a_checkpoint_is_indexed_with_its_append_and_inherited_at_a_spawn() -> Result {
    let parent = AgentPath::root(&id(1));
    let (path, mut state) = child_of(&parent, 2)?;
    state.conversation_tail = 100;
    let entry = |text: &str| Out::Conversation(text.as_bytes().to_vec());
    let driver = script(vec![
        TurnStep {
            checkpoint: None,
            outs: vec![entry("before"), entry("summary"), Out::Checkpoint],
            wait: Wait::Input,
            drops: None,
        },
        TurnStep {
            checkpoint: None,
            outs: vec![entry("later")],
            wait: Wait::Input,
            drops: None,
        },
    ]);
    let wake = |sequence| Wake::Inbox {
        sequence,
        record: InboxRecord::Approval(Approval { id: intent("q1") }),
    };
    let cancel = acyclic_actors::guest::CancellationToken::new;
    let first = step(&driver, &path, Some(state), 2, wake(1), cancel()).await?;
    // The summary is the second append after tail 100.
    assert_eq!(first.state.checkpoint, Some(101));
    assert_eq!(first.state.conversation_tail, 102);
    let later = step(&driver, &path, Some(first.state), 3, wake(2), cancel()).await?;
    // Later turns keep the index, however many there are.
    assert_eq!(later.state.checkpoint, Some(101));
    let spawned = spawning(later.state.clone(), "s1");
    let request = lower(&spawned, 3, delivery_key(&spawned.state, "s1")?)?;
    let adopted = request
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            CommitMutation::Fork { records, .. } => records.first(),
            _ => None,
        })
        .map(|record| InboxRecord::decode(&record.value))
        .transpose()?;
    let Some(InboxRecord::ForkAdopted(adopted)) = adopted else {
        panic!("spawn carries no adoption");
    };
    assert_eq!(adopted.checkpoint, Some(101));
    assert_eq!(
        AgentState::adopted(path.child(&id(9))?, &adopted)
            .expect("adopts")
            .checkpoint,
        Some(101)
    );
    Ok(())
}

#[tokio::test]
async fn awaited_children_are_scoped_to_their_parent() -> Result {
    use acyclic_actors::guest::CancellationToken;
    use std::collections::BTreeSet;

    let path = AgentPath::root(&id(1));
    let child = path.child(&id(2))?;
    let foreign = AgentPath::root(&id(3)).child(&id(2))?;
    for any in [false, true] {
        for done in [false, true] {
            let mut state = AgentState::adopted(path.clone(), &adopted(id(1)))?;
            state.open_children.insert(child.clone()).await?;
            let driver = script(vec![
                TurnStep {
                    checkpoint: Some(b"waiting".to_vec()),
                    outs: Vec::new(),
                    wait: Wait::Children {
                        children: BTreeSet::from([child.clone()]),
                        any,
                    },
                    drops: None,
                },
                TurnStep {
                    checkpoint: None,
                    outs: Vec::new(),
                    wait: Wait::Input,
                    drops: None,
                },
            ]);
            let waiting = step(
                &driver,
                &path,
                Some(state),
                2,
                Wake::Inbox {
                    sequence: 1,
                    record: InboxRecord::Approval(Approval {
                        id: intent("start"),
                    }),
                },
                CancellationToken::new(),
            )
            .await?;
            assert_eq!(waiting.state.phase, Phase::WaitingChildren { any });
            let completion = |child| {
                if done {
                    InboxRecord::ChildDone(ChildDone {
                        child,
                        outcome: AgentOutcome::Succeeded { result: None },
                    })
                } else {
                    InboxRecord::ChildSettled(ChildSettled { child })
                }
            };
            let still_waiting = step(
                &driver,
                &path,
                Some(waiting.state),
                3,
                Wake::Inbox {
                    sequence: 2,
                    record: completion(foreign.clone()),
                },
                CancellationToken::new(),
            )
            .await?;
            assert_eq!(still_waiting.state.phase, Phase::WaitingChildren { any });
            assert!(still_waiting.state.awaiting.contains(&child).await?);
            assert!(still_waiting.state.open_children.contains(&child).await?);
            let settled = step(
                &driver,
                &path,
                Some(still_waiting.state),
                4,
                Wake::Inbox {
                    sequence: 3,
                    record: completion(child.clone()),
                },
                CancellationToken::new(),
            )
            .await?;
            assert_eq!(settled.state.phase, Phase::Idle);
            assert!(settled.state.awaiting.is_empty());
            assert_eq!(settled.state.open_children.contains(&child).await?, !done);
        }
        // A foreign path is not an open child even when its final ID matches.
        let mut state = AgentState::adopted(path.clone(), &adopted(id(1)))?;
        state.open_children.insert(child.clone()).await?;
        let driver = script(vec![TurnStep {
            checkpoint: Some(b"foreign".to_vec()),
            outs: Vec::new(),
            wait: Wait::Children {
                children: BTreeSet::from([foreign.clone()]),
                any,
            },
            drops: None,
        }]);
        let continued = step(
            &driver,
            &path,
            Some(state),
            2,
            Wake::Inbox {
                sequence: 1,
                record: InboxRecord::Approval(Approval {
                    id: intent("start"),
                }),
            },
            CancellationToken::new(),
        )
        .await?;
        assert_eq!(continued.state.phase, Phase::Running);
        assert!(continued.state.awaiting.is_empty());
        assert!(!continued.outs.iter().any(|out| matches!(
            out,
            Out::Send {
                record: InboxRecord::Awaited,
                ..
            }
        )));
    }
    Ok(())
}

#[tokio::test]
async fn done_reports_to_the_parent_exactly_once() -> Result {
    let parent = AgentPath::root(&id(1));
    let (path, state) = child_of(&parent, 2)?;
    let driver = script(vec![TurnStep {
        checkpoint: None,
        outs: vec![Out::Conversation(b"answer".to_vec())],
        wait: Wait::Done(AgentOutcome::Succeeded { result: None }),
        drops: None,
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
        acyclic_actors::guest::CancellationToken::new(),
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
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert_eq!(reports(&again.outs), 0);
    assert!(again.status.is_none());
    Ok(())
}

#[tokio::test]
async fn a_full_window_overflows_into_the_inbox_and_never_drops_input() -> Result {
    let path = AgentPath::root(&id(1));
    let mut state = AgentState::adopted(path.clone(), &adopted(id(1))).expect("adopts");
    state.phase = Phase::WaitingApproval { id: intent("q") };
    state.turn = Some(Progress {
        inbox: 0,
        checkpoint: Vec::new(),
    });
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
            .filter_map(|queued| match &queued.input {
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
        state = step(
            &idle,
            &path,
            Some(state),
            sequence + 1,
            wake,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?
        .state;
    }
    assert_eq!(state.queued.len(), MAX_QUEUED);
    assert_eq!(state.overflow, Some(33));
    // The awaited response is taken out of order, and remembered as taken.
    let resumed = script(vec![TurnStep {
        checkpoint: None,
        outs: Vec::new(),
        wait: Wait::Input,
        drops: None,
    }]);
    let approval = InboxRecord::Approval(Approval { id: intent("q") });
    let wake = Wake::Inbox {
        sequence: 41,
        record: approval.clone(),
    };
    let after = step(
        &resumed,
        &path,
        Some(state),
        42,
        wake,
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
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
            .insert(path.child(&AgentId::from_bytes([byte; 16]))?)
            .await?;
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
        acyclic_actors::guest::CancellationToken::new(),
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
    let second = step(
        &driver,
        &path,
        Some(first.state),
        2,
        Wake::Alarm,
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert_eq!(archives(&second.outs), 40 - MAX_FANOUT);
    assert_eq!(second.alarm, Alarm::Clear);
    assert!(second.state.reported);
    assert!(matches!(
        second.outs.last(),
        Some(Out::Send { to, record: InboxRecord::ChildDone(ChildDone { outcome: AgentOutcome::Archived, .. }) }) if *to == root
    ));
    let third = step(
        &driver,
        &path,
        Some(second.state),
        2,
        Wake::Alarm,
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
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
        acyclic_actors::guest::CancellationToken::new(),
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
        acyclic_actors::guest::CancellationToken::new(),
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
        acyclic_actors::guest::CancellationToken::new(),
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
        let request = send(agent, &user(), &cancel("c1"))?;
        let touched: Vec<_> = request
            .conditions
            .iter()
            .map(|condition| match condition {
                CommitCondition::Head { path, .. }
                | CommitCondition::Tail { path, .. }
                | CommitCondition::Present { path }
                | CommitCondition::Absent { path } => path.clone(),
            })
            .collect();
        assert!(
            touched
                .iter()
                .all(|path| path.as_str().starts_with(agent.as_str()))
        );
        let Submitted::Committed(receipt) = submit(&provider, request.clone()).await? else {
            panic!("send to a dormant agent did not commit");
        };
        shapes.push((
            request.conditions.len(),
            request.mutations.len(),
            receipt.appends.len() + receipt.forks.len(),
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
    let generated = corpus::seeds()?;
    let corpus =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/inbox_record");
    let mut seen = 0;
    for entry in std::fs::read_dir(corpus)? {
        let entry = entry?;
        let name = entry.file_name().into_string().map_err(|_| "corpus name")?;
        let data = std::fs::read(entry.path())?;
        assert_eq!(
            Some(&data),
            generated.get(name.as_str()),
            "stale corpus seed {name}; recovery: bun scripts/generate-contracts.mjs write harness"
        );
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
    assert_eq!(seen, generated.len());
    Ok(())
}

/// An idle root populated independently of any product cap.
async fn populated_parent() -> Result<(AgentPath, AgentState)> {
    let root = AgentPath::root(&id(1));
    let mut state = AgentState::adopted(root.clone(), &adopted(id(1))).expect("adopts");
    for n in 0..300 {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&(n as u64).to_le_bytes());
        state
            .open_children
            .insert(root.child(&AgentId::from_bytes(bytes))?)
            .await?;
    }
    Ok((root, state))
}

fn spawn_request(sequence: u64) -> Wake {
    Wake::Inbox {
        sequence,
        record: InboxRecord::Spawn(SpawnRequest {
            by: user(),
            id: intent(&format!("s{sequence}")),
            brief: Box::new(driver::file("task").expect("file")),
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

/// Spawns `spawn:tool` on its one turn, recording how many children it saw.
struct ToolSpawner(Mutex<Option<usize>>);

impl TurnDriver for ToolSpawner {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        _input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        *self.0.lock().expect("seen lock") = Some(agent.open_children.len());
        Box::pin(async move {
            Ok(TurnStep {
                checkpoint: None,
                outs: vec![Out::Spawn {
                    intent: intent("spawn:tool"),
                    brief: None,
                    volumes: Vec::new(),
                }],
                wait: Wait::Input,
                drops: None,
            })
        })
    }
}

#[tokio::test]
async fn a_client_spawn_commits_before_the_next_turn_reaches_or_counts_it() -> Result {
    let (root, mut state) = populated_parent().await?;
    state.queued.push_back(Queued {
        inbox: 1,
        input: TurnInput::Approval { id: intent("q") },
    });
    let driver = ToolSpawner(Mutex::new(None));
    // The spawn's step carries only the spawn: no turn runs while its child
    // is being created, so none can write to it in the creating commit.
    let requested = step(
        &driver,
        &root,
        Some(state),
        2,
        spawn_request(1),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert_eq!(*driver.0.lock().expect("seen lock"), None);
    let (client, child) = requested_child(&root, 1)?;
    assert_eq!(spawns(&requested.outs), [client.clone()]);
    assert_eq!(requested.outs.len(), 1);
    assert_eq!(
        requested.state.spawn_outcome(&client).await?,
        SpawnOutcome::Spawned {
            child: child.clone()
        }
    );
    // The queued turn continues at once, on the next step.
    assert_eq!(requested.alarm, Alarm::At(CONTINUE_NOW));
    let stepped = step(
        &driver,
        &root,
        Some(requested.state),
        2,
        Wake::Alarm,
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    // It runs with the client's child open and reachable, and the tool's
    // spawn also opens a child.
    assert_eq!(*driver.0.lock().expect("seen lock"), Some(301));
    assert_eq!(spawns(&stepped.outs), [intent("spawn:tool")]);
    assert_eq!(
        stepped.state.spawn_outcome(&client).await?,
        SpawnOutcome::Spawned { child }
    );
    assert_eq!(
        stepped.state.spawn_outcome(&intent("spawn:tool")).await?,
        SpawnOutcome::Spawned {
            child: root.child(&AgentId::derive(&root.id(), "spawn:tool"))?
        }
    );
    assert_eq!(stepped.state.open_children.len(), 302);
    Ok(())
}
