#![cfg(not(target_arch = "wasm32"))]

//! A waiting turn holds no delivery: parents waiting for their children,
//! more of them than the runtime admits at once, all complete. The negative
//! control holds the parent's delivery while it waits, as a blocking `wait`
//! would, and deadlocks the same schedule.

use super::{Result, adopted, id, intent, user, values};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    local::{RuntimeLimits, SystemClock},
};
use acyclic_harness_core::agent::*;
use acyclic_stream::{FollowRequest, MemoryStream, StreamProvider as _, records_only};
use futures::StreamExt as _;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

/// Deliveries the runtime admits at once.
const ACTIVE: usize = 2;
/// Waiting parents: twice what the runtime admits.
const PARENTS: u8 = 4;

/// Each root spawns child `c`, then waits for it; `hold` waits inside the
/// delivery, following the child's conversation until it ran its turn.
struct Waiter {
    provider: Arc<MemoryStream>,
    hold: bool,
    /// One permit per parent that reached its wait.
    waiting: Arc<Semaphore>,
    /// The parents that reached it, in order.
    waiters: Arc<Mutex<Vec<AgentPath>>>,
    /// One permit per parent whose wait completed.
    done: Arc<Semaphore>,
}

fn child(parent: &AgentPath) -> std::result::Result<AgentPath, Error> {
    parent.child(&AgentId::derive(&parent.id(), "c"))
}

fn step(checkpoint: &[u8], outs: Vec<Out>, wait: Wait) -> TurnStep {
    TurnStep {
        checkpoint: Some(checkpoint.to_vec()),
        outs,
        wait,
        drops: None,
    }
}

impl Waiter {
    /// Follows the original child answer, including creation of its stream.
    async fn ran(&self, child: &AgentPath) -> std::result::Result<(), Error> {
        let mut turns = records_only(
            self.provider
                .follow(FollowRequest {
                    when_absent: acyclic_stream::WhenAbsent::Wait,
                    ..FollowRequest::new(child.conversation()?, 0)
                })
                .await
                .map_err(Error::Stream)?,
        );
        turns
            .next()
            .await
            .ok_or(Error::Record("child conversation ended"))?
            .map_err(Error::Stream)?;
        Ok(())
    }
}

impl TurnDriver for Waiter {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        _input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let settled = || TurnStep {
                checkpoint: None,
                outs: Vec::new(),
                wait: Wait::Input,
                drops: None,
            };
            if agent.parent.is_some() {
                // A child's turn appends one entry, which `hold` follows.
                return Ok(TurnStep {
                    outs: vec![Out::Conversation(b"answer".to_vec())],
                    ..settled()
                });
            }
            let resumed = agent.turn.as_ref().map(|turn| turn.checkpoint.as_slice());
            match resumed {
                None => Ok(step(
                    b"spawned",
                    vec![Out::Spawn {
                        profile: None,
                        history: None,
                        intent: intent("c"),
                        brief: None,
                        volumes: Vec::new(),
                    }],
                    Wait::Continue,
                )),
                Some(b"spawned") => {
                    let child = child(&agent.path)?;
                    self.waiters
                        .lock()
                        .map_err(|_| Error::Record("waiters"))?
                        .push(agent.path.clone());
                    self.waiting.add_permits(1);
                    if self.hold {
                        self.ran(&child).await?;
                        self.done.add_permits(1);
                        return Ok(settled());
                    }
                    Ok(step(
                        b"waiting",
                        Vec::new(),
                        Wait::Until(Until::all(BTreeSet::from([child]))),
                    ))
                }
                Some(_) => {
                    self.done.add_permits(1);
                    Ok(settled())
                }
            }
        })
    }
}

/// Runs every parent's wait under `active` admission: `None` once all
/// completed, else the runtime's original delivery refusal. The explicitly
/// selected manifest deadline fails a held delivery once no work can progress.
async fn stuck(hold: bool, active: usize) -> Result<Option<String>> {
    let provider = Arc::new(MemoryStream::default());
    let waiting = Arc::new(Semaphore::new(0));
    let waiters = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(Semaphore::new(0));
    let host = AgentHost::new(
        provider.clone(),
        Waiter {
            provider: provider.clone(),
            hold,
            waiting: waiting.clone(),
            waiters: waiters.clone(),
            done: done.clone(),
        },
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?
    .with_limits(RuntimeLimits::new(active, 64)?);
    for n in 0..PARENTS {
        let root = host
            .create(&intent(&format!("create-{n}")), adopted(id(n + 1)))
            .await?;
        let start = InboxRecord::ApprovalRequested(ApprovalRequested {
            child: child(&root)?,
            id: intent(&format!("start-{n}")),
        });
        host.send(&root, &user(), &start).await?;
    }
    let schedule = async {
        // Every admitted delivery is a waiting parent's before any child is
        // given its turn: a wait that held its delivery would hold them all.
        let admitted = active.min(usize::from(PARENTS));
        waiting
            .acquire_many(u32::try_from(admitted)?)
            .await?
            .forget();
        let first: Vec<AgentPath> = waiters.lock().map_err(|_| "waiters")?.clone();
        for (n, parent) in first.iter().enumerate() {
            let turn = InboxRecord::ApprovalRequested(ApprovalRequested {
                child: parent.clone(),
                id: intent(&format!("turn-{n}")),
            });
            host.send(&child(parent)?, &user(), &turn).await?;
        }
        done.acquire_many(u32::from(PARENTS)).await?.forget();
        Ok::<_, Box<dyn std::error::Error>>(None)
    };
    let failure = Mutex::new(None);
    let failed = Semaphore::new(0);
    let report = |actor: &ActorReference, error: &acyclic_actors::local::Error| {
        if let Ok(mut failure) = failure.lock() {
            failure.get_or_insert_with(|| format!("{actor:?}: {error}"));
        }
        failed.add_permits(1);
    };
    tokio::select! {
        stopped = host.run(report) => Err(format!("runtime stopped: {stopped:?}").into()),
        completed = schedule => completed,
        _ = failed.acquire() => Ok(failure.lock().map_err(|_| "failure")?.clone()),
    }
}

#[tokio::test(start_paused = true)]
async fn waiting_parents_beyond_admission_all_complete() -> Result {
    assert_eq!(stuck(false, ACTIVE).await?, None);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_wait_holding_its_delivery_deadlocks_the_same_schedule() -> Result {
    // Both admitted deliveries wait for children that cannot be admitted,
    // until the runtime fails them for never returning.
    let stuck_by = stuck(true, ACTIVE).await?;
    assert!(
        stuck_by
            .as_deref()
            .is_some_and(|reason| reason.contains("exceeded its manifest timeout")),
        "{stuck_by:?}"
    );
    // The deadlock is admission's: with a slot for every agent, the same
    // holding wait completes.
    assert_eq!(stuck(true, 2 * usize::from(PARENTS)).await?, None);
    Ok(())
}

/// A completion burst is consumed without advancing the clock. Withholding
/// the last child is the negative control: unrelated deliveries cannot wake
/// the root; its final completion must resume it in that very delivery.
#[tokio::test]
async fn completion_burst_wakes_root_on_last_delivery() -> Result {
    use acyclic_actors::local::ControlledClock;

    const CHILDREN: usize = 18;
    const NOW: u64 = 5_000;
    const TIMEOUT: u64 = 200_000;

    struct Burst(Arc<Mutex<Vec<u64>>>);
    impl TurnDriver for Burst {
        fn step<'a>(
            &'a self,
            agent: &'a AgentState,
            cursor: u64,
            _input: Option<Queued>,
            _cancellation: acyclic_actors::guest::CancellationToken,
        ) -> TurnFuture<'a> {
            Box::pin(async move {
                let checkpoint = agent.turn.as_ref().map(|turn| turn.checkpoint.as_slice());
                if agent.parent.is_some() {
                    return Ok(match checkpoint {
                        None => step(
                            b"working",
                            Vec::new(),
                            Wait::Approval {
                                id: intent("finish"),
                                binding: None,
                            },
                        ),
                        Some(_) => step(
                            b"",
                            vec![Out::Conversation(b"answer".to_vec())],
                            Wait::Input,
                        ),
                    });
                }
                Ok(match checkpoint {
                    None => step(
                        b"spawned",
                        (0..CHILDREN)
                            .map(|n| Out::Spawn {
                                profile: None,
                                history: None,
                                intent: intent(&format!("child-{n}")),
                                brief: None,
                                volumes: Vec::new(),
                            })
                            .collect(),
                        Wait::Approval {
                            id: intent("begin-wait"),
                            binding: None,
                        },
                    ),
                    Some(b"spawned") => step(
                        b"waiting",
                        Vec::new(),
                        Wait::Until(Until::all(agent.open_children.clone()).within(Some(TIMEOUT))),
                    ),
                    Some(_) => {
                        self.0
                            .lock()
                            .map_err(|_| Error::Record("resumes"))?
                            .push(cursor);
                        step(b"", Vec::new(), Wait::Input)
                    }
                })
            })
        }
    }

    struct CommitClock;
    impl acyclic_stream::UnixMillisClock for CommitClock {
        fn now_unix_millis(&self) -> u64 {
            NOW
        }
    }
    let provider = Arc::new(MemoryStream::new_with_clock(
        Default::default(),
        Arc::new(CommitClock),
    ));
    let resumed = Arc::new(Mutex::new(Vec::new()));
    let (clock, _time) = ControlledClock::new(NOW);
    let host = AgentHost::new(
        provider.clone(),
        Burst(resumed.clone()),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(clock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    host.drain_event(&root).await?;
    let start = InboxRecord::ApprovalRequested(ApprovalRequested {
        child: root.clone(),
        id: intent("start"),
    });
    host.send(&root, &user(), &start).await?;
    host.drain_event(&root).await?;
    let children = host.state(&root).await?.open_children;
    assert_eq!(children.len(), CHILDREN);
    for child in &children {
        host.send(child, &user(), &start).await?;
        host.drain_event(child).await?;
    }
    let approval = |name| InboxRecord::Approval(Approval { id: intent(name) });
    host.send(&root, &user(), &approval("begin-wait")).await?;
    assert_eq!(host.drain_event(&root).await?, Some(NOW + TIMEOUT));
    for child in &children {
        // Consume the original Awaited registration while each child works.
        host.drain_event(child).await?;
    }
    let before = inbox_tail(&*provider, &root).await?;
    let mut completions = records_only(
        provider
            .follow(FollowRequest::new(root.inbox()?, before))
            .await?,
    );
    let last = children.last().ok_or("missing last child")?;
    for child in children.iter().filter(|child| *child != last) {
        // The burst is queued at NOW, without delivering the root meanwhile.
        host.send(child, &user(), &approval("finish")).await?;
        host.drain_event(child).await?;
    }
    // Negative control: N-1 completions have arrived, but the condition is
    // still false. The root remains parked at the original deadline.
    assert_eq!(host.drain_event(&root).await?, Some(NOW + TIMEOUT));
    assert!(resumed.lock().map_err(|_| "resumes")?.is_empty());
    assert_eq!(host.state(&root).await?.need, 1);
    for bytes in values(&*provider, root.status()?).await? {
        assert!(
            StatusRecord::decode(&bytes)?.child_wakes.is_empty(),
            "a partial completion burst must not report a returned wait"
        );
    }
    // The final child finishes at the very same timestamp. Its one delivery
    // must wake the root; no alarm or additional input may be necessary.
    host.send(last, &user(), &approval("finish")).await?;
    host.drain_event(last).await?;
    for child in &children {
        let record = completions
            .next()
            .await
            .ok_or("completion follow ended")??;
        assert_eq!(record.committed_at_micros, NOW * 1_000);
        assert_eq!(
            InboxRecord::decode(&record.value)?,
            InboxRecord::ChildSettled(ChildSettled {
                child: child.clone()
            })
        );
    }
    assert_eq!(
        inbox_tail(&*provider, &root).await?,
        before + CHILDREN as u64
    );
    assert_eq!(host.drain_event(&root).await?, None);
    assert_eq!(
        *resumed.lock().map_err(|_| "resumes")?,
        vec![before + CHILDREN as u64],
        "root must resume in the delivery consuming the last completion, before its alarm"
    );
    assert_eq!(host.state(&root).await?.phase, Phase::Idle);
    assert!(host.state(&root).await?.child_settlements.is_empty());
    let statuses = values(&*provider, root.status()?)
        .await?
        .iter()
        .map(|bytes| StatusRecord::decode(bytes))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let measured: Vec<_> = statuses
        .iter()
        .filter(|status| !status.child_wakes.is_empty())
        .collect();
    assert_eq!(measured.len(), 1, "one telemetry record per returned wait");
    let wake = measured[0];
    assert_eq!(wake.cursor, before + CHILDREN as u64);
    assert_eq!(
        wake.child_wakes.len(),
        CHILDREN,
        "every child must have a wake measurement"
    );
    assert_eq!(
        wake.child_wakes
            .iter()
            .map(|sample| sample.child.clone())
            .collect::<BTreeSet<_>>(),
        children.iter().map(AgentPath::id).collect()
    );
    for sample in &wake.child_wakes {
        assert_eq!(sample.settled_at_micros, NOW * 1_000);
        assert_eq!(sample.woke_at_micros, NOW * 1_000);
        assert!(
            sample
                .woke_at_micros
                .checked_sub(sample.settled_at_micros)
                .is_some(),
            "settlement-to-wake latency must be non-negative"
        );
    }
    Ok(())
}
