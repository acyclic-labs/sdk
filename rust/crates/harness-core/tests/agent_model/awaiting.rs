//! A waiting turn holds no delivery: parents waiting for their children,
//! more of them than the runtime admits at once, all complete. The negative
//! control holds the parent's delivery while it waits, as a blocking `wait`
//! would, and deadlocks the same schedule.

use super::{Result, adopted, id, intent, user};
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
    time::Duration,
};
use tokio::sync::Semaphore;

/// Deliveries the runtime admits at once.
const ACTIVE: usize = 2;
/// Waiting parents: twice what the runtime admits.
const PARENTS: u8 = 4;

/// Each root spawns child `c`, then waits for it; `hold` waits inside the
/// delivery, following the child's status until it ran its turn.
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
        reserved_children: 0,
        checkpoint: Some(checkpoint.to_vec()),
        outs,
        wait,
        drops: None,
    }
}

impl Waiter {
    /// Follows `child`'s status until a step of it appended its turn.
    async fn ran(&self, child: &AgentPath) -> std::result::Result<(), Error> {
        let mut statuses = records_only(
            self.provider
                .follow(FollowRequest::new(child.status()?, 0))
                .await
                .map_err(Error::Stream)?,
        );
        while let Some(record) = statuses.next().await {
            let status = StatusRecord::decode(&record.map_err(Error::Stream)?.value)?;
            if status.conversation > 0 {
                return Ok(());
            }
        }
        Err(Error::Record("child status ended"))
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
                reserved_children: 0,
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
                        Wait::Children {
                            children: BTreeSet::from([child]),
                            any: false,
                        },
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
/// completed, else why the schedule is stuck. Time is paused, so a deadline
/// fires only once nothing else can progress: the runtime's handler deadline
/// failing a delivery that never returns, or the run's own.
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
        () = tokio::time::sleep(Duration::from_secs(3600)) => Ok(Some("no progress".into())),
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
