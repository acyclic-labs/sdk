#![cfg(not(target_arch = "wasm32"))]

//! A child whose step storage refuses for good records itself failed and
//! reports `ChildDone` once, so its waiting parent settles. The storage is a
//! real provider behind the same adapter seam the conformance suite uses,
//! refusing every step commit of the child's; nothing in product code is a
//! hook.

use super::{Result, adopted, id, intent, user};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_harness_core::agent::*;
use acyclic_stream::{
    BoxProviderFuture, CommitId, CommitMutation, CommittedEnvelope, FollowRequest, FollowStream,
    Heads, HeadsRequest, IdempotencyKey, IdempotencyObservation, MemoryStream, ProviderContext,
    ReadRequest, RecordStream, StreamError, StreamPath, StreamProvider, WriteOutcome, WriteRequest,
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

/// Refuses, with a definite `AccessDenied`, every commit that appends to
/// `path`: the refusal is for good, so retrying the same step cannot pass.
struct Refusing {
    inner: MemoryStream,
    path: StreamPath,
}

impl Refusing {
    fn refuses(&self, request: &WriteRequest) -> bool {
        let (WriteRequest::Commit(commit) | WriteRequest::CommitAccepted(commit)) = request else {
            return false;
        };
        commit.mutations.iter().any(
            |mutation| matches!(mutation, CommitMutation::Append { path, .. } if *path == self.path),
        )
    }
}

#[async_trait::async_trait]
impl StreamProvider for Refusing {
    fn context(&self) -> ProviderContext<'_> {
        self.inner.context()
    }
    async fn heads(&self, request: HeadsRequest) -> std::result::Result<Heads, StreamError> {
        self.inner.heads(request).await
    }
    fn write(
        &self,
        request: WriteRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<WriteOutcome, StreamError>> {
        if self.refuses(&request) {
            return Box::pin(async { Err(StreamError::AccessDenied) });
        }
        self.inner.write(request)
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: FollowRequest,
    ) -> std::result::Result<FollowStream, StreamError> {
        self.inner.follow(request).await
    }
    async fn read_commit(
        &self,
        id: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

impl acyclic_stream::NativeStreamProvider for Refusing {}

fn child(parent: &AgentPath) -> std::result::Result<AgentPath, Error> {
    parent.child(&AgentId::derive(&parent.id(), "c"))
}

/// The root spawns child `c`, then waits for it once the child holds its
/// turn input, so the child is running, not idle, when asked.
struct Parent {
    spawned: Arc<Semaphore>,
    go: Arc<Semaphore>,
    done: Arc<Semaphore>,
}

impl TurnDriver for Parent {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        _input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let step = |checkpoint: Option<&[u8]>, outs, wait| TurnStep {
                reserved_children: 0,
                checkpoint: checkpoint.map(<[u8]>::to_vec),
                outs,
                wait,
                drops: None,
            };
            if agent.parent.is_some() {
                // The child's turn appends its answer: the refused commit.
                return Ok(step(
                    None,
                    vec![Out::Conversation(b"answer".to_vec())],
                    Wait::Input,
                ));
            }
            match agent.turn.as_ref().map(|turn| turn.checkpoint.as_slice()) {
                None => Ok(step(
                    Some(b"spawned"),
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
                    self.spawned.add_permits(1);
                    self.go
                        .acquire()
                        .await
                        .map_err(|_| Error::Record("go"))?
                        .forget();
                    Ok(step(
                        Some(b"waiting"),
                        Vec::new(),
                        Wait::Until(Until::all(BTreeSet::from([child(&agent.path)?]))),
                    ))
                }
                Some(_) => {
                    self.done.add_permits(1);
                    Ok(step(None, Vec::new(), Wait::Input))
                }
            }
        })
    }
}

/// The child's phase once its waiting parent resumed, or `None` if the
/// parent still waits at the simulated deadline. Every step commit of the
/// child's is refused; its failure appends nothing to its conversation.
async fn settled() -> Result<Option<Phase>> {
    let root = AgentPath::root(&id(1));
    let child = child(&root)?;
    let provider = Arc::new(Refusing {
        inner: MemoryStream::default(),
        path: child.conversation()?,
    });
    let (spawned, go, done) = (
        Arc::new(Semaphore::new(0)),
        Arc::new(Semaphore::new(0)),
        Arc::new(Semaphore::new(0)),
    );
    let host = AgentHost::new(
        provider,
        Parent {
            spawned: spawned.clone(),
            go: go.clone(),
            done: done.clone(),
        },
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    assert_eq!(host.create(&intent("create"), adopted(id(1))).await?, root);
    let input = |name: &str| {
        InboxRecord::ApprovalRequested(ApprovalRequested {
            child: child.clone(),
            id: intent(name),
        })
    };
    host.send(&root, &user(), &input("start")).await?;
    let schedule = async {
        // The child exists once the parent's spawning step committed.
        spawned.acquire().await?.forget();
        host.send(&child, &user(), &input("turn")).await?;
        go.add_permits(1);
        done.acquire().await?.forget();
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    tokio::select! {
        stopped = host.run(|_, _| {}) => Err(format!("runtime stopped: {stopped:?}").into()),
        resumed = schedule => {
            resumed?;
            Ok(Some(host.state(&child).await?.phase))
        }
        () = tokio::time::sleep(Duration::from_secs(3600)) => Ok(None),
    }
}

/// Negative control: without the runtime settling the refusal through its
/// handler, the refused input stays queued, nothing reports the child, and
/// the parent still waits at the simulated deadline (`None`).
#[tokio::test(start_paused = true)]
async fn a_child_whose_step_storage_refuses_reports_failed_and_its_waiter_settles() -> Result {
    let phase = settled().await?;
    assert!(
        matches!(
            &phase,
            Some(Phase::Done { outcome: AgentOutcome::Failed { message } }) if !message.is_empty()
        ),
        "{phase:?}"
    );
    Ok(())
}
