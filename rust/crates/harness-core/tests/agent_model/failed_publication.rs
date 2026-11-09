//! Real publication refusals must release the parent's child wait. Removing
//! the handler's failure transition is the negative control on the same stream.
#![cfg(not(target_arch = "wasm32"))]

use super::*;
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    guest::{CancellationToken, Checkpoint, Delivery, GuestFuture, Handler, Manifest, Transition},
    local::SystemClock,
};
use acyclic_stream::{
    BoxProviderFuture, CommitId, CommittedEnvelope, FollowRequest, FollowStream, Heads,
    HeadsRequest, IdempotencyKey, IdempotencyObservation, RecordStream, WriteOutcome, WriteRequest,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct RefusesConversation {
    inner: MemoryStream,
    path: StreamPath,
    refusals: AtomicUsize,
}

#[async_trait::async_trait]
impl StreamProvider for RefusesConversation {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        self.inner.context()
    }

    async fn heads(&self, request: HeadsRequest) -> std::result::Result<Heads, StreamError> {
        self.inner.heads(request).await
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    fn write(
        &self,
        request: WriteRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<WriteOutcome, StreamError>> {
        let refused = match &request {
            WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) => {
                request.mutations.iter().any(|mutation| {
                    matches!(mutation,
                    CommitMutation::Append { path, .. } if path == &self.path)
                })
            }
            _ => false,
        };
        if refused {
            self.refusals.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(StreamError::AccessDenied) })
        } else {
            self.inner.write(request)
        }
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

impl acyclic_stream::NativeStreamProvider for RefusesConversation {}

struct Parent;

impl TurnDriver for Parent {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _: u64,
        _: Option<Queued>,
        _: CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let child = agent
                .path
                .child(&AgentId::derive(&agent.path.id(), "child"))?;
            let (outs, wait) = if agent.parent.is_some() {
                (vec![Out::Conversation(b"refused".to_vec())], Wait::Input)
            } else if agent.turn.is_none() {
                (
                    vec![Out::Spawn {
                        profile: None,
                        history: None,
                        intent: intent("child"),
                        brief: Some(
                            driver::file("child").map_err(|error| Error::Turn(Box::new(error)))?,
                        ),
                        volumes: Vec::new(),
                    }],
                    Wait::Continue,
                )
            } else if agent
                .turn
                .as_ref()
                .is_some_and(|turn| turn.checkpoint.is_empty())
            {
                (Vec::new(), Wait::Until(Until::all(BTreeSet::from([child]))))
            } else {
                (
                    Vec::new(),
                    Wait::Done(AgentOutcome::Succeeded { result: None }),
                )
            };
            Ok(TurnStep {
                reserved_children: 0,
                checkpoint: Some(if agent.turn.is_none() {
                    Vec::new()
                } else {
                    vec![1]
                }),
                outs,
                wait,
                drops: None,
            })
        })
    }
}

/// Retains production execution but removes the missing failure transition.
struct Unsettled(Arc<dyn Handler>);

impl Handler for Unsettled {
    fn manifest(&self) -> &Manifest {
        self.0.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        self.0.on_record(delivery, checkpoint, cancellation)
    }
}

#[tokio::test]
async fn a_definitively_refused_child_publication_settles_its_parent_once() -> Result {
    for settle in [false, true] {
        let root = AgentPath::root(&id(1));
        let child = root.child(&AgentId::derive(&root.id(), "child"))?;
        let provider = Arc::new(RefusesConversation {
            inner: MemoryStream::default(),
            path: child.conversation()?,
            refusals: AtomicUsize::new(0),
        });
        let handler: Arc<dyn Handler> = Arc::new(AgentHandler::new(
            actor_store(&provider)?,
            Parent,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        )?);
        let handler = if settle {
            handler
        } else {
            Arc::new(Unsettled(handler))
        };
        let host = AgentHost::hosting(
            provider.clone(),
            ActorReference::new(b"account", b"agents", b"owner")?,
            handler,
            Arc::new(SystemClock),
            None,
        )?;
        let mut adoption = adopted(id(1));
        adoption.brief = Some(driver::file("parent")?);
        assert_eq!(host.create(&intent("create"), adoption).await?, root);
        let failures = host.runtime().drain().await;
        assert_eq!(provider.refusals.load(Ordering::SeqCst), 1);
        let parent = host.state(&root).await?;
        let child_state = host.state(&child).await?;
        if settle {
            assert!(failures.is_empty(), "{failures:?}");
            assert!(matches!(parent.phase, Phase::Done { .. }));
            assert!(matches!(&child_state.phase,
                Phase::Done { outcome: AgentOutcome::Failed { message } }
                    if message.contains("access denied")));
            assert!(
                values(provider.as_ref(), child.conversation()?)
                    .await?
                    .is_empty()
            );
            let reports = values(provider.as_ref(), root.inbox()?)
                .await?
                .into_iter()
                .filter(|bytes| {
                    matches!(InboxRecord::decode(bytes),
                    Ok(InboxRecord::ChildDone(done)) if done.child == child)
                })
                .count();
            assert_eq!(reports, 1);
            assert!(host.runtime().drain().await.is_empty());
            assert_eq!(host.state(&child).await?, child_state);
            assert_eq!(provider.refusals.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(failures.len(), 1);
            assert!(matches!(parent.phase, Phase::Waiting { .. }));
            assert!(!matches!(child_state.phase, Phase::Done { .. }));
        }
    }
    Ok(())
}
