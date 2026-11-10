//! Real publication refusals must release child waits and stop archive alarms.
//! Removing the handler's failure transition is the child-wait negative control.
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

struct RefusesAppend {
    inner: MemoryStream,
    path: StreamPath,
    refusals: AtomicUsize,
    single_refusal: bool,
}

#[async_trait::async_trait]
impl StreamProvider for RefusesAppend {
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
            let previous = self.refusals.fetch_add(1, Ordering::SeqCst);
            assert!(
                !self.single_refusal || previous == 0,
                "a terminal archive refusal retried instead of settling"
            );
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

impl acyclic_stream::NativeStreamProvider for RefusesAppend {}

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
        let provider = Arc::new(RefusesAppend {
            inner: MemoryStream::default(),
            path: child.conversation()?,
            refusals: AtomicUsize::new(0),
            single_refusal: false,
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
        let failures = AtomicUsize::new(0);
        let serving = Box::pin(host.runtime().run(|_, error| {
            assert!(matches!(
                error,
                acyclic_actors::local::Error::Stream(StreamError::AccessDenied)
            ));
            failures.fetch_add(1, Ordering::SeqCst);
        }));
        let completed =
            match futures::future::select(Box::pin(host.runtime().complete()), serving).await {
                futures::future::Either::Left((completed, _)) => completed,
                futures::future::Either::Right((result, _)) => {
                    return Err(format!("dispatcher stopped: {result:?}").into());
                }
            };
        let refusals = provider.refusals.load(Ordering::SeqCst);
        assert!(
            refusals > 0,
            "the control never attempted its refused publication"
        );
        let parent = host.state(&root).await?;
        let child_state = host.state(&child).await?;
        if settle {
            completed?;
            assert_eq!(refusals, 1);
            assert_eq!(failures.load(Ordering::SeqCst), 0);
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
            assert!(matches!(
                completed,
                Err(acyclic_actors::local::Error::CompletionFailed(_))
            ));
            // The parent's Awaited append is a new event and can legitimately
            // retry the retained input. Every refusal must still be reported.
            assert_eq!(failures.load(Ordering::SeqCst), refusals);
            assert!(matches!(parent.phase, Phase::Waiting { .. }));
            assert!(!matches!(child_state.phase, Phase::Done { .. }));
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_refused_archive_keeps_unfinished_children_without_rearming_its_alarm() -> Result {
    // Refuse either the first batch (the Archive inbox delivery) or the last
    // batch (an alarm on an already-closed checkpoint). Both must settle.
    for refused_index in [0, MAX_FANOUT] {
        let root = AgentPath::root(&id(1));
        let spawns = (0..=MAX_FANOUT)
            .map(|index| Out::Spawn {
                profile: None,
                history: None,
                intent: intent(&format!("child-{index}")),
                brief: None,
                volumes: Vec::new(),
            })
            .collect::<Vec<_>>();
        let children = spawns
            .iter()
            .map(|out| {
                let Out::Spawn { intent, .. } = out else {
                    unreachable!()
                };
                root.child(&AgentId::derive(&root.id(), intent.as_str()))
            })
            .collect::<std::result::Result<BTreeSet<_>, _>>()?;
        let refused = children.iter().nth(refused_index).ok_or("missing child")?;
        let provider = Arc::new(RefusesAppend {
            inner: MemoryStream::default(),
            path: refused.closed()?,
            refusals: AtomicUsize::new(0),
            // Fail directly on the old recovery loop, without a timeout.
            single_refusal: true,
        });
        let host = AgentHost::new(
            provider.clone(),
            script(vec![TurnStep {
                checkpoint: None,
                outs: spawns,
                wait: Wait::Input,
                drops: None,
            }]),
            ActorReference::new(b"account", b"agents", b"owner")?,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(SystemClock),
        )?;
        let mut adoption = adopted(id(1));
        adoption.brief = Some(driver::file("parent")?);
        host.create(&intent("create"), adoption).await?;
        host.drain(&root).await?;
        let open = host.state(&root).await?;
        assert!(!open.closed);
        assert!(open.archive_pending.is_empty());
        assert_eq!(open.open_children.len(), children.len());
        assert_eq!(
            open.open_children.page(None, children.len()).await?,
            children.iter().map(AgentPath::id).collect::<Vec<_>>()
        );
        assert!(matches!(
            host.archive(&root, &user(), &intent("archive")).await?,
            Delivered::Committed { .. }
        ));
        let serving = Box::pin(host.runtime().run(|actor, error| {
            panic!("unsettled delivery for {actor:?}: {error}");
        }));
        match futures::future::select(Box::pin(host.runtime().complete()), serving).await {
            futures::future::Either::Left((completed, _)) => completed?,
            futures::future::Either::Right((result, _)) => {
                return Err(format!("dispatcher stopped: {result:?}").into());
            }
        }
        assert_eq!(provider.refusals.load(Ordering::SeqCst), 1);
        let state = host.state(&root).await?;
        assert!(state.closed);
        assert!(state.archive_refused);
        let pending = children
            .iter()
            .skip(refused_index)
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(state.archive_pending.len(), pending.len());
        assert_eq!(
            state.archive_pending.page(None, pending.len()).await?,
            pending.iter().map(AgentPath::id).collect::<Vec<_>>()
        );
        assert!(matches!(&state.phase,
            Phase::Done { outcome: AgentOutcome::Failed { message } }
                if message.contains("access denied")));
        let statuses = values(provider.as_ref(), root.status()?).await?;
        assert_eq!(
            StatusRecord::decode(statuses.last().ok_or("missing terminal status")?)?.phase,
            state.phase
        );
        // The whole refused commit is atomic: retain every unfinished child,
        // including children whose writes shared the refused child's batch.
        for child in &pending {
            assert!(values(provider.as_ref(), child.closed()?).await?.is_empty());
        }
        let snapshot = host.runtime().store().load(&host.actor(&root)?).await?;
        let committed = snapshot.state().ok_or("missing checkpoint")?.clone();
        assert!(committed.alarm().pending().is_none());
        host.drain(&root).await?;
        let after = host.runtime().store().load(&host.actor(&root)?).await?;
        assert_eq!(after.state(), Some(&committed));
        assert_eq!(host.state(&root).await?, state);
        assert_eq!(provider.refusals.load(Ordering::SeqCst), 1);
        // A terminal refusal also prevents a stale alarm or another archive
        // delivery from silently resuming the failed cascade.
        for wake in [
            Wake::Alarm,
            Wake::Inbox {
                sequence: u64::MAX - 1,
                record: InboxRecord::Archive(Control {
                    by: user(),
                    id: intent("archive-again"),
                }),
            },
        ] {
            let next = step(
                &script(Vec::new()),
                &root,
                Some(state.clone()),
                (u64::MAX, 0),
                wake,
                CancellationToken::new(),
            )
            .await?;
            assert_eq!(next.alarm, Alarm::Clear);
            assert_eq!(next.state.archive_pending, state.archive_pending);
            assert_eq!(next.state.phase, state.phase);
            assert!(next.outs.is_empty());
        }
    }
    Ok(())
}
