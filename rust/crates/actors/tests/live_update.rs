//! Deterministic SDK substrate proof over real Actor/Streams publications.
//! Cloud's multi-member cell, upload/adapter authority and effect admission must
//! additionally run the service acceptance specified by Cloud #7868.
use acyclic_actors::{
    addressing::ActorReference,
    domain::{ActorLimits, CodeSha256},
    guest::{
        AlarmChange, CancellationToken, Checkpoint, CodecIdentity, Delivery, Effect, GuestError,
        GuestFuture, Handler, Manifest, Subscription, Transition,
    },
    hosting::Host,
    invocation::Invocation,
    live_update::*,
    local::{PreparedPublish, Store, SystemClock},
};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, FollowRequest, IdempotencyKey,
    LocalStream, MemoryStream, StreamPath, StreamProvider, StreamProviderExt as _,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroU64,
    sync::Arc,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const AGENTS: usize = 3;
const ACTORS: usize = 7;
const STEPS: u64 = 24;

fn path(value: impl Into<String>) -> Result<StreamPath> {
    Ok(StreamPath::new(value.into())?)
}
fn key(value: impl AsRef<[u8]>) -> Result<IdempotencyKey> {
    Ok(IdempotencyKey::new(value.as_ref().to_vec())?)
}
fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "live-update-proof".into(),
        version: "1".into(),
    }
}
fn fraction(numerator: u64, denominator: u64) -> Result<Fraction> {
    Ok(Fraction {
        numerator,
        denominator: NonZeroU64::new(denominator).ok_or("zero denominator")?,
    })
}
fn publication(previous: u64) -> Result<Publication> {
    let schema = Schema {
        descriptor_set: acyclic_actors::FILE_DESCRIPTOR_SET.to_vec(),
        message: "acyclic.actors.v1.InvokeActorRequest".into(),
    };
    let compatibility = CompatibilityManifest {
        rpcs: BTreeMap::from([(
            "proof.call".into(),
            RpcSchema {
                request: schema.clone(),
                reply: schema.clone(),
            },
        )]),
        messages: BTreeMap::from([("proof.message".into(), schema)]),
        peers: vec![PeerRange {
            deployment: DeploymentId {
                account: b"account".to_vec(),
                name: "proof".into(),
            },
            first: NonZeroU64::MIN,
            last: None,
        }],
        handoff: HandoffRequirements {
            writes: codec(),
            reads: vec![codec()],
            journal_reads: vec![codec()],
            durable_await: true,
        },
        adapters: Vec::new(),
    };
    let schema = compatibility.schema_digest()?;
    let code = u8::try_from(previous + 1)?;
    Ok(Publication {
        deployment: DeploymentId {
            account: b"account".to_vec(),
            name: "proof".into(),
        },
        expected_version: previous,
        artifacts: ["alpha", "beta"]
            .into_iter()
            .map(|name| {
                (
                    name.into(),
                    Artifact {
                        code: [code; 32],
                        configuration: [code; 32],
                        schema,
                    },
                )
            })
            .collect(),
        compatibility,
    })
}

#[derive(Clone)]
struct Route {
    actor: ActorReference,
    state: StreamPath,
    inbox: StreamPath,
    activation: Effect,
    pin: Pin,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Request {
    agent: usize,
    step: u64,
    resume: bool,
}
impl Request {
    fn operation(&self) -> String {
        format!(
            "agent-{}/step-{}{}",
            self.agent,
            self.step,
            if self.resume { "/receipt" } else { "" }
        )
    }
    fn target(&self) -> usize {
        (self.agent + usize::try_from(self.step).expect("small proof step")) % ACTORS
    }
}
#[derive(Default, Serialize, Deserialize)]
struct State {
    seen: BTreeMap<usize, u64>,
    pending: Option<JournalPosition>,
}
#[derive(Serialize, Deserialize)]
struct Evidence {
    request: Request,
    caller: CallEnvelope,
    callee: CallEnvelope,
    next: Option<usize>,
}
struct Package {
    manifest: Manifest,
    routes: Arc<Vec<Route>>,
    envelope: CallEnvelope,
    audit: StreamPath,
}

impl Handler for Package {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn handoff(
        &self,
        checkpoint: &Checkpoint,
    ) -> std::result::Result<Option<DurableAwait>, GuestError> {
        let state: State = serde_json::from_slice(&checkpoint.bytes).map_err(guest)?;
        Ok(Some(DurableAwait {
            continuation: checkpoint.clone(),
            pending: state.pending.into_iter().collect(),
        }))
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let invocation = delivery.invocation()?;
            let record = delivery.record.as_ref().ok_or_else(|| guest("record"))?;
            let (payload, caller) = if let Some(invocation) = &invocation {
                (
                    invocation.request().as_ref(),
                    invocation.deployment_envelope().map_err(guest)?,
                )
            } else {
                (
                    record.value.as_ref(),
                    message_envelope(&record.headers).map_err(guest)?,
                )
            };
            let request: Request = serde_json::from_slice(payload).map_err(guest)?;
            let caller = caller.ok_or_else(|| guest("missing provenance"))?;
            if invocation.is_some() != (request.step % 2 == 0 || request.resume) {
                return Err(guest("RPC/message carrier changed"));
            }
            let mut state: State = checkpoint
                .map(|checkpoint| serde_json::from_slice(&checkpoint.bytes))
                .transpose()
                .map_err(guest)?
                .unwrap_or_default();
            if request.resume {
                let pending = state
                    .pending
                    .take()
                    .ok_or_else(|| guest("missing original operation"))?;
                if pending.operation
                    != format!("agent-{}/step-{}", request.agent, request.step).as_bytes()
                {
                    return Err(guest("changed original operation"));
                }
            } else if let Some(last) = state.seen.insert(request.agent, request.step)
                && last.checked_add(u64::try_from(ACTORS).map_err(guest)?) != Some(request.step)
            {
                return Err(guest("duplicate or reordered delivery"));
            }
            let suspend = request.agent == 0 && request.step == 2 && !request.resume;
            if suspend {
                state.pending = Some(JournalPosition {
                    stream: self
                        .routes
                        .get(request.target())
                        .ok_or_else(|| guest("route"))?
                        .inbox
                        .clone(),
                    commit: *record.commit_id.as_bytes(),
                    sequence: record.sequence,
                    operation: request.operation().into_bytes(),
                });
            }
            let next = (!suspend && request.step + 1 < STEPS).then(|| Request {
                agent: request.agent,
                step: request.step + 1,
                resume: false,
            });
            let evidence = Evidence {
                request: request.clone(),
                caller,
                callee: self.envelope.clone(),
                next: next.as_ref().map(Request::target),
            };
            let bytes = Bytes::from(serde_json::to_vec(&evidence).map_err(guest)?);
            let marker =
                StreamPath::new(format!("proof/effects/{}", request.operation())).map_err(guest)?;
            let mut effects = vec![
                Effect {
                    conditions: vec![CommitCondition::Absent {
                        path: marker.clone(),
                    }],
                    mutation: CommitMutation::Append {
                        path: marker,
                        records: vec![bytes.clone().into()],
                    },
                },
                Effect {
                    conditions: Vec::new(),
                    mutation: CommitMutation::Append {
                        path: self.audit.clone(),
                        records: vec![bytes.clone().into()],
                    },
                },
            ];
            if let Some(next) = next {
                let route = self
                    .routes
                    .get(next.target())
                    .ok_or_else(|| guest("next route"))?;
                let payload = Bytes::from(serde_json::to_vec(&next).map_err(guest)?);
                let record = if next.step % 2 == 0 {
                    Invocation::new(
                        route.actor.clone(),
                        "inbox".into(),
                        Bytes::from(next.operation()),
                        payload,
                    )?
                    .with_deployment_envelope(&self.envelope)
                    .map_err(guest)?
                    .record(&route.state)?
                } else {
                    self.envelope.message(payload.into()).map_err(guest)?
                };
                effects.extend([
                    Effect {
                        conditions: Vec::new(),
                        mutation: CommitMutation::Append {
                            path: route.inbox.clone(),
                            records: vec![record],
                        },
                    },
                    route.activation.clone(),
                ]);
            }
            Ok(Transition {
                response: invocation.is_some().then_some(bytes),
                checkpoint: Checkpoint {
                    codec: codec(),
                    bytes: serde_json::to_vec(&state).map_err(guest)?.into(),
                },
                alarm: AlarmChange::Keep,
                effects,
                participant: None,
            })
        })
    }
}
fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

fn envelope(
    selection: &Selection,
    route: &Route,
    publication: &Publication,
) -> Result<CallEnvelope> {
    Ok(CallEnvelope {
        deployment: selection.deployment.clone(),
        pin: route.pin.clone(),
        epoch: selection.epoch,
        version: selection.version(&route.pin)?.clone(),
        contract: publication.compatibility.schema_digest()?,
    })
}
fn host<P: StreamProvider + 'static>(
    store: &Store<P>,
    routes: &Arc<Vec<Route>>,
    target: usize,
    selection: &Selection,
    fence: &SelectionFence,
    publications: &[Publication],
) -> Result<(Host<P>, Arc<FencedHandler>)> {
    let route = routes.get(target).ok_or("route")?;
    let version = selection.version(&route.pin)?;
    let publication = publications
        .iter()
        .find(|publication| {
            publication
                .version()
                .is_ok_and(|identity| identity == *version)
        })
        .ok_or("publication")?;
    let namespace = route.actor.namespace().as_str();
    let code = publication.artifacts.get(namespace).ok_or("artifact")?.code;
    let package = Arc::new(VersionedHandler::new(
        Arc::new(Package {
            manifest: Manifest {
                code: CodeSha256::try_from(code.to_vec())?,
                codec: codec(),
                subscriptions: vec!["inbox".into()],
                interrupts: Vec::new(),
                bindings: Vec::new(),
                limits: ActorLimits::new(1_000, 1_048_576, 1_048_576)?,
            },
            routes: Arc::clone(routes),
            envelope: envelope(selection, route, publication)?,
            audit: path("proof/audit")?,
        }),
        publication,
        namespace,
    )?);
    let handler = Arc::new(FencedHandler::new(
        package,
        selection,
        route.pin.clone(),
        fence.clone(),
    )?);
    Ok((
        Host::new(
            Store::new(Arc::clone(store.provider()), store.root().clone()),
            route.actor.clone(),
            handler.clone(),
            Arc::new(SystemClock),
            Default::default(),
        ),
        handler,
    ))
}

async fn select<P: StreamProvider>(
    provider: &P,
    selection: &Selection,
    previous: Option<&SelectionFence>,
    guard: GuardDecision,
) -> Result<SelectionFence> {
    let stream = path("proof/deployment/selection.json")?;
    let status = RolloutStatus {
        selection: selection.clone(),
        next_page: None,
        adopted: 0,
        guard,
        calls: Vec::new(),
        rollback_epoch: (selection.epoch.get() == 4).then_some(selection.epoch),
    };
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![previous.map_or_else(
            || CommitCondition::Absent {
                path: stream.clone(),
            },
            SelectionFence::condition,
        )],
        mutations: vec![
            CommitMutation::Append {
                path: stream.clone(),
                records: vec![encode(selection)?.into()],
            },
            CommitMutation::Append {
                path: path("proof/deployment/rollout.json")?,
                records: vec![encode(&status)?.into()],
            },
        ],
        idempotency_key: key(format!("selection-{}", selection.epoch))?,
    };
    let CommitOutcome::Committed(receipt) = provider.commit(request).await? else {
        return Err("selection conflict".into());
    };
    Ok(SelectionFence {
        path: stream,
        commit: receipt.commit_id,
        tail: previous.map_or(1, |previous| previous.tail + 1),
    })
}

async fn drive<P: StreamProvider + 'static>(provider: Arc<P>) -> Result {
    let store = Store::new(Arc::clone(&provider), path("proof/actors")?);
    let publications = [publication(0)?, publication(1)?];
    let v1 = publications[0].version()?;
    let v2 = publications[1].version()?;
    let mut selection = Selection {
        deployment: publications[0].deployment.clone(),
        epoch: NonZeroU64::MIN,
        split: Split {
            stable: v1.clone(),
            target: v1.clone(),
            wave: fraction(0, 1)?,
        },
    };
    let mut fence = select(
        &*provider,
        &selection,
        None,
        GuardDecision::AwaitingCoverage,
    )
    .await?;
    let routes = Arc::new(
        (0..ACTORS)
            .map(|index| -> Result<Route> {
                let actor = ActorReference::new(
                    b"account",
                    if index % 2 == 0 { b"alpha" } else { b"beta" },
                    format!("actor-{index}").as_bytes(),
                )?;
                let pin = if index == ACTORS - 1 {
                    Pin::Actor(actor.encode())
                } else {
                    Pin::Cohort(format!("tree-{}", index / 2))
                };
                Ok(Route {
                    state: store.path(&actor)?,
                    inbox: path(format!("proof/inbox/{index}"))?,
                    activation: store.activation(&actor),
                    actor,
                    pin,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    );
    let mut creation = PreparedPublish::foreign(key("create-topology")?);
    for route in routes.iter() {
        for effect in store.create(
            &route.actor,
            BTreeMap::from([(
                "inbox".into(),
                Subscription {
                    path: route.inbox.clone(),
                    cursor: 0,
                },
            )]),
            None,
        )? {
            creation.join(effect)?;
        }
    }
    store.submit(creation).await?;
    let mut ready = VecDeque::new();
    let mut queued = [0_usize; ACTORS];
    let mut tickets = Vec::new();
    for agent in 0..AGENTS {
        let request = Request {
            agent,
            step: 0,
            resume: false,
        };
        let route = &routes[request.target()];
        let call = Invocation::new(
            route.actor.clone(),
            "inbox".into(),
            Bytes::from(request.operation()),
            serde_json::to_vec(&request)?.into(),
        )?
        .with_deployment_envelope(&envelope(&selection, route, &publications[0])?)?;
        let ticket = store.enqueue_invoke(&call).await?;
        assert_eq!(
            store.enqueue_invoke(&call).await?,
            ticket,
            "retry must attach to original accepted input"
        );
        tickets.push(ticket);
        queued[request.target()] += 1;
        ready.push_back(request.target());
    }
    let mut audits = None;
    let mut statuses = acyclic_stream::records_only(
        provider
            .follow(FollowRequest::new(
                path("proof/deployment/rollout.json")?,
                0,
            ))
            .await?,
    );
    let mut delivered = BTreeSet::new();
    let mut observed = Vec::new();
    let mut suspended = None;
    let mut resumed = false;
    let (release, completion) = tokio::sync::oneshot::channel::<()>();
    let mut completion = Some(completion);
    let mut release = Some(release);
    let mut promoted = false;
    let mut full = false;
    let mut rollback = false;
    while let Some(target) = ready.pop_front() {
        let count = std::mem::take(&mut queued[target]);
        let (host, package) = host(&store, &routes, target, &selection, &fence, &publications)?;
        host.wake().await?;
        if audits.is_none() {
            audits = Some(acyclic_stream::records_only(
                provider
                    .follow(FollowRequest::new(path("proof/audit")?, 0))
                    .await?,
            ));
        }
        for _ in 0..count {
            let record = audits
                .as_mut()
                .ok_or("audit follow")?
                .try_next()
                .await?
                .ok_or("audit ended")?;
            let evidence: Evidence = serde_json::from_slice(&record.value)?;
            assert_eq!(evidence.request.target(), target);
            assert!(
                delivered.insert(evidence.request.operation()),
                "duplicate logical delivery/effect"
            );
            assert_eq!(
                selection.version(&routes[target].pin)?,
                &evidence.callee.version
            );
            if evidence.caller.pin == evidence.callee.pin {
                // Accepted old calls keep their provenance across a cut. New
                // execution still uses only the cohort's current version set.
                assert!(evidence.caller.epoch <= evidence.callee.epoch);
            }
            if evidence.request.agent == 0 && evidence.request.step == 2 && !evidence.request.resume
            {
                let snapshot = store.load(&routes[target].actor).await?;
                let checkpoint = snapshot
                    .state()
                    .and_then(|state| state.checkpoint())
                    .ok_or("checkpoint")?;
                let point = package
                    .handoff(checkpoint)?
                    .ok_or("durable await unsupported")?;
                assert_eq!(point.pending.len(), 1);
                suspended = Some((target, evidence.caller.clone(), point));
            }
            if let Some(next) = evidence.next {
                if queued[next] == 0 {
                    ready.push_back(next);
                }
                queued[next] += 1;
            }
            observed.push(evidence);
        }
        if !promoted && observed.len() >= 9 {
            let change = selection.promote(Split {
                stable: v1.clone(),
                target: v2.clone(),
                wave: fraction(1, 2)?,
            })?;
            change.validate_against(&selection)?;
            selection = change.next;
            fence = select(
                &*provider,
                &selection,
                Some(&fence),
                GuardDecision::Advance { policy: [7; 32] },
            )
            .await?;
            promoted = true;
        } else if promoted && !full && observed.len() >= 24 {
            selection = selection
                .promote(Split {
                    stable: v1.clone(),
                    target: v2.clone(),
                    wave: fraction(1, 1)?,
                })?
                .next;
            fence = select(
                &*provider,
                &selection,
                Some(&fence),
                GuardDecision::Advance { policy: [7; 32] },
            )
            .await?;
            full = true;
        } else if full && !rollback && observed.len() >= 36 {
            selection = selection
                .rollback(Split {
                    stable: v2.clone(),
                    target: v1.clone(),
                    wave: fraction(1, 1)?,
                })?
                .next;
            fence = select(
                &*provider,
                &selection,
                Some(&fence),
                GuardDecision::Rollback {
                    policy: [7; 32],
                    reason: "independent mixed-version regression input".into(),
                },
            )
            .await?;
            assert_eq!(selection.epoch.get(), 4);
            rollback = true;
            release
                .take()
                .ok_or("original release")?
                .send(())
                .map_err(|()| "original producer lost")?;
        }
        if rollback && !resumed {
            // This is the original external operation's completion event, held
            // across all cuts. There is one producer and no readiness polling.
            completion.take().ok_or("original completion")?.await?;
            let (target, original, point) = suspended.take().ok_or("suspension was not reached")?;
            assert_eq!(point.pending[0].operation, b"agent-0/step-2");
            let request = Request {
                agent: 0,
                step: 2,
                resume: true,
            };
            let call = Invocation::new(
                routes[target].actor.clone(),
                "inbox".into(),
                Bytes::from(request.operation()),
                serde_json::to_vec(&request)?.into(),
            )?
            .with_deployment_envelope(&original)?;
            tickets.push(store.enqueue_invoke(&call).await?);
            if queued[target] == 0 {
                ready.push_back(target);
            }
            queued[target] += 1;
            resumed = true;
        }
    }
    assert!(rollback && resumed);
    assert_eq!(delivered.len(), AGENTS * usize::try_from(STEPS)? + 1);
    for agent in 0..AGENTS {
        for step in 0..STEPS {
            assert!(delivered.contains(&format!("agent-{agent}/step-{step}")));
        }
    }
    assert!(
        observed
            .iter()
            .any(|entry| entry.caller.version != entry.callee.version),
        "mixed-version coverage is required"
    );
    assert!(
        observed
            .iter()
            .any(|entry| entry.caller.pin == entry.callee.pin)
    );
    assert!(
        observed
            .iter()
            .any(|entry| entry.caller.pin != entry.callee.pin)
    );
    for ticket in tickets {
        let result = store.wait_invocation(ticket.clone()).await?;
        assert_eq!(
            store.wait_invocation(ticket).await?,
            result,
            "retained original outcome changed"
        );
    }
    for epoch in 1..=4 {
        let record = statuses.try_next().await?.ok_or("status ended")?;
        let status: RolloutStatus = decode(&record.value)?;
        assert_eq!(status.selection.epoch.get(), epoch);
        assert_eq!(
            status.rollback_epoch.map(NonZeroU64::get),
            (epoch == 4).then_some(4)
        );
    }
    eprintln!(
        "SDK live-update proof: N={AGENTS} M={ACTORS} steps={STEPS} deliveries={} epochs=1,2,3,4; full Cloud cell acceptance remains separate",
        delivered.len()
    );
    Ok(())
}

#[tokio::test]
async fn cyclic_traffic_retains_original_results_through_rollout_and_rollback() -> Result {
    drive(Arc::new(MemoryStream::default())).await?;
    let directory = tempfile::tempdir()?;
    drive(Arc::new(
        LocalStream::open(directory.path(), Default::default()).await?,
    ))
    .await
}

struct Held {
    manifest: Manifest,
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    calls: std::sync::atomic::AtomicUsize,
    fail: bool,
}

impl Handler for Held {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn on_record<'a>(
        &'a self,
        _: Delivery,
        _: Option<&'a Checkpoint>,
        _: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let entered = self.entered.lock().map_err(guest)?.take();
            let release = self.release.lock().map_err(guest)?.take();
            if let Some(entered) = entered {
                entered.send(()).map_err(|()| guest("entry owner lost"))?;
            }
            if let Some(release) = release {
                release.await.map_err(guest)?;
            }
            if self.fail {
                return Err(guest("late old failure"));
            }
            Ok(Transition {
                response: Some(Bytes::from_static(b"accepted")),
                checkpoint: Checkpoint {
                    codec: codec(),
                    bytes: Bytes::new(),
                },
                alarm: AlarmChange::Keep,
                effects: Vec::new(),
                participant: None,
            })
        })
    }
}

async fn late_reply<P: StreamProvider + 'static>(provider: Arc<P>, fail: bool) -> Result {
    let store = Store::new(Arc::clone(&provider), path("proof/actors")?);
    let publications = [publication(0)?, publication(1)?];
    let old = publications[0].version()?;
    let selection = Selection {
        deployment: publications[0].deployment.clone(),
        epoch: NonZeroU64::MIN,
        split: Split {
            stable: old.clone(),
            target: old,
            wave: fraction(0, 1)?,
        },
    };
    let fence = select(
        &*provider,
        &selection,
        None,
        GuardDecision::AwaitingCoverage,
    )
    .await?;
    let actor = ActorReference::new(b"account", b"alpha", b"late")?;
    let pin = Pin::Actor(actor.encode());
    let inbox = path("proof/inbox")?;
    let mut creation = PreparedPublish::foreign(key("create-late")?);
    for effect in store.create(
        &actor,
        BTreeMap::from([(
            "inbox".into(),
            Subscription {
                path: inbox,
                cursor: 0,
            },
        )]),
        None,
    )? {
        creation.join(effect)?;
    }
    store.submit(creation).await?;
    let (entered, entry) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let make = |version: u8, entered, release, fail| -> Result<Arc<Held>> {
        Ok(Arc::new(Held {
            manifest: Manifest {
                code: CodeSha256::try_from(vec![version; 32])?,
                codec: codec(),
                subscriptions: vec!["inbox".into()],
                interrupts: Vec::new(),
                bindings: Vec::new(),
                limits: ActorLimits::new(1_000, 1_048_576, 1_048_576)?,
            },
            entered: std::sync::Mutex::new(entered),
            release: std::sync::Mutex::new(release),
            calls: Default::default(),
            fail,
        }))
    };
    let original = make(1, Some(entered), Some(released), fail)?;
    let handler = Arc::new(FencedHandler::new(
        Arc::new(VersionedHandler::new(
            original.clone(),
            &publications[0],
            "alpha",
        )?),
        &selection,
        pin.clone(),
        fence.clone(),
    )?);
    assert!(
        handler
            .handoff(&Checkpoint {
                codec: codec(),
                bytes: Bytes::new()
            })?
            .is_none(),
        "an arbitrary await is not a durable handoff"
    );
    let invocation = Invocation::new(
        actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"original"),
        Bytes::new(),
    )?
    .with_deployment_envelope(handler.envelope())?;
    let ticket = store.enqueue_invoke(&invocation).await?;
    let owner = Host::new(
        Store::new(Arc::clone(&provider), store.root().clone()),
        actor.clone(),
        handler,
        Arc::new(SystemClock),
        Default::default(),
    );
    let wake = owner.wake();
    futures::pin_mut!(wake);
    tokio::select! {
        result = &mut wake => return Err(format!("handler ended before cut: {result:?}").into()),
        entered = entry => entered?,
    }
    let next = selection
        .promote(Split {
            stable: selection.split.stable.clone(),
            target: publications[1].version()?,
            wave: fraction(1, 1)?,
        })?
        .next;
    let next_fence = select(
        &*provider,
        &next,
        Some(&fence),
        GuardDecision::Advance { policy: [7; 32] },
    )
    .await?;
    release.send(()).map_err(|()| "original handler lost")?;
    assert!(
        matches!(wake.await, Err(acyclic_actors::local::Error::Conflict)),
        "late success and failure must both retain the original unconsumed input"
    );
    assert_eq!(
        original.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a fence conflict must never reenter old code"
    );
    let snapshot = store.load(&actor).await?;
    let state = snapshot.state().ok_or("state")?;
    assert_eq!(
        state
            .subscriptions()
            .get("inbox")
            .ok_or("subscription")?
            .cursor,
        0
    );
    assert!(state.checkpoint().is_none());
    let current = make(2, None, None, false)?;
    let handler = Arc::new(FencedHandler::new(
        Arc::new(VersionedHandler::new(
            current.clone(),
            &publications[1],
            "alpha",
        )?),
        &next,
        pin,
        next_fence,
    )?);
    let owner = Host::new(
        Store::new(provider, store.root().clone()),
        actor,
        handler,
        Arc::new(SystemClock),
        Default::default(),
    );
    owner.wake().await?;
    let result = store.wait_invocation(ticket.clone()).await?;
    assert_eq!(result, store.wait_invocation(ticket).await?);
    assert_eq!(current.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn selection_cut_refuses_late_success_and_failure_without_reentry() -> Result {
    for fail in [false, true] {
        late_reply(Arc::new(MemoryStream::default()), fail).await?;
        let directory = tempfile::tempdir()?;
        late_reply(
            Arc::new(LocalStream::open(directory.path(), Default::default()).await?),
            fail,
        )
        .await?;
    }
    Ok(())
}

#[test]
fn v1_identity_refuses_ambiguous_bytes_scopes_and_epoch_wrap() -> Result {
    let fixture = include_str!("fixtures/live-update-v1.json").trim_end();
    let value: serde_json::Value = decode(fixture.as_bytes())?;
    assert_eq!(encode(&value)?, fixture.as_bytes());
    let mut selection = Selection {
        deployment: DeploymentId {
            account: b"account".to_vec(),
            name: "proof".into(),
        },
        epoch: NonZeroU64::new(u64::MAX).ok_or("epoch")?,
        split: Split {
            stable: Version {
                number: NonZeroU64::MIN,
                digest: [1; 32],
            },
            target: Version {
                number: NonZeroU64::new(2).ok_or("version")?,
                digest: [2; 32],
            },
            wave: fraction(1, 2)?,
        },
    };
    let envelope = CallEnvelope {
        deployment: selection.deployment.clone(),
        pin: Pin::Cohort("original".into()),
        epoch: selection.epoch,
        version: selection.split.target.clone(),
        contract: [3; 32],
    };
    let bytes = encode(&envelope)?;
    assert_eq!(decode_envelope(&bytes)?, envelope);
    let text = String::from_utf8(bytes)?;
    for changed in [
        text.replacen("\"v\":1", "\"v\":2", 1),
        text.replacen("\"epoch\":", "\"future\":1,\"epoch\":", 1),
        text.replacen("\"epoch\":", "\"epoch\":\"1\",\"epoch\":", 1),
        text.replace("18446744073709551615", "018446744073709551615"),
        format!("{text}\n"),
    ] {
        assert!(decode_envelope(changed.as_bytes()).is_err());
    }
    assert!(matches!(
        selection.promote(selection.split.clone()),
        Err(Error::Exhausted)
    ));
    assert!(matches!(
        selection.rollback(Split {
            stable: selection.split.target.clone(),
            target: selection.split.stable.clone(),
            wave: fraction(1, 1)?
        }),
        Err(Error::Exhausted)
    ));
    let foreign = Pin::Actor(ActorReference::new(b"another-account", b"alpha", b"actor")?.encode());
    assert!(selection.version(&foreign).is_err());
    selection.epoch = NonZeroU64::MIN;
    let change = selection.promote(selection.split.clone())?;
    let mut other = selection.clone();
    other.deployment.name = "other".into();
    assert!(change.validate_against(&other).is_err());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, rng_seed: proptest::test_runner::RngSeed::Fixed(7868), ..ProptestConfig::default() })]
    #[test]
    fn wave_advancement_preserves_the_entire_cohort_pin(cohort in ".{1,64}", a in any::<u64>(), b in any::<u64>()) {
        let pin = Pin::Cohort(cohort);
        let mut selection = Selection {
            deployment: DeploymentId { account: b"account".to_vec(), name: "proof".into() }, epoch: NonZeroU64::MIN,
            split: Split { stable: Version { number: NonZeroU64::MIN, digest: [1; 32] }, target: Version { number: NonZeroU64::MIN.saturating_add(1), digest: [2; 32] }, wave: Fraction { numerator: a.min(b), denominator: NonZeroU64::MAX } },
        };
        let before = selection.version(&pin)?.clone();
        selection.split.wave.numerator = a.max(b);
        let after = selection.version(&pin)?;
        prop_assert!(before != selection.split.target || after == &selection.split.target);
        // Lazy adoption by another member uses exactly the same key and cut.
        prop_assert_eq!(after, selection.version(&pin.clone())?);
    }
}
