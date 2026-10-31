//! One Actors conformance suite for every host: `local::Runtime` and the
//! hosted service run the same checks through the same Stream-level surface.
//!
//! The host under test runs [`Package`] for every actor under the target's
//! state root. The suite creates actors with the companion create mutation,
//! appends commands to their inboxes and observes their committed outboxes;
//! only pause/resume and restart go through [`Backend`].
use crate::addressing::ActorReference;
use crate::guest::{
    AlarmChange, Checkpoint, CodecIdentity, Delivery, Effect, GuestError, GuestFuture, Handler,
    Manifest, Transition, Trigger,
};
use crate::local::{AppliedCut, Clock, Error, PreparedPublish, Store, Subscription};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::testing::{RequestLedger, Requests};
use acyclic_stream::{
    AppendRecord, BoxProviderFuture, CommitCondition, CommitMutation, IdempotencyKey,
    ProviderPlatform, StreamError, StreamPath, StreamProvider, UnixMillisClock as _,
};
use bytes::Bytes;
use futures::TryFutureExt as _;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Upper bound for one observation; an event-driven wait ends earlier.
const BUDGET_MS: u64 = 5_000;
/// Alarm lead time; long enough to clear or restart before it fires.
const ALARM_MS: u64 = 100;
/// Quiet window that proves nothing further is published.
const QUIET_MS: u64 = 250;

/// The host under test, already running [`Package`] under `store().root()`.
pub trait Backend: ProviderPlatform {
    /// Fresh namespace retained for this suite invocation.
    fn namespace(&self) -> &str {
        "conformance"
    }
    /// Dispatch accounting for this host, including its scheduler and storage.
    fn requests(&self) -> RequestLedger;
    /// Stream provider holding actor state, inboxes and outboxes.
    type Provider: StreamProvider + ?Sized;
    /// The store whose root the host watches.
    fn store(&self) -> &Store<Self::Provider>;
    /// The host clock that also schedules alarms.
    fn clock(&self) -> &dyn Clock;
    /// Durably pause or resume one actor through the host's control surface.
    fn pause<'a>(
        &'a self,
        actor: &'a ActorReference,
        paused: bool,
    ) -> BoxProviderFuture<'a, Result<(), Error>>;
    /// Stop and restart the host; the next placement takes a new epoch.
    fn restart(&self) -> BoxProviderFuture<'_, std::result::Result<(), Error>>;
}

/// Compatibility spelling for the same backend contract.
pub use Backend as Target;

/// Register the complete suite using an async factory for a fresh running host.
/// The backend must keep every runtime and disposable storage resource alive.
pub use crate::actors_conformance_suite as suite;

/// Implementation of [`suite!`].
#[macro_export]
macro_rules! actors_conformance_suite {
    ($factory:path) => {
        $crate::actors_conformance_suite!(actors_conformance, $factory);
    };
    ($name:ident, $factory:path) => {
        #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
        #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
        async fn $name() -> std::result::Result<(), Box<dyn std::error::Error>> {
            let backend = $factory().await?;
            $crate::conformance::verify(&backend).await?;
            Ok(())
        }
    };
}

/// The conformance package's checkpoint codec.
pub fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "acyclic.actors.conformance".into(),
        version: "1".into(),
    }
}

#[derive(Serialize, Deserialize)]
struct Ledger {
    outbox: StreamPath,
    published: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reservations: Option<crate::reservation::Book>,
}

/// Reference package. Inbox commands: `emit` appends `r<seq>`; `set <ms>`
/// arms the alarm `ms` after the delivery; `clear` clears it. A fired alarm
/// appends `a<generation>`. Every outbox append carries a Tail condition, so a
/// duplicate or lost publication is observable.
pub struct Package {
    manifest: Manifest,
}

impl Package {
    /// Build the package manifest.
    pub fn new() -> Result<Self, crate::domain::DomainError> {
        Ok(Self {
            manifest: Manifest {
                code: crate::domain::CodeSha256::try_from(vec![0xa4; 32])?,
                codec: codec(),
                interrupts: vec![crate::guest::Interrupt {
                    subscription: "inbox".into(),
                    value_prefix: Bytes::from_static(b"cancel"),
                }],
                subscriptions: vec!["inbox".into(), "mirror".into()],
                bindings: Vec::new(),
                limits: crate::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            },
        })
    }
}

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

impl Handler for Package {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let checkpoint = checkpoint.ok_or_else(|| guest("missing ledger"))?;
            let mut ledger: Ledger = serde_json::from_slice(&checkpoint.bytes).map_err(guest)?;
            let invocation = delivery.invocation()?;
            if invocation.is_none()
                && delivery
                    .record
                    .as_ref()
                    .map(crate::invocation::Invocation::decode)
                    .transpose()?
                    .flatten()
                    .is_some()
            {
                return Ok(Transition {
                    response: None,
                    checkpoint: checkpoint.clone(),
                    alarm: AlarmChange::Keep,
                    effects: Vec::new(),
                    participant: None,
                });
            }
            if let Some(book) = &mut ledger.reservations {
                book.validate()?;
                let step = book.step(&delivery)?;
                return Ok(Transition {
                    response: step.response,
                    participant: None,
                    checkpoint: Checkpoint {
                        codec: codec(),
                        bytes: serde_json::to_vec(&ledger).map_err(guest)?.into(),
                    },
                    alarm: step.alarm,
                    effects: Vec::new(),
                });
            }
            let (alarm, output) = match (&delivery.trigger, &delivery.record) {
                (Trigger::Alarm { generation, .. }, _) => {
                    (AlarmChange::Keep, Some(format!("a{generation}")))
                }
                (Trigger::Record { sequence, .. }, Some(record)) => {
                    let command = std::str::from_utf8(
                        invocation
                            .as_ref()
                            .map_or(record.value.as_ref(), |call| call.request().as_ref()),
                    )
                    .map_err(guest)?;
                    match command.split_once(' ') {
                        None if command == "emit" => {
                            (AlarmChange::Keep, Some(format!("r{sequence}")))
                        }
                        None if command == "clear" => (AlarmChange::Clear, None),
                        None if command == "cancel" => (AlarmChange::Keep, None),
                        None if command == "wait" => {
                            cancellation.cancelled().await;
                            (AlarmChange::Keep, Some(format!("c{sequence}")))
                        }
                        Some(("set", delay)) => (
                            AlarmChange::Set {
                                deadline_unix_ms: delivery
                                    .now_unix_ms
                                    .saturating_add(delay.parse::<u64>().map_err(guest)?),
                            },
                            None,
                        ),
                        Some(("pool", capacity)) => {
                            ledger.reservations = Some(crate::reservation::Book::new(
                                capacity.parse::<u64>().map_err(guest)?,
                            )?);
                            (AlarmChange::Keep, None)
                        }
                        _ => return Err(guest("unknown command")),
                    }
                }
                (Trigger::Record { .. }, None) => {
                    return Err(guest("record trigger without record"));
                }
            };
            let mut effects = Vec::new();
            if let Some(output) = output {
                let condition = if ledger.published == 0 {
                    CommitCondition::Absent {
                        path: ledger.outbox.clone(),
                    }
                } else {
                    CommitCondition::Tail {
                        path: ledger.outbox.clone(),
                        expected: ledger.published,
                    }
                };
                effects.push(Effect {
                    conditions: vec![condition],
                    mutation: CommitMutation::Append {
                        path: ledger.outbox.clone(),
                        records: vec![AppendRecord::from(Bytes::from(output))],
                    },
                });
                ledger.published += 1;
            }
            Ok(Transition {
                response: invocation.map(|call| call.request().clone()),
                participant: None,
                checkpoint: Checkpoint {
                    codec: codec(),
                    bytes: serde_json::to_vec(&ledger).map_err(guest)?.into(),
                },
                alarm,
                effects,
            })
        })
    }
}

/// A failed conformance case.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// The host violated an observable guarantee.
    #[error("{case}: {detail}")]
    Violation {
        /// Case name.
        case: &'static str,
        /// What was observed.
        detail: String,
    },
    /// The suite could not drive the target.
    #[error(transparent)]
    Target(#[from] Error),
}

struct Fixture<'a, T: Backend + ?Sized> {
    target: &'a T,
    case: &'static str,
    actor: ActorReference,
    inbox: StreamPath,
    outbox: StreamPath,
    /// Creation effects that ride in the first send's commit.
    creation: Option<[Effect; 2]>,
    sent: u64,
}

impl<'a, T: Backend + ?Sized> Fixture<'a, T> {
    /// Prepare a fresh actor. Its companion creation rides in the commit that
    /// creates its inbox with the first command, as a spawn does; the host
    /// discovers it from the activation log without placement.
    async fn create(target: &'a T, run: &str, case: &'static str) -> Result<Self, Failure> {
        let actor = ActorReference::new(
            b"conformance",
            b"actors",
            format!("{run}-{case}").as_bytes(),
        )
        .map_err(|_| Error::Invalid("conformance actor id"))?;
        let inbox =
            StreamPath::new(format!("conformance/{run}/{case}/inbox")).map_err(Error::from)?;
        let outbox =
            StreamPath::new(format!("conformance/{run}/{case}/outbox")).map_err(Error::from)?;
        let ledger = Ledger {
            outbox: outbox.clone(),
            published: 0,
            reservations: None,
        };
        let creation = target.store().create(
            &actor,
            BTreeMap::from([(
                "inbox".into(),
                Subscription {
                    path: inbox.clone(),
                    cursor: 0,
                },
            )]),
            Some(Checkpoint {
                codec: codec(),
                bytes: serde_json::to_vec(&ledger)
                    .map_err(|_| Error::Invalid("conformance ledger does not encode"))?
                    .into(),
            }),
        )?;
        Ok(Self {
            target,
            case,
            actor,
            inbox,
            outbox,
            creation: Some(creation),
            sent: 0,
        })
    }

    async fn send(&mut self, command: &str) -> Result<(), Failure> {
        let store = self.target.store();
        let condition = if self.sent == 0 {
            CommitCondition::Absent {
                path: self.inbox.clone(),
            }
        } else {
            CommitCondition::Tail {
                path: self.inbox.clone(),
                expected: self.sent,
            }
        };
        let mut prepared =
            PreparedPublish::foreign(idempotency(&format!("{}/send/{}", self.inbox, self.sent))?);
        // The creation carries the activation entry; every later input wakes
        // the actor with its own same-commit entry.
        match self.creation.take() {
            Some(creation) => {
                for effect in creation {
                    prepared.join(effect)?;
                }
            }
            None => prepared.join(store.activation(&self.actor))?,
        }
        prepared.join(Effect {
            conditions: vec![condition],
            mutation: CommitMutation::Append {
                path: self.inbox.clone(),
                records: vec![AppendRecord::from(Bytes::from(command.to_owned()))],
            },
        })?;
        let outcome = store
            .provider()
            .commit(prepared.into_request()?)
            .await
            .map_err(Error::from)?;
        PreparedPublish::settle(&outcome)?;
        self.sent += 1;
        Ok(())
    }

    /// Follow the outbox until it holds `expected` records, then require a
    /// quiet window with nothing more.
    async fn expect(&self, expected: &[&str]) -> Result<(), Failure> {
        let observed = self.observe(expected.len(), BUDGET_MS).await?;
        if observed != expected {
            return self.violation(format!("outbox {observed:?}, expected {expected:?}"));
        }
        let extra = self.observe(expected.len() + 1, QUIET_MS).await?;
        if extra.len() > expected.len() {
            return self.violation(format!("outbox grew to {extra:?} after {expected:?}"));
        }
        Ok(())
    }

    async fn observe(&self, count: usize, budget_ms: u64) -> Result<Vec<String>, Failure> {
        let provider = self.target.store().provider();
        let clock = self.target.clock();
        let mut records = Vec::new();
        let mut deadline = clock.sleep_until(clock.now_unix_ms().saturating_add(budget_ms));
        // The outbox is created by a publication, which always appends the
        // actor's state in the same commit: wake on state records until it exists.
        let mut state = provider
            .follow(acyclic_stream::FollowRequest::new(
                self.target.store().path(&self.actor)?,
                0,
            ))
            .map_ok(acyclic_stream::records_only)
            .await
            .map_err(Error::from)?;
        let mut follow = loop {
            match provider
                .follow(acyclic_stream::FollowRequest::new(self.outbox.clone(), 0))
                .map_ok(acyclic_stream::records_only)
                .await
            {
                Ok(follow) => break follow,
                Err(StreamError::NotFound) => {}
                Err(error) => return Err(Error::from(error).into()),
            }
            futures::select_biased! {
                record = state.next().fuse() => match record {
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(Error::from(error).into()),
                    None => return Err(Error::from(StreamError::Unavailable).into()),
                },
                () = (&mut deadline).fuse() => return Ok(records),
            }
        };
        while records.len() < count {
            futures::select_biased! {
                record = follow.next().fuse() => match record {
                    Some(Ok(record)) => records.push(String::from_utf8_lossy(&record.value).into_owned()),
                    Some(Err(error)) => return Err(Error::from(error).into()),
                    None => break,
                },
                () = (&mut deadline).fuse() => break,
            }
        }
        Ok(records)
    }

    async fn cursor(&self) -> Result<Option<u64>, Failure> {
        let snapshot = self.target.store().load(&self.actor).await?;
        Ok(snapshot.state().and_then(|state| state.cursor("inbox")))
    }

    fn violation<R>(&self, detail: String) -> Result<R, Failure> {
        Err(Failure::Violation {
            case: self.case,
            detail,
        })
    }
}

fn idempotency(value: &str) -> Result<IdempotencyKey, Error> {
    Ok(IdempotencyKey::new(value.as_bytes().to_vec())?)
}

/// Run every case against the backend's fresh disposable resources.
pub async fn verify<T: Backend + ?Sized>(target: &T) -> Result<(), Failure> {
    let run = target.namespace();
    ordered(target, run).await?;
    applied(target, run).await?;
    bounded_case(target, "invoked", BUDGET_MS, invoked(target, run)).await?;
    bounded_case(
        target,
        "invocation-failure",
        BUDGET_MS,
        invocation_failure(target, run),
    )
    .await?;
    bounded_case(
        target,
        "shared-invocation",
        BUDGET_MS,
        shared_invocation(target, run),
    )
    .await?;
    bounded_case(target, "reserved", 2 * BUDGET_MS, reserved(target, run)).await?;
    alarms(target, run).await?;
    paused(target, run).await?;
    interrupted(target, run).await?;
    restarted(target, run).await
}

/// A backend that never publishes a response must fail the verifier, rather
/// than retaining an unbounded public invocation wait in the suite. The
/// reservation case includes its deliberate lease-expiry wait in this budget.
async fn bounded_case<T: Target + ?Sized>(
    target: &T,
    case: &'static str,
    budget_ms: u64,
    future: impl std::future::Future<Output = Result<(), Failure>>,
) -> Result<(), Failure> {
    let deadline = target
        .clock()
        .sleep_until(target.clock().now_unix_ms().saturating_add(budget_ms));
    match futures::future::select(Box::pin(future), deadline).await {
        futures::future::Either::Left((result, _)) => result,
        futures::future::Either::Right(_) => Err(Failure::Violation {
            case,
            detail: "backend did not complete committed operations within the case budget".into(),
        }),
    }
}

/// One operation survives queued retries, pause, restart and a lost response.
/// Both invocation and ordinary sends run through the original Package.
async fn invoked<T: Target + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    use crate::invocation::Invocation;
    let mut fixture = Fixture::create(target, run, "invoked").await?;
    fixture.send("clear").await?;
    // Observe creation's first command before placing the durable pause fence.
    let input = target
        .store()
        .provider()
        .read(acyclic_stream::ReadRequest {
            key: None,
            path: fixture.inbox.clone(),
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .map_err(Error::from)?
        .next()
        .await
        .ok_or(Error::Invalid("invocation fixture input is missing"))?
        .map_err(Error::from)?;
    let cut = AppliedCut::through("inbox".into(), fixture.inbox.clone(), &input)?;
    let deadline = target
        .clock()
        .sleep_until(target.clock().now_unix_ms().saturating_add(BUDGET_MS));
    match futures::future::select(
        Box::pin(target.store().wait_applied(&fixture.actor, cut)),
        deadline,
    )
    .await
    {
        futures::future::Either::Left((result, _)) => {
            result?;
        }
        futures::future::Either::Right(_) => {
            return fixture.violation("initial input was not applied".into());
        }
    }
    target.pause(&fixture.actor, true).await?;
    let invocation = Invocation::new(
        fixture.actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"operation"),
        Bytes::from_static(b"emit"),
    )
    .map_err(Error::from)?;
    let ticket = target.store().enqueue_invoke(&invocation).await?;
    if target.store().enqueue_invoke(&invocation).await? != ticket {
        return fixture.violation("queued retry changed its original input".into());
    }
    let changed = Invocation::new(
        fixture.actor.clone(),
        "inbox".into(),
        invocation.operation().clone(),
        Bytes::from_static(b"clear"),
    )
    .map_err(Error::from)?;
    if !matches!(
        target.store().enqueue_invoke(&changed).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ) {
        return fixture.violation("operation accepted changed request bytes".into());
    }
    let mut waiting = Box::pin(target.store().wait_invocation(ticket.clone()));
    if futures::poll!(waiting.as_mut()).is_ready() || fixture.cursor().await? != Some(1) {
        return fixture.violation("invocation consumed paused input".into());
    }
    target.restart().await?;
    if target.store().enqueue_invoke(&invocation).await? != ticket
        || futures::poll!(waiting.as_mut()).is_ready()
    {
        return fixture.violation("restart lost queued operation or pause".into());
    }
    target.pause(&fixture.actor, false).await?;
    let deadline = target
        .clock()
        .sleep_until(target.clock().now_unix_ms().saturating_add(BUDGET_MS));
    let committed = match futures::future::select(waiting, deadline).await {
        futures::future::Either::Left((result, _)) => result?,
        futures::future::Either::Right(_) => {
            return fixture.violation("committed invocation missed its reply".into());
        }
    };
    let receipt = committed.receipt();
    if committed.response().as_ref() != b"emit"
        || receipt.ticket() != &ticket
        || receipt.state().commit_id != receipt.reply().commit_id
        || receipt.revision() != receipt.state().sequence + 1
    {
        return fixture
            .violation("invocation changed response or original publication evidence".into());
    }
    target.restart().await?;
    if target.store().invoke(&invocation).await? != committed {
        return fixture
            .violation("lost response reconciliation returned another publication".into());
    }
    if !matches!(
        target.store().enqueue_invoke(&changed).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ) {
        return fixture.violation("completed operation accepted changed bytes".into());
    }
    fixture.expect(&["r1"]).await?;
    Ok(())
}

/// A failed guest settles the original call with the same typed cause on every
/// host, including after restart, without publishing speculative guest state.
pub(crate) async fn invocation_failure<T: Backend + ?Sized>(
    target: &T,
    run: &str,
) -> Result<(), Failure> {
    use crate::invocation::Invocation;
    let mut fixture = Fixture::create(target, run, "invocation-failure").await?;
    fixture.send("clear").await?;
    let checkpoint = target
        .store()
        .load(&fixture.actor)
        .await?
        .state()
        .and_then(|state| state.checkpoint())
        .cloned();
    let invocation = Invocation::new(
        fixture.actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"failed"),
        Bytes::from_static(b"unknown"),
    )
    .map_err(Error::from)?;
    let failure = target.store().invoke(&invocation).await;
    let Err(Error::InvocationFailed { source, receipt }) = failure else {
        return fixture
            .violation("handler failure did not settle as a terminal invocation failure".into());
    };
    if source != GuestError::Handler("unknown command".into())
        || receipt.state().commit_id != receipt.reply().commit_id
        || receipt.ticket().invocation() != &invocation
    {
        return fixture
            .violation("handler failure changed its cause or publication identity".into());
    }
    if target
        .store()
        .load(&fixture.actor)
        .await?
        .state()
        .and_then(|state| state.checkpoint())
        != checkpoint.as_ref()
    {
        return fixture.violation("failed guest changed its committed checkpoint".into());
    }
    target.restart().await?;
    match target.store().invoke(&invocation).await {
        Err(Error::InvocationFailed {
            source: repeated,
            receipt: original,
        }) if source == repeated && receipt == original => {}
        _ => return fixture.violation("retry replaced the original committed failure".into()),
    }
    let successful = Invocation::new(
        fixture.actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"after-failure"),
        Bytes::from_static(b"clear"),
    )
    .map_err(Error::from)?;
    if target
        .store()
        .invoke(&successful)
        .await?
        .response()
        .as_ref()
        != b"clear"
        || target
            .store()
            .load(&fixture.actor)
            .await?
            .state()
            .and_then(|state| state.checkpoint())
            != checkpoint.as_ref()
    {
        return fixture
            .violation("failed invocation blocked later input or changed the checkpoint".into());
    }
    Ok(())
}

/// A targeted call cannot poison another actor or alias sharing its input.
/// Only the addressed subscription replies; all readers reach later input.
async fn shared_invocation<T: Backend + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    use crate::invocation::Invocation;
    let mut owner = Fixture::create(target, run, "shared-invocation").await?;
    let observer = Fixture::create(target, run, "shared-observer").await?;
    let store = target.store();
    let mut prepared = PreparedPublish::foreign(idempotency(&format!("{run}/shared/create"))?);
    for effect in owner
        .creation
        .take()
        .ok_or(Error::Invalid("shared creation missing"))?
    {
        prepared.join(effect)?;
    }
    let subscriptions = ["inbox", "mirror"]
        .into_iter()
        .map(|id| {
            (
                id.into(),
                Subscription {
                    path: owner.inbox.clone(),
                    cursor: 0,
                },
            )
        })
        .collect();
    for effect in store.create(
        &observer.actor,
        subscriptions,
        Some(Checkpoint {
            codec: codec(),
            bytes: serde_json::to_vec(&Ledger {
                outbox: observer.outbox.clone(),
                published: 0,
                reservations: None,
            })
            .map_err(|_| Error::Invalid("shared ledger does not encode"))?
            .into(),
        }),
    )? {
        prepared.join(effect)?;
    }
    prepared.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: owner.inbox.clone(),
        }],
        mutation: CommitMutation::Append {
            path: owner.inbox.clone(),
            records: vec![Bytes::from_static(b"clear").into()],
        },
    })?;
    store.submit(prepared).await?;
    let call = Invocation::new(
        owner.actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"shared-call"),
        Bytes::from_static(b"emit"),
    )
    .map_err(Error::from)?;
    let committed = store.invoke(&call).await?;
    let mut send = PreparedPublish::foreign(idempotency(&format!("{run}/shared/after"))?);
    send.join(Effect {
        conditions: vec![CommitCondition::Tail {
            path: owner.inbox.clone(),
            expected: 2,
        }],
        mutation: CommitMutation::Append {
            path: owner.inbox.clone(),
            records: vec![Bytes::from_static(b"emit").into()],
        },
    })?;
    for actor in [&owner.actor, &observer.actor] {
        send.join(store.activation(actor))?;
    }
    store.submit(send).await?;
    owner.expect(&["r1", "r2"]).await?;
    observer.expect(&["r2", "r2"]).await?;
    for subscription in ["inbox", "mirror"] {
        let state = store.load(&observer.actor).await?;
        if state.state().and_then(|state| state.cursor(subscription)) != Some(3) {
            return observer.violation("shared input stopped at a foreign invocation".into());
        }
    }
    if store.invoke(&call).await? != committed {
        return owner.violation("another shared reader replaced the original response".into());
    }
    Ok(())
}

/// Every backend uses the same reservation accounting and generation fences.
/// Expiry is a logical fence, never an assertion of external execution death.
async fn reserved<T: Target + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    use crate::reservation::{Decision, OwnerEvidence, Pool};
    let mut fixture = Fixture::create(target, run, "reserved").await?;
    fixture.send("pool 2").await?;
    wait_pool_used(&fixture, 0).await?;
    let pool = Pool::new(target.store(), &fixture.actor, "inbox");
    let owner = OwnerEvidence::new(
        Bytes::from_static(b"placement"),
        Bytes::from_static(b"admitted-owner-proof"),
    )
    .map_err(Error::from)?;
    let deadline = target.clock().now_unix_ms().saturating_add(BUDGET_MS);
    let first_id = Bytes::from_static(b"first");
    let first = pool
        .reserve(2, first_id.clone(), owner.clone(), deadline)
        .await?;
    let Decision::Granted(first_grant) = first.decision() else {
        return fixture.violation("explicit capacity was not granted".into());
    };
    let foreign_actor = ActorReference::new(
        b"conformance",
        b"actors",
        format!("{run}-foreign-pool").as_bytes(),
    )
    .map_err(|_| Error::Invalid("foreign pool fixture actor"))?;
    if !matches!(
        Pool::new(target.store(), &foreign_actor, "inbox")
            .release(first_grant)
            .await,
        Err(Error::Conflict)
    ) {
        return fixture.violation("grant released a different pool actor".into());
    }
    let another_root = Store::new(
        std::sync::Arc::clone(target.store().provider()),
        StreamPath::new(format!("{}/other-root", target.store().root())).map_err(Error::from)?,
    );
    if first_grant.state_path() != &target.store().path(&fixture.actor)?
        || !matches!(
            Pool::new(&another_root, &fixture.actor, "inbox")
                .release(first_grant)
                .await,
            Err(Error::Conflict)
        )
    {
        return fixture.violation("grant crossed the original Store root".into());
    }
    if pool
        .reserve(2, first_id.clone(), owner.clone(), deadline)
        .await?
        != first
    {
        return fixture.violation("reserve replay changed its decision or receipt".into());
    }
    if !matches!(
        pool.reserve(1, first_id.clone(), owner.clone(), deadline)
            .await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ) {
        return fixture.violation("reserve accepted changed units under one request id".into());
    }
    let another = OwnerEvidence::new(
        Bytes::from_static(b"another"),
        Bytes::from_static(b"another-proof"),
    )
    .map_err(Error::from)?;
    if !matches!(
        pool.reserve(2, first_id.clone(), another, deadline).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ) {
        return fixture.violation("reserve accepted changed owner evidence".into());
    }
    if pool
        .reserve(1, Bytes::from_static(b"full"), owner.clone(), deadline)
        .await?
        .decision()
        != &Decision::Exhausted
    {
        return fixture.violation("reservation exceeded explicit pool capacity".into());
    }
    // Derive the wrong-owner control from the actual returned grant.
    let mut altered =
        serde_json::to_value(first_grant).map_err(|_| Error::Invalid("grant control encode"))?;
    altered["owner"]["identity"] = serde_json::to_value(Bytes::from_static(b"wrong"))
        .map_err(|_| Error::Invalid("grant owner control encode"))?;
    let altered =
        serde_json::from_value(altered).map_err(|_| Error::Invalid("grant control decode"))?;
    if pool.release(&altered).await?.decision() != &Decision::Stale {
        return fixture.violation("another owner released the original grant".into());
    }
    wait_pool_used(&fixture, 2).await?;
    let released = pool.release(first_grant).await?;
    if released.decision() != &Decision::Released || pool.release(first_grant).await? != released {
        return fixture
            .violation("release failed or replay changed its original publication".into());
    }
    let second = pool
        .reserve(2, Bytes::from_static(b"second"), owner.clone(), deadline)
        .await?;
    let Decision::Granted(second_grant) = second.decision() else {
        return fixture.violation("released capacity was not reusable".into());
    };
    if second_grant.generation() <= first_grant.generation()
        || pool.release(first_grant).await? != released
    {
        return fixture
            .violation("capacity reuse lost the generation fence or original reply".into());
    }
    wait_pool_used(&fixture, 2).await?;
    target.pause(&fixture.actor, true).await?;
    target.restart().await?;
    // A paused owner retains its due cleanup, including across physical reopen.
    target.clock().sleep_until(deadline).await;
    wait_pool_used(&fixture, 2).await?;
    target.pause(&fixture.actor, false).await?;
    wait_pool_used(&fixture, 0).await?;
    let third = pool
        .reserve(
            2,
            Bytes::from_static(b"third"),
            owner.clone(),
            target.clock().now_unix_ms().saturating_add(BUDGET_MS),
        )
        .await?;
    let Decision::Granted(third_grant) = third.decision() else {
        return fixture.violation("durable cleanup did not free capacity".into());
    };
    if third_grant.generation() <= second_grant.generation()
        || pool.release(second_grant).await?.decision() != &Decision::Stale
    {
        return fixture.violation("expired generation released its replacement".into());
    }
    wait_pool_used(&fixture, 2).await?;
    if pool.reserve(2, first_id, owner, deadline).await? != first {
        return fixture
            .violation("cleanup changed a retained original reservation decision".into());
    }
    Ok(())
}

/// Observe only committed checkpoint records; this does not poll or advance work.
async fn wait_pool_used<T: Target + ?Sized>(
    fixture: &Fixture<'_, T>,
    expected: u64,
) -> Result<(), Failure> {
    fn used(state: &crate::local::State) -> Result<Option<u64>, Error> {
        let checkpoint = state
            .checkpoint()
            .ok_or(Error::Invalid("reservation checkpoint is missing"))?;
        let ledger: Ledger = serde_json::from_slice(&checkpoint.bytes)
            .map_err(|_| Error::Invalid("reservation checkpoint does not decode"))?;
        Ok(ledger.reservations.map(|book| book.used()))
    }
    let store = fixture.target.store();
    let observed = store
        .wait_applied(
            &fixture.actor,
            AppliedCut::empty("inbox".into(), fixture.inbox.clone())?,
        )
        .await?;
    if used(
        observed
            .snapshot()
            .state()
            .ok_or(Error::Invalid("reservation actor is missing"))?,
    )? == Some(expected)
    {
        return Ok(());
    }
    let records = store
        .provider()
        .follow(acyclic_stream::FollowRequest {
            preceding: Some(observed.commit_id()),
            ..acyclic_stream::FollowRequest::new(
                observed.snapshot().path().clone(),
                observed.revision(),
            )
        })
        .await
        .map_err(Error::from)?;
    let mut records = acyclic_stream::records_only(records);
    let wait = async {
        while let Some(record) = records.next().await {
            let state = crate::local::State::decode(&record.map_err(Error::from)?.value)?;
            if used(&state)? == Some(expected) {
                return Ok(());
            }
        }
        fixture.violation("reservation state observation closed".into())
    };
    let deadline = fixture.target.clock().sleep_until(
        fixture
            .target
            .clock()
            .now_unix_ms()
            .saturating_add(BUDGET_MS),
    );
    match futures::future::select(Box::pin(wait), deadline).await {
        futures::future::Either::Left((result, _)) => result,
        futures::future::Either::Right(_) => {
            fixture.violation(format!("reservation units did not reach {expected}"))
        }
    }
}

/// A committed input cut is observed through the same original state stream.
/// Waiting neither executes a paused input nor weakens its source anchor.
async fn applied<T: Target + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    let mut fixture = Fixture::create(target, run, "applied").await?;
    fixture.send("emit").await?;
    fixture.expect(&["r0"]).await?;
    target.pause(&fixture.actor, true).await?;
    fixture.send("emit").await?;
    let provider = target.store().provider();
    let input = provider
        .read(acyclic_stream::ReadRequest {
            key: None,
            path: fixture.inbox.clone(),
            from: 1,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .map_err(Error::from)?
        .next()
        .await
        .ok_or(Error::Invalid("applied fixture input is missing"))?
        .map_err(Error::from)?;
    let cut = AppliedCut::through("inbox".into(), fixture.inbox.clone(), &input)?;
    let mut wait = Box::pin(target.store().wait_applied(&fixture.actor, cut.clone()));
    if futures::poll!(wait.as_mut()).is_ready() {
        return fixture
            .violation("wait applied completed before the paused input was consumed".into());
    }
    if fixture.cursor().await? != Some(1) {
        return fixture.violation("wait applied consumed a paused input".into());
    }
    target.pause(&fixture.actor, false).await?;
    let deadline = target
        .clock()
        .sleep_until(target.clock().now_unix_ms().saturating_add(BUDGET_MS));
    let observed = match futures::future::select(wait, deadline).await {
        futures::future::Either::Left((observed, _)) => observed?,
        futures::future::Either::Right(_) => {
            return fixture.violation("wait applied missed the committed wake".into());
        }
    };
    if observed.cut() != &cut
        || observed
            .snapshot()
            .state()
            .and_then(|state| state.cursor("inbox"))
            != Some(2)
    {
        return fixture.violation(
            "wait applied changed its original cut or returned an unconsumed state".into(),
        );
    }
    fixture.expect(&["r0", "r1"]).await?;
    target.restart().await?;
    let reopened = target
        .store()
        .wait_applied(&fixture.actor, cut.clone())
        .await?;
    if reopened.cut() != &cut || reopened.revision() < observed.revision() {
        return fixture.violation("restart lost the applied cut".into());
    }
    let wrong_path = AppliedCut::empty("inbox".into(), fixture.outbox.clone())?;
    let missing = AppliedCut::empty("missing".into(), fixture.inbox.clone())?;
    for wrong in [wrong_path, missing] {
        if !matches!(
            target.store().wait_applied(&fixture.actor, wrong).await,
            Err(Error::Conflict)
        ) {
            return fixture
                .violation("an absent or differently bound subscription satisfied the cut".into());
        }
    }
    let mut foreign_input = input;
    let mut anchor = *foreign_input.commit_id.as_bytes();
    anchor[0] ^= 1;
    foreign_input.commit_id = acyclic_stream::CommitId::from_bytes(anchor);
    if !matches!(
        target
            .store()
            .wait_applied(
                &fixture.actor,
                AppliedCut::through("inbox".into(), fixture.inbox.clone(), &foreign_input)?
            )
            .await,
        Err(Error::Stream(StreamError::PrefixNotRetained))
    ) {
        return fixture.violation("a consumed cursor accepted a different input history".into());
    }
    Ok(())
}

/// A later control interrupts the current handler without consuming the
/// intervening record or the control. Restart preserves all three cursors.
async fn interrupted<T: Target + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    let mut actor = Fixture::create(target, run, "interrupted").await?;
    actor.send("wait").await?;
    actor.send("emit").await?;
    actor.send("cancel").await?;
    actor.expect(&["c0", "r1"]).await?;
    target.restart().await?;
    actor.send("emit").await?;
    actor.expect(&["c0", "r1", "r3"]).await
}

/// Records are consumed in order, exactly once, by an actor nobody placed.
async fn ordered<T: Backend + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    let mut ordered = Fixture::create(target, run, "ordered").await?;
    for _ in 0..3 {
        ordered.send("emit").await?;
    }
    ordered.expect(&["r0", "r1", "r2"]).await?;
    if ordered.cursor().await? != Some(3) {
        return ordered.violation("cursor did not advance with the outbox".into());
    }
    Ok(())
}

/// An alarm saved with the checkpoint fires once; Clear invalidates the
/// armed generation; replacing fires only the newest generation.
async fn alarms<T: Backend + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    let mut alarm = Fixture::create(target, run, "alarm").await?;
    alarm.send(&format!("set {ALARM_MS}")).await?;
    alarm.expect(&["a1"]).await?;

    let mut cleared = Fixture::create(target, run, "cleared").await?;
    cleared.send(&format!("set {ALARM_MS}")).await?;
    cleared.send("clear").await?;
    cleared.send("emit").await?;
    cleared.expect(&["r2"]).await?;

    let mut replaced = Fixture::create(target, run, "replaced").await?;
    replaced.send(&format!("set {}", 10 * ALARM_MS)).await?;
    replaced.send(&format!("set {ALARM_MS}")).await?;
    replaced.expect(&["a2"]).await
}

/// Pause holds input; resume delivers it.
async fn paused<T: Backend + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    let mut paused = Fixture::create(target, run, "paused").await?;
    paused.send("emit").await?;
    paused.expect(&["r0"]).await?;
    target.pause(&paused.actor, true).await?;
    paused.send("emit").await?;
    if paused.observe(2, QUIET_MS).await?.len() != 1 {
        return paused.violation("a paused actor consumed input".into());
    }
    target.pause(&paused.actor, false).await?;
    paused.expect(&["r0", "r1"]).await
}

/// A restart keeps the checkpoint, cursor and pending alarm, with no
/// duplicate or lost publication.
async fn restarted<T: Backend + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    let mut restarted = Fixture::create(target, run, "restarted").await?;
    restarted.send("emit").await?;
    restarted.expect(&["r0"]).await?;
    restarted.send(&format!("set {}", 2 * ALARM_MS)).await?;
    // Let the set commit before the restart.
    if restarted.observe(2, ALARM_MS / 2).await?.len() != 1 {
        return restarted.violation("unexpected output before restart".into());
    }
    target.restart().await?;
    restarted.send("emit").await?;
    restarted.expect(&["r0", "r2", "a1"]).await
}

/// Measurements from the common ordered-delivery workload.
#[derive(Debug)]
pub struct Measurement {
    /// Committed inbox deliveries and corresponding outbox publications.
    pub deliveries: usize,
    /// Wall-clock time from the first measured send to the final observed
    /// publication, at millisecond resolution on native and Wasm hosts.
    pub elapsed: std::time::Duration,
    /// Provider calls during the measured steady-state workload.
    pub requests: Requests,
}

/// Measure the same 256-delivery workload for every backend. Setup and warmup
/// are excluded; every emitted sequence is checked before returning a sample.
/// Use a fresh backend, independently of [`verify`]. This is opt-in only.
pub async fn bench<T: Backend + ?Sized>(target: &T) -> Result<Measurement, Failure> {
    const DELIVERIES: usize = 256;
    let mut fixture = Fixture::create(target, target.namespace(), "bench").await?;
    fixture.send("emit").await?;
    if fixture.observe(1, BUDGET_MS).await? != ["r0"] {
        return fixture.violation("benchmark warmup did not publish r0".into());
    }
    let ledger = target.requests();
    let before = ledger.requests();
    let clock = acyclic_stream::SystemUnixMillisClock;
    let started = clock.now_unix_millis();
    for _ in 0..DELIVERIES {
        fixture.send("emit").await?;
    }
    let observed = fixture.observe(DELIVERIES + 1, BUDGET_MS).await?;
    let elapsed = std::time::Duration::from_millis(clock.now_unix_millis().saturating_sub(started));
    let expected = (0..=DELIVERIES)
        .map(|i| format!("r{i}"))
        .collect::<Vec<_>>();
    if observed != expected {
        return fixture.violation("benchmark lost, reordered or duplicated a delivery".into());
    }
    Ok(Measurement {
        deliveries: DELIVERIES,
        elapsed,
        requests: ledger.requests().since(before),
    })
}
