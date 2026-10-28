//! One Actors conformance suite for every host: `local::Runtime` and the
//! hosted service run the same checks through the same Stream-level surface.
//!
//! The host under test runs [`Package`] for every actor under the target's
//! state root. The suite creates actors with the companion create mutation,
//! appends commands to their inboxes and observes their committed outboxes;
//! only pause/resume and restart go through [`Backend`].
use futures::TryFutureExt as _;
use crate::addressing::ActorReference;
use crate::guest::{
    AlarmChange, Checkpoint, CodecIdentity, Delivery, Effect, GuestError, GuestFuture, Handler,
    Manifest, Transition, Trigger,
};
use crate::local::{Clock, Error, PreparedPublish, Store, Subscription};
use acyclic_stream::testing::{RequestLedger, Requests};
use acyclic_stream::{
    AppendRecord, BoxProviderFuture, CommitCondition, CommitMutation, IdempotencyKey,
    ProviderPlatform, StreamError, StreamPath, StreamProvider, UnixMillisClock as _,
};
use bytes::Bytes;
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
    fn restart(&self) -> BoxProviderFuture<'_, Result<(), Error>>;
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
                subscriptions: vec!["inbox".into()],
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
            let (alarm, output) = match (&delivery.trigger, &delivery.record) {
                (Trigger::Alarm { generation, .. }, _) => {
                    (AlarmChange::Keep, Some(format!("a{generation}")))
                }
                (Trigger::Record { sequence, .. }, Some(record)) => {
                    let command = std::str::from_utf8(&record.value).map_err(guest)?;
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
            .follow(acyclic_stream::FollowRequest::new(self.target.store().path(&self.actor)?, 0)).map_ok(acyclic_stream::records_only)
            .await
            .map_err(Error::from)?;
        let mut follow = loop {
            match provider
                .follow(acyclic_stream::FollowRequest::new(self.outbox.clone(), 0)).map_ok(acyclic_stream::records_only)
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
    alarms(target, run).await?;
    paused(target, run).await?;
    interrupted(target, run).await?;
    restarted(target, run).await
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
