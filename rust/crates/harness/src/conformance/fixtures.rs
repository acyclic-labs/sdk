//! Shared host provisioning for in-process and public-client scenarios.
//!
//! An endpoint fixture can install [`Driver`], create [`adopted`], and send a
//! message with the same intent as [`request`]. Both routes exercise the same
//! turn and produce the same canonical intent and outcome. Protocol fixtures must capture
//! their actual host binding, invocation and reply; these helpers invent none.

use crate::conformance::Result;
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_harness_core::agent::*;
use acyclic_stream::UnixMillisClock as _;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// The original issuance and verification clock, advanced at an exact cut.
pub(super) struct AuthorityClock(AtomicU64);

impl AuthorityClock {
    pub(super) fn new() -> Self {
        Self(AtomicU64::new(
            acyclic_stream::SystemUnixMillisClock.now_unix_millis(),
        ))
    }

    pub(super) fn advance_to(&self, millis: u64) {
        self.0.store(millis, Ordering::SeqCst);
    }
}

impl acyclic_stream::UnixMillisClock for AuthorityClock {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Save a real spawn without activating its child. Endpoint hosts can then
/// adopt that child through their public placement API, including after reopen.
pub async fn spawned<P: acyclic_harness_core::store::JournalProvider + ?Sized>(
    provider: Arc<P>,
    within: ActorReference,
) -> Result<AgentPath> {
    let handler = Arc::new(AgentHandler::new(
        actor_store(&provider)?,
        Driver,
        ActorLimits::new(1_000, 1 << 20, 1 << 16)?,
    )?);
    let host = AgentHost::hosting(
        acyclic_actors::guest::Requests::default(),
        provider,
        within,
        handler,
        Arc::new(SystemClock),
        None,
    )?;
    let root = host
        .create(&IntentId::try_from("fixture-root")?, adopted())
        .await?;
    host.drain(&root).await?;
    host.send(&root, &sender(), &request("spawn")?).await?;
    host.drain(&root).await?;
    Ok(root.child(&AgentId::derive(&root.id(), "child"))?)
}

/// Initial root used by creation, exact replay, spawn and restart scenarios.
pub fn adopted() -> ForkAdopted {
    ForkAdopted {
        profile: None,
        agent: AgentId::from_bytes([1; 16]),
        parent: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    }
}

/// Caller identity used by direct host controls.
pub fn sender() -> Sender {
    Sender::User {
        principal: "conformance".into(),
    }
}

/// File-free input for a host-side scenario. A public client can instead submit
/// a real message with this same intent; [`Driver`] applies the same oracle.
pub fn request(id: &str) -> Result<InboxRecord> {
    Ok(InboxRecord::Approval(Approval {
        id: IntentId::try_from(id)?,
    }))
}

/// Route role-selected models through the fixture's original deterministic provider.
pub(super) fn models(
    provider: Arc<dyn crate::model::ModelProvider>,
) -> crate::Result<(crate::model::ModelBinding, Arc<crate::model::ModelResolver>)> {
    let model = crate::model::Model::new("conformance".into(), "scripted".into(), "1".into())?;
    let selected = provider.clone();
    let models = crate::model::ModelResolver::new("conformance/scripted", move |selection| {
        let (family, name) = selection
            .split_once('/')
            .ok_or_else(|| crate::Error::Invalid("expected fixture provider/model".into()))?;
        Ok((
            crate::model::Model::new(family.into(), name.into(), "1".into())?,
            selected.clone(),
        ))
    });
    Ok(((model, provider), Arc::new(models)))
}

pub(super) fn entry(
    agent: &AgentState,
    queued: Queued,
    outcome: TurnOutcome,
) -> std::result::Result<ConversationRecord, Error> {
    ConversationEntry {
        inbox: queued.inbox,
        input: queued.input,
        output: super::vectors::prompt().map_err(|error| Error::Turn(Box::new(error)))?,
        outcome,
        goal: None,
        observed_through: 0,
        announcements: Vec::new(),
        tool_results: None,
        steering: None,
        notices: None,
    }
    .with_observed_prefix(agent.steered_through)
    .record()
}

/// Deterministic turn shared by host and endpoint fixtures. Every intent publishes
/// its canonical conversation entry; `spawn` also forks `child` at the saved cut;
/// `respawn` requests that same child with a conflicting prompt. A host fixture
/// can use delegated requests and a public client can use ordinary messages.
pub struct Driver;

impl TurnDriver for Driver {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let queued = input.ok_or(Error::Record("missing conformance input"))?;
            let id = match &queued.input {
                TurnInput::Approval { id } | TurnInput::Message { id, .. } => id.clone(),
                _ => return Err(Error::Record("unexpected conformance input")),
            };
            let mut outs = vec![Out::Conversation(entry(
                agent,
                queued,
                TurnOutcome::Completed,
            )?)];
            if matches!(id.as_str(), "spawn" | "respawn") {
                outs.push(Out::Spawn {
                    profile: None,
                    intent: IntentId::try_from("child")?.into(),
                    brief: match id.as_str() {
                        "respawn" => Some(
                            super::vectors::prompt()
                                .map_err(|error| Error::Turn(Box::new(error)))?,
                        ),
                        _ => None,
                    },
                    volumes: Vec::new(),
                    history: None,
                });
            }
            Ok(TurnStep {
                checkpoint: None,
                outs,
                wait: Wait::Input,
                drops: None,
            })
        })
    }
}
