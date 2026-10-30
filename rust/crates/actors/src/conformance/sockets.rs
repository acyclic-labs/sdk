//! One actor socket conformance suite for every socket host: the local
//! runtime's `local::Sockets` and the hosted service run the same cases.
//!
//! The host runs [`SocketPackage`] for every actor under the backend's store
//! root. The suite creates actors with the companion create mutation, opens
//! sockets as a peer through [`SocketBackend::connect`] and reads each actor's
//! journal: one committed outbox record per socket event it was delivered.
use super::{BUDGET_MS, Failure};
use crate::addressing::ActorReference;
use crate::guest::{
    AlarmChange, Checkpoint, CodecIdentity, Delivery, Effect, GuestError, GuestEventResult,
    GuestFuture, GuestSocketEvent, Handler, Manifest, SocketCloseInitiator, SocketHost,
    SocketPayload, Transition, WebSocketAutoResponse, WebSocketId,
};
use crate::local::{Clock, Error, PreparedPublish, Store, Subscription};
use crate::sockets::{SocketFrame, SocketOpen, socket_reply};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    AppendRecord, BoxProviderFuture, CommitCondition, CommitMutation, IdempotencyKey,
    ProviderPlatform, StreamError, StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::{FutureExt as _, StreamExt as _, TryFutureExt as _};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// One peer of an open socket.
pub trait Peer: ProviderPlatform {
    /// Send one frame to the host.
    fn send(&self, frame: SocketFrame) -> BoxProviderFuture<'_, Result<(), Error>>;
    /// The next frame from the host; `None` once the host ended the socket.
    fn next(&mut self) -> BoxProviderFuture<'_, Result<Option<SocketFrame>, Error>>;
}

/// The socket host under test, already running [`SocketPackage`] for every
/// actor under `store().root()`.
pub trait SocketBackend: ProviderPlatform {
    /// Stream provider holding actor state, inboxes and journals.
    type Provider: StreamProvider + ?Sized;
    /// The store whose root the host watches.
    fn store(&self) -> &Store<Self::Provider>;
    /// The host clock.
    fn clock(&self) -> &dyn Clock;
    /// Open one socket as a peer, through the host's own transport.
    fn connect(&self, open: SocketOpen) -> BoxProviderFuture<'_, Result<Box<dyn Peer>, Error>>;
    /// Crash and restart the host. Durable state survives; sockets do not.
    fn restart(&self) -> BoxProviderFuture<'_, Result<(), Error>>;
}

/// The socket package's checkpoint codec.
pub fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "acyclic.actors.conformance.sockets".into(),
        version: "1".into(),
    }
}

#[derive(Serialize, Deserialize)]
struct Journal {
    outbox: StreamPath,
    published: u64,
}

/// Reference socket package. It accepts every Open with tag `room` and
/// attachment `a`, except app channel `reject`, which it rejects with 403.
/// Peer text commands: `echo <text>` sends `<text>` back; `close` closes with
/// 4000; `auto` installs the `ping`/`pong` auto-response; `state` sends back
/// `<tags>|<attachment>|<sockets tagged room>`. It journals every socket event
/// and every plain inbox record.
pub struct SocketPackage {
    manifest: Manifest,
    host: Arc<dyn SocketHost>,
}

impl SocketPackage {
    /// The package over its host's socket effects.
    pub fn new(host: Arc<dyn SocketHost>) -> Result<Self, crate::domain::DomainError> {
        Ok(Self {
            manifest: Manifest {
                code: crate::domain::CodeSha256::try_from(vec![0xa5; 32])?,
                codec: codec(),
                interrupts: Vec::new(),
                subscriptions: vec!["inbox".into()],
                bindings: Vec::new(),
                limits: crate::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            },
            host,
        })
    }

    /// Apply one peer command; the journal line it records.
    fn command(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        text: &str,
    ) -> Result<(), GuestError> {
        let reply = |text: String| {
            self.host
                .web_socket_send(actor, socket, SocketPayload::Text { text })
        };
        match text.split_once(' ') {
            Some(("echo", rest)) => reply(rest.into()),
            None if text == "close" => self.host.web_socket_close(actor, socket, 4000, "bye"),
            None if text == "auto" => self.host.set_web_socket_auto_response(
                actor,
                Some(WebSocketAutoResponse {
                    request: "ping".into(),
                    response: "pong".into(),
                }),
            ),
            None if text == "state" => {
                let tags = self.host.get_tags(actor, socket)?.join(",");
                let attachment = self
                    .host
                    .deserialize_attachment(actor, socket)?
                    .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                    .unwrap_or_default();
                let tagged = self.host.get_web_sockets(actor, Some("room"))?.len();
                reply(format!("{tags}|{attachment}|{tagged}"))
            }
            _ => Ok(()),
        }
    }
}

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

fn initiator(initiator: &SocketCloseInitiator) -> &'static str {
    match initiator {
        SocketCloseInitiator::Peer => "peer",
        SocketCloseInitiator::Actor => "actor",
        SocketCloseInitiator::Runtime => "runtime",
    }
}

impl Handler for SocketPackage {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let checkpoint = checkpoint.ok_or_else(|| guest("missing journal"))?;
            let mut journal: Journal = serde_json::from_slice(&checkpoint.bytes).map_err(guest)?;
            let mut response = None;
            let line = match delivery.socket()? {
                Some(socket) => {
                    let id = WebSocketId(socket.connection_id);
                    match socket.event {
                        GuestSocketEvent::Open { app_channel, .. } => {
                            let verdict = if app_channel.as_deref() == Some("reject") {
                                GuestEventResult::SocketReject {
                                    status: 403,
                                    reason: "rejected".into(),
                                }
                            } else {
                                GuestEventResult::SocketAccept {
                                    subprotocol: None,
                                    attachment: Bytes::from_static(b"a"),
                                    tags: vec!["room".into()],
                                }
                            };
                            response = Some(socket_reply(&verdict)?);
                            "open".to_owned()
                        }
                        GuestSocketEvent::Message {
                            payload: SocketPayload::Text { text },
                            ..
                        } => {
                            self.command(&delivery.actor, &id, &text)?;
                            format!("message {text}")
                        }
                        GuestSocketEvent::Message { .. } => "message binary".to_owned(),
                        GuestSocketEvent::Close {
                            code,
                            initiator: by,
                            ..
                        } => format!("close {} {code}", initiator(&by)),
                        GuestSocketEvent::Error { code, .. } => format!("error {code}"),
                        _ => "other".to_owned(),
                    }
                }
                None => match &delivery.record {
                    Some(record) => String::from_utf8_lossy(&record.value).into_owned(),
                    None => return Err(guest("alarm without a socket event")),
                },
            };
            let effect = Effect {
                conditions: vec![CommitCondition::Tail {
                    path: journal.outbox.clone(),
                    expected: journal.published,
                }],
                mutation: CommitMutation::Append {
                    path: journal.outbox.clone(),
                    records: vec![AppendRecord::from(Bytes::from(line))],
                },
            };
            journal.published += 1;
            Ok(Transition {
                response,
                participant: None,
                checkpoint: Checkpoint {
                    codec: codec(),
                    bytes: serde_json::to_vec(&journal).map_err(guest)?.into(),
                },
                alarm: AlarmChange::Keep,
                effects: vec![effect],
            })
        })
    }
}

struct Case<'a, T: SocketBackend + ?Sized> {
    target: &'a T,
    case: &'static str,
    actor: ActorReference,
    inbox: StreamPath,
    outbox: StreamPath,
    marks: u64,
}

impl<'a, T: SocketBackend + ?Sized> Case<'a, T> {
    /// A fresh actor whose creation commit also creates its journal.
    async fn create(target: &'a T, case: &'static str) -> Result<Self, Failure> {
        let actor = ActorReference::new(b"conformance", b"sockets", case.as_bytes())
            .map_err(|_| Error::Invalid("conformance actor id"))?;
        let inbox = StreamPath::new(format!("conformance/sockets/{case}/inbox"))?;
        let outbox = StreamPath::new(format!("conformance/sockets/{case}/journal"))?;
        let journal = Journal {
            outbox: outbox.clone(),
            published: 1,
        };
        let store = target.store();
        let mut prepared = PreparedPublish::foreign(key(&format!("{outbox}/create"))?);
        for effect in store.create(
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
                bytes: serde_json::to_vec(&journal)
                    .map_err(|_| Error::Invalid("conformance journal does not encode"))?
                    .into(),
            }),
        )? {
            prepared.join(effect)?;
        }
        prepared.join(Effect {
            conditions: vec![CommitCondition::Absent {
                path: outbox.clone(),
            }],
            mutation: CommitMutation::Append {
                path: outbox.clone(),
                records: vec![AppendRecord::from(Bytes::from_static(b"created"))],
            },
        })?;
        PreparedPublish::settle(
            &store
                .provider()
                .commit(prepared.into_request()?)
                .await
                .map_err(Error::from)?,
        )?;
        Ok(Self {
            target,
            case,
            actor,
            inbox,
            outbox,
            marks: 0,
        })
    }

    async fn connect(&self, app_channel: Option<&str>) -> Result<Box<dyn Peer>, Failure> {
        Ok(self
            .target
            .connect(SocketOpen {
                actor: self.actor.clone(),
                subscription: "inbox".into(),
                app_channel: app_channel.map(str::to_owned),
                subprotocols: Vec::new(),
                headers: Vec::new(),
                resume_after: 0,
                reconnected: false,
            })
            .await?)
    }

    /// Append a plain inbox record; it is journaled after every socket event
    /// committed before it, so it bounds what the socket delivered.
    async fn mark(&mut self) -> Result<String, Failure> {
        self.marks += 1;
        let mark = format!("mark {}", self.marks);
        let store = self.target.store();
        let mut prepared = PreparedPublish::foreign(key(&format!("{}/{mark}", self.inbox))?);
        prepared.join(store.activation(&self.actor))?;
        prepared.join(Effect {
            conditions: Vec::new(),
            mutation: CommitMutation::Append {
                path: self.inbox.clone(),
                records: vec![AppendRecord::from(Bytes::from(mark.clone()))],
            },
        })?;
        PreparedPublish::settle(
            &store
                .provider()
                .commit(prepared.into_request()?)
                .await
                .map_err(Error::from)?,
        )?;
        Ok(mark)
    }

    /// Follow the journal until it holds `count` events after creation.
    async fn journal(&self, count: usize) -> Result<Vec<String>, Failure> {
        let clock = self.target.clock();
        let mut deadline = clock.sleep_until(clock.now_unix_ms().saturating_add(BUDGET_MS));
        let mut follow = self
            .target
            .store()
            .provider()
            .follow(acyclic_stream::FollowRequest::new(self.outbox.clone(), 1))
            .map_ok(acyclic_stream::records_only)
            .await
            .map_err(Error::from)?;
        let mut lines = Vec::new();
        while lines.len() < count {
            futures::select_biased! {
                record = follow.next().fuse() => match record {
                    Some(Ok(record)) => lines.push(String::from_utf8_lossy(&record.value).into_owned()),
                    Some(Err(error)) => return Err(Error::from(error).into()),
                    None => break,
                },
                () = (&mut deadline).fuse() => break,
            }
        }
        Ok(lines)
    }

    /// The journal reaches exactly `expected`, and a fresh mark lands right
    /// after it: nothing else was delivered before the mark committed.
    async fn expect(&mut self, expected: &[&str]) -> Result<(), Failure> {
        let mut wanted: Vec<String> = expected.iter().map(|line| (*line).to_owned()).collect();
        let observed = self.journal(wanted.len()).await?;
        if observed != wanted {
            return self.violation(format!("journal {observed:?}, expected {wanted:?}"));
        }
        wanted.push(self.mark().await?);
        let observed = self.journal(wanted.len()).await?;
        if observed != wanted {
            return self.violation(format!("journal {observed:?}, expected {wanted:?}"));
        }
        Ok(())
    }

    async fn frame(&self, peer: &mut Box<dyn Peer>) -> Result<Option<SocketFrame>, Failure> {
        let clock = self.target.clock();
        futures::select_biased! {
            frame = peer.next().fuse() => Ok(frame?),
            () = clock.sleep_until(clock.now_unix_ms().saturating_add(BUDGET_MS)).fuse() => {
                self.violation("no frame from the host".into())
            }
        }
    }

    async fn expect_frame(
        &self,
        peer: &mut Box<dyn Peer>,
        expected: Option<SocketFrame>,
    ) -> Result<(), Failure> {
        let frame = self.frame(peer).await?;
        if frame != expected {
            return self.violation(format!("frame {frame:?}, expected {expected:?}"));
        }
        Ok(())
    }

    fn violation<R>(&self, detail: String) -> Result<R, Failure> {
        Err(Failure::Violation {
            case: self.case,
            detail,
        })
    }
}

fn key(value: &str) -> Result<IdempotencyKey, Error> {
    Ok(IdempotencyKey::new(value.as_bytes().to_vec())?)
}

fn text(text: &str) -> SocketFrame {
    SocketFrame::Message(SocketPayload::Text { text: text.into() })
}

fn close(code: u16, reason: &str, initiator: SocketCloseInitiator) -> Option<SocketFrame> {
    Some(SocketFrame::Close {
        code,
        reason: reason.into(),
        initiator,
    })
}

const ACCEPTED: Option<SocketFrame> = Some(SocketFrame::Accepted { subprotocol: None });

/// Run every case against the backend's fresh disposable resources.
pub async fn verify<T: SocketBackend + ?Sized>(target: &T) -> Result<(), Failure> {
    // Open, ordered messages, actor sends and the peer's close.
    let mut case = Case::create(target, "echo").await?;
    let mut peer = case.connect(None).await?;
    case.expect_frame(&mut peer, ACCEPTED).await?;
    for word in ["one", "two"] {
        peer.send(text(&format!("echo {word}"))).await?;
        case.expect_frame(&mut peer, Some(text(word))).await?;
    }
    peer.send(SocketFrame::Close {
        code: 1000,
        reason: "done".into(),
        initiator: SocketCloseInitiator::Peer,
    })
    .await?;
    case.expect_frame(&mut peer, close(1000, "done", SocketCloseInitiator::Peer))
        .await?;
    case.expect_frame(&mut peer, None).await?;
    case.expect(&[
        "open",
        "message echo one",
        "message echo two",
        "close peer 1000",
    ])
    .await?;

    // Tags and attachment from the accept stay with the host, outside the
    // actor's heap and checkpoint.
    let mut case = Case::create(target, "state").await?;
    let mut peer = case.connect(None).await?;
    case.expect_frame(&mut peer, ACCEPTED).await?;
    peer.send(text("state")).await?;
    case.expect_frame(&mut peer, Some(text("room|a|1"))).await?;
    drop(peer);
    case.expect(&["open", "message state", "close peer 1006"])
        .await?;

    // A rejected open is still followed by exactly one Close.
    let mut case = Case::create(target, "reject").await?;
    let mut peer = case.connect(Some("reject")).await?;
    case.expect_frame(
        &mut peer,
        Some(SocketFrame::Rejected {
            status: 403,
            reason: "rejected".into(),
        }),
    )
    .await?;
    case.expect_frame(&mut peer, None).await?;
    case.expect(&["open", "close actor 1000"]).await?;

    // The actor closes; the peer's later frames reach nobody.
    let mut case = Case::create(target, "actor-close").await?;
    let mut peer = case.connect(None).await?;
    case.expect_frame(&mut peer, ACCEPTED).await?;
    peer.send(text("close")).await?;
    case.expect_frame(&mut peer, close(4000, "bye", SocketCloseInitiator::Actor))
        .await?;
    case.expect(&["open", "message close", "close actor 4000"])
        .await?;

    // Auto-response answers without delivering anything to the actor.
    let mut case = Case::create(target, "auto").await?;
    let mut peer = case.connect(None).await?;
    case.expect_frame(&mut peer, ACCEPTED).await?;
    peer.send(text("auto")).await?;
    peer.send(text("echo ready")).await?;
    case.expect_frame(&mut peer, Some(text("ready"))).await?;
    peer.send(text("ping")).await?;
    case.expect_frame(&mut peer, Some(text("pong"))).await?;
    peer.send(text("echo after")).await?;
    case.expect_frame(&mut peer, Some(text("after"))).await?;
    case.expect(&[
        "open",
        "message auto",
        "message echo ready",
        "message echo after",
    ])
    .await?;
    drop(peer);

    // A crash closes every socket it held exactly once, with Close(Runtime).
    let mut case = Case::create(target, "recovered").await?;
    let mut peer = case.connect(None).await?;
    case.expect_frame(&mut peer, ACCEPTED).await?;
    target.restart().await?;
    case.expect(&["open", "close runtime 1012"]).await?;
    // The crashed host's peer leaving later finds the socket already closed.
    drop(peer);
    case.expect(&["open", "close runtime 1012", "mark 1"]).await
}

/// Register the socket suite using an async factory for a fresh running host.
pub use crate::actors_socket_conformance_suite as suite;

/// Implementation of [`suite!`].
#[macro_export]
macro_rules! actors_socket_conformance_suite {
    ($name:ident, $factory:path) => {
        #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
        #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
        async fn $name() -> std::result::Result<(), Box<dyn std::error::Error>> {
            let backend = $factory().await?;
            $crate::conformance::sockets::verify(&backend).await?;
            Ok(())
        }
    };
}

impl From<StreamError> for Failure {
    fn from(error: StreamError) -> Self {
        Self::Target(error.into())
    }
}
