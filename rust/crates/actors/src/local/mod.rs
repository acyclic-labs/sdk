//! Runtime-owned socket state, independent of actor heaps and the harness.
use crate::{domain as d, wire as w};
use std::collections::{BTreeMap, VecDeque};

mod connection;
mod transaction;

/// Failure applying a local actor socket operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Input failed the canonical Actors domain validation.
    #[error(transparent)]
    Domain(#[from] d::DomainError),
    /// A connection is missing, closed, or has not been accepted.
    #[error("connection is unavailable")]
    Connection,
    /// A bounded queue is full; the transport must stop reading.
    #[error("socket backpressure; stop reading the source")]
    Backpressure,
    /// A checkpoint or offset is stale, conflicting, or out of order.
    #[error("socket checkpoint conflict")]
    Conflict,
    /// Offset, generation or accounting cannot advance without overflow.
    #[error("socket counter exhausted")]
    Exhausted,
    /// Timestamp moved backwards.
    #[error("runtime time moved backwards")]
    Clock,
}

/// An application delivery awaiting an actor checkpoint.
#[derive(Clone, Debug, PartialEq)]
pub struct Delivery {
    /// Stable connection identity, independent of transport/channel generation.
    pub connection_id: String,
    /// Zero only for initial open; application deliveries start at one.
    pub offset: u64,
    /// Canonically validated event.
    pub event: d::SocketEvent,
}

/// Runtime-to-peer output. Protocol controls never enter actor deliveries.
#[derive(Clone, Debug, PartialEq)]
pub enum Output {
    /// Ordered application frame, retained until the client applies its offset.
    Message {
        /// Runtime-assigned per-connection outbound offset.
        offset: u64,
        /// Text or binary application payload.
        payload: d::SocketPayload,
    },
    /// Runtime protocol Pong, separate from application auto-response.
    Pong(Vec<u8>),
    /// Runtime-scheduled keepalive; not billed as an actor send.
    Keepalive(d::SocketPayload),
    /// Final socket close.
    Close {
        /// WebSocket close status.
        code: u32,
        /// Close reason.
        reason: String,
    },
}

#[derive(Clone)]
struct Connection {
    metadata: w::SocketConnection,
    accepted: bool,
    closed: bool,
    received: u64,
    sent: u64,
    acked: u64,
    pending: VecDeque<Delivery>,
    inbound_bytes: usize,
    output: VecDeque<Output>,
    output_bytes: usize,
    full_since: Option<u64>,
    last_seen: u64,
    keepalive: Option<d::SocketKeepalive>,
    next_keepalive: Option<u64>,
    metering: w::SocketMetering,
}

/// In-process socket engine for one actor. It holds persisted checkpoint bytes,
/// connection records and bounded runtime queues; it never retains an actor handler
/// or execution heap. A handler is supplied only for the duration of `dispatch`.
#[derive(Clone)]
pub struct ActorSockets {
    actor_id: d::ActorId,
    limits: d::ActorLimits,
    config: d::SocketConfig,
    connections: BTreeMap<String, Connection>,
    state: Vec<u8>,
    checkpoint_sequence: u64,
    last_checkpoint: Vec<u8>,
    last_decision: Option<d::SocketDecision>,
    connect_outbox: VecDeque<d::Connect>,
    auto_response: Option<d::SocketAutoResponse>,
    generation: u64,
    now_ms: u64,
}

impl ActorSockets {
    /// Creates a runtime without any actor heap or connections.
    pub fn new(actor_id: d::ActorId, limits: d::ActorLimits) -> Self {
        Self {
            actor_id,
            config: limits.socket_limits(),
            limits,
            connections: BTreeMap::new(),
            state: Vec::new(),
            checkpoint_sequence: 0,
            last_checkpoint: Vec::new(),
            last_decision: None,
            connect_outbox: VecDeque::new(),
            auto_response: None,
            generation: 1,
            now_ms: 0,
        }
    }

    /// Checkpoint state used to reconstruct an actor when an application frame arrives.
    pub fn state(&self) -> &[u8] {
        &self.state
    }

    /// Current fencing generation. Transport callbacks must pass this value.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Transfers the runtime-owned records and replay queues to a new owner generation.
    /// Old-owner callbacks are rejected before they can enqueue or apply anything.
    pub fn move_owner(&mut self) -> Result<u64, Error> {
        self.generation = self.generation.checked_add(1).ok_or(Error::Exhausted)?;
        Ok(self.generation)
    }

    fn fence(&self, generation: u64) -> Result<(), Error> {
        if generation == self.generation {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }

    /// Opens an incoming logical connection. Reopening its stable identity resumes
    /// retained state rather than invoking the actor's connect handler twice.
    pub fn open(&mut self, generation: u64, request: d::SocketOpen) -> Result<(), Error> {
        self.fence(generation)?;
        if request.actor_id() != &self.actor_id {
            return Err(Error::Connection);
        }
        if let Some(connection) = self.connections.get(request.connection_id()) {
            if request.resume_after() < connection.acked || request.resume_after() > connection.sent {
                return Err(Error::Conflict);
            }
            return Ok(());
        }
        if self.connections.len() >= self.config.max_connections() as usize {
            return Err(Error::Backpressure);
        }
        let id = request.connection_id().to_owned();
        let event = d::SocketEvent::new(d::socket_event::Value::Open(request.clone()));
        let mut pending = VecDeque::new();
        pending.push_back(Delivery {
            connection_id: id.clone(),
            offset: 0,
            event,
        });
        self.connections.insert(
            id.clone(),
            Connection {
                metadata: w::SocketConnection {
                    connection_id: id.clone(),
                    transport: request.transport() as i32,
                    ..Default::default()
                },
                accepted: false,
                closed: false,
                received: 0,
                sent: 0,
                acked: 0,
                pending,
                inbound_bytes: 0,
                output: VecDeque::new(),
                output_bytes: 0,
                full_since: None,
                last_seen: self.now_ms,
                keepalive: None,
                next_keepalive: None,
                metering: w::SocketMetering {
                    actor_id: self.actor_id.as_str().to_owned(),
                    connection_id: Some(id),
                    ..Default::default()
                },
            },
        );
        Ok(())
    }

    /// Reads the next ordered application event without acknowledging it.
    /// An uncommitted handler sees the identical event again.
    pub fn next(&self, connection_id: &str) -> Option<&Delivery> {
        self.connections.get(connection_id)?.pending.front()
    }

    /// Runs a temporary actor handler, then atomically commits its checkpoint and
    /// effects. Returning an error leaves delivery and outbox positions unchanged.
    pub fn dispatch<F>(
        &mut self,
        generation: u64,
        connection_id: &str,
        handler: F,
    ) -> Result<bool, Error>
    where
        F: FnOnce(
            &[u8],
            &Delivery,
        ) -> Result<(d::SocketCheckpoint, Option<d::SocketDecision>), Error>,
    {
        self.fence(generation)?;
        let Some(delivery) = self.next(connection_id).cloned() else {
            return Ok(false);
        };
        let connection = self
            .connections
            .get_mut(connection_id)
            .ok_or(Error::Connection)?;
        connection.metering.wakes = connection
            .metering
            .wakes
            .checked_add(1)
            .ok_or(Error::Exhausted)?;
        let (checkpoint, decision) = handler(&self.state, &delivery)?;
        self.commit(generation, &delivery, checkpoint, decision)?;
        Ok(true)
    }

    /// Last runtime auto-response timestamp, readable when the actor next wakes.
    pub fn get_web_socket_auto_response_timestamp(
        &self,
        connection_id: &str,
    ) -> Result<Option<u64>, Error> {
        Ok(self
            .connections
            .get(connection_id)
            .ok_or(Error::Connection)?
            .metadata
            .last_auto_response_at)
    }

    /// Returns an actor default or a connection override. None means no setting
    /// at that scope (the receive path falls back from connection to actor).
    pub fn get_web_socket_auto_response(
        &self,
        scope: &d::SocketScope,
    ) -> Result<Option<d::SocketAutoResponse>, Error> {
        match scope.value() {
            d::socket_scope::Value::Actor(_) => Ok(self.auto_response.clone()),
            d::socket_scope::Value::ConnectionId(id) => {
                let connection = self.connections.get(id).ok_or(Error::Connection)?;
                Ok(connection
                    .metadata
                    .auto_response
                    .clone()
                    .map(d::SocketAutoResponse::try_from)
                    .transpose()?)
            }
        }
    }

    /// Checkpointable connection metadata including attachment, tags and timestamp.
    pub fn connection_metadata(&self, id: &str) -> Result<d::SocketConnection, Error> {
        Ok(d::SocketConnection::try_from(
            self.connections
                .get(id)
                .ok_or(Error::Connection)?
                .metadata
                .clone(),
        )?)
    }

    /// Runtime accounting record for one connection. Auto-responses and pings
    /// have independent, non-billable counters.
    pub fn metering(&self, id: &str) -> Result<d::SocketMetering, Error> {
        Ok(d::SocketMetering::try_from(
            self.connections
                .get(id)
                .ok_or(Error::Connection)?
                .metering
                .clone(),
        )?)
    }
}
