use super::connection::{add, push_message, runtime_close};
use super::*;
use prost::Message;

impl ActorSockets {
    /// Builds a checkpoint draft with the pending delivery's offset. Effects
    /// are committed together with these bytes; no handler state survives it.
    pub fn checkpoint(
        &self,
        delivery: &Delivery,
        state: Vec<u8>,
        effects: Vec<d::SocketEffect>,
    ) -> Result<d::SocketCheckpoint, Error> {
        let mut connections: Vec<_> = self
            .connections
            .values()
            .map(|connection| connection.metadata.clone())
            .collect();
        let metadata = connections
            .iter_mut()
            .find(|connection| connection.connection_id == delivery.connection_id)
            .ok_or(Error::Connection)?;
        metadata.delivered_offset = delivery.offset;
        Ok(d::SocketCheckpoint::try_from(w::SocketCheckpoint {
            checkpoint_sequence: add(self.checkpoint_sequence, 1)?,
            state,
            connections,
            effects: effects.into_iter().map(Into::into).collect(),
            auto_response: self.auto_response.clone().map(Into::into),
        })?)
    }

    /// Atomically installs state, delivered offset and the node's effect queue.
    /// Conflicting or failed effects roll the entire transaction back. A replay
    /// of the latest identical checkpoint executes no effects twice.
    pub fn commit(
        &mut self,
        generation: u64,
        delivery: &Delivery,
        checkpoint: d::SocketCheckpoint,
        decision: Option<d::SocketDecision>,
    ) -> Result<bool, Error> {
        self.fence(generation)?;
        let sequence = checkpoint.checkpoint_sequence().get();
        let bytes = w::SocketCheckpoint::from(checkpoint.clone()).encode_to_vec();
        if sequence == self.checkpoint_sequence {
            return if bytes == self.last_checkpoint && decision == self.last_decision {
                Ok(false)
            } else {
                Err(Error::Conflict)
            };
        }
        if sequence != add(self.checkpoint_sequence, 1)?
            || checkpoint.state().len() as u64 > self.limits.checkpoint_bytes()
            || checkpoint
                .effects()
                .iter()
                .any(|effect| effect.effect_id().actor_id() != &self.actor_id)
            || self.next(&delivery.connection_id) != Some(delivery)
        {
            return Err(Error::Conflict);
        }
        let mut updated = self.clone();
        updated.apply_checkpoint(delivery, &checkpoint, decision.as_ref())?;
        updated.state = checkpoint.state().to_vec();
        updated.checkpoint_sequence = sequence;
        updated.last_checkpoint = bytes;
        updated.last_decision = decision;
        *self = updated;
        Ok(true)
    }

    fn apply_checkpoint(
        &mut self,
        delivery: &Delivery,
        checkpoint: &d::SocketCheckpoint,
        decision: Option<&d::SocketDecision>,
    ) -> Result<(), Error> {
        if checkpoint.connections().len() != self.connections.len()
            || checkpoint.auto_response() != &self.auto_response
        {
            return Err(Error::Conflict);
        }
        for metadata in checkpoint.connections() {
            let old = &self
                .connections
                .get(metadata.connection_id())
                .ok_or(Error::Connection)?
                .metadata;
            let mut expected = old.clone();
            if metadata.connection_id() == delivery.connection_id {
                expected.delivered_offset = delivery.offset;
            }
            if w::SocketConnection::from(metadata.clone()) != expected {
                return Err(Error::Conflict);
            }
        }
        let is_open = matches!(delivery.event.value(), d::socket_event::Value::Open(_));
        if is_open != decision.is_some() {
            return Err(Error::Conflict);
        }
        let connection = self
            .connections
            .get_mut(&delivery.connection_id)
            .ok_or(Error::Connection)?;
        if let Some(decision) = decision {
            match decision.value() {
                d::socket_decision::Value::Accept(accept) => {
                    let d::socket_event::Value::Open(open) = delivery.event.value() else {
                        return Err(Error::Conflict);
                    };
                    if accept
                        .subprotocol()
                        .as_ref()
                        .is_some_and(|protocol| !open.subprotocols().contains(protocol))
                    {
                        return Err(Error::Conflict);
                    }
                    connection.metadata.attachment = accept.attachment().to_vec();
                    connection.metadata.tags = accept.tags().to_vec();
                    connection.accepted = true;
                }
                d::socket_decision::Value::Reject(reject) => {
                    runtime_close(
                        connection,
                        &delivery.connection_id,
                        1008,
                        reject.reason(),
                        d::SocketCloseInitiator::Actor,
                    )?;
                }
            }
        }
        let bytes = match delivery.event.value() {
            d::socket_event::Value::Message(message) => message.payload().len(),
            _ => 0,
        };
        connection.pending.pop_front();
        connection.inbound_bytes -= bytes;
        connection.metadata.delivered_offset = delivery.offset;
        connection.full_since = None;
        for effect in checkpoint.effects() {
            self.apply_effect(effect)?;
        }
        Ok(())
    }

    fn apply_effect(&mut self, effect: &d::SocketEffect) -> Result<(), Error> {
        match effect.value() {
            d::socket_effect::Value::Send(send) => {
                let payload = match send.payload() {
                    d::send::Payload::Text(value) => d::SocketPayload::text(value.clone())?,
                    d::send::Payload::Binary(value) => d::SocketPayload::binary(value.clone())?,
                };
                push_message(
                    self.connections
                        .get_mut(send.connection_id())
                        .ok_or(Error::Connection)?,
                    payload,
                    &self.config,
                    true,
                )?;
            }
            d::socket_effect::Value::Close(close) => {
                runtime_close(
                    self.connections
                        .get_mut(close.connection_id())
                        .ok_or(Error::Connection)?,
                    close.connection_id(),
                    close.code(),
                    close.reason(),
                    d::SocketCloseInitiator::Actor,
                )?;
            }
            d::socket_effect::Value::Broadcast(broadcast) => {
                let recipients: Vec<_> = match broadcast.target().value() {
                    d::socket_broadcast_target::Value::Tag(tag) => self
                        .connections
                        .iter()
                        .filter(|(_, connection)| {
                            connection.metadata.tags.contains(tag)
                                && connection.accepted
                                && !connection.closed
                        })
                        .map(|(id, _)| id.clone())
                        .collect(),
                    d::socket_broadcast_target::Value::Connections(set) => {
                        set.connection_ids().to_vec()
                    }
                };
                for id in recipients {
                    if !broadcast.except().contains(&id) {
                        push_message(
                            self.connections.get_mut(&id).ok_or(Error::Connection)?,
                            broadcast.payload().clone(),
                            &self.config,
                            true,
                        )?;
                    }
                }
            }
            d::socket_effect::Value::SetAttachment(setting) => {
                self.connections
                    .get_mut(setting.connection_id())
                    .ok_or(Error::Connection)?
                    .metadata
                    .attachment = setting.attachment().to_vec();
            }
            d::socket_effect::Value::SetTags(setting) => {
                self.connections
                    .get_mut(setting.connection_id())
                    .ok_or(Error::Connection)?
                    .metadata
                    .tags = setting.tags().to_vec();
            }
            d::socket_effect::Value::SetAutoResponse(setting) => {
                let pair = match setting.setting().value() {
                    d::socket_auto_response_update::Value::Pair(pair) => Some(pair.clone()),
                    d::socket_auto_response_update::Value::Clear(_) => None,
                };
                self.set_web_socket_auto_response(setting.scope(), pair)?;
            }
            d::socket_effect::Value::Connect(connect) => {
                if self.connect_outbox.len() >= self.config.max_connections() as usize {
                    return Err(Error::Backpressure);
                }
                self.connect_outbox.push_back(connect.clone());
            }
        }
        Ok(())
    }

    /// Applies an auto-response setting inside a checkpoint transaction. Public
    /// callers should emit SetAutoResponse so retries share its effect identity.
    fn set_web_socket_auto_response(
        &mut self,
        scope: &d::SocketScope,
        pair: Option<d::SocketAutoResponse>,
    ) -> Result<(), Error> {
        if pair.as_ref().is_some_and(|pair| {
            pair.request().len() > self.config.max_frame_bytes() as usize
                || pair.response().len() > self.config.max_frame_bytes() as usize
        }) {
            return Err(d::DomainError::InvalidSocket.into());
        }
        match scope.value() {
            d::socket_scope::Value::Actor(_) => self.auto_response = pair,
            d::socket_scope::Value::ConnectionId(id) => {
                self.connections
                    .get_mut(id)
                    .ok_or(Error::Connection)?
                    .metadata
                    .auto_response = pair.map(Into::into);
            }
        }
        Ok(())
    }

    /// Pending outbound connects retain stable effect IDs until the runtime
    /// transport driver has applied them. The actor never receives socket handles.
    pub fn pending_connects(&self) -> impl Iterator<Item = &d::Connect> {
        self.connect_outbox.iter()
    }
}
