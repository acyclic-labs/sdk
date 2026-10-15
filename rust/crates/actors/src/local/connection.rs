use super::*;

impl ActorSockets {
    /// Receives one application frame. Exact text auto-responses are answered
    /// without scheduling an actor handler or advancing its inbound offset.
    pub fn receive(
        &mut self,
        generation: u64,
        id: &str,
        payload: d::SocketPayload,
    ) -> Result<(), Error> {
        self.fence(generation)?;
        if !self.config.admits_payload(&payload) {
            return Err(d::DomainError::InvalidSocket.into());
        }
        let total: usize = self
            .connections
            .values()
            .map(|connection| connection.inbound_bytes)
            .sum();
        let connection = self.connections.get_mut(id).ok_or(Error::Connection)?;
        if !connection.accepted || connection.closed {
            return Err(Error::Connection);
        }
        connection.last_seen = self.now_ms;
        let pair = connection
            .metadata
            .auto_response
            .as_ref()
            .map(|pair| (pair.request.as_str(), pair.response.as_str()))
            .or_else(|| {
                self.auto_response
                    .as_ref()
                    .map(|pair| (pair.request(), pair.response()))
            });
        if let (d::socket_payload::Value::Text(text), Some((request, response))) =
            (payload.value(), pair)
        {
            if text.as_bytes() == request.as_bytes() {
                let response = d::SocketPayload::text(response.to_owned())?;
                push_message(connection, response, &self.config, false)?;
                connection.metadata.last_auto_response_at = Some(self.now_ms);
                connection.metering.auto_responses = add(connection.metering.auto_responses, 1)?;
                return Ok(());
            }
        }
        if connection.pending.len() >= self.config.inbound_buffer_frames() as usize
            || connection.inbound_bytes + payload.len()
                > self.config.inbound_buffer_bytes() as usize
            || total + payload.len() > self.config.actor_buffer_bytes() as usize
        {
            connection.full_since.get_or_insert(self.now_ms);
            return Err(Error::Backpressure);
        }
        let offset = add(connection.received, 1)?;
        let message = d::SocketMessage::try_from(w::SocketMessage {
            offset,
            payload: Some(match payload.value() {
                d::socket_payload::Value::Text(value) => {
                    w::socket_message::Payload::Text(value.clone())
                }
                d::socket_payload::Value::Binary(value) => {
                    w::socket_message::Payload::Binary(value.clone())
                }
            }),
        })?;
        connection.pending.push_back(Delivery {
            connection_id: id.to_owned(),
            offset,
            event: d::SocketEvent::new(d::socket_event::Value::Message(message)),
        });
        connection.received = offset;
        connection.inbound_bytes += payload.len();
        connection.metering.messages_in = add(connection.metering.messages_in, 1)?;
        connection.metering.bytes_in = add(connection.metering.bytes_in, payload.len() as u64)?;
        if connection.pending.len() >= self.config.inbound_buffer_frames() as usize
            || connection.inbound_bytes >= self.config.inbound_buffer_bytes() as usize
        {
            connection.full_since.get_or_insert(self.now_ms);
        }
        Ok(())
    }

    /// Runtime-owned WebSocket protocol Ping/Pong, never an actor event.
    pub fn ping(&mut self, generation: u64, id: &str, payload: Vec<u8>) -> Result<(), Error> {
        self.fence(generation)?;
        if payload.len() > 125 {
            return Err(d::DomainError::InvalidSocket.into());
        }
        let connection = self.connections.get_mut(id).ok_or(Error::Connection)?;
        if connection.closed {
            return Err(Error::Connection);
        }
        if connection
            .output
            .iter()
            .filter(|output| !matches!(output, Output::Message { .. }))
            .count()
            >= 8
        {
            return Err(Error::Backpressure);
        }
        connection.output.push_back(Output::Pong(payload));
        connection.last_seen = self.now_ms;
        connection.metering.protocol_pings = add(connection.metering.protocol_pings, 1)?;
        Ok(())
    }

    /// Session channel zero is answered by the transport runtime. It never has
    /// an actor identity and cannot schedule any actor delivery.
    pub fn channel_zero_keepalive(
        &self,
        generation: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.fence(generation)?;
        if payload.len() > 125 {
            return Err(d::DomainError::InvalidSocket.into());
        }
        Ok(payload.to_vec())
    }

    /// Marks activity received from a peer, including Pong, without waking actors.
    pub fn peer_activity(&mut self, generation: u64, id: &str) -> Result<(), Error> {
        self.fence(generation)?;
        self.connections
            .get_mut(id)
            .ok_or(Error::Connection)?
            .last_seen = self.now_ms;
        Ok(())
    }

    /// Runtime deadlines. The caller schedules the next deadline; this method
    /// does not poll transport data. Idle/dead-peer and buffer timeouts emit only
    /// a final close event; fixed outbound keepalive never invokes actor code.
    pub fn advance_time(&mut self, now_ms: u64, idle_timeout_ms: u64) -> Result<(), Error> {
        let elapsed = now_ms.checked_sub(self.now_ms).ok_or(Error::Clock)?;
        for (id, connection) in &mut self.connections {
            connection.metering.buffered_byte_seconds +=
                connection.inbound_bytes as f64 * elapsed as f64 / 1000.0;
            if !connection.closed
                && connection.metadata.transport == w::SocketTransport::External as i32
            {
                connection.metering.real_socket_seconds += elapsed as f64 / 1000.0;
            }
            if connection.closed {
                continue;
            }
            let full_timeout = connection
                .full_since
                .is_some_and(|since| now_ms - since >= self.limits.handler_timeout_millis());
            if full_timeout
                || (idle_timeout_ms > 0 && now_ms - connection.last_seen >= idle_timeout_ms)
            {
                runtime_close(
                    connection,
                    id,
                    if full_timeout { 1013 } else { 1001 },
                    if full_timeout {
                        "try again later"
                    } else {
                        "idle timeout"
                    },
                    d::SocketCloseInitiator::Runtime,
                )?;
                continue;
            }
            if connection
                .next_keepalive
                .is_some_and(|deadline| now_ms >= deadline)
            {
                if let Some(template) = &connection.keepalive {
                    if connection
                        .output
                        .iter()
                        .filter(|output| !matches!(output, Output::Message { .. }))
                        .count()
                        < 8
                    {
                        connection
                            .output
                            .push_back(Output::Keepalive(template.frame().clone()));
                        connection.next_keepalive =
                            Some(add(now_ms, template.interval_ms().get())?);
                    }
                }
            }
        }
        self.now_ms = now_ms;
        Ok(())
    }

    /// Installs a validated keepalive on a node-owned outbound connection.
    pub fn set_keepalive(
        &mut self,
        id: &str,
        template: Option<d::SocketKeepalive>,
    ) -> Result<(), Error> {
        let connection = self.connections.get_mut(id).ok_or(Error::Connection)?;
        if template
            .as_ref()
            .is_some_and(|template| !self.config.admits_payload(template.frame()))
        {
            return Err(d::DomainError::InvalidSocket.into());
        }
        connection.next_keepalive = template
            .as_ref()
            .map(|template| add(self.now_ms, template.interval_ms().get()))
            .transpose()?;
        connection.keepalive = template;
        Ok(())
    }

    /// Replays retained frames strictly after the client's applied offset.
    /// Reopening after a move does not alter message bytes or offsets.
    pub fn replay(&self, generation: u64, id: &str, after: u64) -> Result<Vec<Output>, Error> {
        self.fence(generation)?;
        let connection = self.connections.get(id).ok_or(Error::Connection)?;
        if after < connection.acked || after > connection.sent {
            return Err(Error::Conflict);
        }
        Ok(connection
            .output
            .iter()
            .filter(|output| matches!(output, Output::Message { offset, .. } if *offset > after))
            .cloned()
            .collect())
    }

    /// Removes retained data only after peer application, never on byte receipt.
    pub fn acknowledge(&mut self, generation: u64, id: &str, applied: u64) -> Result<(), Error> {
        self.fence(generation)?;
        let connection = self.connections.get_mut(id).ok_or(Error::Connection)?;
        if applied < connection.acked || applied > connection.sent {
            return Err(Error::Conflict);
        }
        connection.output.retain(|output| {
            if let Output::Message { offset, payload } = output {
                if *offset <= applied {
                    connection.output_bytes -= payload.len();
                    return false;
                }
            }
            true
        });
        connection.acked = applied;
        Ok(())
    }

    /// Takes a runtime control response. Application frames remain in replay
    /// storage until acknowledged; controls use the separately bounded queue.
    pub fn take_control(&mut self, id: &str) -> Option<Output> {
        let connection = self.connections.get_mut(id)?;
        let position = connection
            .output
            .iter()
            .position(|output| !matches!(output, Output::Message { .. }))?;
        connection.output.remove(position)
    }
}

pub(super) fn add(value: u64, increment: u64) -> Result<u64, Error> {
    value.checked_add(increment).ok_or(Error::Exhausted)
}

pub(super) fn push_message(
    connection: &mut Connection,
    payload: d::SocketPayload,
    config: &d::SocketConfig,
    bill: bool,
) -> Result<(), Error> {
    if !connection.accepted || connection.closed {
        return Err(Error::Connection);
    }
    if !config.admits_payload(&payload)
        || connection.output_bytes + payload.len() > config.inbound_buffer_bytes() as usize
        || connection
            .output
            .iter()
            .filter(|output| matches!(output, Output::Message { .. }))
            .count()
            >= config.inbound_buffer_frames() as usize
    {
        return Err(Error::Backpressure);
    }
    let offset = add(connection.sent, 1)?;
    let bytes = payload.len() as u64;
    connection.output_bytes += payload.len();
    connection
        .output
        .push_back(Output::Message { offset, payload });
    connection.sent = offset;
    if bill {
        connection.metering.messages_out = add(connection.metering.messages_out, 1)?;
        connection.metering.bytes_out = add(connection.metering.bytes_out, bytes)?;
    }
    Ok(())
}

pub(super) fn runtime_close(
    connection: &mut Connection,
    id: &str,
    code: u32,
    reason: &str,
    initiator: d::SocketCloseInitiator,
) -> Result<(), Error> {
    if connection.closed {
        return Ok(());
    }
    let offset = add(connection.received, 1)?;
    let event = d::SocketEvent::new(d::socket_event::Value::Close(d::SocketClose::try_from(
        w::SocketClose {
            offset,
            code,
            reason: reason.to_owned(),
            initiator: initiator as i32,
        },
    )?));
    connection.pending.push_back(Delivery {
        connection_id: id.to_owned(),
        offset,
        event,
    });
    connection.received = offset;
    connection.closed = true;
    connection.output.push_back(Output::Close {
        code,
        reason: reason.to_owned(),
    });
    Ok(())
}
