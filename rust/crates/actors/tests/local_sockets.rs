//! Focused platform socket hibernation, runtime controls and checkpoint tests.
use acyclic_actors::{
    domain as d,
    local::{ActorSockets, Delivery, Error, Output},
    wire as w,
};

fn actor() -> Result<ActorSockets, d::DomainError> {
    Ok(ActorSockets::new(
        d::ActorId::new("actor-a".into())?,
        d::ActorLimits::new(50, 1_048_576, 65_536)?,
    ))
}

fn open(runtime: &mut ActorSockets, id: &str, transport: w::SocketTransport) -> Result<(), Error> {
    let request = d::SocketOpen::try_from(w::SocketOpen {
        actor_id: "actor-a".into(),
        connection_id: id.into(),
        transport: transport as i32,
        ..Default::default()
    })?;
    runtime.open(runtime.generation(), request)?;
    let delivery = runtime.next(id).cloned().ok_or(Error::Connection)?;
    let checkpoint = runtime.checkpoint(&delivery, vec![], vec![])?;
    let decision = d::SocketDecision::try_from(w::SocketDecision {
        value: Some(w::socket_decision::Value::Accept(w::SocketAccept {
            subprotocol: None,
            attachment: b"persisted".to_vec(),
            tags: vec!["watchers".into()],
        })),
    })?;
    runtime.commit(runtime.generation(), &delivery, checkpoint, Some(decision))?;
    Ok(())
}

fn effect_id(sequence: u64, index: u32) -> w::EffectId {
    w::EffectId {
        actor_id: "actor-a".into(),
        checkpoint_sequence: sequence,
        index_in_outbox: index,
    }
}

fn auto_response(
    sequence: u64,
    scope: w::socket_scope::Value,
    request: &str,
    response: &str,
) -> Result<d::SocketEffect, d::DomainError> {
    d::SocketEffect::try_from(w::SocketEffect {
        value: Some(w::socket_effect::Value::SetAutoResponse(
            w::SetAutoResponse {
                effect_id: Some(effect_id(sequence, 0)),
                scope: Some(w::SocketScope { value: Some(scope) }),
                setting: Some(w::SocketAutoResponseUpdate {
                    value: Some(w::socket_auto_response_update::Value::Pair(
                        w::SocketAutoResponse {
                            request: request.into(),
                            response: response.into(),
                        },
                    )),
                }),
            },
        )),
    })
}

fn send(sequence: u64, id: &str, text: &str) -> Result<d::SocketEffect, d::DomainError> {
    d::SocketEffect::try_from(w::SocketEffect {
        value: Some(w::socket_effect::Value::Send(w::Send {
            effect_id: Some(effect_id(sequence, 0)),
            connection_id: id.into(),
            payload: Some(w::send::Payload::Text(text.into())),
        })),
    })
}

fn commit(
    runtime: &mut ActorSockets,
    id: &str,
    effects: Vec<d::SocketEffect>,
) -> Result<Delivery, Error> {
    let delivery = runtime.next(id).cloned().ok_or(Error::Connection)?;
    let checkpoint =
        runtime.checkpoint(&delivery, delivery.offset.to_le_bytes().to_vec(), effects)?;
    runtime.commit(runtime.generation(), &delivery, checkpoint, None)?;
    Ok(delivery)
}

#[test]
fn runtime_protocol_ping_and_channel_zero_never_dispatch_an_actor()
-> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = actor()?;
    open(&mut runtime, "socket", w::SocketTransport::Internal)?;
    runtime.ping(1, "socket", b"probe".to_vec())?;
    assert_eq!(
        runtime.take_control("socket"),
        Some(Output::Pong(b"probe".to_vec()))
    );
    assert_eq!(runtime.channel_zero_keepalive(1, b"alive")?, b"alive");
    assert!(runtime.next("socket").is_none());
    let meter = runtime.metering("socket")?;
    assert_eq!(meter.wakes(), 0);
    assert_eq!(meter.messages_in(), 0);
    assert_eq!(meter.messages_out(), 0);
    assert_eq!(meter.protocol_pings(), 1);
    assert_eq!(
        runtime.get_web_socket_auto_response_timestamp("socket")?,
        None
    );
    Ok(())
}

#[test]
fn exact_text_auto_response_survives_hibernation_and_move_without_wake()
-> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = actor()?;
    open(&mut runtime, "socket", w::SocketTransport::Internal)?;
    runtime.receive(1, "socket", d::SocketPayload::text("configure".into())?)?;
    commit(
        &mut runtime,
        "socket",
        vec![auto_response(
            2,
            w::socket_scope::Value::Actor(true),
            "ping",
            "pong",
        )?],
    )?;
    // No handler is retained after commit; only checkpoint and node socket state remain.
    runtime.advance_time(20, 0)?;
    let generation = runtime.move_owner()?;
    runtime.receive(generation, "socket", d::SocketPayload::text("ping".into())?)?;
    assert!(runtime.next("socket").is_none());
    assert_eq!(
        runtime.get_web_socket_auto_response_timestamp("socket")?,
        Some(20)
    );
    assert_eq!(
        runtime.connection_metadata("socket")?.attachment(),
        b"persisted"
    );
    assert_eq!(
        runtime.replay(generation, "socket", 0)?,
        vec![Output::Message {
            offset: 1,
            payload: d::SocketPayload::text("pong".into())?
        }]
    );
    assert_eq!(runtime.metering("socket")?.auto_responses(), 1);
    assert_eq!(runtime.metering("socket")?.messages_in(), 1);
    runtime.receive(
        generation,
        "socket",
        d::SocketPayload::binary(b"ping".to_vec())?,
    )?;
    assert_eq!(
        runtime.next("socket").map(|delivery| delivery.offset),
        Some(2)
    );
    Ok(())
}

#[test]
fn checkpoint_retry_is_atomic_and_effect_replay_sends_once()
-> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = actor()?;
    open(&mut runtime, "socket", w::SocketTransport::Internal)?;
    runtime.receive(1, "socket", d::SocketPayload::text("one".into())?)?;
    let delivery = runtime.next("socket").cloned().ok_or(Error::Connection)?;
    let invalid = runtime.checkpoint(
        &delivery,
        b"not committed".to_vec(),
        vec![send(2, "missing", "reply")?],
    )?;
    assert!(runtime.commit(1, &delivery, invalid, None).is_err());
    assert_eq!(runtime.state(), b"");
    assert_eq!(runtime.next("socket"), Some(&delivery));
    assert_eq!(runtime.connection_metadata("socket")?.delivered_offset(), 0);
    let checkpoint = runtime.checkpoint(
        &delivery,
        b"committed".to_vec(),
        vec![send(2, "socket", "reply")?],
    )?;
    assert!(runtime.commit(1, &delivery, checkpoint.clone(), None)?);
    assert!(!runtime.commit(1, &delivery, checkpoint, None)?);
    let generation = runtime.move_owner()?;
    assert!(
        runtime
            .receive(1, "socket", d::SocketPayload::text("stale".into())?)
            .is_err()
    );
    assert!(runtime.replay(1, "socket", 0).is_err());
    assert_eq!(runtime.replay(generation, "socket", 0)?.len(), 1);
    runtime.acknowledge(generation, "socket", 1)?;
    assert!(runtime.replay(generation, "socket", 1)?.is_empty());
    assert!(runtime.replay(generation, "socket", 0).is_err());
    assert_eq!(runtime.metering("socket")?.messages_out(), 1);
    Ok(())
}

#[test]
fn internal_and_external_connections_share_the_same_application_lifecycle()
-> Result<(), Box<dyn std::error::Error>> {
    fn scenario(transport: w::SocketTransport) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut runtime = actor()?;
        open(&mut runtime, "socket", transport)?;
        let mut events = vec!["open".to_owned()];
        for (index, text) in ["one", "two", "three"].into_iter().enumerate() {
            runtime.receive(
                runtime.generation(),
                "socket",
                d::SocketPayload::text(text.to_owned())?,
            )?;
            let delivered = commit(
                &mut runtime,
                "socket",
                vec![send(index as u64 + 2, "socket", text)?],
            )?;
            events.push(format!("message:{}", delivered.offset));
        }
        runtime.advance_time(100, 100)?;
        let close = runtime.next("socket").ok_or(Error::Connection)?;
        assert!(
            matches!(close.event.value(), d::socket_event::Value::Close(event) if event.initiator() == d::SocketCloseInitiator::Runtime)
        );
        events.push(format!("close:{}", close.offset));
        let meter = runtime.metering("socket")?;
        assert_eq!(meter.messages_in(), 3);
        assert_eq!(meter.messages_out(), 3);
        assert_eq!(
            meter.real_socket_seconds(),
            if transport == w::SocketTransport::External {
                0.1
            } else {
                0.0
            }
        );
        Ok(events)
    }
    assert_eq!(
        scenario(w::SocketTransport::Internal)?,
        scenario(w::SocketTransport::External)?
    );
    Ok(())
}

#[test]
fn outbound_keepalive_and_dead_peer_detection_belong_to_runtime()
-> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = actor()?;
    open(&mut runtime, "socket", w::SocketTransport::External)?;
    runtime.set_keepalive(
        "socket",
        Some(d::SocketKeepalive::try_from(w::SocketKeepalive {
            frame: Some(w::SocketPayload {
                value: Some(w::socket_payload::Value::Text("heartbeat".into())),
            }),
            interval_ms: 10,
        })?),
    )?;
    runtime.advance_time(10, 30)?;
    assert!(matches!(
        runtime.take_control("socket"),
        Some(Output::Keepalive(_))
    ));
    assert!(runtime.next("socket").is_none());
    assert_eq!(runtime.metering("socket")?.wakes(), 0);
    runtime.advance_time(30, 30)?;
    assert!(
        matches!(runtime.next("socket").map(|delivery| delivery.event.value()), Some(d::socket_event::Value::Close(close)) if close.initiator() == d::SocketCloseInitiator::Runtime)
    );
    Ok(())
}

#[test]
fn connection_auto_response_overrides_actor_default_and_clear_restores_it() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = actor()?;
    open(&mut runtime, "socket", w::SocketTransport::Internal)?;
    runtime.receive(1, "socket", d::SocketPayload::text("configure".into())?)?;
    commit(&mut runtime, "socket", vec![auto_response(2, w::socket_scope::Value::Actor(true), "ping", "actor")?])?;
    runtime.receive(1, "socket", d::SocketPayload::text("override".into())?)?;
    commit(&mut runtime, "socket", vec![auto_response(3, w::socket_scope::Value::ConnectionId("socket".into()), "ping", "connection")?])?;
    runtime.receive(1, "socket", d::SocketPayload::text("ping".into())?)?;
    assert_eq!(runtime.replay(1, "socket", 0)?, vec![Output::Message { offset: 1, payload: d::SocketPayload::text("connection".into())? }]);
    runtime.receive(1, "socket", d::SocketPayload::text("clear".into())?)?;
    let clear = d::SocketEffect::try_from(w::SocketEffect {
        value: Some(w::socket_effect::Value::SetAutoResponse(w::SetAutoResponse {
            effect_id: Some(effect_id(4, 0)), scope: Some(w::SocketScope { value: Some(w::socket_scope::Value::ConnectionId("socket".into())) }),
            setting: Some(w::SocketAutoResponseUpdate { value: Some(w::socket_auto_response_update::Value::Clear(true)) }),
        })),
    })?;
    commit(&mut runtime, "socket", vec![clear])?;
    runtime.receive(1, "socket", d::SocketPayload::text("ping".into())?)?;
    assert_eq!(runtime.replay(1, "socket", 1)?, vec![Output::Message { offset: 2, payload: d::SocketPayload::text("actor".into())? }]);
    assert!(runtime.get_web_socket_auto_response(&d::SocketScope::connection("socket".into())?)?.is_none());
    Ok(())
}

#[test]
fn full_buffer_stops_reads_then_closes_1013_without_dropping_accepted_frames() -> Result<(), Box<dyn std::error::Error>> {
    let config = d::SocketConfig::try_from(w::SocketConfig {
        max_frame_bytes: 16, max_connections: 2, inbound_buffer_bytes: 16,
        inbound_buffer_frames: 1, actor_buffer_bytes: 16,
    })?;
    let limits = d::ActorLimits::new(50, 1_048_576, 65_536)?.with_sockets(config);
    let mut runtime = ActorSockets::new(d::ActorId::new("actor-a".into())?, limits);
    open(&mut runtime, "socket", w::SocketTransport::External)?;
    runtime.receive(1, "socket", d::SocketPayload::text("accepted".into())?)?;
    assert!(matches!(runtime.receive(1, "socket", d::SocketPayload::text("not read".into())?), Err(Error::Backpressure)));
    runtime.ping(1, "socket", b"control".to_vec())?;
    assert_eq!(runtime.take_control("socket"), Some(Output::Pong(b"control".to_vec())));
    runtime.advance_time(50, 0)?;
    let message = commit(&mut runtime, "socket", vec![])?;
    assert_eq!(message.offset, 1);
    assert!(matches!(message.event.value(), d::socket_event::Value::Message(message) if matches!(message.payload(), d::socket_message::Payload::Text(text) if text == "accepted")));
    assert!(matches!(runtime.next("socket").map(|delivery| delivery.event.value()), Some(d::socket_event::Value::Close(close)) if close.code() == 1013 && close.reason() == "try again later"));
    assert_eq!(runtime.metering("socket")?.messages_in(), 1);
    Ok(())
}
