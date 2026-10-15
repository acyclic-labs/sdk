//! Socket contract ingress and schema regression tests.
use acyclic_actors::{domain as d, wire as w};
use prost::Message;

fn effect_id() -> w::EffectId {
    w::EffectId {
        actor_id: "actor-a".into(),
        checkpoint_sequence: 1,
        index_in_outbox: 0,
    }
}

fn connection() -> w::SocketConnection {
    w::SocketConnection {
        connection_id: "connection-a".into(),
        attachment: vec![0; 2048],
        tags: (0..8).map(|index| format!("tag-{index}")).collect(),
        delivered_offset: 0,
        outbound: false,
        auto_response: None,
        last_auto_response_at: None,
        transport: w::SocketTransport::Internal as i32,
    }
}

#[test]
fn socket_attachment_and_tag_bounds_apply_on_wire_ingress() -> Result<(), d::DomainError> {
    let accepted = d::SocketConnection::try_from(connection())?;
    assert_eq!(accepted.attachment().len(), 2048);
    assert_eq!(accepted.tags().len(), 8);
    let mut oversized = connection();
    oversized.attachment.push(0);
    assert_eq!(
        d::SocketConnection::try_from(oversized),
        Err(d::DomainError::InvalidSocket)
    );
    for tags in [
        vec!["x".repeat(65)],
        vec!["x".into(); 2],
        vec!["x".into(); 9],
    ] {
        assert!(
            d::SocketConnection::try_from(w::SocketConnection {
                tags,
                ..connection()
            })
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn socket_payload_is_required_bounded_and_binary_preserving()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        d::SocketMessage::try_from(w::SocketMessage {
            offset: 1,
            payload: None
        })
        .is_err()
    );
    assert!(
        d::SocketMessage::try_from(w::SocketMessage {
            offset: 0,
            payload: Some(w::socket_message::Payload::Text("hello".into())),
        })
        .is_err()
    );
    let raw = w::SocketMessage {
        offset: u64::MAX,
        payload: Some(w::socket_message::Payload::Binary(vec![0, 255, 128])),
    };
    let decoded = w::SocketMessage::decode(raw.encode_to_vec().as_slice())?;
    let admitted = d::SocketMessage::try_from(decoded)?;
    assert_eq!(w::SocketMessage::from(admitted), raw);
    assert!(
        d::SocketMessage::try_from(w::SocketMessage {
            offset: 1,
            payload: Some(w::socket_message::Payload::Binary(vec![
                0;
                d::MAX_SOCKET_FRAME_BYTES
                    + 1
            ])),
        })
        .is_err()
    );
    Ok(())
}

#[test]
fn socket_auto_response_is_an_exact_bounded_text_pair() -> Result<(), d::DomainError> {
    let pair = d::SocketAutoResponse::try_from(w::SocketAutoResponse {
        request: "é".repeat(1024),
        response: String::new(),
    })?;
    assert_eq!(pair.request().len(), 2048);
    assert!(
        d::SocketAutoResponse::try_from(w::SocketAutoResponse {
            request: "é".repeat(1025),
            response: String::new(),
        })
        .is_err()
    );
    assert!(
        d::SocketScope::try_from(w::SocketScope {
            value: Some(w::socket_scope::Value::Actor(false)),
        })
        .is_err()
    );
    assert!(
        d::SocketAutoResponseUpdate::try_from(w::SocketAutoResponseUpdate {
            value: Some(w::socket_auto_response_update::Value::Clear(false)),
        })
        .is_err()
    );
    Ok(())
}

fn reconnect() -> w::SocketReconnect {
    w::SocketReconnect {
        mode: w::SocketReconnectMode::None as i32,
        ..Default::default()
    }
}

fn connect(url: &str) -> Result<d::Connect, d::DomainError> {
    d::Connect::try_from(w::Connect {
        effect_id: Some(effect_id()),
        binding: "feed".into(),
        target: Some(w::connect::Target::Url(url.into())),
        subprotocols: vec![],
        reconnect: Some(reconnect()),
        keepalive: None,
    })
}

#[test]
fn socket_external_capability_checks_hosts_ports_and_resolved_ips()
-> Result<(), Box<dyn std::error::Error>> {
    let capability = d::SocketConnectCapability::try_from(w::SocketConnectCapability {
        binding: "feed".into(),
        rules: vec![w::SocketHostRule {
            host: "*.example.test".into(),
            ports: vec![],
        }],
        headers: vec![w::SocketSecretHeader {
            name: "Authorization".into(),
            secret_binding: "api-key".into(),
        }],
    })?;
    assert!(capability.permits(
        &connect("wss://feed.example.test/events")?,
        &["8.8.8.8".parse()?],
        false
    ));
    assert!(!capability.permits(
        &connect("wss://feed.example.test/events")?,
        &["127.0.0.1".parse()?],
        false
    ));
    assert!(!capability.permits(
        &connect("wss://example.test/events")?,
        &["8.8.8.8".parse()?],
        false
    ));
    assert!(!capability.permits(
        &connect("wss://feed.example.test:444/events")?,
        &["8.8.8.8".parse()?],
        false
    ));
    assert!(connect("ws://feed.example.test/events").is_err());
    assert!(connect("wss://user:secret@feed.example.test/events").is_err());
    assert!(connect("wss://feed.example.test/events#fragment").is_err());
    Ok(())
}

#[test]
fn socket_local_loopback_requires_an_explicit_ip_rule() -> Result<(), Box<dyn std::error::Error>> {
    let capability = d::SocketConnectCapability::try_from(w::SocketConnectCapability {
        binding: "feed".into(),
        rules: vec![w::SocketHostRule {
            host: "127.0.0.0/8".into(),
            ports: vec![w::SocketPortRange {
                first: 8000,
                last: 8999,
            }],
        }],
        headers: vec![],
    })?;
    let request = connect("ws://127.0.0.1:8080/events")?;
    assert!(capability.permits(&request, &["127.0.0.1".parse()?], true));
    assert!(!capability.permits(&request, &["127.0.0.1".parse()?], false));
    assert!(!capability.permits(&request, &["127.0.0.2".parse()?], true));
    Ok(())
}

#[test]
fn socket_backpressure_limits_are_admitted_with_actor_limits() {
    let mut sockets: w::SocketConfig = d::SocketConfig::default().into();
    sockets.inbound_buffer_frames = 1025;
    assert!(
        d::ActorLimits::try_from(w::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 1,
            checkpoint_bytes: 1,
            sockets: Some(sockets),
        })
        .is_err()
    );
}

#[test]
fn socket_checkpoint_rejects_non_deterministic_effect_indices() {
    let raw = w::SocketCheckpoint {
        checkpoint_sequence: 1,
        state: vec![],
        connections: vec![],
        auto_response: None,
        effects: vec![w::SocketEffect {
            value: Some(w::socket_effect::Value::Send(w::Send {
                effect_id: Some(w::EffectId {
                    index_in_outbox: 1,
                    ..effect_id()
                }),
                connection_id: "connection-a".into(),
                payload: Some(w::send::Payload::Text("once".into())),
            })),
        }],
    };
    assert!(d::SocketCheckpoint::try_from(raw).is_err());
}

#[test]
fn socket_common_connect_accepts_an_actor_without_an_external_binding() -> Result<(), d::DomainError>
{
    let request = d::Connect::try_from(w::Connect {
        effect_id: Some(effect_id()),
        binding: String::new(),
        target: Some(w::connect::Target::ActorAddress(w::ActorAddress {
            actor_id: "actor-b".into(),
            app_channel: None,
        })),
        subprotocols: vec![],
        reconnect: Some(reconnect()),
        keepalive: Some(w::SocketKeepalive {
            frame: Some(w::SocketPayload {
                value: Some(w::socket_payload::Value::Text("ping".into())),
            }),
            interval_ms: 30_000,
        }),
    })?;
    assert!(matches!(
        request.target(),
        d::connect::Target::ActorAddress(_)
    ));
    Ok(())
}

#[test]
fn socket_metering_rejects_nonfinite_or_negative_resource_totals() {
    for seconds in [f64::NAN, f64::INFINITY, -1.0] {
        assert!(
            d::SocketMetering::try_from(w::SocketMetering {
                actor_id: "actor-a".into(),
                real_socket_seconds: seconds,
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            d::SocketMetering::try_from(w::SocketMetering {
                actor_id: "actor-a".into(),
                buffered_byte_seconds: seconds,
                ..Default::default()
            })
            .is_err()
        );
    }
    assert!(
        d::SocketKeepalive::try_from(w::SocketKeepalive {
            frame: Some(w::SocketPayload {
                value: Some(w::socket_payload::Value::Text("ping".into()))
            }),
            interval_ms: 0,
        })
        .is_err()
    );
}
