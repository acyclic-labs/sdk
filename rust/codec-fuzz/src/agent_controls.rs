//! Exercise public control admission and the exact request retained by Unknown.
use acyclic_harness_client::{
    AgentBinding, AgentCall, AgentCommand, AgentControlFailure, AgentError, AgentPageRequest,
    AgentPath, AgentReply, Delivered, IntentId, PreparedCommand, ReadTarget, decode_agent_reply,
    encode_agent_reply,
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath,
};
use prost::Message as _;

/// Unframed inputs exercise public command, invocation and binding admission.
/// A NUL after a JSON command selects its following raw or JSON reply. This is
/// corpus framing only: all protocol parsing stays in the shipping entry points.
pub fn check(bytes: &[u8]) {
    if let Ok(command) = PreparedCommand::from_bytes(bytes.to_vec()) {
        assert_eq!(command.command(), bytes);
        let decoded = command.decoded().unwrap();
        let normalized = PreparedCommand::new(&decoded).unwrap();
        assert_eq!(json(&normalized.decoded().unwrap()), json(&decoded));
    }
    if let Ok(call) = AgentCall::decode(bytes) {
        assert_eq!(call.invocation().encode_to_vec(), bytes);
        assert!(AgentCall::decode(&with_unknown_field(bytes)).is_err());
        assert!(AgentCall::decode(&[bytes, bytes].concat()).is_err());
    }
    if AgentBinding::new(bytes).is_ok() {
        assert!(AgentBinding::new(&with_unknown_field(bytes)).is_err());
    }
    let Some(separator) = bytes.iter().position(|byte| *byte == 0) else {
        return;
    };
    let Ok(command) = PreparedCommand::decode(&bytes[..separator]) else {
        return;
    };
    let reply = &bytes[separator + 1..];
    if let Ok(Ok(decoded)) = decode_agent_reply(reply, &command) {
        success(&decoded, &command);
        let normalized = encode_agent_reply(&Ok(decoded.clone()), &command).unwrap();
        let restored = decode_agent_reply(&normalized, &command).unwrap().unwrap();
        assert_eq!(json(&restored), json(&decoded));
        assert!(decode_agent_reply(&with_unknown_field(reply), &command).is_err());
        let foreign = if matches!(command, AgentCommand::Root {}) {
            AgentCommand::Bind { agent: None }
        } else {
            AgentCommand::Root {}
        };
        assert!(decode_agent_reply(reply, &foreign).is_err());
    }
    if let Ok(Err(AgentControlFailure::Unknown(request))) = decode_agent_reply(reply, &command) {
        let result = Err(AgentError::Unknown(request));
        assert_eq!(encode_agent_reply(&result, &command).unwrap(), reply);
        assert!(decode_agent_reply(&with_unknown_field(reply), &command).is_err());
    }
    // Typed JSON reaches the actual host writer as well as the wire reader.
    if let Ok(decoded) = serde_json::from_slice::<AgentReply>(reply)
        && let Ok(wire) = encode_agent_reply(&Ok(decoded.clone()), &command)
    {
        success(&decoded, &command);
        assert_eq!(
            json(&decode_agent_reply(&wire, &command).unwrap().unwrap()),
            json(&decoded)
        );
    }
}

fn json(value: &impl serde::Serialize) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}

fn with_unknown_field(bytes: &[u8]) -> Vec<u8> {
    let mut changed = bytes.to_vec();
    changed.extend_from_slice(&[0xf8, 0x07, 0x01]); // protobuf field 127
    changed
}

fn success(reply: &AgentReply, command: &AgentCommand) {
    if let AgentReply::Content { data, .. } = reply {
        reply.content().expect("admitted content must decode");
        if !data.is_empty() {
            let mut changed = json(reply);
            // Preserve the original encoded length, file range and padding.
            // Invalid alphabet alone must fail at admission, not at display.
            let mut invalid = data.clone().into_bytes();
            invalid[0] = b'!';
            changed["data"] = String::from_utf8(invalid).unwrap().into();
            let changed: AgentReply = serde_json::from_value(changed).unwrap();
            // Replace the complete typed body, so content equal to a JSON key
            // cannot accidentally mutate the key instead of its value.
            let body = serde_json::to_vec(reply).unwrap();
            let invalid = serde_json::to_vec(&changed).unwrap();
            assert_eq!(body.len(), invalid.len());
            let mut wire = encode_agent_reply(&Ok(reply.clone()), command).unwrap();
            let start = wire
                .windows(body.len())
                .position(|part| part == body)
                .unwrap();
            wire[start..start + body.len()].copy_from_slice(&invalid);
            assert!(
                decode_agent_reply(&wire, command).is_err(),
                "invalid base64 wire admitted"
            );
            assert!(
                encode_agent_reply(&Ok(changed), command).is_err(),
                "invalid base64 admitted"
            );
        }
    }
    let quiet = match (reply, command) {
        (AgentReply::Usage { page, settled, .. }, AgentCommand::Usage { after, .. }) => {
            !settled && page.as_ref().is_none_or(|page| page.end == *after)
        }
        (AgentReply::Events { page, settled, .. }, AgentCommand::Events { after, .. }) => {
            !settled && page.as_ref().is_none_or(|page| page.end == *after)
        }
        _ => false,
    };
    if quiet {
        // A snapshot without source progress cannot answer a live wait.
        let mut live = json(command);
        live["wait"] = true.into();
        let live: AgentCommand = serde_json::from_value(live).unwrap();
        let wire = encode_agent_reply(&Ok(reply.clone()), command).unwrap();
        assert!(
            decode_agent_reply(&wire, &live).is_err(),
            "quiet snapshot admitted as live progress"
        );
        assert!(encode_agent_reply(&Ok(reply.clone()), &live).is_err());
    }
}

/// Every positive input is emitted by the public SDK writer it exercises.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
    let agent = AgentPath::try_from("harness/agents/00000000000000000000000000000001".to_owned())?;
    let binding = acyclic_actors::wire::InvokeActorRequest {
        actor_id: "agents".into(),
        method: "POST".into(),
        url: "/agents/bind".into(),
        ..Default::default()
    };
    let route = AgentBinding::new(&binding.encode_to_vec())?;
    let mut seeds = vec![("binding".into(), binding.encode_to_vec())];
    let cases = [
        (
            "root",
            AgentCommand::Root {},
            AgentReply::Root {
                agent: agent.clone(),
            },
        ),
        (
            "bind",
            AgentCommand::Bind { agent: None },
            AgentBinding::reply(&binding, "fs:/tools".into()),
        ),
        (
            "cancel",
            AgentCommand::Cancel {
                agent: agent.clone(),
                id: IntentId::try_from("cancel")?,
            },
            AgentReply::Delivered {
                delivery: Delivered::Committed { inbox: 7 },
            },
        ),
        (
            "children",
            AgentCommand::Children {
                agent: agent.clone(),
                page: AgentPageRequest::default(),
            },
            AgentReply::Children {
                items: Vec::new(),
                next: None,
            },
        ),
        (
            "read-data",
            AgentCommand::Read {
                agent: agent.clone(),
                target: ReadTarget::Approval { status: 0 },
                offset: 0,
            },
            AgentReply::read(b"\x75\xab\x5a", 3),
        ),
        (
            "read-kind",
            AgentCommand::Read {
                agent: agent.clone(),
                target: ReadTarget::Approval { status: 0 },
                offset: 0,
            },
            AgentReply::read(b"\x92\x29\xdd", 3),
        ),
        (
            "read",
            AgentCommand::Read {
                agent: agent.clone(),
                target: ReadTarget::Approval { status: 0 },
                offset: 3,
            },
            AgentReply::read(b"original payload", 19),
        ),
        (
            "history",
            AgentCommand::History {
                agent: agent.clone(),
                page: AgentPageRequest::default(),
            },
            AgentReply::History {
                items: Vec::new(),
                next: None,
            },
        ),
        (
            "approvals",
            AgentCommand::Approvals {
                agent: agent.clone(),
                page: AgentPageRequest::default(),
            },
            AgentReply::Approvals {
                items: Vec::new(),
                next: None,
            },
        ),
        (
            "projects",
            AgentCommand::ProjectHeads {
                agent: agent.clone(),
            },
            AgentReply::ProjectHeads { items: Vec::new() },
        ),
        (
            "diff",
            AgentCommand::Diff {
                agent: agent.clone(),
                page: AgentPageRequest::default(),
            },
            AgentReply::Diff { items: Vec::new() },
        ),
        (
            "usage-tail",
            AgentCommand::Usage {
                agent: agent.clone(),
                input: 3,
                after: 0,
                wait: false,
            },
            AgentReply::Usage {
                input: 3,
                page: None,
                settled: false,
            },
        ),
        (
            "events-tail",
            AgentCommand::Events {
                agent: agent.clone(),
                input: 3,
                after: 0,
                wait: false,
            },
            AgentReply::Events {
                input: 3,
                inbox_tail: 3,
                page: None,
                settled: false,
            },
        ),
    ];
    for (name, command, reply) in cases {
        let prepared = PreparedCommand::new(&command)?;
        seeds.push((format!("command-{name}"), prepared.command().to_vec()));
        for (index, invocation) in route.prepare(&prepared)?.invocations().iter().enumerate() {
            AgentCall::decode(invocation)?;
            seeds.push((format!("invocation-{name}-{index}"), invocation.to_vec()));
        }
        let wire = encode_agent_reply(&Ok(reply.clone()), &command)?;
        seeds.push((format!("reply-{name}"), pair(&prepared, &wire)));
        seeds.push((
            format!("json-{name}"),
            pair(&prepared, &serde_json::to_vec(&reply)?),
        ));
    }
    let root = PreparedCommand::new(&AgentCommand::Root {})?;
    let path = StreamPath::new("agent/inbox")?;
    let original = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: 7,
        }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![AppendRecord::from("original payload")],
        }],
        idempotency_key: IdempotencyKey::new(bytes::Bytes::from_static(b"original-key"))?,
    };
    for (name, error) in [
        ("unknown", AgentError::Unknown(Box::new(original))),
        ("refused", AgentError::Conflict("original refusal")),
    ] {
        let code = error.code();
        let wire = encode_agent_reply(&Err(error), &AgentCommand::Root {})?;
        let decoded = decode_agent_reply(&wire, &AgentCommand::Root {})?.unwrap_err();
        assert_eq!(decoded.code(), code);
        if let AgentControlFailure::Refused { reason, .. } = decoded {
            assert!(reason.contains("original refusal"));
        }
        seeds.push((name.into(), pair(&root, &wire)));
    }
    Ok(seeds)
}

fn pair(command: &PreparedCommand, reply: &[u8]) -> Vec<u8> {
    let mut bytes = command.command().to_vec();
    bytes.push(0);
    bytes.extend_from_slice(reply);
    bytes
}
