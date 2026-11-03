//! Exercise public control admission and the exact request retained by Unknown.
use acyclic_harness_client::{
    AgentBinding, AgentCall, AgentCommand, AgentControlFailure, AgentError, AgentExchangeError,
    AgentPageRequest, AgentPath, AgentReply, Delivered, IntentId, PreparedCommand, ReadTarget,
    decode_agent_reply, encode_agent_reply,
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath,
};
use prost::Message as _;

/// Unframed inputs exercise public command, invocation and binding admission.
/// A NUL after a JSON command selects its following raw or JSON reply. This is
/// corpus framing only: all protocol parsing stays in the shipping entry points.
pub fn check(bytes: &[u8]) {
    // Conditional arbitrary-input checks cannot detect an admission path that
    // refuses everything. Retain the original public writer's positive witness.
    assert!(
        matches!(
            PreparedCommand::decode(include_bytes!("../corpus/agent_controls/command-root")),
            Ok(AgentCommand::Root {})
        ),
        "captured writer command refused"
    );
    for captured in [
        include_bytes!("../corpus/agent_controls/actor-response-root-0").as_slice(),
        include_bytes!("../corpus/agent_controls/actor-response-refused").as_slice(),
        include_bytes!("../corpus/agent_controls/assembled-cancel").as_slice(),
    ] {
        let separator = captured.iter().position(|byte| *byte == 0).unwrap();
        let call =
            AgentCall::decode(&captured[..separator]).expect("captured writer invocation refused");
        let body = &captured[separator + 1..];
        if matches!(call.command(), AgentCommand::Staged { .. }) {
            assert!(assembled(&call, body), "captured writer assembly refused");
        } else {
            assert!(
                actor_response(&call, body),
                "captured writer response refused"
            );
        }
    }
    let captured = include_bytes!("../corpus/agent_controls/unknown");
    let separator = captured.iter().position(|byte| *byte == 0).unwrap();
    let command = PreparedCommand::decode(&captured[..separator]).unwrap();
    assert!(
        unknown(&captured[separator + 1..], &command),
        "captured writer recovery request refused"
    );
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
    if let Ok(head) = AgentCall::decode(&bytes[..separator]) {
        if matches!(head.command(), AgentCommand::Staged { .. }) {
            assembled(&head, &bytes[separator + 1..]);
        } else {
            actor_response(&head, &bytes[separator + 1..]);
        }
        return;
    }
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
    unknown(reply, &command);
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

fn unknown(bytes: &[u8], command: &AgentCommand) -> bool {
    let Ok(Err(AgentControlFailure::Unknown(request))) = decode_agent_reply(bytes, command) else {
        return false;
    };
    let result = Err(AgentError::Unknown(request));
    assert_eq!(encode_agent_reply(&result, command).unwrap(), bytes);
    assert!(decode_agent_reply(&with_unknown_field(bytes), command).is_err());
    true
}

fn actor_response(call: &AgentCall, bytes: &[u8]) -> bool {
    let mut route = call.invocation().clone();
    let command = PreparedCommand::from_bytes(std::mem::take(&mut route.body)).unwrap();
    route.invocation_order = None;
    let binding = AgentBinding::new(&route.encode_to_vec()).unwrap();
    let Ok(invocation) = binding.prepare(&command) else {
        return false;
    };
    // Only the transport is supplied here; the public send path owns response
    // admission. This immediate byte source models no network or authority.
    let exchange = |bytes: &[u8]| {
        futures::executor::block_on(invocation.send::<AgentExchangeError, _, _>(|_| {
            futures::future::ready(Ok(bytes::Bytes::copy_from_slice(bytes)))
        }))
    };
    let Ok(result) = exchange(bytes) else {
        return false;
    };
    let response = acyclic_actors::wire::InvokeActorResponse::decode(bytes).unwrap();
    assert_eq!(response.encode_to_vec(), bytes);
    assert_eq!(response.status, 200);
    let decoded = decode_agent_reply(&response.body, call.command()).unwrap();
    match (result, decoded) {
        (Ok(actual), Ok(expected)) => assert_eq!(json(&actual), json(&expected)),
        (Err(actual), Err(expected)) => assert_eq!(actual.code(), expected.code()),
        _ => panic!("actor response changed its original control outcome"),
    }
    assert!(
        exchange(&with_unknown_field(bytes)).is_err(),
        "noncanonical actor response admitted"
    );
    assert!(exchange(&[bytes, bytes].concat()).is_err());
    let mut denied = response;
    denied.status = 403;
    assert!(
        matches!(
            exchange(&denied.encode_to_vec()),
            Err(AgentExchangeError::ActorStatus(403))
        ),
        "actor status discarded"
    );
    true
}

fn assembled(head: &AgentCall, bytes: &[u8]) -> bool {
    let Ok(call) = head.assembled(bytes) else {
        return false;
    };
    let AgentCommand::Staged { agent, command, .. } = head.command() else {
        panic!("assembly admitted a non-staged head");
    };
    assert_eq!(call.command().agent(), Some(agent));
    assert_eq!(call.command().kind(), command);
    assert!(!matches!(
        call.command(),
        AgentCommand::Stage { .. } | AgentCommand::Staged { .. }
    ));
    assert_eq!(
        call.invocation().encode_to_vec(),
        head.invocation().encode_to_vec()
    );
    assert_eq!(
        json(call.command()),
        json(&PreparedCommand::decode_whole(bytes).unwrap())
    );
    let mut foreign = json(call.command());
    let other = if agent.as_str().ends_with('1') {
        '2'
    } else {
        '1'
    };
    let mut path = agent.as_str().to_owned();
    path.pop();
    path.push(other);
    let key = if foreign.get("parent").is_some() {
        "parent"
    } else {
        "agent"
    };
    foreign[key] = path.into();
    assert!(
        head.assembled(&serde_json::to_vec(&foreign).unwrap())
            .is_err(),
        "assembled command changed its admitted agent"
    );
    let mut foreign = json(head.command());
    foreign["command"] = "root".into();
    let mut invocation = head.invocation().clone();
    invocation.body = serde_json::to_vec(&foreign).unwrap();
    let foreign = AgentCall::decode(&invocation.encode_to_vec()).unwrap();
    assert!(
        foreign.assembled(bytes).is_err(),
        "assembled command changed its admitted kind"
    );
    true
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
            let call = AgentCall::decode(invocation)?;
            seeds.push((format!("invocation-{name}-{index}"), invocation.to_vec()));
            let mut exchange = invocation.to_vec();
            exchange.push(0);
            exchange.extend_from_slice(&call.reply(&Ok(reply.clone()))?);
            seeds.push((format!("actor-response-{name}-{index}"), exchange));
        }
        let wire = encode_agent_reply(&Ok(reply.clone()), &command)?;
        seeds.push((format!("reply-{name}"), pair(&prepared, &wire)));
        seeds.push((
            format!("json-{name}"),
            pair(&prepared, &serde_json::to_vec(&reply)?),
        ));
    }
    let whole = PreparedCommand::new(&AgentCommand::Cancel {
        agent: agent.clone(),
        id: IntentId::try_from("staged-cancel")?,
    })?;
    let head = PreparedCommand::new(&AgentCommand::Staged {
        agent,
        command: whole.decoded()?.kind().to_owned(),
        digest: blake3::hash(whole.command()).to_hex().to_string(),
        parts: 1,
        bytes: whole.command().len() as u64,
    })?;
    for invocation in route.prepare(&head)?.invocations() {
        let mut input = invocation.to_vec();
        input.push(0);
        input.extend_from_slice(whole.command());
        seeds.push(("assembled-cancel".into(), input));
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
        let result = Err(error);
        let wire = encode_agent_reply(&result, &AgentCommand::Root {})?;
        for invocation in route.prepare(&root)?.invocations() {
            let mut exchange = invocation.to_vec();
            exchange.push(0);
            exchange.extend_from_slice(&AgentCall::decode(invocation)?.reply(&result)?);
            seeds.push((format!("actor-response-{name}"), exchange));
        }
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
