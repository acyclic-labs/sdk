//! Guest ABI properties, parser corpus replay and generated declaration drift.
use acyclic_actors::guest::*;
use proptest::prelude::*;

fn identity(version: &str) -> CodecIdentity {
    CodecIdentity {
        name: "test-bytes".into(),
        version: version.into(),
    }
}

fn codec(version: &str, readable: Vec<CodecIdentity>) -> CheckpointCodec<Vec<u8>> {
    CheckpointCodec {
        identity: identity(version),
        readable,
        encode: |state| Ok(state.clone().into()),
        decode: |checkpoint| Ok(checkpoint.bytes.to_vec()),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Constructors accept exactly nonempty, trimmed, control-free identities
    /// and nonzero versions/expiries, preserving all admitted values verbatim.
    #[test]
    fn host_metadata_construction_preserves_valid_input(value in ".{0,128}", version in any::<u32>(), expiry in any::<u64>()) {
        let valid = value.len() <= MAX_GUEST_METADATA_BYTES && !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control);
        let capability = CallerCapability::new(version, value.clone());
        let runtime = RuntimeHandle::new(version, value.clone());
        prop_assert_eq!(capability.is_ok(), valid && version != 0);
        prop_assert_eq!(runtime.is_ok(), valid && version != 0);
        if let Ok(capability) = capability {
            prop_assert_eq!(capability.handle(), value.as_str());
            prop_assert_eq!(capability.version(), version);
        }
        let caller = AdmittedCaller::new(value.clone(), value.clone(), value.clone(), value.clone(), value.clone(), expiry);
        prop_assert_eq!(caller.is_ok(), valid && expiry != 0);
        if let Ok(caller) = caller {
            prop_assert_eq!(caller.principal(), &value);
            prop_assert_eq!(caller.expires_at_ms(), &expiry.to_string());
            prop_assert_eq!(serde_json::from_slice::<AdmittedCaller>(&serde_json::to_vec(&caller)?)?, caller);
        }
    }

    #[test]
    fn artifact_codec_round_trip(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        let codec = codec("1", vec![identity("1")]);
        let checkpoint = Checkpoint { codec: codec.identity.clone(), bytes: (codec.encode)(&bytes)? };
        let wire = serde_json::to_vec(&checkpoint)?;
        let decoded: Checkpoint = serde_json::from_slice(&wire)?;
        prop_assert_eq!((codec.decode)(&decoded)?, bytes);
    }

    #[test]
    fn rejected_update_preserves_entire_installed_boundary(
        configuration in any::<u64>(), subscriptions in prop::collection::vec(any::<u64>(), 0..16),
        bytes in prop::collection::vec(any::<u8>(), 0..512), decoder_fails in any::<bool>(),
    ) {
        let mut installed = GuestInstallation {
            configuration,
            subscriptions,
            checkpoint: Some(Checkpoint { codec: identity("1"), bytes: bytes.into() }),
        };
        let prior = installed.clone();
        let mut candidate = codec("2", vec![identity("2")]);
        if decoder_fails {
            candidate.readable.push(identity("1"));
            candidate.decode = |_| Err(GuestError::Decode("bad state".into()));
        }
        prop_assert!(installed.update(configuration.wrapping_add(1), vec![], &candidate).is_err());
        prop_assert_eq!(installed, prior);
    }

    #[test]
    fn compatible_update_decodes_prior_format_without_rewriting_checkpoint(
        bytes in prop::collection::vec(any::<u8>(), 0..512),
    ) {
        let checkpoint = Checkpoint { codec: identity("1"), bytes: bytes.clone().into() };
        let mut installed = GuestInstallation {
            configuration: "old", subscriptions: vec![1], checkpoint: Some(checkpoint.clone()),
        };
        let candidate = codec("2", vec![identity("1"), identity("2")]);
        prop_assert_eq!(installed.update("new", vec![2], &candidate)?, Some(bytes));
        prop_assert_eq!(installed.configuration, "new");
        prop_assert_eq!(installed.subscriptions, vec![2]);
        prop_assert_eq!(installed.checkpoint, Some(checkpoint));
    }

    #[test]
    fn effect_keys_are_stable_and_injective(a in ".{1,64}", b in ".{1,64}", c in ".{1,64}", d in ".{1,64}") {
        let first = retry_key(&a, &b)?;
        prop_assert_eq!(&first, &retry_key(&a, &b)?);
        if a != c || b != d { prop_assert_ne!(first, retry_key(&c, &d)?); }
    }

    #[test]
    fn generated_events_round_trip(
        kind in 0u8..9, identity in ".{0,32}", number in any::<u64>(),
        bytes in prop::collection::vec(any::<u8>(), 0..512),
    ) {
        let event = match kind {
            0 => GuestEvent::Stream { subscription: identity.clone(), stream: identity.clone(),
                sequence: number.to_string(), committed_at_micros: number.to_string(),
                value: identity.clone(), commit_id: identity },
            1 => GuestEvent::Invoke { payload: bytes.into() },
            2 => GuestEvent::Fetch { request: GuestRequest { url: identity.clone(), method: identity.clone(),
                headers: vec![(identity.clone(), identity)], body: bytes.into() } },
            3 => GuestEvent::Timer { timer_id: identity, deadline_unix_ms: number.to_string() },
            kind => GuestEvent::Socket { connection_id: identity.clone(), event: match kind {
                4 => GuestSocketEvent::Open { actor_id: identity.clone(), app_channel: Some(identity.clone()), subprotocols: vec![identity], headers: vec![], resume_after: number.to_string(), reconnected: false, transport: Some(SocketTransport::Internal) },
                5 => GuestSocketEvent::Message { offset: number.to_string(), payload: if number % 2 == 0 { SocketPayload::Text { text: identity } } else { SocketPayload::Binary { binary: bytes.into() } } },
                6 => GuestSocketEvent::Close { offset: number.to_string(), code: u16::try_from(number % 65536)?, reason: identity, initiator: SocketCloseInitiator::Runtime },
                7 => GuestSocketEvent::Error { offset: number.to_string(), code: identity.clone(), message: identity },
                _ => GuestSocketEvent::Reconnecting { attempt: 1, delay_ms: number.to_string() },
            } },
        };
        let encoded = serde_json::to_vec(&event)?;
        prop_assert_eq!(serde_json::from_slice::<GuestEvent>(&encoded)?, event);
    }

    #[test]
    fn older_guest_accepts_new_host_fields_and_unknown_event_types(
        timer in ".{0,64}", future_field in "[a-z]{1,24}", value in any::<u64>(),
    ) {
        let mut newer = serde_json::json!({"type":"timer", "timerId":timer, "deadlineUnixMs":value.to_string()});
        newer.as_object_mut().ok_or_else(|| TestCaseError::fail("event object"))?.insert(format!("future_{future_field}"), serde_json::json!({"nested":[value,true]}));
        prop_assert_eq!(serde_json::from_value::<GuestEvent>(newer)?, GuestEvent::Timer { timer_id: timer, deadline_unix_ms: value.to_string() });
        let socket = serde_json::json!({"type":"socket", "connectionId":"app", "future":"added", "event":{"type":"close", "offset":"2", "code":1013, "reason":"backpressure", "initiator":"runtime", "future":value}});
        prop_assert!(matches!(serde_json::from_value::<GuestEvent>(socket)?, GuestEvent::Socket { event: GuestSocketEvent::Close { .. }, .. }), "extended socket Close must decode");
        prop_assert_eq!(serde_json::from_value::<GuestEvent>(serde_json::json!({"type":format!("future_{future_field}"),"body":value}))?, GuestEvent::Unknown);
        prop_assert_eq!(serde_json::from_value::<GuestSocketEvent>(serde_json::json!({"type":format!("future_{future_field}"),"body":value}))?, GuestSocketEvent::Unknown);
    }

    #[test]
    fn accepted_untrusted_json_is_round_trip_stable(input in prop::collection::vec(any::<u8>(), 0..2048)) {
        if let Ok(event) = serde_json::from_slice::<GuestEvent>(&input) {
            prop_assert_eq!(serde_json::from_slice::<GuestEvent>(&serde_json::to_vec(&event)?)?, event);
        }
    }
}

#[test]
fn downstream_host_constructs_context_without_serde() -> Result<(), Box<dyn std::error::Error>> {
    let capability = CallerCapability::new(1, "invocation-capability".into())?;
    let caller = AdmittedCaller::new(
        "account".into(),
        "org".into(),
        "principal".into(),
        "credential".into(),
        "actor".into(),
        u64::MAX,
    )?
    .with_capability(capability)?;
    let mut actor = ActorRuntime::default();
    actor.alarms = Some(RuntimeHandle::new(2, "alarm-handle".into())?);
    let context = GuestContext::builder((), Box::new(|| 1))
        .admitted_identity(caller)
        .actor(actor)
        .build();
    assert_eq!(
        context
            .admitted_identity
            .as_ref()
            .map(|v| v.expires_at_ms()),
        Some("18446744073709551615")
    );
    assert_eq!(context.actor.alarms.as_ref().map(|v| v.version()), Some(2));
    for invalid in ["", " ", " leading", "trailing ", "nul\0", "line\nfeed"] {
        assert!(CallerCapability::new(1, invalid.into()).is_err());
        assert!(RuntimeHandle::new(1, invalid.into()).is_err());
        for field in 0..5 {
            let mut fields = ["a", "o", "p", "c", "i"];
            *fields.get_mut(field).ok_or("caller field")? = invalid;
            assert!(
                AdmittedCaller::new(
                    fields[0].into(),
                    fields[1].into(),
                    fields[2].into(),
                    fields[3].into(),
                    fields[4].into(),
                    1
                )
                .is_err()
            );
        }
    }
    assert!(serde_json::from_str::<CallerCapability>(r#"{"version":0,"handle":"cap"}"#).is_err());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn socket_admission_results_round_trip(attachment in prop::collection::vec(any::<u8>(), 0..2048), tag in ".{0,64}") {
        for result in [
            GuestEventResult::SocketAccept { subprotocol: Some("chat".into()), attachment: attachment.clone().into(), tags: vec![tag.clone()] },
            GuestEventResult::SocketReject { status: 403, reason: tag },
            GuestEventResult::Reply { payload: attachment.into() },
        ] {
            prop_assert_eq!(serde_json::from_slice::<GuestEventResult>(&serde_json::to_vec(&result)?)?, result);
        }
    }

    #[test]
    fn admitted_caller_projection_accepts_extensions_and_round_trips(principal in ".{0,64}", handle in ".{0,64}", expiry in any::<u64>()) {
        let caller = serde_json::from_value::<AdmittedCaller>(serde_json::json!({
            "accountId":"account", "orgId":"org", "principal":principal,
            "credentialId":"credential", "actorId":"actor", "expiresAtMs":expiry.to_string(),
            "capability":{"version":1,"handle":handle,"future":true}, "future":true
        }));
        let valid = |value: &str| !value.is_empty() && value.trim() == value && !value.chars().any(char::is_control);
        prop_assert_eq!(caller.is_ok(), valid(&principal) && valid(&handle) && expiry != 0);
        if let Ok(caller) = caller {
            prop_assert_eq!(serde_json::from_slice::<AdmittedCaller>(&serde_json::to_vec(&caller)?)?, caller);
        }
    }
}

#[test]
fn artifact_handshake_and_context_defaults_are_explicit() {
    assert_eq!(negotiate_abi(GUEST_ABI_VERSION), Ok(()));
    for version in [0, 2, u32::MAX] {
        assert_eq!(
            negotiate_abi(version),
            Err(GuestError::UnsupportedAbi(version))
        );
    }
    let lifecycle = GuestContext::builder((), Box::new(|| 42)).build();
    assert!(lifecycle.admitted_identity.is_none());
    assert!(lifecycle.actor.alarms.is_none());
    assert!((lifecycle.retry_key)("effect").is_err());
    let delivery = GuestContext::builder((), Box::new(|| 42))
        .delivery("stable".into())
        .build();
    assert_eq!(
        (delivery.retry_key)("effect"),
        retry_key("stable", "effect")
    );
}

#[tokio::test]
async fn native_fetch_supports_lazy_chunks_and_buffered_adaptation()
-> Result<(), Box<dyn std::error::Error>> {
    use bytes::Bytes;
    use futures::StreamExt as _;
    let bytes = Bytes::from_static(b"data: first\n\n");
    let buffered = GuestResponse {
        status: 200,
        headers: vec![("x-test".into(), "a".into()), ("x-test".into(), "b".into())],
        body: bytes.clone(),
    }
    .into_streaming()?;
    assert_eq!(buffered.headers().get_all("x-test").iter().count(), 2);
    if let GuestBody::Buffered(body) = buffered.into_body() {
        assert_eq!(body.as_ptr(), bytes.as_ptr());
    } else {
        return Err("buffered body changed representation".into());
    }
    let stream = futures::stream::iter([Ok(bytes), Ok(Bytes::from_static(b"data: second\n\n"))]);
    let response = http::Response::builder()
        .header("content-type", "text/event-stream")
        .body(GuestBody::Stream(Box::pin(stream)))?;
    if let GuestBody::Stream(mut body) = response.into_body() {
        assert_eq!(
            body.next().await.transpose()?,
            Some(Bytes::from_static(b"data: first\n\n"))
        );
        assert_eq!(
            body.next().await.transpose()?,
            Some(Bytes::from_static(b"data: second\n\n"))
        );
    } else {
        return Err("stream was buffered".into());
    }
    Ok(())
}

#[test]
fn fuzz_seed_corpus_round_trips_accepted_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../fuzz/corpus/guest_json");
    for entry in std::fs::read_dir(root)? {
        let input = std::fs::read(entry?.path())?;
        if let Ok(event) = serde_json::from_slice::<GuestEvent>(&input) {
            assert_eq!(
                serde_json::from_slice::<GuestEvent>(&serde_json::to_vec(&event)?)?,
                event
            );
        }
        if let Ok(checkpoint) = serde_json::from_slice::<Checkpoint>(&input) {
            assert_eq!(
                serde_json::from_slice::<Checkpoint>(&serde_json::to_vec(&checkpoint)?)?,
                checkpoint
            );
        }
    }
    Ok(())
}

#[test]
fn absent_checkpoint_still_requires_valid_codec_and_never_calls_decoder() {
    let mut candidate = codec("1", vec![]);
    assert_eq!(
        prepare_update(None, &candidate),
        Err(GuestError::IncompatibleCodec)
    );
    candidate.readable.push(identity("1"));
    candidate.decode = |_| {
        Err(GuestError::Decode(
            "absent checkpoint must not be decoded".into(),
        ))
    };
    assert_eq!(prepare_update(None, &candidate), Ok(None));
    assert!(retry_key("", "effect").is_err());
    assert!(retry_key("delivery", "").is_err());
}

#[test]
fn every_event_round_trips_with_a_single_discriminant() -> Result<(), Box<dyn std::error::Error>> {
    let request = GuestRequest {
        url: "https://example.test/".into(),
        method: "POST".into(),
        headers: vec![],
        body: vec![0, 255].into(),
    };
    let events = [
        GuestEvent::Stream {
            subscription: "inbox".into(),
            stream: "agents/a/inbox".into(),
            sequence: u64::MAX.to_string(),
            committed_at_micros: "0".into(),
            value: "AP8=".into(),
            commit_id: "ab".repeat(16),
        },
        GuestEvent::Invoke {
            payload: vec![0, 255].into(),
        },
        GuestEvent::Fetch { request },
        GuestEvent::Timer {
            timer_id: "timer-1".into(),
            deadline_unix_ms: "123".into(),
        },
        GuestEvent::Socket {
            connection_id: "app-1".into(),
            event: GuestSocketEvent::Reconnecting {
                attempt: 1,
                delay_ms: "10".into(),
            },
        },
    ];
    for event in events {
        assert_eq!(
            serde_json::from_slice::<GuestEvent>(&serde_json::to_vec(&event)?)?,
            event
        );
    }
    Ok(())
}

#[test]
fn generated_authoring_contract_has_no_drift() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../typescript/packages/actors/src/generated/guest.ts");
    let generated = typescript();
    assert_eq!(std::fs::read_to_string(path)?, generated);
    Ok(())
}
