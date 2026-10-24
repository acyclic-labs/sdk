use super::*;

struct Dispatcher(std::sync::atomic::AtomicUsize);

impl ConcurrentControlRequestDispatcher for Dispatcher {
    fn dispatch_request<'a>(
        &'a self,
        _request: ControlRequest,
        work: &'a mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatch<'a>> {
        let execution = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        acyclic_harness::conversation::BoundedOperationFuture::on_native_work(work, |_| {
            std::future::ready(Ok((json!({ "execution": execution }), None)))
        })
    }
}

#[tokio::test]
async fn completed_hook_replays_after_restart_without_reexecution() {
    let fixture = Fixture::new();
    let dispatcher = Arc::new(Dispatcher(std::sync::atomic::AtomicUsize::new(0)));
    let envelope = ControlEnvelope::new(native_hook_request(
        "codex",
        "PreToolUse",
        "session",
        &fixture.root,
        json!({"tool_use_id":"tool"}),
    ));
    let ledger = Arc::new(ControlLedger::open(&fixture.data).unwrap());
    let first = dispatch_control_envelope(&dispatcher, &ledger, envelope.clone(), &mut None)
        .await
        .unwrap();
    drop(ledger);
    let ledger = Arc::new(ControlLedger::open(&fixture.data).unwrap());
    let replay = dispatch_control_envelope(&dispatcher, &ledger, envelope, &mut None)
        .await
        .unwrap();
    assert!(first.2.is_none() && replay.2.is_none());
    assert_eq!(
        serde_json::from_slice::<Value>(&first.0).unwrap()["ok"],
        true
    );
    assert_eq!(first.0, replay.0);
    assert_eq!(dispatcher.0.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn protocol_mismatch_never_reaches_dispatch() {
    let fixture = Fixture::new();
    let dispatcher = Arc::new(Dispatcher(std::sync::atomic::AtomicUsize::new(0)));
    let ledger = Arc::new(ControlLedger::open(&fixture.data).unwrap());
    for command in [ControlCommand::Ping, ControlCommand::Agents] {
        let mut envelope = ControlEnvelope::new(ControlRequest {
            version: 1,
            command,
            cwd: fixture.root.clone(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        });
        envelope.protocol.major += 1;
        let response = dispatch_control_envelope(&dispatcher, &ledger, envelope, &mut None)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&response.0).unwrap()["ok"],
            false
        );
    }
    assert_eq!(dispatcher.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn responses_are_bounded_and_preserve_correlation() {
    let payload = json!({"payload":"x".repeat(MAXIMUM_CONTROL_MESSAGE_BYTES)});
    let request_id = control_protocol::RequestId::fresh();
    let legacy = encode_control_response(&payload, None);
    let (current, _allocation, _) =
        control_response_for(&request_id, Ok((payload, None)), None).unwrap();
    for encoded in [&legacy[..], &current[..]] {
        assert!(encoded.len() < MAXIMUM_CONTROL_MESSAGE_BYTES);
        assert_eq!(
            serde_json::from_slice::<Value>(encoded).unwrap()["ok"],
            false
        );
    }
    assert_eq!(
        serde_json::from_slice::<Value>(&current).unwrap()["requestId"],
        request_id.as_str()
    );
}

#[test]
fn malformed_envelopes_preserve_only_valid_request_identity() {
    let request_id = control_protocol::RequestId::fresh();
    for input in [
        json!({"requestId":request_id.as_str(),"unexpected":true}),
        json!({"unexpected":true}),
    ] {
        let bytes = serde_json::to_vec(&input).unwrap();
        let error = serde_json::from_slice::<ControlEnvelope<ControlRequest>>(&bytes).unwrap_err();
        let (encoded, _allocation, _) = invalid_control_request_response(&bytes, &error).unwrap();
        let response: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(response["ok"], false);
        if input.get("requestId").is_some() {
            assert_eq!(response["requestId"], request_id.as_str());
        } else {
            assert!(response.get("requestId").is_none());
            assert!(matches!(
                decode_control_response(&encoded, &request_id),
                Err(ControlRequestError::Indeterminate(_))
            ));
        }
    }
}

#[test]
fn rpc_frames_handle_stream_boundaries_and_reject_oversize() {
    let mut input = io::Cursor::new(b"first\r\nsecond\nlast".to_vec());
    for expected in [
        Some(b"first".to_vec()),
        Some(b"second".to_vec()),
        Some(b"last".to_vec()),
        None,
    ] {
        assert_eq!(read_bounded_rpc_line(&mut input).unwrap(), expected);
    }
    let mut input = io::Cursor::new(vec![b'x'; MAXIMUM_CONTROL_MESSAGE_BYTES + 1]);
    assert!(read_bounded_rpc_line(&mut input).is_err());
}

/// Refuses its own decode for budget and leaves the starved ledger as the
/// request's original native work, so no frame can be funded from it either.
struct Starved;

impl ConcurrentControlRequestDispatcher for Starved {
    fn dispatch_request<'a>(
        &'a self,
        _request: ControlRequest,
        work: &'a mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatch<'a>> {
        let starved =
            acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget {
                    peak_allocation_bytes:
                        acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 64,
                    ..pi_root_provider::allocation_budget()
                },
                &CancellationToken::new(),
            )
            .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
        let decoded = pi_root_provider::guarded_pi_decode::<Vec<String>>(
            br#"["refused","by","budget"]"#,
            1024,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            0,
            &starved,
        );
        *work = Some(starved);
        let response = decoded.map(|(value, allocation)| (json!(value), Some(allocation)))?;
        acyclic_harness::conversation::BoundedOperationFuture::on_native_work(work, |_| {
            std::future::ready(Ok(response))
        })
    }
}

fn starved_envelope(fixture: &Fixture) -> ControlEnvelope<ControlRequest> {
    ControlEnvelope::new(native_hook_request(
        "codex",
        "PreToolUse",
        "session",
        &fixture.root,
        json!({"tool_use_id":"tool"}),
    ))
}

#[tokio::test]
async fn budget_refused_decode_reaches_the_client_as_a_typed_refusal() {
    let fixture = Fixture::new();
    let ledger = Arc::new(ControlLedger::open(&fixture.data).unwrap());
    let envelope = starved_envelope(&fixture);
    let request_id = envelope.request_id.clone();
    let mut work = None;
    let (frame, allocation, failure) =
        dispatch_control_envelope(&Arc::new(Starved), &ledger, envelope, &mut work)
            .await
            .unwrap();
    let failure = failure.unwrap();
    // Negative control: the starved ledger funds no encoded frame, so only
    // the reserved inline refusal can answer.
    assert!(
        service_process::encode_native_control_value(&json!({}), work.as_ref().unwrap()).is_err()
    );
    assert!(allocation.is_none());
    assert!(frame.is_refusal());
    assert!(frame.len() <= REFUSAL_FRAME_BYTES);
    let Err(ControlRequestError::Refused { code, message }) =
        decode_control_response(&frame, &request_id)
    else {
        panic!("not a typed refusal: {frame:?}");
    };
    assert_eq!(code, failure.code());
    assert_eq!(message, failure.to_string());
}

#[test]
fn refusal_frames_stay_valid_json_when_the_message_is_cut() {
    let request_id = control_protocol::RequestId::fresh();
    // Quotes, backslashes and control characters all expand when escaped.
    let error = acyclic_harness::Error::Invalid("\"\\\u{1}é".repeat(REFUSAL_FRAME_BYTES));
    let frame = RefusalFrame::new(Some(&request_id), &error);
    let bytes = frame.as_bytes();
    assert!(bytes.len() <= REFUSAL_FRAME_BYTES);
    let decoded: Value = serde_json::from_slice(bytes).unwrap();
    let cut = decoded["error"].as_str().unwrap();
    let rendered = error.to_string();
    assert!(!cut.is_empty() && cut.len() < rendered.len() && rendered.starts_with(cut));
    assert_eq!(decoded["requestId"], request_id.as_str());
    assert_eq!(decoded["code"], "ERROR_CODE_INVALID");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_budget_refusal_is_answered_over_the_mailbox_before_any_deadline() {
    let fixture = Fixture::new();
    let mailbox = prepare_linux_control_mailbox(&fixture.data).unwrap();
    let ledger = Arc::new(ControlLedger::open(&fixture.data).unwrap());
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(serve_linux_control_mailbox(
        mailbox,
        Arc::new(Starved),
        ledger,
        stop.clone(),
        stopped,
    ));
    let envelope = starved_envelope(&fixture);
    let result = send_control_envelope_once(&fixture.data, &envelope).await;
    let _ = stop.send(true);
    server.await.unwrap().unwrap();
    assert!(
        matches!(result, Err(ControlRequestError::Refused { .. })),
        "{result:?}"
    );
}
