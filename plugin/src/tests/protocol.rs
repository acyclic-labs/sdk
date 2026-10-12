use super::*;

struct Dispatcher(std::sync::atomic::AtomicUsize);

impl ConcurrentControlRequestDispatcher for Dispatcher {
    async fn dispatch_request(
        &self,
        _request: ControlRequest,
        _work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        Ok((
            json!({"execution":self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1}),
            None,
        ))
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
    for encoded in [&legacy, &current] {
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
