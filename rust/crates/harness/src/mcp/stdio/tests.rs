use super::*;

use super::*;

fn ids() -> (OperationId, OperationId) {
    (
        OperationId::from_bytes([1; 16]),
        OperationId::from_bytes([2; 16]),
    )
}

fn initialized(id: OperationId) -> Vec<u8> {
    line(&json!({"jsonrpc":"2.0","id":id.to_string(),"result":{
            "protocolVersion":PROTOCOL_VERSION,"capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}})).unwrap()
}

#[test]
fn exact_descriptor_and_retained_response_have_one_protocol_boundary() {
    let (initialization, operation) = ids();
    let request = McpStdioRequest {
        initialization,
        operation,
        method: McpStdioMethod::CallTool,
        params: json!({"name":"echo","arguments":{}}),
        maximum_bytes: 4096,
    };
    request.validate().unwrap();
    let expected = json!({"content":[{"type":"text","text":"héllo"}],
            "structuredContent":{"sequence":u64::MAX}});
    let mut transcript = initialized(initialization);
    transcript.extend(
        line(&json!({"jsonrpc":"2.0","id":operation.to_string(),"result":expected})).unwrap(),
    );
    assert_eq!(request.decode_response(&transcript).unwrap(), expected);
    assert!(
        request
            .decode_response(transcript.get(..transcript.len() - 1).unwrap())
            .is_err()
    );
    for params in [
        json!({}),
        json!([]),
        json!({"name":"echo","arguments":[]}),
        json!({"name":"echo","task":{}}),
        json!({"name":"echo","request_state":"retry"}),
        json!({"name":"echo","requestState":"retry"}),
    ] {
        assert!(
            McpStdioRequest {
                params: params.clone(),
                ..request
            }
            .validate()
            .is_err(),
            "accepted {params}"
        );
    }
    assert!(
        McpStdioRequest {
            operation: initialization,
            ..request.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        McpStdioRequest {
            maximum_bytes: 0,
            ..request.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        McpStdioRequest {
            method: McpStdioMethod::ListTools,
            params: json!({"cursor":1}),
            ..request
        }
        .validate()
        .is_err()
    );
    let mut malformed = initialized(initialization);
    malformed.extend(
        line(
            &json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{"structuredContent":{}}}),
        )
        .unwrap(),
    );
    assert!(request.decode_response(&malformed).is_err());
    let mut encoded = serde_json::to_value(&request).unwrap();
    encoded["method"] = json!("sampling/createMessage");
    assert!(serde_json::from_value::<McpStdioRequest>(encoded).is_err());
}

#[test]
fn every_stream_cut_preserves_order_and_exact_result() {
    let (init, op) = ids();
    let expected = json!({"content":[{"type":"text","text":"héllo"}]});
    let mut transcript = initialized(init);
    transcript.extend(
        line(&json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"})).unwrap(),
    );
    transcript
        .extend(line(&json!({"jsonrpc":"2.0","id":op.to_string(),"result":expected})).unwrap());
    for width in 1..=transcript.len() {
        let (mut codec, first, _) = McpStdioRequest {
            initialization: init,
            operation: op,
            method: McpStdioMethod::CallTool,
            params: json!({"name":"echo","arguments":{}}),
            maximum_bytes: 4096,
        }
        .exchange(None)
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&first).unwrap()["method"],
            "initialize"
        );
        let mut writes = Vec::new();
        for chunk in transcript.chunks(width) {
            writes.extend(codec.push(chunk, None).unwrap().0);
        }
        let messages: Vec<Value> = writes
            .split(|byte| *byte == b'\n')
            .filter(|part| !part.is_empty())
            .map(|part| serde_json::from_slice(part).unwrap())
            .collect();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["method"], "notifications/initialized");
        assert_eq!(messages[1]["id"], op.to_string());
        assert_eq!(codec.finish().unwrap().0, expected);
    }
}

#[test]
fn malformed_foreign_callback_and_truncated_inputs_cannot_replay() {
    let (init, op) = ids();
    for bytes in [b"not json\n".to_vec(), line(&json!({"jsonrpc":"2.0","id":op.to_string(),"result":{}})).unwrap(),
            line(&json!({"jsonrpc":"2.0","id":init.to_string(),"method":"sampling/createMessage","params":{}})).unwrap()] {
            let (mut codec,_,_) = McpStdioRequest {
                initialization: init, operation: op, method: McpStdioMethod::ListTools,
                params: json!({}), maximum_bytes: 4096,
            }.exchange(None).unwrap();
            assert!(codec.push(&bytes, None).is_err());
            assert!(codec.push(&initialized(init), None).is_err());
            assert!(codec.finish().is_err());
        }
    let (mut codec, _, _) = McpStdioRequest {
        initialization: init,
        operation: op,
        method: McpStdioMethod::ListTools,
        params: json!({}),
        maximum_bytes: 4096,
    }
    .exchange(None)
    .unwrap();
    assert!(codec.push(&vec![b' '; 4097], None).is_err());
    let (mut codec, _, _) = McpStdioRequest {
        initialization: init,
        operation: op,
        method: McpStdioMethod::ListTools,
        params: json!({}),
        maximum_bytes: 4096,
    }
    .exchange(None)
    .unwrap();
    let mut truncated = initialized(init);
    truncated.pop();
    assert!(codec.push(&truncated, None).unwrap().0.is_empty());
    assert!(matches!(codec.finish(), Err(Error::Indeterminate(_))));
}

#[test]
fn admitted_deep_json_and_crlf_bom_survive_stream_cuts() {
    let (init, op) = ids();
    let nested = format!("{}0{}", "[".repeat(192), "]".repeat(192));
    let expected: Value = crate::contract::json_from_slice(nested.as_bytes()).unwrap();
    let mut transcript = initialized(init);
    transcript.extend_from_slice(
        format!("\u{feff}{{\"jsonrpc\":\"2.0\",\"id\":\"{op}\",\"result\":{nested}}}\r\n")
            .as_bytes(),
    );
    for width in [1, 2, 3, 17, transcript.len()] {
        let (mut codec, _, _) = McpStdioRequest {
            initialization: init,
            operation: op,
            method: McpStdioMethod::ListTools,
            params: json!({}),
            maximum_bytes: 4096,
        }
        .exchange(None)
        .unwrap();
        for chunk in transcript.chunks(width) {
            codec.push(chunk, None).unwrap().0;
        }
        assert_eq!(codec.finish().unwrap().0, expected);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn malformed_private_stdout_never_reaches_library_debug_log() {
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("log capture lock poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let logs = Capture(Arc::default());
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let private = b"PRIVATE_STDOUT_FIXTURE invalid json\n";
        let (init, op) = ids();
        let (mut guarded, _, _) = McpStdioRequest {
            initialization: init,
            operation: op,
            method: McpStdioMethod::ListTools,
            params: json!({}),
            maximum_bytes: 4096,
        }
        .exchange(None)
        .unwrap();
        assert!(guarded.push(private, None).is_err());
        assert!(guarded.push(&initialized(init), None).is_err());
        assert!(logs.0.lock().unwrap().is_empty());
    });
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn finite_budget() -> acyclic_fs::WorkBudget {
    let mut encoded = serde_json::to_value(acyclic_fs::WorkBudget::UNBOUNDED).unwrap();
    for value in encoded.as_object_mut().unwrap().values_mut() {
        *value = Value::from(100_000_000u64);
    }
    serde_json::from_value(encoded).unwrap()
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_work() -> acyclic_fs::NativeMaterializationWork {
    acyclic_fs::NativeMaterializationWork::new(
        finite_budget(),
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap()
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn original_native_all_seven_methods_preserve_protocol_and_result_custody() {
    let (initialization, operation) = ids();
    for (method, params, expected) in [
        (McpStdioMethod::ListTools, json!({}), json!({"tools":[]})),
        (
            McpStdioMethod::CallTool,
            json!({"name":"echo","arguments":{"n":u64::MAX}}),
            json!({"content":[{"type":"text","text":"héllo"}],"structuredContent":null}),
        ),
        (
            McpStdioMethod::ListResources,
            json!({}),
            json!({"resources":[]}),
        ),
        (
            McpStdioMethod::ListResourceTemplates,
            json!({}),
            json!({"resourceTemplates":[]}),
        ),
        (
            McpStdioMethod::ReadResource,
            json!({"uri":"fixture://binary"}),
            json!({"contents":[{"uri":"fixture://binary","blob":"AAE="}]}),
        ),
        (
            McpStdioMethod::ListPrompts,
            json!({}),
            json!({"prompts":[]}),
        ),
        (
            McpStdioMethod::GetPrompt,
            json!({"name":"fixture","arguments":{}}),
            json!({"messages":[]}),
        ),
    ] {
        let request = McpStdioRequest {
            initialization,
            operation,
            method,
            params,
            maximum_bytes: 4096,
        };
        let work = native_work();
        let baseline = work.live_allocation_bytes();
        let probe = work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .unwrap();
        let (mut ordinary, ordinary_first, _) = request.exchange(None).unwrap();
        let (mut native, first, first_guard) = request.exchange(Some(&work)).unwrap();
        assert_eq!(first, ordinary_first);
        assert!(first_guard.as_ref().unwrap().shares_ledger(&probe));
        assert!(first_guard.as_ref().unwrap().bytes() >= first.capacity() as u64);
        let mut transcript = initialized(initialization);
        transcript.extend_from_slice(b"\xef\xbb\xbf");
        let mut result =
            line(&json!({"jsonrpc":"2.0","id":operation.to_string(),"result":expected})).unwrap();
        result.pop();
        result.extend_from_slice(b"\r\n");
        transcript.extend_from_slice(&result);
        for byte in &transcript {
            let chunk = std::slice::from_ref(byte);
            let ordinary_write = ordinary.push(chunk, None).unwrap().0;
            let (write, guard) = native.push(chunk, Some(&work)).unwrap();
            assert_eq!(write, ordinary_write);
            if !write.is_empty() {
                let guard = guard.as_ref().unwrap();
                assert!(guard.shares_ledger(&probe));
                assert!(guard.bytes() >= write.capacity() as u64);
            }
            drop(write);
            drop(guard);
        }
        assert_eq!(ordinary.finish().unwrap().0, expected);
        drop(first);
        drop(first_guard);
        let (value, guard) = native.finish().unwrap();
        assert_eq!(value, expected);
        assert!(guard.as_ref().unwrap().shares_ledger(&probe));
        assert_eq!(
            guard.as_ref().unwrap().bytes(),
            value_storage(&value, None).unwrap()
        );
        assert_eq!(
            work.live_allocation_bytes(),
            baseline + guard.as_ref().unwrap().bytes()
        );
        drop(value);
        drop(guard);
        assert_eq!(work.live_allocation_bytes(), baseline);
        work.receipt().verify(finite_budget()).unwrap();
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn original_native_partial_line_max_denial_and_failed_state_precede_capture() {
    let (initialization, operation) = ids();
    let work = native_work();
    let request = McpStdioRequest {
        initialization,
        operation,
        method: McpStdioMethod::ListTools,
        params: json!({}),
        maximum_bytes: 4096,
    };
    let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
    let (empty, empty_guard) = exchange.push(b"{", Some(&work)).unwrap();
    assert!(empty.is_empty());
    drop(empty);
    drop(empty_guard);
    let copied = work.receipt().bytes_copied;
    assert!(exchange.push(&vec![b' '; 4096], Some(&work)).is_err());
    assert_eq!(work.receipt().bytes_copied, copied);
    assert!(
        exchange
            .push(&initialized(initialization), Some(&work))
            .is_err()
    );
    assert!(exchange.finish().is_err());
    drop(first);
    drop(guard);
    assert_eq!(
        work.live_allocation_bytes(),
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn original_native_cancellation_and_foreign_ledger_cannot_resume() {
    let (initialization, operation) = ids();
    let request = McpStdioRequest {
        initialization,
        operation,
        method: McpStdioMethod::ListTools,
        params: json!({}),
        maximum_bytes: 4096,
    };
    let work = native_work();
    let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
    let copied = work.receipt().bytes_copied;
    work.cancellation().cancel();
    let error = exchange
        .push(&initialized(initialization), Some(&work))
        .err()
        .unwrap();
    assert!(matches!(
        error,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        }
    ));
    assert_eq!(work.receipt().bytes_copied, copied);
    assert!(exchange.line.is_empty());
    drop(first);
    drop(guard);
    drop(exchange);
    assert_eq!(
        work.live_allocation_bytes(),
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );

    let work = native_work();
    let foreign = native_work();
    let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
    assert!(
        exchange
            .push(&initialized(initialization), Some(&foreign))
            .is_err()
    );
    assert!(
        exchange
            .push(&initialized(initialization), Some(&work))
            .is_err()
    );
    drop(first);
    drop(guard);
    drop(exchange);
    let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
    assert!(exchange.push(&initialized(initialization), None).is_err());
    drop(first);
    drop(guard);
    drop(exchange);
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn original_native_frame_quota_precedes_trusted_parse_and_preserves_owned_cause() {
    let (initialization, operation) = ids();
    let request = McpStdioRequest {
        initialization,
        operation,
        method: McpStdioMethod::ListTools,
        params: json!({}),
        maximum_bytes: 4096,
    };
    let sizing = native_work();
    let (exchange, first, guard) = request.exchange(Some(&sizing)).unwrap();
    let preparation_peak = sizing.receipt().peak_allocation_bytes;
    drop(exchange);
    drop(first);
    drop(guard);
    let mut budget = finite_budget();
    budget.peak_allocation_bytes = preparation_peak + 1024;
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
            .unwrap();
    let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
    let error = exchange
        .push(&initialized(initialization), Some(&work))
        .err()
        .unwrap();
    assert!(matches!(error, Error::NativeWork { .. }));
    assert!(exchange.result.is_none());
    drop(exchange);
    drop(first);
    drop(guard);
    work.receipt().verify(budget).unwrap();

    let work = native_work();
    let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
    let error = exchange
        .push(b"{\"jsonrpc\":,\"id\":1}\n", Some(&work))
        .err()
        .unwrap();
    match error {
        Error::NativeJson {
            source,
            custody: Some(custody),
            ..
        } => {
            assert!(source.is_syntax());
            assert!(custody.shares_ledger(guard.as_ref().unwrap()));
            work.cancellation().cancel();
            drop(source);
            drop(custody);
        }
        other => panic!("unexpected actual decode cause: {other}"),
    }
    drop(first);
    drop(guard);
    drop(exchange);
    assert_eq!(
        work.live_allocation_bytes(),
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
    );
}
