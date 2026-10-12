use super::*;
use serde_json::json;

#[test]
fn response_moves_original_payload_and_matches_exact_uuid() {
    let operation = OperationId::from_bytes([7; 16]);
    let payload = String::from("original response");
    let mut value = json!({"jsonrpc":"2.0","id":operation.to_string()});
    value
        .as_object_mut()
        .expect("envelope")
        .insert("result".into(), Value::String(payload));
    let result = rpc_result(value, operation, None).expect("matching response");
    assert_eq!(result, Value::String("original response".into()));
    let foreign = OperationId::from_bytes([8; 16]);
    assert!(
        rpc_result(
            json!({"jsonrpc":"2.0","id":foreign.to_string(),"result":null}),
            operation,
            None
        )
        .is_err()
    );
    assert!(
        rpc_result(
            json!({"jsonrpc":"2.0","id":operation.to_string(),"result":null,"error":{}}),
            operation,
            None
        )
        .is_err()
    );
    assert!(
        rpc_result(
            json!({"jsonrpc":"1.0","id":operation.to_string(),"result":null}),
            operation,
            None
        )
        .is_err()
    );
}

#[test]
fn notification_and_initialization_share_the_closed_policy() {
    notification(
        &json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{}}),
        None,
    )
    .expect("hint only");
    assert!(matches!(
        notification(
            &json!({"jsonrpc":"2.0","method":"sampling/createMessage","id":"callback"}),
            None
        ),
        Err(Error::Unsupported(_))
    ));
    assert!(notification(&json!({"jsonrpc":"2.0","method":7}), None).is_err());
    initialization(&json!({
        "protocolVersion":super::super::PROTOCOL_VERSION,
        "capabilities":{},"serverInfo":{"name":"external","version":"1"},"instructions":"not authority"
    }), None).expect("supported negotiation");
    assert!(
        initialization(
            &json!({
                "protocolVersion":super::super::PROTOCOL_VERSION,"capabilities":[],
                "serverInfo":{"name":"external","version":"1"}
            }),
            None
        )
        .is_err()
    );
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_work(bytes: u64, items: u64) -> acyclic_fs::NativeMaterializationWork {
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: bytes,
            items_examined: items,
            allocation_operations: 32,
            ..Default::default()
        },
        &acyclic_fs::CancellationToken::new(),
    )
    .expect("finite original operation")
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn closed_dto_quota_denial_precedes_serde_and_does_not_report_allocator_calls() {
    let work = native_work(
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 1,
        100,
    );
    let before = work.receipt();
    assert!(matches!(
        check::<rmcp::model::GetPromptResult>(&json!({"messages":[]}), Some(&work)),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(_),
            custody: None
        })
    ));
    assert_eq!(
        work.receipt().allocation_operations,
        before.allocation_operations
    );
    let bounded = native_work(1 << 20, 1);
    assert!(
        check::<rmcp::model::ReadResourceResult>(
            &json!({"contents":[{"uri":"x","text":"y"}]}),
            Some(&bounded)
        )
        .is_err()
    );
    assert_eq!(bounded.receipt().items_examined, 1);
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn malformed_typed_cause_keeps_original_quota_until_error_drop() {
    let work = native_work(1 << 20, 1000);
    let original = work
        .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .expect("original ledger witness");
    let baseline = work.remaining().expect("remaining").peak_allocation_bytes;
    let before = work.receipt();
    let live_before = work.live_allocation_bytes();
    let failure = check::<rmcp::model::GetPromptResult>(
        &json!({
            "messages":[{"role":"untrusted\u{1}role","content":{"type":"text","text":"external"}}]
        }),
        Some(&work),
    )
    .expect_err("invalid role");
    match &failure {
        Error::NativeJson {
            source,
            custody: Some(custody),
            ..
        } => {
            assert!(source.is_data());
            assert!(custody.bytes() > 0);
            assert!(custody.shares_ledger(&original));
        }
        other => panic!("actual typed cause was replaced: {other:?}"),
    }
    assert!(
        work.remaining()
            .expect("retained error")
            .peak_allocation_bytes
            < baseline
    );
    work.cancellation().cancel();
    drop(failure);
    assert_eq!(
        original.work().receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(work.live_allocation_bytes(), live_before);
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn foreign_identity_and_callback_keep_static_original_classification() {
    let work = native_work(1 << 20, 100);
    let operation = OperationId::from_bytes([2; 16]);
    assert!(matches!(
        rpc_result(
            json!({
                "jsonrpc":"2.0","id":OperationId::from_bytes([3; 16]).to_string(),"result":{}
            }),
            operation,
            Some(&work)
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    assert!(matches!(
        notification(
            &json!({
                "jsonrpc":"2.0","method":"roots/list","id":"server"
            }),
            Some(&work)
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
}
