#![cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]

use acyclic_harness::{
    Result,
    model::{ModelEvent, ToolCall},
};
use serde_json::json;

#[test]
fn continuation_metadata_decodes_with_custody_on_the_original_native_ledger() -> Result<()> {
    let call = ToolCall::new("call".into(), "echo".into(), json!({"value":1}))
        .with_metadata(json!({"signature":"opaque", "blocks":[{"thinking":"plan"}]}));
    let event = ModelEvent::ToolCall(call);
    let bytes = serde_json::to_vec(&event).expect("fixture event JSON");
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1_048_576),
        &cancellation,
    )
    .expect("native fixture ledger");
    let (decoded, custody) = ModelEvent::decode_with_native_work(&bytes, &work)?;
    assert_eq!(decoded, event);
    assert!(custody.work().shares_allocation_context(&work));
    assert!(custody.bytes() >= std::mem::size_of::<ModelEvent>() as u64);
    drop(decoded);
    drop(custody);
    cancellation.cancel();
    assert!(ModelEvent::decode_with_native_work(&bytes, &work).is_err());
    Ok(())
}
