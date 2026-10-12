use super::*;

use super::bindings::{
    test_checkout_commit_result_js, test_checkout_export_manifest_result_js,
    test_checkout_metadata_result_js, test_checkout_mutation_result_js,
    test_oversized_transaction_array_rejected, test_speculation_results_js,
    test_transaction_rebase_js, test_transfer_and_residency_results_js,
    test_workspace_extent_plan_js,
};
use js_sys::{Array, BigInt, Reflect};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn transaction_conflicts_preserve_full_width_integers_bytes_and_undefined() -> Result<(), JsValue> {
    for offset in [3, 9_007_199_254_740_993, u64::MAX] {
        let result = test_transaction_rebase_js(offset)?;
        assert!(Reflect::get(&result, &"generationId".into())?.is_undefined());
        let conflicts = Reflect::get(&result, &"conflicts".into())?.dyn_into::<Array>()?;
        let conflict = conflicts.get(0);
        let actual_offset = Reflect::get(&conflict, &"offset".into())?.dyn_into::<BigInt>()?;
        assert_eq!(
            actual_offset.to_string(10)?.as_string(),
            Some(offset.to_string())
        );
        let length = Reflect::get(&conflict, &"length".into())?.dyn_into::<BigInt>()?;
        assert_eq!(
            length.to_string(10)?.as_string(),
            Some(u64::MAX.to_string())
        );
        let file = Reflect::get(&conflict, &"fileId".into())?.dyn_into::<js_sys::Uint8Array>()?;
        assert_eq!(file.to_vec(), vec![7; 16]);
        assert!(Reflect::get(&conflict, &"directoryId".into())?.is_undefined());
        assert!(Reflect::get(&conflict, &"expected".into())?.is_undefined());
        assert!(Reflect::get(&conflict, &"actual".into())?.is_undefined());
        assert_eq!(
            Reflect::get(&conflict, &"region".into())?
                .as_string()
                .as_deref(),
            Some("content-range")
        );
        assert_eq!(
            Reflect::get(&conflict, &"usage".into())?
                .as_string()
                .as_deref(),
            Some("observation-and-mutation")
        );
    }
    Ok(())
}

#[wasm_bindgen_test]
fn workspace_extent_plan_preserves_u64_as_bigint() -> Result<(), JsValue> {
    let plan = test_workspace_extent_plan_js()?;
    let spans = Reflect::get(&plan, &JsValue::from_str("spans"))?.dyn_into::<Array>()?;
    let span = spans.get(0).dyn_into::<js_sys::Object>()?;
    let offset = Reflect::get(&span, &JsValue::from_str("offset"))?.dyn_into::<BigInt>()?;
    assert_eq!(
        offset.to_string(10)?.as_string().as_deref(),
        Some("9007199254740993")
    );
    Ok(())
}

#[wasm_bindgen_test]
fn checkout_dtos_preserve_bytes_options_bigints_and_work_counters() -> Result<(), JsValue> {
    let mutation = test_checkout_mutation_result_js()?;
    let file_id =
        Reflect::get(&mutation, &JsValue::from_str("fileId"))?.dyn_into::<js_sys::Uint8Array>()?;
    assert_eq!(file_id.length(), 16);

    let metadata = test_checkout_metadata_result_js()?;
    let canonical = Reflect::get(&metadata, &JsValue::from_str("canonicalBytes"))?
        .dyn_into::<js_sys::Uint8Array>()?;
    assert_eq!(canonical.to_vec(), vec![0, 255, 1]);

    let manifest = test_checkout_export_manifest_result_js()?;
    let objects = Reflect::get(&manifest, &JsValue::from_str("objects"))?.dyn_into::<Array>()?;
    assert_eq!(objects.length(), 2);

    let commit = test_checkout_commit_result_js()?;
    let epoch = Reflect::get(&commit, &JsValue::from_str("epoch"))?.dyn_into::<BigInt>()?;
    assert_eq!(
        epoch.to_string(10)?.as_string().as_deref(),
        Some("9007199254740993")
    );
    let work = Reflect::get(&commit, &JsValue::from_str("work"))?;
    let copied = Reflect::get(&work, &JsValue::from_str("bytesCopied"))?.dyn_into::<BigInt>()?;
    assert_eq!(copied.to_string(10)?.as_string().as_deref(), Some("17"));
    assert!(Reflect::get(&commit, &JsValue::from_str("sequence"))?.is_undefined());
    Ok(())
}

#[wasm_bindgen_test]
fn transfer_and_residency_dtos_preserve_bigints_and_bytes() -> Result<(), JsValue> {
    let transfer = test_transfer_and_residency_results_js()?;
    let first = Reflect::get(&transfer, &JsValue::from_str("firstObject"))?.dyn_into::<BigInt>()?;
    assert_eq!(
        first.to_string(10)?.as_string().as_deref(),
        Some("9007199254740993")
    );
    let objects = Reflect::get(&transfer, &JsValue::from_str("objects"))?.dyn_into::<Array>()?;
    assert_eq!(
        objects.get(0).dyn_into::<js_sys::Uint8Array>()?.to_vec(),
        vec![1, 2, 3]
    );

    let speculation = test_speculation_results_js()?;
    let residency = Reflect::get(&speculation, &JsValue::from_str("residencyOperationIds"))?
        .dyn_into::<Array>()?;
    assert_eq!(
        residency.get(0).dyn_into::<js_sys::Uint8Array>()?.length(),
        16
    );
    Ok(())
}

#[wasm_bindgen_test]
fn oversized_transaction_array_is_rejected_before_decode() {
    let result = test_oversized_transaction_array_rejected();
    assert!(
        result.is_err(),
        "oversized transaction array unexpectedly decoded"
    );
}
