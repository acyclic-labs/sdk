//! Canonical Rust executor bytes cross the actual WASM admission boundary.
#![cfg(feature = "wasm")]

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

use acyclic_harness::{
    AgentId, OperationId, Result,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    executor::{ExecutionEvent, ExecutionRecord, decode_json, encode_json},
    resources::ProviderRef,
};
#[cfg(target_arch = "wasm32")]
use acyclic_harness::{
    conversation::{Attachment, ModelContextSelection},
    executor::{TurnInput, TurnOutput},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
};
use serde_json::json;

fn original_record() -> Result<ExecutionRecord> {
    let file = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("executor-contract", "filesystem", "2")?,
            "original-private-volume",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
        )?,
        "original/context.json",
        "original-generation",
        FileDescriptor::from_bytes(b"{}\n", "application/json")?,
        "context.json",
    )?;
    Ok(ExecutionRecord {
        operation_id: OperationId::from_bytes([9; 16]),
        sequence: (1_u64 << 53) + 19,
        idempotency_key: "original-context:7".into(),
        event: ExecutionEvent::ContextPrepared {
            step: 7,
            projection: file,
            accounting: None,
        },
    })
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn canonical_native_records_preserve_integer_and_resource_constraints() -> Result<()> {
    let original = original_record()?;
    let bytes = encode_json(&original)?;
    assert_eq!(decode_json::<ExecutionRecord>(&bytes)?, original);
    assert!(decode_json::<ExecutionRecord>(&serde_json::to_vec(&original).unwrap()).is_err());
    let value = serde_json::to_value(&original).unwrap();
    for mutation in [
        ("sequence", json!(-1)),
        ("sequence", json!("9007199254741011")),
        ("event", json!({"kind": "invented_native_effect"})),
    ] {
        let mut changed = value.clone();
        changed[mutation.0] = mutation.1;
        assert!(decode_json::<ExecutionRecord>(&encode_json(&changed)?).is_err());
    }
    let mut changed = value.clone();
    changed["event"]["projection"]["path"] = json!("../another-owner/context.json");
    assert!(decode_json::<ExecutionRecord>(&encode_json(&changed)?).is_err());
    let mut changed = value;
    changed["event"]["private_scope"] = json!("not-an-execution-grant");
    assert!(decode_json::<ExecutionRecord>(&encode_json(&changed)?).is_err());
    Ok(())
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test::wasm_bindgen_test]
fn actual_native_turn_bytes_decode_through_wasm_without_narrowing()
-> std::result::Result<(), wasm_bindgen::JsValue> {
    use acyclic_harness::wasm::{
        decode_execution_record_json, decode_turn_input_json, decode_turn_output_json,
        encode_canonical_json,
    };
    use js_sys::{Array, BigInt, Reflect};
    use wasm_bindgen::{JsCast as _, JsValue};
    let error = |value: acyclic_harness::Error| JsValue::from_str(&value.to_string());
    let record = original_record().map_err(error)?;
    let bytes = encode_json(&record).map_err(error)?;
    let decoded = decode_execution_record_json(&bytes)?;
    let sequence = Reflect::get(&decoded, &"sequence".into())?.dyn_into::<BigInt>()?;
    assert_eq!(
        sequence.to_string(10)?.as_string(),
        Some(record.sequence.to_string())
    );
    assert_eq!(encode_canonical_json(decoded)?, bytes);
    let ExecutionEvent::ContextPrepared {
        projection: file, ..
    } = record.event
    else {
        unreachable!()
    };
    let input = TurnInput {
        operation_id: record.operation_id,
        input: ModelContent::Text("original turn input".into()),
        selected_context: Some(SelectedModelContext {
            selection: ModelContextSelection {
                conversation_revision: record.sequence,
                message_ids: vec![],
                checkpoint: Some(file.clone()),
            },
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("original turn input".into()),
            }],
        }),
        max_steps: 3,
    };
    let input_bytes = encode_json(&input).map_err(error)?;
    let decoded_input = decode_turn_input_json(&input_bytes)?;
    assert_eq!(
        Reflect::get(&decoded_input, &"input".into())?
            .as_string()
            .as_deref(),
        Some("original turn input")
    );
    let selected = Reflect::get(&decoded_input, &"selected_context".into())?;
    let selection = Reflect::get(&selected, &"selection".into())?;
    let revision =
        Reflect::get(&selection, &"conversation_revision".into())?.dyn_into::<BigInt>()?;
    assert_eq!(
        revision.to_string(10)?.as_string(),
        Some(record.sequence.to_string())
    );
    assert_eq!(encode_canonical_json(decoded_input)?, input_bytes);

    let output = TurnOutput {
        text: "original native output".into(),
        attachments: vec![Attachment {
            file,
            label: Some("original retained artifact".into()),
        }],
        metadata: json!({
            "provider_integer": u64::MAX,
            "opaque_descriptor": {"sha256": [], "byte_length": u64::MAX, "media_type": "provider-data"},
        }),
        steps: 3,
    };
    let output_bytes = encode_json(&output).map_err(error)?;
    let decoded_output = decode_turn_output_json(&output_bytes)?;
    let metadata = Reflect::get(&decoded_output, &"metadata".into())?.dyn_into::<js_sys::Map>()?;
    let provider_integer = metadata
        .get(&"provider_integer".into())
        .dyn_into::<BigInt>()?;
    assert_eq!(
        provider_integer.to_string(10)?.as_string(),
        Some(u64::MAX.to_string())
    );
    let opaque = metadata
        .get(&"opaque_descriptor".into())
        .dyn_into::<js_sys::Map>()?;
    let opaque_bytes = opaque.get(&"byte_length".into()).dyn_into::<BigInt>()?;
    assert_eq!(
        opaque_bytes.to_string(10)?.as_string(),
        Some(u64::MAX.to_string())
    );
    let attachment = Reflect::get(&decoded_output, &"attachments".into())?
        .dyn_into::<Array>()?
        .get(0);
    let file = Reflect::get(&attachment, &"file".into())?;
    assert_eq!(
        Reflect::get(&file, &"path".into())?.as_string().as_deref(),
        Some("original/context.json")
    );
    let descriptor = Reflect::get(&file, &"descriptor".into())?;
    assert_eq!(
        Reflect::get(&descriptor, &"byte_length".into())?.as_f64(),
        Some(3.0)
    );
    assert_eq!(encode_canonical_json(decoded_output)?, output_bytes);
    let mut changed = serde_json::to_value(&output).unwrap();
    changed["attachments"][0]["file"]["path"] = json!("../foreign/artifact");
    assert!(decode_turn_output_json(&encode_json(&changed).map_err(error)?).is_err());
    Ok(())
}
