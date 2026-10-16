use super::super::tool_execution::metadata_for_call;
use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Property: lookup returns the exact indexed payload without a copy, even
    // when another step reuses a call ID; native work is one item per call.
    #[test]
    fn indexed_metadata_has_linear_work_and_exact_identity(
        count in 1usize..=256, prefix in 0usize..16, signature in ".{0,32}",
    ) {
        let mut messages = vec![ModelMessage {role:ModelRole::User, content:ModelContent::Text("user".into())}; prefix];
        for i in 0..count {
            messages.push(ModelMessage {role:ModelRole::Assistant, content:ModelContent::Part(ModelContentPart::ToolCall(
                crate::model::ToolCall::new("reused".into(), "example.echo".into(), json!({}))
                    .with_metadata(json!({"signature":format!("{i}:{signature}")}))
            ))});
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(100_000, 4*1024*1024),
            &acyclic_fs::CancellationToken::new(),
        ).unwrap();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = PreparationIo::from_native_work(&work).unwrap();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let observed = Some(&io);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let observed = None;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let before = work.receipt().items_examined;
        for i in 0..count {
            let metadata = metadata_for_call(&messages, prefix + i, "reused", observed).unwrap();
            let ModelContentPart::ToolCall(expected) = &messages[prefix+i].content.parts()[0] else {unreachable!()};
            prop_assert!(std::ptr::eq(metadata, &expected.metadata));
            prop_assert_eq!(&metadata["signature"], &json!(format!("{i}:{signature}")));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        prop_assert_eq!(work.receipt().items_examined - before, count as u64);
        prop_assert!(metadata_for_call(&messages, messages.len(), "reused", observed).is_err());
        prop_assert!(metadata_for_call(&messages, prefix, "different", observed).is_err());
    }
}

#[test]
fn reverse_scan_negative_control_selects_the_wrong_steps_signature() {
    let messages: Vec<_> = ["first", "second"]
        .into_iter()
        .map(|signature| ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::ToolCall(
                crate::model::ToolCall::new("reused".into(), "echo".into(), json!({}))
                    .with_metadata(json!({"signature":signature})),
            )),
        })
        .collect();
    let correct = metadata_for_call(&messages, 0, "reused", None).unwrap();
    let broken = messages
        .iter()
        .rev()
        .flat_map(|m| m.content.parts())
        .find_map(|p| match p {
            ModelContentPart::ToolCall(call) if call.call_id == "reused" => Some(&call.metadata),
            _ => None,
        })
        .unwrap();
    assert_eq!(correct["signature"], "first");
    assert_ne!(correct, broken);
}
