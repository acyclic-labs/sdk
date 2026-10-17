use super::*;
use proptest::prelude::*;

fn file(path: &str, body: &str, label: &str) -> FileRef {
    FileRef::new(
        serde_json::from_value(serde_json::json!({
            "provider":{"namespace":"test","family":"filesystem","version":"2"},
            "id":"project","class":"project","owner":{"kind":"project","id":"project"}
        }))
        .unwrap(),
        path.into(),
        "1".into(),
        crate::conversation::FileDescriptor::from_bytes(
            body.as_bytes(),
            "text/markdown",
            &mut None,
        )
        .unwrap(),
        label.into(),
        &mut None,
    )
    .unwrap()
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn read_deduplication_uses_original_ledger_and_preserves_the_exchange() {
    let body = "verbatim project instructions Î»";
    let file = file("nested/AGENTS.md", body, "rules");
    for native in [false, true] {
        let result =
            crate::test_support::accounted::accounted(native, 8 * 1024 * 1024, false, |io| {
                let mut messages = vec![
                    project_instruction(&file.clone().into(), Some(io)).unwrap(),
                    ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
                            "call".into(),
                            "read".into(),
                            serde_json::json!({"file":file}),
                        ))),
                    },
                    ModelMessage {
                        role: ModelRole::Tool,
                        content: ModelContent::Part(ModelContentPart::ToolResult {
                            call_id: "call".into(),
                            name: "read".into(),
                            content: ToolResultContent::Json {
                                value: serde_json::json!(body),
                            },
                        }),
                    },
                ];
                let original_call = messages[1].clone();
                super::instruction_reads::deduplicate_reads(&mut messages, Some(io)).unwrap();
                assert_eq!(messages[1], original_call);
                let ModelContent::Part(ModelContentPart::ToolResult {
                    call_id,
                    name,
                    content: ToolResultContent::Json { value },
                }) = &messages[2].content
                else {
                    panic!("exchange lost")
                };
                assert_eq!((call_id.as_str(), name.as_str()), ("call", "read"));
                assert_eq!(
                    value,
                    "Read completed; instructions already included from AGENTS.md for nested."
                );
                super::instruction_reads::deduplicate_reads(&mut messages, Some(io)).unwrap();
            })
            .unwrap();
        assert!(result.1.bytes_hashed >= body.len() as u64);
        if native {
            assert_eq!(result.5, 0, "original custody leaked");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Exact reservation includes every raw byte, source scope, and framing byte;
    // arbitrary display labels cannot change it. A per-file-only control misses
    // aggregate overflow even though every descriptor is individually valid.
    #[test]
    fn framing_reservation_and_source_identity_round_trip(
        body in prop::collection::vec(any::<char>(), 0..64),
        directory in "[a-z]{1,12}", label in "[a-z]{1,12}",
        name in prop::sample::select(vec!["AGENTS.md", "CLAUDE.md"]),
    ) {
        let body: String = body.into_iter().collect();
        let path = format!("{directory}/{name}");
        let file = file(&path, &body, &label);
        let source = InstructionSource::new(&file);
        let expected = format!("# {name} instructions for {directory}\n\n<INSTRUCTIONS>\n{body}\n</INSTRUCTIONS>");
        let rendered = source.render(body.as_bytes(), None).unwrap();
        prop_assert_eq!(&rendered, &expected);
        prop_assert_eq!(source.framed_bytes().unwrap(), expected.len() as u64);
        let mut remaining = expected.len() as u64;
        source.reserve(&mut remaining, None).unwrap();
        prop_assert_eq!(remaining, 0);
        prop_assert!(source.reserve(&mut remaining, None).is_err());
        let per_file_only = [source, source].iter().all(|source| source.framed_bytes().unwrap() <= expected.len() as u64);
        prop_assert!(per_file_only, "negative control must accept overflowing total");
        let message = project_instruction(&file.clone().into(), None).unwrap();
        let reservation = super::instructions::instruction_reservation(std::slice::from_ref(&file.clone().into()), u64::MAX, None).unwrap();
        prop_assert_eq!(reservation, serde_json::to_vec(&message).unwrap().len() as u64 + expected.len() as u64 + 1);
        let decoded: ModelMessage = serde_json::from_slice(&serde_json::to_vec(&message).unwrap()).unwrap();
        prop_assert_eq!(decoded, message);
        prop_assert!(super::instructions::instruction_reservation(std::slice::from_ref(&file.clone().into()), expected.len() as u64, None).is_err());
        let alternate = self::file(&path, &body, "arbitrary");
        prop_assert!(source.same_content(&alternate));
        let different = self::file(&format!("other/{path}"), &body, &label);
        prop_assert!(!source.same_content(&different));
        let legacy: ProjectInstruction = serde_json::from_slice(&serde_json::to_vec(&file).unwrap()).unwrap();
        prop_assert!(legacy.source().same_content(&file));
        let captured = ProjectInstruction::Captured {
            file: self::file("archive/opaque", &body, "rules"),
            source: InstructionIdentity::new(file.volume().clone(), path).unwrap(),
        };
        prop_assert_eq!(captured.source().render(body.as_bytes(), None).unwrap(), expected);
        prop_assert!(captured.source().same_content(&file));
        prop_assert!(!InstructionSource::new(&captured).same_content(&file), "archive-location negative control must lose identity");
        let decoded: ProjectInstruction = serde_json::from_slice(&serde_json::to_vec(&captured).unwrap()).unwrap();
        prop_assert_eq!(decoded, captured);
    }
}
