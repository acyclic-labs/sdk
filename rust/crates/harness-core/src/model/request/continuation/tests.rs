use super::*;
use serde_json::json;

fn text(value: &str) -> ModelMessage {
    ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text(value.into()),
    }
}

fn request(count: usize) -> Result<ModelRequest> {
    Ok(ModelRequest {
        model: Model::new("scripted".into(), "continuation".into(), "1".into())?,
        messages: (0..count).map(|_| text("retained text")).collect(),
        tools: Vec::new(),
        max_output_tokens: Some(1),
    })
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[test]
fn suffix_validation_work_is_flat_from_one_to_one_thousand_messages() -> Result<()> {
    for native in [false, true] {
        let mut expected = None;
        let mut full_work = Vec::new();
        for count in [1, 10, 100, 1_000] {
            let measured =
                crate::test_support::accounted::accounted(native, 16 * 1024 * 1024, false, |io| {
                    let prepared = PreparedModelRequest::prepare(
                        request(count)?,
                        Limits::default(),
                        Some(io),
                    )?;
                    let first = match &prepared.request().messages.first().unwrap().content {
                        ModelContent::Text(value) => value.as_ptr(),
                        _ => unreachable!(),
                    };
                    let mut continuation = prepared.into_continuation();
                    crate::executor::stock_vec_push(
                        continuation.messages(),
                        text("new text"),
                        Some(io),
                    )?;
                    let before = io.receipt();
                    continuation.validate_suffix(Limits::default(), Some(io))?;
                    let suffix_items = io.receipt().items_examined - before.items_examined;
                    let (mut continued, _) = continuation.validate(Limits::default(), Some(io))?;
                    let ModelContent::Text(value) = &continued.messages.first().unwrap().content
                    else {
                        unreachable!()
                    };
                    assert_eq!(value.as_ptr(), first, "continuation copied retained text");
                    assert_eq!(continued.messages.last(), Some(&text("new text")));
                    let before = io.receipt();
                    continued.validate(Limits::default(), Some(io))?;
                    let full_items = io.receipt().items_examined - before.items_examined;
                    Ok::<_, Error>((suffix_items, full_items))
                })
                .map_err(|(cause, _)| Error::Storage(cause))?;
            let (suffix_items, full_items) = measured.0?;
            assert_eq!(
                *expected.get_or_insert(suffix_items),
                suffix_items,
                "suffix validation revisited history at {count} messages, native={native}"
            );
            full_work.push(full_items);
            if native {
                assert_eq!(
                    measured.5,
                    acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                );
            }
            eprintln!(
                "CONTINUATION_VALIDATION native={native} messages={count} suffix_items={suffix_items} full_items={full_items}"
            );
        }
        assert!(
            full_work.windows(2).all(|pair| pair[1] > pair[0]),
            "full-validation negative control did not grow"
        );
    }
    Ok(())
}

#[test]
fn continuing_preserves_canonical_bytes_digests_and_original_buffers() -> Result<()> {
    for count in [1, 10, 100, 1_000] {
        let original = request(count)?;
        let mut complete = original.clone();
        complete.messages.push(text("new text"));
        let expected = PreparedModelRequest::prepare(complete, Limits::default(), None)?;
        let prepared = PreparedModelRequest::prepare(original, Limits::default(), None)?;
        let ModelContent::Text(first) = &prepared.request().messages.first().unwrap().content
        else {
            unreachable!()
        };
        let pointer = first.as_ptr();
        let mut continuation = prepared.into_continuation();
        continuation.messages().push(text("new text"));
        let continued = continuation.finish(Limits::default(), None)?;
        assert_eq!(continued.request(), expected.request());
        assert_eq!(continued.bytes(), expected.bytes());
        assert_eq!(continued.request_digest(), expected.request_digest());
        let ModelContent::Text(first) = &continued.request().messages.first().unwrap().content
        else {
            unreachable!()
        };
        assert_eq!(first.as_ptr(), pointer);
    }
    Ok(())
}

fn tools() -> Vec<crate::tool::ToolDefinition> {
    vec![crate::tool::ToolDefinition {
        name: "echo".into(),
        revision: "1".into(),
        description: "Echo text".into(),
        input_schema: json!({"type":"string"}),
        output_schema: json!({"type":"string"}),
        projection_schema: json!({"type":"object","properties":{"kind":{"const":"json"},"value":{"type":"string"}},"required":["kind","value"],"additionalProperties":false}),
    }]
}

fn call() -> ModelMessage {
    ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
            "call".into(),
            "echo".into(),
            json!("argument"),
        ))),
    }
}

fn result() -> ModelMessage {
    ModelMessage {
        role: ModelRole::Tool,
        content: ModelContent::Part(ModelContentPart::ToolResult {
            call_id: "call".into(),
            name: "echo".into(),
            content: ToolResultContent::Json {
                value: json!("answer"),
            },
        }),
    }
}

#[test]
fn untrusted_suffixes_keep_full_tool_exchange_validation() -> Result<()> {
    let mut bad_role = call();
    bad_role.role = ModelRole::User;
    let mut bad_projection = result();
    let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) =
        &mut bad_projection.content
    else {
        unreachable!()
    };
    *content = ToolResultContent::Json { value: json!(17) };
    let mut bad_arguments = call();
    let ModelContent::Part(ModelContentPart::ToolCall(value)) = &mut bad_arguments.content else {
        unreachable!()
    };
    value.arguments = json!(17);
    for (suffix, admitted) in [
        (vec![call(), result()], true),
        (vec![result()], false),
        (vec![call()], false),
        (vec![call(), text("interrupted"), result()], false),
        (vec![call(), call(), result()], false),
        (vec![bad_role, result()], false),
        (vec![call(), bad_projection], false),
        (vec![bad_arguments, result()], false),
        (
            vec![ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Text("unpaired".into()),
            }],
            false,
        ),
    ] {
        let mut original = request(10)?;
        original.tools = tools();
        let mut full = original.clone();
        full.messages.extend(suffix.clone());
        let expected = PreparedModelRequest::prepare(full, Limits::default(), None);
        let prepared = PreparedModelRequest::prepare(original, Limits::default(), None)?;
        let mut continuation = prepared.into_continuation();
        continuation.messages().extend(suffix);
        let actual = continuation.finish(Limits::default(), None);
        assert_eq!(actual.is_ok(), admitted, "suffix validation was bypassed");
        assert_eq!(actual.is_ok(), expected.is_ok());
        if let (Ok(actual), Ok(expected)) = (actual, expected) {
            assert_eq!(actual.bytes(), expected.bytes());
        }
    }
    Ok(())
}

#[test]
fn changed_limits_revalidate_the_admitted_prefix() -> Result<()> {
    let mut original = request(1)?;
    original.messages = vec![text("retained text exceeding the new bound")];
    let prepared = PreparedModelRequest::prepare(original, Limits::default(), None)?;
    let mut continuation = prepared.into_continuation();
    continuation.messages().push(text("new"));
    assert!(
        continuation
            .finish(
                Limits {
                    render_bytes: 8,
                    ..Limits::default()
                },
                None
            )
            .is_err()
    );
    let prepared = PreparedModelRequest::prepare(request(10)?, Limits::default(), None)?;
    let mut continuation = prepared.into_continuation();
    continuation.messages().push(text("new"));
    assert!(
        continuation
            .finish(
                Limits {
                    context_messages: 10,
                    ..Limits::default()
                },
                None
            )
            .is_err()
    );
    Ok(())
}

#[test]
fn instruction_reservations_span_the_immutable_prefix_and_new_suffix() -> Result<()> {
    let file =
        crate::model::instruction_tests::file("nested/AGENTS.md", &"x".repeat(1_024), "rules");
    let instruction = project_instruction(&file.into(), None)?;
    let limits = Limits {
        render_bytes: 2_000,
        ..Limits::default()
    };
    let mut original = request(1)?;
    original.messages = vec![instruction.clone()];
    let prepared = PreparedModelRequest::prepare(original.clone(), limits, None)?;
    assert!(prepared.instruction_remaining < 1_000);
    let mut continuation = prepared.into_continuation();
    continuation.messages().push(instruction.clone());
    assert!(
        continuation.finish(limits, None).is_err(),
        "new suffix reset the instruction quota"
    );
    // A genuinely enlarged bound requires fresh admission of both instructions.
    let prepared = PreparedModelRequest::prepare(original, limits, None)?;
    let mut continuation = prepared.into_continuation();
    continuation.messages().push(instruction);
    let enlarged = Limits {
        render_bytes: 4_000,
        ..limits
    };
    let continued = continuation.finish(enlarged, None)?;
    assert!(continued.instruction_remaining < 2_000);
    Ok(())
}

#[cfg(all(
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
#[path = "ownership_tests.rs"]
mod ownership_tests;
