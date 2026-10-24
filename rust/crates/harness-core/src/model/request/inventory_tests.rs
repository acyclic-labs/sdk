use super::*;

fn request(count: usize) -> Result<ModelRequest> {
    let mut messages = Vec::with_capacity(count + 17);
    for _ in 0..count {
        let mut text = String::with_capacity(128);
        text.push_str("retained text");
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(text),
        });
    }
    Ok(ModelRequest {
        model: Model::new(
            "test".into(),
            "inventory".into(),
            "1".into(),
            serde_json::json!({"nested": [{"sparse": "value"}]}),
        )?,
        messages,
        tools: vec![crate::tool::ToolDefinition {
            name: "inventory".into(),
            revision: "1".into(),
            description: "Return an object".into(),
            input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({"type":"object"}),
            projection_schema: serde_json::json!({"type":"object","properties":{"kind":{"const":"json"},"value":{"type":"object"}},"required":["kind","value"],"additionalProperties":false}),
        }],
        max_output_tokens: None,
    })
}

fn assert_inventory(prepared: &PreparedModelRequest) -> Result<()> {
    assert_eq!(
        prepared.resident_payload_bytes()?,
        prepared.request.retained_payload_bytes()?,
        "continuation inventory differs from its actual retained storage"
    );
    Ok(())
}

#[test]
fn retained_inventory_updates_only_new_storage_and_explicit_clones_reset_it() -> Result<()> {
    for count in [1, 10, 100, 1_000] {
        let mut prepared = PreparedModelRequest::prepare(request(count)?, Limits::default(), None)?;
        assert_inventory(&prepared)?;
        for index in 0..20 {
            // Explicit cloning changes Vec and String capacities and shares the
            // encoded bytes. Both facts must survive the next continuation.
            if index == 5 {
                prepared = prepared.clone();
                assert!(
                    prepared.resident_payload.is_none(),
                    "explicit clone reused producer capacities"
                );
                assert_inventory(&prepared)?;
            }
            let mut continuation = prepared.into_continuation();
            let mut text = String::with_capacity(37 + index);
            text.push_str("new text");
            continuation.messages().push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(text),
            });
            let sparse = serde_json::json!({"outer": [{"inner": "value"}]});
            continuation.messages().extend([
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall(
                        ToolCall::new(format!("call-{index}"), "inventory".into(), sparse.clone())
                            .with_metadata(sparse.clone()),
                    )),
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: format!("call-{index}"),
                        name: "inventory".into(),
                        content: ToolResultContent::Json { value: sparse },
                    }),
                },
            ]);
            prepared = continuation.finish(Limits::default(), None)?;
            assert!(prepared.resident_payload.is_some());
            assert_inventory(&prepared)?;
        }
        let mut continuation = prepared.into_continuation();
        continuation.messages().push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("changed limits".into()),
        });
        prepared = continuation.finish(
            Limits {
                render_bytes: Limits::default().render_bytes - 1,
                ..Limits::default()
            },
            None,
        )?;
        assert_inventory(&prepared)?;
    }
    Ok(())
}

#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
#[test]
fn finite_preparation_admits_sparse_json_capacity_before_retaining_the_request() -> Result<()> {
    let measured =
        crate::test_support::accounted::accounted(false, 16 * 1024 * 1024, false, |io| {
            // This is caller-owned input, not a copy already funded by the
            // preparation ledger. Sparse tree nodes make logical JSON bytes
            // strictly smaller than their resident allocation.
            let mut input = request(1)?;
            input.model.options = serde_json::json!({
                "sparse": (0..64).map(|_| serde_json::json!({"one":"value"})).collect::<Vec<_>>()
            });
            assert!(input.retained_payload_bytes()? > input.payload_bytes()?);
            let prepared = PreparedModelRequest::prepare(input, Limits::default(), Some(io))?;
            assert!(
                io.owned_bytes()? >= prepared.preparation_owned_bytes(io)?,
                "finite preparation retained sparse JSON without funding its node capacity"
            );
            Ok::<_, Error>(())
        })
        .map_err(|(cause, _)| Error::Storage(cause))?;
    measured.0
}

#[test]
fn prepared_request_retention_does_not_recount_historical_payloads() -> Result<()> {
    use crate::model::retained_inventory::REQUEST_MESSAGES_EXAMINED;
    let mut full_work = Vec::new();
    for count in [1, 10, 100, 1_000] {
        let prepared = PreparedModelRequest::prepare(request(count)?, Limits::default(), None)?;
        REQUEST_MESSAGES_EXAMINED.set(0);
        for _ in 0..100 {
            prepared.resident_payload_bytes()?;
            #[cfg(not(target_arch = "wasm32"))]
            prepared.logical_owned_bytes()?;
        }
        assert_eq!(
            REQUEST_MESSAGES_EXAMINED.get(),
            0,
            "request retention recounted historical payloads at {count} messages"
        );
        // An explicit clone is independently allocated, so the previous
        // request's capacities cannot stand in for a physical census.
        let cloned = prepared.clone();
        REQUEST_MESSAGES_EXAMINED.set(0);
        assert_inventory(&cloned)?;
        let work = REQUEST_MESSAGES_EXAMINED.get();
        assert!(
            work >= count,
            "uncached inventory control examined no history"
        );
        full_work.push(work);
        eprintln!(
            "CONTINUATION_INVENTORY messages={count} cached_examined=0 uncached_examined={work}"
        );
    }
    assert!(full_work.windows(2).all(|pair| pair[1] > pair[0]));
    Ok(())
}
