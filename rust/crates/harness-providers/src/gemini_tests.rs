use super::*;

#[tokio::test]
async fn absent_arguments_are_empty_but_invalid_supplied_arguments_are_rejected() -> Result<()> {
    for args in [
        None,
        Some(json!({})),
        Some(json!(null)),
        Some(json!([])),
        Some(json!("{}")),
    ] {
        let valid = args.as_ref().is_none_or(serde_json::Value::is_object);
        let mut call = json!({"name":"echo"});
        if let Some(args) = args {
            call["args"] = args;
        }
        let frame = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":call}]},"finishReason":"STOP"}]});
        let parsed = crate::event_driver::events(
            Adapter::Gemini,
            chunks(&format!("data: {frame}\n\n"), 1),
            4096,
            parser_dispatch(),
        )
        .try_collect::<Vec<_>>()
        .await;
        if valid {
            let events = parsed?;
            let ModelEvent::ToolCall(call) = &events[0] else {
                panic!("missing call")
            };
            assert_eq!(call.arguments, json!({}));
        } else {
            assert!(matches!(
                parsed,
                Err(acyclic_harness_core::Error::Invalid(_))
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_or_malformed_native_tool_batches_never_execute() -> Result<()> {
    // Generated adversarial protocol data, not captured provider fixtures.
    let mut frames = Vec::new();
    for id in [json!(null), json!(42), json!("")] {
        let function = json!({"name":"echo","args":{},"id":id});
        frames.push(json!({"candidates":[{"content":{"parts":[{
            "functionCall":function,"thoughtSignature":"opaque"
        }]},"finishReason":"STOP"}]}));
    }
    let valid = json!({"content":{"role":"model","parts":[{
        "functionCall":{"name":"echo","args":{}},"thoughtSignature":"opaque"
    }]},"finishReason":"STOP"});
    frames.push(json!({"candidates":true}));
    frames.push(json!({"candidates":[true]}));
    frames.push(json!({"candidates":[valid,valid]}));
    for index in [json!(1), json!(-1), json!(true), json!("0")] {
        let mut candidate = valid.clone();
        candidate["index"] = index;
        frames.push(json!({"candidates":[candidate]}));
    }
    for role in [json!("user"), json!(true)] {
        let mut candidate = valid.clone();
        candidate["content"]["role"] = role;
        frames.push(json!({"candidates":[candidate]}));
    }
    for parts in [json!(true), json!({}), json!([true]), json!([{"text":42}])] {
        let mut candidate = valid.clone();
        candidate["content"]["parts"] = parts;
        frames.push(json!({"candidates":[candidate]}));
    }
    let mut truncated = valid.clone();
    truncated["finishReason"] = json!("MAX_TOKENS");
    let mut candidate = valid;
    candidate["finishReason"] = json!(true);
    frames.push(json!({"candidates":[candidate]}));
    let prefix = json!({"candidates":[{"content":{"parts":[{
        "functionCall":{"name":"prior","args":{}},"thoughtSignature":"prior-signature"
    }]}}]});
    for frame in frames {
        let events = crate::event_driver::events(
            Adapter::Gemini,
            chunks(&format!("data: {prefix}\n\ndata: {frame}\n\n"), 1),
            4096,
            parser_dispatch(),
        )
        .collect::<Vec<_>>()
        .await;
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            Err(acyclic_harness_core::Error::Invalid(_))
        ));
    }
    // The original signed batch is withheld on truncation too, while the
    // shared completion retains the token-limit reason instead of refusing it.
    let frame = json!({"candidates":[truncated]});
    let events = crate::event_driver::events(
        Adapter::Gemini,
        chunks(&format!("data: {prefix}\n\ndata: {frame}\n\n"), 1),
        4096,
        parser_dispatch(),
    )
    .try_collect::<Vec<_>>()
    .await?;
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], ModelEvent::Completed { metadata } if metadata["finish_reason"] == "length")
    );
    Ok(())
}
