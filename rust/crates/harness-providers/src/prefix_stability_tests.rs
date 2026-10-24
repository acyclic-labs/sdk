use super::*;

fn text(role: ModelRole, value: &str) -> ModelMessage {
    ModelMessage {
        role,
        content: ModelContent::Text(value.into()),
    }
}

// Compare the actual HTTP bytes through the pinned context, even when a
// provider folds adjacent user messages into a single content array.
fn prefix(provider: &ResolvedModel, request: &ModelRequest) -> Result<Vec<u8>> {
    let names = crate::tool_names::Names::new(request);
    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
    let mut files = crate::files::FileText::default();
    let body = projection::body_named(provider, request, &names, &origin, &mut files)?;
    let mut bytes = serde_json::to_vec(&body).map_err(|error| invalid(error.to_string()))?;
    let boundary = bytes
        .windows(b"HISTORY_BOUNDARY".len())
        .position(|bytes| bytes == b"HISTORY_BOUNDARY")
        .ok_or_else(|| invalid("fixture history boundary missing"))?;
    bytes.truncate(boundary);
    Ok(bytes)
}

fn messages(date: &str, hook: &str, history: &str) -> Vec<ModelMessage> {
    vec![
        text(
            ModelRole::System,
            "STATIC_SYSTEM: follow the repository rules",
        ),
        text(ModelRole::User, "PINNED_CONTEXT: repository instructions"),
        text(ModelRole::User, history),
        text(ModelRole::System, date),
        text(ModelRole::Assistant, "checking"),
        text(ModelRole::System, hook),
    ]
}

#[test]
fn anthropic_cache_fields_cover_static_prefix_and_last_message() -> Result<()> {
    let (provider, _) = provider(Adapter::Anthropic, 200);
    let (prepared, _) = request(&provider)?;
    let mut request = prepared.request().clone();
    request.tools = tool_name_tests::request_with(&["read", "write"]).tools;
    let body = projection::body(&provider, &request)?;
    let control = json!({"type":"ephemeral"});
    let tools = usize::from(body.pointer("/tools/1/cache_control") == Some(&control));
    let system = usize::from(body.pointer("/system/0/cache_control") == Some(&control));
    let history =
        usize::from(body.pointer("/messages/0/content/0/cache_control") == Some(&control));
    println!(
        "CACHE_FIELDS tools={tools} system={system} history={history} total={}",
        tools + system + history
    );
    assert_eq!(body.pointer("/tools/1/cache_control"), Some(&control));
    assert!(body.pointer("/tools/0/cache_control").is_none());
    assert_eq!(body.pointer("/system/0/cache_control"), Some(&control));
    assert_eq!(
        body.pointer("/messages/0/content/0/cache_control"),
        Some(&control)
    );
    Ok(())
}

#[test]
fn anthropic_history_breakpoint_keeps_signed_thinking_bytes() -> Result<()> {
    use acyclic_harness_core::model::{
        ModelContentPart, ReasoningBlock, ReasoningKind, ReasoningOutcome,
    };
    let (provider, _) = provider(Adapter::Anthropic, 200);
    let (prepared, _) = request(&provider)?;
    let mut request = prepared.request().clone();
    let signed = br#"{ "type":"thinking", "thinking":"private", "signature":"fixture-signature" }"#;
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Parts(vec![
            ModelContentPart::Text {
                text: "answer".into(),
            },
            ModelContentPart::Reasoning {
                block: ReasoningBlock {
                    kind: ReasoningKind::Readable,
                    text: Some("private".into()),
                    origin: provider.reasoning_origin(provider.key.as_deref(), None),
                    continuity: signed.to_vec(),
                    outcome: ReasoningOutcome::PreservedReadable,
                },
            },
        ]),
    });
    let names = crate::tool_names::Names::new(&request);
    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
    let mut files = crate::files::FileText::default();
    let body = projection::body_named(&provider, &request, &names, &origin, &mut files)?;
    let encoded = serde_json::to_vec(&body).map_err(|error| invalid(error.to_string()))?;
    assert!(encoded.windows(signed.len()).any(|bytes| bytes == signed));
    let body: Value =
        serde_json::from_slice(&encoded).map_err(|error| invalid(error.to_string()))?;
    assert!(
        body.pointer("/messages/0/content/0/cache_control")
            .is_none()
    );
    assert!(
        body.pointer("/messages/1/content/0/cache_control")
            .is_none()
    );
    assert!(
        body.pointer("/messages/1/content/1/cache_control")
            .is_none()
    );
    assert_eq!(
        body.pointer("/messages/2/content/0/cache_control/type"),
        Some(&json!("ephemeral"))
    );
    Ok(())
}

#[test]
fn golden_prefix_stability_across_turns_hooks_and_compaction() -> Result<()> {
    let mut stable = 0;
    for (adapter, codex) in [
        (Adapter::Anthropic, false),
        (Adapter::Gemini, false),
        (Adapter::Responses, false),
        (Adapter::Chat, false),
        (Adapter::Responses, true),
    ] {
        let (mut provider, _) = provider(adapter, 200);
        if codex {
            // Projection only: this fixture path is never opened.
            provider.codex_login = Some(Arc::new(crate::CodexLogin::from_path(
                std::path::PathBuf::from("fixture-unused-auth.json"),
            )));
        }
        let (prepared, _) = request(&provider)?;
        let mut request = prepared.request().clone();
        request.tools = tool_name_tests::request_with(&["read"]).tools;
        request.messages = messages(
            "date=2026-10-23; git=clean",
            "hook: verification required",
            "HISTORY_BOUNDARY: first turn",
        );
        let golden = prefix(&provider, &request)?;
        request.messages = messages(
            "date=2026-10-24; git=modified",
            "hook: verification passed",
            "HISTORY_BOUNDARY: second turn",
        );
        let turn = prefix(&provider, &request)?;
        request.messages = messages(
            "date=2026-10-25",
            "hook: verified",
            "HISTORY_BOUNDARY: compacted summary",
        );
        request.messages.truncate(3);
        request
            .messages
            .push(text(ModelRole::System, "date=2026-10-25"));
        let compacted = prefix(&provider, &request)?;
        let unchanged = golden == turn && golden == compacted;
        stable += usize::from(unchanged);
        println!(
            "PREFIX_BASELINE adapter={adapter:?} codex={codex} prefix_bytes={} turn_stable={} compaction_stable={}",
            golden.len(),
            golden == turn,
            golden == compacted
        );
        // Negative control: moving a changing timestamp into static instructions
        // must invalidate the prefix; the oracle must not ignore prefix changes.
        request
            .messages
            .insert(0, text(ModelRole::System, "date=2026-10-26"));
        assert_ne!(prefix(&provider, &request)?, compacted);
    }
    assert_eq!(
        stable, 5,
        "all provider paths must preserve the golden prefix"
    );
    Ok(())
}
