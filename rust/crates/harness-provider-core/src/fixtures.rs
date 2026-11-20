use crate::{Adapter, Result};

/// Model redelivery backoff completes on the fixture's explicit exchange step.
pub fn delay(_: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, Result<()>> {
    Box::pin(async { Ok(()) })
}

/// The frozen fixture clock: it never advances, so no deadline measured on it elapses.
pub fn now() -> std::time::Duration {
    std::time::Duration::ZERO
}

/// [`delay`] admitted on the native ledger, with no allocation beyond the admitted future.
#[cfg(not(target_arch = "wasm32"))]
pub fn native_delay(
    _: std::time::Duration,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness_core::Result<
    acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>,
> {
    acyclic_fs::NativeFutureBundle::new(work, move || async move { Ok(()) })
}

/// The chat-completions stream every adapter's fixture is compared against.
pub const CHAT: &str = concat!(
    "data: {\"id\":\"run\",\"choices\":[{\"delta\":{\"content\":\"hé\",\"reasoning_content\":\"think\",\"tool_calls\":[{\"index\":0,\"id\":\"call\",\"function\":{\"name\":\"echo\",\"arguments\":\"{\\\"value\\\":\"}}]}}]}\n\n",
    "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n",
    "data: [DONE]\n\n"
);
const ANTHROPIC: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"run\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hé\"}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"think\"}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call\",\"name\":\"echo\",\"input\":{}}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"value\\\":1}\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":2}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":3}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n"
);
const RESPONSES: &str = concat!(
    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hé\"}\n\n",
    "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"think\"}\n\n",
    "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc\",\"call_id\":\"call\",\"name\":\"echo\",\"arguments\":\"\"}}\n\n",
    "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\\\"value\\\":1}\"}\n\n",
    "data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":1,\"arguments\":\"{\\\"value\\\":1}\"}\n\n",
    "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call\",\"name\":\"echo\",\"arguments\":\"{\\\"value\\\":1}\"}}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"run\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n"
);
const GEMINI: &str = concat!(
    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hé\"},{\"text\":\"think\",\"thought\":true}]}}]}\n\n",
    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"id\":\"call\",\"name\":\"echo\",\"args\":{\"value\":1}}}]},\"finishReason\":\"STOP\"}],\"responseId\":\"run\",\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":3,\"totalTokenCount\":8}}\n\n"
);

/// One recorded stream per adapter, in `Adapter` order.
pub fn fixtures() -> [(Adapter, &'static str); 4] {
    [
        (Adapter::Chat, CHAT),
        (Adapter::Anthropic, ANTHROPIC),
        (Adapter::Responses, RESPONSES),
        (Adapter::Gemini, GEMINI),
    ]
}

fn definition(name: String) -> acyclic_harness_core::tool::ToolDefinition {
    acyclic_harness_core::tool::ToolDefinition {
        name,
        revision: "1".into(),
        description: "Read a file".into(),
        input_schema: serde_json::json!({"type":"object"}),
        output_schema: serde_json::json!({"type":"object"}),
        projection_schema: serde_json::json!({"type":"object"}),
    }
}

/// A request carrying exactly `tools`, with no messages or sampling.
pub fn request_with(tools: &[&str]) -> acyclic_harness_core::model::ModelRequest {
    acyclic_harness_core::model::ModelRequest {
        summary: false,
        compose_tools: false,
        model: acyclic_harness_core::model::Model {
            provider: "fixture".into(),
            name: "fixture".into(),
            revision: "1".into(),
        },
        messages: Vec::new(),
        tools: tools
            .iter()
            .map(|name| definition((*name).into()))
            .collect(),
        sampling: Default::default(),
    }
}

/// The reviewer's input: an alias, its declared readable name, and the
/// alias's first `taken` salted digests, all declared as canonical names.
pub fn adversarial(taken: u32) -> Vec<String> {
    let mut tools = vec!["mcp.a.b".to_owned(), "mcp__a__b".to_owned()];
    tools.extend((1..=taken).map(|salt| crate::tool_names::Names::candidate("mcp.a.b", salt)));
    tools
}
