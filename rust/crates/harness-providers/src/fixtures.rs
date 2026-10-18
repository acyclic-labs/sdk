use crate::Adapter;

/// Frozen fixture clock: redelivery waits elapse immediately; default exchange deadlines
/// never elapse. Deadline tests use their own progressing virtual clock.
pub(crate) fn delay(wait: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
    if wait >= std::time::Duration::from_millis(crate::TimeoutSpec::DEFAULT.idle_ms.into()) {
        Box::pin(futures::future::pending())
    } else {
        Box::pin(async {})
    }
}

pub(crate) const CHAT: &str = concat!(
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

pub(crate) fn fixtures() -> [(Adapter, &'static str); 4] {
    [
        (Adapter::Chat, CHAT),
        (Adapter::Anthropic, ANTHROPIC),
        (Adapter::Responses, RESPONSES),
        (Adapter::Gemini, GEMINI),
    ]
}
