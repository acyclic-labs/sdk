use crate::{
    Adapter, Error, HttpRequest, HttpResponse, HttpTransport, Result, tests::provider_with,
};
use acyclic_harness_core::{
    context::ContextPipeline,
    executor::{Executor, StockExecutor, TurnInput},
    filesystem::MemoryHarnessStorage,
    model::ModelContent,
    tool::ToolRegistry,
};
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use serde_json::json;
use std::sync::{Arc, Mutex};

struct Pages(Adapter, Mutex<Vec<HttpRequest>>);

impl HttpTransport for Pages {
    fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let mut sent = self.1.lock().unwrap();
            sent.push(request);
            let body = match (self.0, sent.len()) {
                (Adapter::Responses, 1) => concat!(
                    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_only\",\"summary\":[],\"encrypted_content\":\"opaque==\"}}\n\n",
                    "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":17,\"output_tokens\":32768}}}\n\n",
                ),
                (Adapter::Responses, 2) => concat!(
                    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"complete report\"}\n\n",
                    "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":32785,\"output_tokens\":2}}}\n\n",
                ),
                (Adapter::Anthropic, 1) => concat!(
                    "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":17}}}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"redacted_thinking\",\"data\":\"opaque==\"}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"model_context_window_exceeded\"},\"usage\":{\"output_tokens\":32768}}\n\n",
                    "data: {\"type\":\"message_stop\"}\n\n",
                ),
                (Adapter::Anthropic, 2) => concat!(
                    "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":32785}}}\n\n",
                    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"complete report\"}}\n\n",
                    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
                    "data: {\"type\":\"message_stop\"}\n\n",
                ),
                _ => return Err(Error::Invalid("length continuation redispatched".into())),
            };
            Ok(HttpResponse {
                request_id: None,
                retry_after: None,
                status: 200,
                body: Box::pin(futures::stream::iter([Ok(Bytes::from_static(
                    body.as_bytes(),
                ))])),
            })
        })
    }
}

#[tokio::test]
async fn reasoning_only_length_stops_continue_through_real_protocol_encoding() -> Result<()> {
    for adapter in [Adapter::Responses, Adapter::Anthropic] {
        let transport = Arc::new(Pages(adapter, Mutex::new(Vec::new())));
        let mut provider = provider_with(adapter, "http://127.0.0.1:1234/v1", transport.clone());
        provider.capacity = acyclic_harness_core::context::ModelContextCapacity {
            context_tokens: 1_048_576,
            output_tokens: Some(1_048_576),
            output_shares_context: true,
        };
        Arc::make_mut(&mut provider.config).token_counter = None;
        let model = provider.model.clone();
        let executor = StockExecutor::new(
            model,
            Arc::new(provider),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let storage =
            MemoryHarnessStorage::new(acyclic_harness_core::AgentId::new(), 131_072).await?;
        let journal = storage.journal();
        let input = TurnInput {
            operation_id: acyclic_harness_core::OperationId::new(),
            max_steps: None,
            input: ModelContent::Text("report".into()),
            selected_context: None,
        };
        let output = executor.execute(input.clone(), journal.as_ref()).await?;
        assert_eq!(output.text, "complete report");
        assert_eq!(output.steps, 2);
        assert_eq!(executor.execute(input, journal.as_ref()).await?, output);
        let sent = transport.1.lock().unwrap();
        assert_eq!(sent.len(), 2);
        let first = crate::json(&sent[0].body)?;
        let second = crate::json(&sent[1].body)?;
        assert!(first.get("max_output_tokens").is_none());
        assert!(second.get("max_output_tokens").is_none());
        if adapter == Adapter::Anthropic {
            assert_eq!(second["max_tokens"], 1_048_576);
            assert_eq!(
                sent[1].headers["anthropic-beta"],
                "model-context-window-exceeded-2025-08-26"
            );
            assert_eq!(second["messages"][0], first["messages"][0]);
            assert_eq!(second["messages"][1]["role"], "assistant");
            assert_eq!(
                second["messages"][1]["content"][0],
                json!({"type":"redacted_thinking","data":"opaque=="})
            );
            continue;
        }
        assert_eq!(second["input"][0], first["input"][0]);
        assert_eq!(
            second["input"][1],
            json!({"type":"reasoning","id":"rs_only","summary":[],"encrypted_content":"opaque=="})
        );
        assert_eq!(second["input"][2]["role"], "assistant");
        assert_eq!(second["input"][2]["content"][0]["text"], "");
        assert_eq!(second["input"].as_array().unwrap().len(), 3);
    }
    Ok(())
}
