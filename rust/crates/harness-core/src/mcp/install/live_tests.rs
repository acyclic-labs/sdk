use super::tests::{advertised, server};
use super::*;
use crate::{
    context::{CompactionPolicy, ContextPipeline, ModelTokenCount},
    executor::{Executor as _, StockExecutor, TurnInput},
    model::{Model, ModelContent, ModelContentPart, ModelEvent, ModelRole, ToolCall},
    runtime::{RuntimeScope, ToolPolicyDecision},
    test_support::{FixedToolPolicy, ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

struct Remote {
    changed: AtomicU64,
    lists: AtomicUsize,
}
impl McpToolTransport for Remote {
    fn tools_generation(&self) -> u64 {
        self.changed.load(Ordering::Acquire)
    }
    fn list(
        &self,
        _: Option<String>,
    ) -> BoxProviderFuture<'_, Result<rmcp::model::ListToolsResult>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            serde_json::from_value(
                json!({"tools":[{"name":"new","inputSchema":{"type":"object"}}]}),
            )
            .map_err(|error| Error::Invalid(error.to_string()))
        })
    }
    fn call<'a>(
        &'a self,
        _: crate::OperationId,
        _: &'a str,
        _: serde_json::Value,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<rmcp::model::CallToolResult>> {
        // The connection reader publishes this same event for list_changed.
        self.changed.fetch_add(1, Ordering::Release);
        Box::pin(async {
            Ok(rmcp::model::CallToolResult::success(vec![
                rmcp::model::ContentBlock::text("ok"),
            ]))
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: crate::OperationId,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<rmcp::model::CallToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

#[tokio::test]
async fn live_mcp_tools_append_deltas_preserve_prefix_and_refuse_removed_calls() -> Result<()> {
    let remote = Arc::new(Remote {
        changed: AtomicU64::new(0),
        lists: AtomicUsize::new(0),
    });
    let (tools, grants) = live::LiveCatalog::new(
        "watched".into(),
        server(
            json!({"command":"fake"}),
            super::super::config::Dialect::Codex,
        )?,
        Connected {
            transport: remote.clone(),
            tools: advertised(&["old"]),
            binding: [1; 32],
            generation: 0,
        },
        McpClientLimits::default(),
    )?;
    let call = |id: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                id.into(),
                "mcp.watched.old".into(),
                json!({}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::Value::Null,
            }),
        ])
    };
    let model = Arc::new(
        ScriptedModelProvider::new([
            call("first"),
            call("removed"),
            ScriptedTurn::Text("done".into()),
        ])
        .with_token_counter(|request| {
            Ok(ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                request.bytes().len() as u32,
                vec![0; request.request().messages.len()],
            ))
        }),
    );
    let executor = StockExecutor::new(
        Model::new("test".into(), "model".into(), "1".into())?,
        model.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_compaction_policy(CompactionPolicy::Disabled)
    .with_tool_authority(
        RuntimeScope::new(grants, crate::conversation::Limits::default())?,
        Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?),
    )?;
    let journal = crate::executor::tests::Journal::default();
    executor
        .execute(
            TurnInput {
                max_steps: None,
                operation_id: crate::OperationId::new(),
                input: ModelContent::Text("go".into()),
                selected_context: None,
            },
            &journal,
        )
        .await?;
    let requests = model.requests();
    assert_eq!(requests.len(), 3);
    let prefix = |request: &crate::model::ModelRequest| {
        serde_json::to_vec(&(
            request.model_tools().collect::<Vec<_>>(),
            request
                .messages
                .iter()
                .take_while(|message| message.role == ModelRole::System)
                .collect::<Vec<_>>(),
        ))
        .unwrap()
    };
    assert_eq!(prefix(&requests[0]), prefix(&requests[1]));
    assert_eq!(
        requests[0].messages,
        requests[1].messages[..requests[0].messages.len()]
    );
    let notices: Vec<_> = requests[1]
        .messages
        .iter()
        .filter_map(|message| message.content.notice_parts())
        .flat_map(|(notices, _)| notices)
        .filter(|notice| notice.kind == "tools.changed")
        .collect();
    assert_eq!(notices.len(), 2);
    assert!(
        notices[1].body["json"]
            .as_str()
            .unwrap()
            .contains("mcp.watched.new")
    );
    assert_eq!(notices[1].body["removed"], json!(["mcp.watched.old"]));
    let failure = requests[2]
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            ModelContentPart::ToolResult {
                call_id,
                content: crate::model::ToolResultContent::Error { message, .. },
                ..
            } if call_id == "removed" => Some(message),
            _ => None,
        })
        .expect("removed call result");
    assert_eq!(
        failure,
        "tool execution failed: mcp.watched.old was removed"
    );
    assert_eq!(
        remote.lists.load(Ordering::SeqCst),
        1,
        "clean requests do no list RPC"
    );
    // Negative control: the old path rendered the changing catalog as native
    // declarations. The exact same prefix oracle detects that rewrite.
    let mut old_first = requests[0].clone();
    old_first.compose_tools = false;
    let mut old_next = requests[1].clone();
    old_next.compose_tools = false;
    assert_ne!(prefix(&old_first), prefix(&old_next));
    Ok(())
}
