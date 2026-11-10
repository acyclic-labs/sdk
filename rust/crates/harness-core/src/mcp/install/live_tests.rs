use super::tests::{advertised, server};
use super::*;
use crate::{
    context::{CompactionPolicy, ContextPipeline, ModelTokenCount},
    executor::{Executor as _, StockExecutor, TurnInput},
    model::{Model, ModelContent, ModelContentPart, ModelEvent, ModelRole, ToolCall},
    runtime::{RuntimeScope, ToolPolicyDecision},
    test_support::{FixedToolPolicy, ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

struct Remote {
    changed: AtomicU64,
    lists: AtomicUsize,
    fail: AtomicBool,
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
        Box::pin(async move {
            if self.fail.load(Ordering::Acquire) {
                return Err(Error::Storage("transient list failure".into()));
            }
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

// The production configured catalog nests per-server live sources. Keep
// declaration selection and actual dispatch under the same server-only grant.
struct Configured(ToolRegistry);
impl crate::tool::ToolSource for Configured {
    fn refresh(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move { self.0.refresh().await })
    }
    fn snapshot(&self) -> Result<ToolRegistry> {
        Ok(self.0.clone())
    }
    fn was_removed(&self, name: &str) -> Result<bool> {
        self.0.was_removed(name)
    }
    fn capability(&self) -> Option<&str> {
        Some("mcp:configured")
    }
}

#[tokio::test]
async fn live_mcp_tools_append_deltas_preserve_prefix_and_refuse_removed_calls() -> Result<()> {
    for composed in [false, true] {
        let remote = Arc::new(Remote {
            changed: AtomicU64::new(0),
            lists: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
        });
        let (mut tools, mut grants) = live::LiveCatalog::new(
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
        tools = ToolRegistry::default().with_source(Arc::new(Configured(tools)));
        grants.0.clear();
        grants.0.insert("mcp:call:watched".into());
        let direct_model = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("direct".into())]).with_token_counter(
                |request| {
                    Ok(ModelTokenCount::from_upper_bounds(
                        request.request_digest(),
                        request.bytes().len() as u32,
                        vec![0; request.request().messages.len()],
                    ))
                },
            ),
        );
        let direct = StockExecutor::new(
            Model::new("test".into(), "model".into(), "1".into())?,
            direct_model.clone(),
            ContextPipeline::default(),
            tools.clone(),
        )
        .with_compaction_policy(CompactionPolicy::Disabled)
        .with_tool_authority(
            RuntimeScope::new(grants.clone(), Default::default())?,
            Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?),
        )?;
        direct
            .execute(
                TurnInput {
                    max_steps: None,
                    operation_id: crate::OperationId::new(),
                    input: ModelContent::Text("direct".into()),
                    selected_context: None,
                },
                &crate::executor::tests::Journal::default(),
            )
            .await?;
        let direct_requests = direct_model.requests();
        assert!(!direct_requests[0].compose_tools);
        assert!(
            direct_requests[0]
                .model_tools()
                .any(|tool| tool.name == "mcp.watched.old")
        );
        // A role ceiling attenuates both source-wide grant spellings, even
        // when a model fabricates a call to an excluded remote declaration.
        for source_grant in ["mcp:configured", "mcp:call:watched"] {
            let denied_model = Arc::new(ScriptedModelProvider::new([
                ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                        "excluded".into(),
                        "mcp.watched.old".into(),
                        json!({}),
                    ))),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: serde_json::Value::Null,
                    }),
                ]),
                ScriptedTurn::Text("denied".into()),
            ]));
            let restricted = StockExecutor::new(
                Model::new("test".into(), "model".into(), "1".into())?,
                denied_model.clone(),
                ContextPipeline::default(),
                tools.clone(),
            )
            .with_compaction_policy(CompactionPolicy::Disabled)
            .with_tool_authority(
                RuntimeScope::new(crate::Capabilities::new([source_grant]), Default::default())?,
                Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?),
            )?
            .with_tool_ceiling(&["mcp.watched.new".into()])?;
            restricted
                .execute(
                    TurnInput {
                        max_steps: None,
                        operation_id: crate::OperationId::new(),
                        input: ModelContent::Text("restricted".into()),
                        selected_context: None,
                    },
                    &crate::executor::tests::Journal::default(),
                )
                .await?;
            let requests = denied_model.requests();
            assert!(
                requests[0]
                    .model_tools()
                    .all(|tool| tool.name != "mcp.watched.old")
            );
            assert!(requests[1].messages.iter().flat_map(|message| message.content.parts()).any(|part| matches!(part,
                ModelContentPart::ToolResult { call_id, content: crate::model::ToolResultContent::Error { .. }, .. } if call_id == "excluded")));
            assert_eq!(
                remote.changed.load(Ordering::Acquire),
                0,
                "role ceiling allowed a remote effect"
            );
        }
        let watched = tools.clone();
        if composed {
            let code = crate::tools::catalog()
                .find(|spec| spec.name == "code")
                .expect("code tool")
                .bind()?;
            tools.register((code, None), &mut None, None)?;
            grants.0.insert("tool:call:code".into());
        }
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
        if composed {
            assert_eq!(prefix(&requests[0]), prefix(&requests[1]));
        }
        assert!(
            requests[1]
                .model_tools()
                .all(|tool| tool.name != "mcp.watched.old"),
            "historical schemas exposed a removed tool as currently callable"
        );
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
        if composed {
            let mut old_first = requests[0].clone();
            old_first.compose_tools = false;
            let mut old_next = requests[1].clone();
            old_next.compose_tools = false;
            assert_ne!(prefix(&old_first), prefix(&old_next));
        }
        let admitted = watched.definitions(None)?;
        remote.changed.fetch_add(1, Ordering::Release);
        remote.fail.store(true, Ordering::Release);
        watched.refresh().await?;
        assert_eq!(
            watched.definitions(None)?,
            admitted,
            "an optional refresh failure retains its admitted catalog"
        );
        remote.fail.store(false, Ordering::Release);
        watched.refresh().await?;
        assert_eq!(
            remote.lists.load(Ordering::SeqCst),
            3,
            "the failed generation remains pending until discovery succeeds"
        );
    }
    Ok(())
}
