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
    change_schema: bool,
    changed: AtomicU64,
    lists: AtomicUsize,
    fail: AtomicBool,
    stall: AtomicBool,
    calls: AtomicUsize,
    reconciles: AtomicUsize,
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
            if self.stall.load(Ordering::Acquire) {
                return futures::future::pending().await;
            }
            if self.fail.load(Ordering::Acquire) {
                return Err(Error::Storage("transient list failure".into()));
            }
            serde_json::from_value(if self.change_schema {
                json!({"tools":[{"name":"old","inputSchema":{"type":"object"},"outputSchema":{"type":"object","required":["new_field"]}}]})
            } else {
                json!({"tools":[{"name":"new","inputSchema":{"type":"object"}}]})
            })
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
        self.calls.fetch_add(1, Ordering::SeqCst);
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
        self.reconciles.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(Some(rmcp::model::CallToolResult::success(vec![
                rmcp::model::ContentBlock::text("ok"),
            ])))
        })
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
    for compose in [false, true] {
        live_mcp_case(compose).await?;
    }
    Ok(())
}

struct Nested(crate::tool::ToolRegistry);
impl crate::tool::ToolSource for Nested {
    fn refresh(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async { self.0.refresh().await })
    }
    fn snapshot(&self) -> Result<crate::tool::ToolRegistry> {
        Ok(self.0.clone())
    }
    fn capability(&self) -> Option<&str> {
        Some("mcp:configured")
    }
    fn was_removed(&self, name: &str) -> Result<bool> {
        self.0.was_removed(name)
    }
}

async fn live_mcp_case(compose: bool) -> Result<()> {
    let remote = Arc::new(Remote {
        change_schema: false,
        changed: AtomicU64::new(0),
        lists: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
        stall: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
        reconciles: AtomicUsize::new(0),
    });
    let (mut tools, _) = live::LiveCatalog::new(
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
    tools = crate::tool::ToolRegistry::default().with_source(Arc::new(Nested(tools)));
    let mut grants = crate::Capabilities::new(["mcp:call:watched"]);
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
    let watched = tools.clone();
    let code = crate::tools::catalog()
        .find(|spec| spec.name == "code")
        .expect("code tool")
        .bind()?;
    if compose {
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
        tools.clone(),
    )
    .with_compaction_policy(CompactionPolicy::Disabled)
    .with_tool_authority(
        RuntimeScope::new(grants.clone(), crate::conversation::Limits::default())?,
        Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?),
    )?;
    let journal = crate::executor::tests::Journal::default();
    let input = TurnInput {
        max_steps: None,
        operation_id: crate::OperationId::new(),
        input: ModelContent::Text("go".into()),
        selected_context: None,
    };
    let output = executor.execute(input.clone(), &journal).await?;
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
    if compose {
        assert_eq!(prefix(&requests[0]), prefix(&requests[1]));
    } else {
        assert_ne!(prefix(&requests[0]), prefix(&requests[1]));
    }
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
    assert_eq!(executor.execute(input.clone(), &journal).await?, output);
    let mut detached = tools.clone();
    detached.retain_sources(|_| false);
    let detached = StockExecutor::new(
        Model::new("test".into(), "model".into(), "1".into())?,
        model.clone(),
        ContextPipeline::default(),
        detached,
    )
    .with_compaction_policy(CompactionPolicy::Disabled)
    .with_tool_authority(
        RuntimeScope::new(grants.clone(), crate::conversation::Limits::default())?,
        Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?),
    )?;
    assert_eq!(
        detached.execute(input.clone(), &journal).await?,
        output,
        "uninstalling a source cannot invalidate its committed results"
    );
    assert_eq!(
        remote.calls.load(Ordering::SeqCst),
        1,
        "settled calls replay without dispatch"
    );
    let pending = journal.reopen_before_tool_completion("first");
    let recovery_model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
        "recovered".into(),
    )]));
    let recovery = StockExecutor::new(
        Model::new("test".into(), "model".into(), "1".into())?,
        recovery_model,
        ContextPipeline::default(),
        tools,
    )
    .with_compaction_policy(CompactionPolicy::Disabled)
    .with_tool_authority(
        RuntimeScope::new(grants, crate::conversation::Limits::default())?,
        Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?),
    )?;
    recovery.execute(input, &pending).await?;
    assert_eq!(
        remote.reconciles.load(Ordering::SeqCst),
        1,
        "an interrupted removed call retains its producer"
    );
    assert_eq!(
        remote.calls.load(Ordering::SeqCst),
        1,
        "reconciliation never reissues the effect"
    );
    let original = requests[0]
        .tools
        .iter()
        .find(|tool| tool.name == "mcp.watched.old")
        .expect("original admitted schema");
    let (different, _) = live::LiveCatalog::new(
        "watched".into(),
        server(
            json!({"command":"fake"}),
            super::super::config::Dialect::Codex,
        )?,
        Connected {
            transport: remote.clone(),
            tools: advertised(&["new"]),
            binding: [2; 32],
            generation: 1,
        },
        McpClientLimits::default(),
    )?;
    assert!(
        different.recover(original, None)?.is_none(),
        "a replacement producer cannot reconcile the original effect"
    );
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
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn changed_mcp_schema_is_refused_before_dispatch_and_replays() -> Result<()> {
    let remote = Arc::new(Remote {
        change_schema: true,
        changed: AtomicU64::new(0),
        lists: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
        stall: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
        reconciles: AtomicUsize::new(0),
    });
    let (tools, _) = live::LiveCatalog::new(
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
    let changed = remote.clone();
    let model = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "changed".into(),
                    "mcp.watched.old".into(),
                    json!({}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ]),
            ScriptedTurn::Text("done".into()),
        ])
        .with_dispatch_observer(move |_| {
            changed.changed.store(1, Ordering::Release);
            Box::pin(async { Ok(()) })
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
        RuntimeScope::new(
            crate::Capabilities::new(["mcp:call:watched"]),
            Default::default(),
        )?,
        Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?),
    )?;
    let journal = crate::executor::tests::Journal::default();
    let input = TurnInput {
        max_steps: None,
        operation_id: crate::OperationId::new(),
        input: ModelContent::Text("go".into()),
        selected_context: None,
    };
    let output = executor.execute(input.clone(), &journal).await?;
    assert!(
        model.requests()[1]
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .any(|part| {
                matches!(part, ModelContentPart::ToolResult {
            content: crate::model::ToolResultContent::Error { message, .. }, ..
        } if message.contains("tool definition changed after the model request"))
            })
    );
    assert_eq!(executor.execute(input, &journal).await?, output);
    assert_eq!(remote.calls.load(Ordering::SeqCst), 0);
    assert_eq!(remote.reconciles.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn live_refresh_deadline_retains_optional_catalog_and_releases_the_refresh_lock() -> Result<()>
{
    for required in [false, true] {
        let remote = Arc::new(Remote {
            change_schema: false,
            changed: AtomicU64::new(0),
            lists: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            stall: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            reconciles: AtomicUsize::new(0),
        });
        let mut config = server(
            json!({"command":"fake","required":required}),
            super::super::config::Dialect::Codex,
        )?;
        config.startup_timeout_ms = 1;
        let (tools, _) = live::LiveCatalog::new(
            "deadline".into(),
            config,
            Connected {
                transport: remote.clone(),
                tools: advertised(&["old"]),
                binding: [1; 32],
                generation: 0,
            },
            McpClientLimits::default(),
        )?;
        let admitted = tools.definitions(None)?;
        remote.changed.store(1, Ordering::Release);
        assert_eq!(tools.refresh().await.is_err(), required);
        assert_eq!(
            tools.definitions(None)?,
            admitted,
            "a timed out refresh never publishes a partial catalog"
        );
        remote.stall.store(false, Ordering::Release);
        tools.refresh().await?;
        assert!(
            tools
                .definitions(None)?
                .iter()
                .any(|tool| tool.name == "mcp.deadline.new")
        );
    }
    Ok(())
}
