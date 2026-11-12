use super::*;
use crate::{
    config::Tree,
    conversation::VolumeOperation,
    runtime::{Bindings, TaskContext},
    tool::{Tool, ToolSource},
};
use acyclic_stream::{BoxProviderFuture, OwnedBytes};

/// The settings evaluator and canonical adapter are real; the two immutable
/// source states model the original tree and an unavailable producer on reopen.
struct Programs {
    available: bool,
    shell: Arc<Tool>,
    catalog: ToolRegistry,
}

impl Tree for Programs {
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Option<OwnedBytes>>> {
        Box::pin(async move {
            if !self.available {
                return Err(Error::Storage(
                    "original program source is unavailable".into(),
                ));
            }
            let bytes = match path {
                "tools/echo/program.ts" => b"export default {version: 1, name: 'echo', description: 'Echo arguments', inputSchema: {type: 'object'}, entry: {sh: 'main.sh', ps1: 'main.ps1'}};".as_slice(),
                "tools/echo/main.sh" | "tools/echo/main.ps1" => b"exit 0".as_slice(),
                _ => return Ok(None),
            };
            Ok(Some(OwnedBytes::from_static(bytes)))
        })
    }
}

impl ToolSource for Programs {
    fn refresh(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn snapshot(&self) -> Result<ToolRegistry> {
        Ok(self.catalog.clone())
    }

    fn has_lazy_tools(&self) -> bool {
        true
    }

    fn capability(&self) -> Option<&str> {
        Some("tool:call:shell")
    }

    fn load<'a>(&'a self, name: &'a str) -> BoxProviderFuture<'a, Result<Option<Arc<Tool>>>> {
        Box::pin(async move {
            let program = crate::plugins::load_program(self, &format!("tools/{name}")).await?;
            program.bind(self.shell.clone()).map(Arc::new).map(Some)
        })
    }
}

fn executor(
    task: &TaskContext,
    scope: RuntimeScope,
    operation: OperationId,
    available: bool,
) -> Result<StockExecutor> {
    let tools = crate::tools::defaults()?;
    let shell = Arc::new(
        tools
            .get("shell", None, None)?
            .ok_or_else(|| Error::NotFound("shell".into()))?
            .clone(),
    );
    StockExecutor::new(
        Model::new("test".into(), "model".into(), "1".into())?,
        Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        tools,
    )
    .with_compaction_policy(crate::context::CompactionPolicy::Disabled)
    .with_tool_source(Arc::new(Programs {
        available,
        shell,
        catalog: ToolRegistry::default(),
    }))
    .with_tool_authority(
        scope,
        Arc::new(crate::approvals::ApprovalPolicy::accept_all("owner").bind()?),
    )?
    .with_tool_ceiling(&["shell".into(), "code".into()])?
    .with_task_context(task, operation)
}

#[tokio::test]
async fn rebuilt_executor_retains_selection_without_source_reads_and_refuses_changed_inputs()
-> Result<()> {
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65536).await?;
    let content = storage.content_bindings();
    let scope = RuntimeScope::new(
        crate::Capabilities::new([
            "tool:call:shell".into(),
            "tool:call:code".into(),
            content
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            content
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        ]),
        Limits::default(),
    )?;
    let task = || {
        Bindings {
            scope: scope.clone(),
            content: Some(content.clone()),
            ..Bindings::local()
        }
        .build()?
        .agent_context(&crate::Capabilities::default(), content.clone())
    };
    let operation = OperationId::new();
    let call = || ToolInvocation {
        operation_id: OperationId::from_bytes([7; 16]),
        call_id: "authored-call".into(),
        name: "echo".into(),
        arguments: serde_json::json!({"text": "original"}),
    };
    let first = executor(&task()?, scope.clone(), operation, true)?;
    let selected = StockExecution::ordinary(&first)
        .lazy_call(call(), (operation, 0), ("echo", r#"{"text":"original"}"#))
        .await?
        .map_err(|cause| Error::Invalid(cause.message))?;
    drop(first);
    // A fresh executor and task read the original once-publication. An accidental
    // source reload fails instead of silently selecting the replacement program.
    let mut rebuilt = executor(&task()?, scope, operation, false)?;
    crate::tools::native::NativeTools::new(crate::model::NativeToolProfile::Openai)?
        .bind(&mut rebuilt.tools)?;
    let retained = StockExecution::ordinary(&rebuilt)
        .lazy_call(call(), (operation, 0), ("echo", r#"{"text":"original"}"#))
        .await?
        .map_err(|cause| Error::Invalid(cause.message))?;
    assert_eq!(retained, selected);
    let (canonical, pinned) = retained.bind(&rebuilt.tools, None)?;
    assert_eq!(canonical.name, "shell");
    assert_eq!(pinned.as_ref().unwrap().definition.name, canonical.name);
    let mut changed = call();
    changed.arguments = serde_json::json!({"text": "changed"});
    let refusal = StockExecution::ordinary(&rebuilt)
        .lazy_call(changed, (operation, 0), ("echo", r#"{"text":"changed"}"#))
        .await;
    assert!(matches!(refusal, Err(Error::Conflict(_))));
    Ok(())
}
