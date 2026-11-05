//! Stock policy admission followed by a real native process, with unconfined,
//! denied and unsupported-executor controls on the identical invocation.
use super::*;
use crate::runtime::ToolContext;
use acyclic_native_runtime::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork};
use acyclic_native_runtime::{ProcessCommand, ProcessStdio, ProcessTree};

struct NativeTool {
    workspace: std::path::PathBuf,
    supported: bool,
    calls: AtomicUsize,
    revision: AtomicUsize,
    change_policy: bool,
}

impl ToolExecutor for NativeTool {
    fn authorize_execution_grant<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        _: &'a ToolContext,
        grant: &'a ExecutionGrant,
        _: crate::NativeWork<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            assert_eq!(invocation.name, "shell");
            assert_eq!(
                invocation.arguments,
                json!({"command":"ignored", "timeout_ms":5000})
            );
            if self.supported {
                grant
                    .check_attenuation_async(grant, None)
                    .await
                    .map_err(Error::process_io)?;
                if self.change_policy {
                    self.revision.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            } else {
                Err(Error::Unsupported(
                    "executor cannot enforce this grant".into(),
                ))
            }
        })
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            assert_eq!(invocation.name, "shell");
            assert_eq!(
                invocation.arguments,
                json!({"command":"ignored", "timeout_ms":5000})
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            let context = context.expect("original task");
            assert_eq!(context.operation_id(), invocation.operation_id);
            assert_eq!(context.call_id(), invocation.call_id);
            let mut command = ProcessCommand::new("/bin/sh");
            command
                .args([
                    "-c",
                    "{ printf changed > ../outside; printf created > inside; } 2>/dev/null; true",
                ])
                .current_dir(&self.workspace)
                .env_clear()
                .stdin(ProcessStdio::null())
                .stdout(ProcessStdio::null())
                .stderr(ProcessStdio::null());
            let process = match context.execution_grant() {
                Some(grant) => ProcessTree::spawn_granted(command, grant, None),
                None => ProcessTree::spawn(&mut command),
            };
            let mut tree = match process {
                Ok(tree) => tree,
                Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                    let refusal = error
                        .get_ref()
                        .unwrap()
                        .downcast_ref::<acyclic_native_runtime::execution::ExecutionRefusal>()
                        .expect("typed unsupported boundary");
                    return Err(Error::Unsupported(refusal.reason().into()));
                }
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            // The original OS child-exit producer settles the wait; no pipe,
            // channel, timer, readiness probe or separate process owner exists.
            let status = tree
                .wait_exit(None)
                .map_err(|error| Error::Storage(error.to_string()))?;
            assert!(status.success());
            tree.terminate(None)
                .map_err(|error| Error::Storage(error.to_string()))?;
            Ok(ToolResult { value: json!({}) })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { panic!("completed native effect must not be dispatched again") })
    }
}

struct PhysicalPolicy {
    executor: Arc<NativeTool>,
    grant: Option<ExecutionGrant>,
    deny: bool,
    selections: AtomicUsize,
}

impl ToolPolicy for PhysicalPolicy {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            name: "test.physical".into(),
            version: "1".into(),
            digest: [self.executor.revision.load(Ordering::SeqCst) as u8; 32],
        }
    }

    fn evaluate<'a>(
        &'a self,
        _: &'a ToolInvocation,
        _: &'a RuntimeScope,
        _: Option<&'a PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>
    {
        Ok(crate::store::HistoryContentFuture::Ordinary {
            future: Box::pin(async move {
                Ok((
                    if self.deny {
                        ToolPolicyDecision::Deny {
                            reason: "declined".into(),
                        }
                    } else {
                        ToolPolicyDecision::Allow
                    },
                    None,
                ))
            }),
            frame_bytes: 0,
        })
    }

    fn execution_grant(
        &self,
        tool: &crate::tool::Tool,
        invocation: &ToolInvocation,
        context: Option<&ToolContext>,
        _: Option<&PreparationIo>,
    ) -> Result<Option<ExecutionGrant>> {
        assert!(!self.deny);
        assert!(std::ptr::addr_eq(
            Arc::as_ptr(&tool.executor),
            Arc::as_ptr(&self.executor),
        ));
        let context = context.expect("original task before dispatch");
        assert_eq!(context.operation_id(), invocation.operation_id);
        assert_eq!(context.call_id(), invocation.call_id);
        assert!(context.execution_grant().is_none());
        self.selections.fetch_add(1, Ordering::SeqCst);
        Ok(self.grant.clone())
    }
}

#[tokio::test]
async fn physical_grant_admission_reaches_native_dispatch_and_refusals_never_launch() -> Result<()>
{
    for (access, deny, supported, change_policy) in [
        (None, false, true, false),
        (Some(ExecutionAccess::ReadOnly), false, true, false),
        (Some(ExecutionAccess::WorkspaceWrite), false, true, false),
        (Some(ExecutionAccess::WorkspaceWrite), true, true, false),
        (Some(ExecutionAccess::WorkspaceWrite), false, false, false),
        (Some(ExecutionAccess::WorkspaceWrite), false, true, true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::write(root.path().join("outside"), "original").unwrap();
        let native = Arc::new(NativeTool {
            workspace: workspace.clone(),
            supported,
            calls: AtomicUsize::new(0),
            revision: AtomicUsize::new(1),
            change_policy,
        });
        let grant = access.map(|access| {
            ExecutionGrant::new(
                &workspace,
                access,
                ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
                    .into_iter()
                    .map(std::path::PathBuf::from)
                    .filter(|path| path.exists()),
                ExecutionNetwork::Off,
            )
            .unwrap()
        });
        let policy = Arc::new(PhysicalPolicy {
            executor: native.clone(),
            grant,
            deny,
            selections: AtomicUsize::new(0),
        });
        let scope = RuntimeScope::new(
            Capabilities::new(["model:generate", "tool:call:shell"]),
            Limits::default(),
        )?;
        let task = admitted_context(scope.clone())?;
        let operation = OperationId::new();
        let provider = Arc::new(
            ScriptedModelProvider::new([
                tool_turn(vec![ModelEvent::ToolCall(crate::model::ToolCall::new(
                    "call-1".into(),
                    "shell".into(),
                    json!({"command":"ignored", "timeout":5000}),
                ))]),
                done_turn(),
            ])
            .with_token_counter(fixture_token_count),
        );
        let original = echo_tools(
            native.clone(),
            json!({"type":"object"}),
            json!({"type":"object"}),
            crate::tool::json_projection_schema(json!({"type":"object"})),
        )?;
        let mut shell = original.get("example.echo", None, None)?.unwrap().clone();
        shell.definition.name = "shell".into();
        let mut tools = ToolRegistry::default();
        tools.register((shell, None), &mut None, None)?;
        crate::tools::native::bind(&mut tools, crate::model::NativeToolProfile::Anthropic)?;
        let executor = uncompacted_executor(
            Model::new("test".into(), "physical".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(scope, policy.clone())?
        .with_task_context(&task, operation)?;
        let input = TurnInput {
            max_steps: None,
            operation_id: operation,
            input: ModelContent::Text("input".into()),
            selected_context: None,
        };
        let journal = Journal::default();
        let output = executor.execute(input.clone(), &journal).await;
        if change_policy {
            assert!(matches!(output, Err(Error::Conflict(_))));
            assert_eq!(native.calls.load(Ordering::SeqCst), 0);
            assert_eq!(policy.selections.load(Ordering::SeqCst), 1);
            assert_eq!(
                std::fs::read_to_string(root.path().join("outside")).unwrap(),
                "original"
            );
            assert!(!workspace.join("inside").exists());
            continue;
        }
        let output = output?;
        let dispatched = !deny && supported;
        let requests = provider.requests();
        let refusal = super::super::tools_1::tool_error_text(&requests[1].messages, "call-1");
        let unavailable = refusal
            .as_deref()
            .is_some_and(|text| text.contains("Landlock "));
        assert_eq!(
            refusal.is_some(),
            !dispatched || unavailable,
            "physical-grant authorization must reach the selected executor: {refusal:?}",
        );
        assert_eq!(policy.selections.load(Ordering::SeqCst), usize::from(!deny));
        assert_eq!(native.calls.load(Ordering::SeqCst), usize::from(dispatched));
        assert_eq!(
            std::fs::read_to_string(root.path().join("outside")).unwrap(),
            if access.is_none() {
                "changed"
            } else {
                "original"
            }
        );
        assert_eq!(
            workspace.join("inside").exists(),
            dispatched && !unavailable && access != Some(ExecutionAccess::ReadOnly)
        );
        assert_eq!(executor.execute(input, &journal).await?, output);
        assert_eq!(policy.selections.load(Ordering::SeqCst), usize::from(!deny));
        assert_eq!(native.calls.load(Ordering::SeqCst), usize::from(dispatched));
        let accepted = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "accepted".into(),
            name: "shell".into(),
            arguments: json!({"command":"ignored", "timeout":5000}),
        };
        let selected = executor.tools.get(&accepted.name, None, None)?.unwrap();
        let reconciled = StockExecution::ordinary(&executor)
            .require_tool_policy(&journal, operation, &accepted, (selected, None), true, None)
            .await;
        if deny {
            assert!(matches!(reconciled, Err(Error::Indeterminate(_))));
        } else {
            assert!(matches!(reconciled, Ok(None)));
        }
        assert_eq!(policy.selections.load(Ordering::SeqCst), usize::from(!deny));
    }
    Ok(())
}
