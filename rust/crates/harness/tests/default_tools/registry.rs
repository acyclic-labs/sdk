use super::*;
use proptest::prelude::*;
use std::collections::BTreeSet;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Disjoint catalogs compose as set union; any overlap refuses atomically,
    // preserving every original selected definition.
    #[test]
    fn registry_extension_is_disjoint_union_or_unchanged(
        left in proptest::collection::btree_set("[a-c]{1,4}", 0..8),
        right in proptest::collection::btree_set("[a-c]{1,4}", 0..8),
    ) {
        let registry = |names: &BTreeSet<String>| {
            let mut registry = ToolRegistry::default();
            for name in names {
                let mut tool = example_tool();
                tool.definition.name = name.clone();
                registry.register((tool, None), &mut None, None).unwrap();
            }
            registry
        };
        let mut merged = registry(&left);
        let before = merged.definitions(None).unwrap();
        let accepted = merged.extend(registry(&right)).is_ok();
        prop_assert_eq!(accepted, left.is_disjoint(&right));
        if accepted {
            let actual: BTreeSet<_> = merged.definitions(None).unwrap().into_iter().map(|definition| definition.name).collect();
            prop_assert_eq!(actual, left.union(&right).cloned().collect::<BTreeSet<_>>());
        } else {
            prop_assert_eq!(merged.definitions(None).unwrap(), before);
        }
    }
}

#[test]
fn defaults_are_exported_additive_and_explicitly_disabled() -> Result<()> {
    let defaults = crate::tools::defaults()?;
    let names = ["bash", "fs", "read", "write", "edit"];
    for name in names {
        assert_eq!(
            defaults
                .get(name, None, None)?
                .map(|tool| tool.definition.name.as_str()),
            Some(name)
        );
    }
    let mut custom = ToolRegistry::default();
    custom.register((example_tool(), None), &mut None, None)?;
    let mut second = example_tool();
    second.definition.name = "example.second".into();
    let enabled = HarnessBuilder::new()
        .agent_loop(Arc::new(DelegatingLoop))
        .tool(second)?
        .tools(custom)?
        .build()?;
    for name in names {
        assert_eq!(enabled.runtime().tool(name)?.name, name);
        assert!(
            enabled
                .capabilities()
                .contains(&format!("tool:call:{name}"))
        );
    }
    assert_eq!(enabled.runtime().tool("example.echo")?.name, "example.echo");
    assert_eq!(
        enabled.runtime().tool("example.second")?.name,
        "example.second"
    );
    let disabled = HarnessBuilder::new()
        .agent_loop(Arc::new(DelegatingLoop))
        .default_tools(false)
        .tool(example_tool())?
        .build()?;
    for name in names {
        assert!(matches!(
            disabled.runtime().tool(name),
            Err(Error::NotFound(_))
        ));
        assert!(
            !disabled
                .capabilities()
                .contains(&format!("tool:call:{name}"))
        );
    }
    assert_eq!(
        disabled.runtime().tool("example.echo")?.name,
        "example.echo"
    );
    Ok(())
}

#[test]
fn defaults_cannot_replace_custom_tools_or_broaden_bound_authority() -> Result<()> {
    assert!(matches!(
        HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .tool(crate::tool::files::read()?)?
            .build(),
        Err(Error::Conflict(_))
    ));
    let bound = HarnessBuilder::new()
        .bindings(Bindings::local())
        .agent_loop(Arc::new(DelegatingLoop))
        .build()?;
    for name in ["bash", "fs", "read", "write", "edit"] {
        assert_eq!(bound.runtime().tool(name)?.name, name);
        assert!(!bound.capabilities().contains(&format!("tool:call:{name}")));
    }
    Ok(())
}

#[test]
fn registry_merge_rejects_duplicates_before_mutating_destination() -> Result<()> {
    let mut destination = crate::tools::defaults()?;
    let mut incoming = ToolRegistry::default();
    incoming.register((example_tool(), None), &mut None, None)?;
    incoming.register((crate::tool::files::read()?, None), &mut None, None)?;
    assert!(matches!(
        destination.extend(incoming),
        Err(Error::Conflict(_))
    ));
    assert!(destination.get("example.echo", None, None)?.is_none());
    assert!(destination.get("read", None, None)?.is_some());
    Ok(())
}

#[test]
fn registry_merge_retains_pinned_revisions_and_hidden_model_selections() -> Result<()> {
    let mut custom = ToolRegistry::default();
    custom.register((example_tool(), None), &mut None, None)?;
    let mut second = example_tool();
    second.definition.revision = "2".into();
    custom.register((second, None), &mut None, None)?;
    custom.select_model_version("example.echo", "2")?;
    custom.remove_from_model("example.echo")?;
    let mut combined = crate::tools::defaults()?;
    combined.extend(custom)?;
    assert!(combined.get("example.echo", None, None)?.is_none());
    assert!(combined.get("example.echo", Some("1"), None)?.is_some());
    assert!(combined.get("example.echo", Some("2"), None)?.is_some());
    assert_eq!(combined.definitions(None)?.len(), 5);
    combined.select_model_version("example.echo", "2")?;
    assert_eq!(
        combined
            .get("example.echo", None, None)?
            .map(|tool| tool.definition.revision.as_str()),
        Some("2")
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn workspace_commands_reject_unbound_and_forged_calls_without_native_allocation() -> Result<()>
{
    let limits = Limits::default();
    let storage = MemoryHarnessStorage::new(AgentId::new(), limits.file_bytes).await?;
    run_admitted_file_case(&storage, limits, |task| async move {
        let tools = crate::tools::defaults()?;
        for name in ["bash", "fs"] {
            let tool = tools
                .get(name, None, None)?
                .ok_or_else(|| Error::NotFound(name.into()))?;
            let mut invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "bound".into(),
                name: name.into(),
                arguments: json!({"command":"status"}),
            };
            let context = runtime::ToolContext::new(
                Arc::new(task.clone()),
                invocation.operation_id,
                &invocation.call_id,
                None,
            )?;
            assert!(matches!(
                tool.executor
                    .authorize(None, Some(&context), &invocation, None),
                Err(Error::Unsupported(_))
            ));
            let cancellation = acyclic_fs::CancellationToken::new();
            assert!(
                acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::WorkBudget::default(),
                    &cancellation
                )
                .is_err()
            );
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(100_000, 16_000_000),
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let before = work.receipt();
            let before_live = work.live_allocation_bytes();
            assert!(matches!(
                tool.executor
                    .authorize(None, Some(&context), &invocation, Some(&work)),
                Err(Error::NativeBoundary {
                    code: wire::ErrorCode::Unsupported,
                    ..
                })
            ));
            invocation.call_id = "forged".into();
            assert!(matches!(
                tool.executor
                    .authorize(None, Some(&context), &invocation, Some(&work)),
                Err(Error::NativeBoundary {
                    code: wire::ErrorCode::Unauthorized,
                    ..
                })
            ));
            assert_eq!(work.receipt(), before);
            assert_eq!(work.live_allocation_bytes(), before_live);
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn workspace_commands_use_the_calling_runtime_without_approval() -> Result<()> {
    struct Workspace(&'static str);
    impl ToolExecutor for Workspace {
        fn execute<'a>(
            &'a self,
            invocation: ToolInvocation,
            context: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<ToolResult>> {
            Box::pin(async move {
                let context = context.ok_or_else(|| Error::Unauthorized("missing task".into()))?;
                assert_eq!(context.operation_id(), invocation.operation_id);
                assert_eq!(context.call_id(), invocation.call_id);
                Ok(ToolResult {
                    value: json!({"workspace":self.0,"command":invocation.arguments["command"],"tool":invocation.name}),
                })
            })
        }
        fn reconcile<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<crate::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            Box::pin(async { Ok(None) })
        }
    }
    for workspace in ["checkout", "child-view"] {
        let bundle = HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .workspace_tools(Arc::new(Workspace(workspace)))
            .grant("task:spawn:commands@1")
            .task(TaskDefinition::live(
                "commands",
                "1",
                move |context, (): ()| async move {
                    for (name, command) in
                        [("bash", "printf captured"), ("fs", "diff agents/child")]
                    {
                        let result: Value = context
                            .call(&context.tool(name)?, json!({"command":command}))
                            .await?;
                        assert_eq!(result["tool"], name);
                        assert_eq!(result["command"], command);
                        assert_eq!(result["workspace"], workspace);
                    }
                    Ok(())
                },
            )?)?
            .build()?;
        let task = bundle.runtime().task::<(), ()>("commands")?;
        assert_eq!(
            bundle.runtime().spawn(&task, ()).await?.result().await?,
            crate::Outcome::Succeeded(())
        );
    }
    Ok(())
}
