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
        let mut original = registry(&left);
        let before = original.definitions(None).unwrap();
        let result = original.extend(registry(&right));
        let accepted = result.is_ok();
        prop_assert_eq!(accepted, left.is_disjoint(&right));
        if result.is_ok() {
            let actual: BTreeSet<_> = original.definitions(None).unwrap().into_iter().map(|definition| definition.name).collect();
            prop_assert_eq!(actual, left.union(&right).cloned().collect::<BTreeSet<_>>());
        } else {
            prop_assert_eq!(original.definitions(None).unwrap(), before);
        }
    }

    #[test]
    fn task_file_errors_preserve_file_read_classification(kind in 0u8..8, message in ".{0,64}") {
        if kind < 6 {
        let cause = match kind {
            0 => Error::NotFound(message),
            1 => Error::Conflict(message),
            2 => Error::Unsupported(message),
            3 => Error::Invalid(message),
            4 => Error::Unauthorized(message),
            _ => Error::Storage(message),
        };
        let expected = cause.code();
        let display = cause.to_string();
        let wrapped = Error::from(acyclic_harness::task_volume::Error::from(cause));
        prop_assert_eq!(wrapped.code(), expected);
        prop_assert_eq!(wrapped.to_string(), display);
        } else {
            let cause = if kind == 6 { acyclic_harness::task_volume::Error::Unauthorized }
                else { acyclic_harness::task_volume::Error::FenceConflict };
            let display = cause.to_string();
            let wrapped = Error::from(cause);
            prop_assert_eq!(wrapped.code(), acyclic_harness::wire::ErrorCode::Unauthorized);
            prop_assert_eq!(wrapped.to_string(), display);
        }
    }

    // A complete replacement preserves exactly the selected contracts for any
    // subset of builtin names and never synthesizes registrations or grants.
    #[test]
    fn registry_replacement_preserves_selections_with_any_bindings(
        names in proptest::collection::btree_set(0usize..9, 0..=9),
        volume in any::<bool>(), workspace in any::<bool>(), hidden in any::<bool>(),
        revision in 1u8..=2, description in ".{0,32}",
    ) {
        let builtins = [acyclic_harness::tools::shell::PLATFORM.name, "fs", "read", "write", "edit", "spawn", "send", "wait", "archive"];
        let mut registry = ToolRegistry::default();
        for &index in &names {
            for version in 1..=2 {
                let mut tool = example_tool();
                tool.definition.name = builtins[index].into();
                tool.definition.revision = version.to_string();
                tool.definition.description = format!("Custom {description}");
                registry.register((tool, None), &mut None, None).unwrap();
            }
            registry.select_model_version(builtins[index], &revision.to_string()).unwrap();
            if hidden {
                registry.remove_from_model(builtins[index]).unwrap();
            }
        }
        let expected = registry.definitions(None).unwrap();
        let mut builder = HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .tool(example_tool())
            .replace_tools(registry)
            .tool(example_tool());
        if volume { builder = builder.content(content()); }
        if workspace { builder = builder.workspace(Arc::new(Echo)); }
        let bundle = builder.build().unwrap();
        for (index, name) in builtins.into_iter().enumerate() {
            let actual = bundle.runtime().tool(name);
            match expected.iter().find(|tool| tool.name == name) {
                Some(definition) => prop_assert_eq!(&actual.unwrap(), definition),
                None => prop_assert!(matches!(actual, Err(Error::NotFound(_)))),
            }
            let capability = format!("tool:call:{name}");
            prop_assert!(!bundle.capabilities().contains(&capability));
            prop_assert_eq!(expected.iter().any(|tool| tool.name == name), names.contains(&index) && !hidden);
        }
        prop_assert_eq!(bundle.runtime().tool("example.echo").unwrap(), example_tool().definition);
    }
}

#[test]
fn exported_defaults_can_replace_a_complete_bound_registry() -> Result<()> {
    let defaults = acyclic_harness::tools::defaults()?;
    let expected = defaults.definitions(None)?;
    let bundle = HarnessBuilder::new()
        .agent_loop(Arc::new(DelegatingLoop))
        .content(content())
        .workspace(Arc::new(Echo))
        .replace_tools(defaults)
        .build()?;
    for definition in expected {
        assert_eq!(bundle.runtime().tool(&definition.name)?, definition);
    }
    Ok(())
}

#[test]
fn defaults_are_exported_additive_and_explicitly_disabled() -> Result<()> {
    let defaults = acyclic_harness::tools::defaults()?;
    let names = [
        acyclic_harness::tools::shell::PLATFORM.name,
        "fs",
        "read",
        "write",
        "edit",
        "spawn",
        "send",
        "wait",
        "archive",
    ];
    for name in names {
        assert_eq!(
            defaults
                .get(name, None, None)?
                .map(|tool| tool.definition.name.as_str()),
            Some(name)
        );
    }
    let mut second = example_tool();
    second.definition.name = "example.second".into();
    let enabled = HarnessBuilder::new()
        .agent_loop(Arc::new(DelegatingLoop))
        .content(content())
        .workspace(Arc::new(Echo))
        .tool(second)
        .tools([example_tool()])
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
        .without_default_tools()
        .tool(example_tool())
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
    let artifacts = HarnessBuilder::new()
        .agent_loop(Arc::new(DelegatingLoop))
        .artifacts(content())
        .build()?;
    for name in ["read", "write", "edit"] {
        assert!(matches!(
            artifacts.runtime().tool(name),
            Err(Error::NotFound(_))
        ));
        assert!(
            !artifacts
                .capabilities()
                .contains(&format!("tool:call:{name}"))
        );
    }
    assert!(matches!(
        HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .content(content())
            .tool(acyclic_harness::tool::files::read()?)
            .build(),
        Err(Error::Conflict(_))
    ));
    let bound = HarnessBuilder::new()
        .bindings(Bindings::local())
        .agent_loop(Arc::new(DelegatingLoop))
        .build()?;
    for name in [
        acyclic_harness::tools::shell::PLATFORM.name,
        "fs",
        "read",
        "write",
        "edit",
    ] {
        assert!(matches!(
            bound.runtime().tool(name),
            Err(Error::NotFound(_))
        ));
        assert!(!bound.capabilities().contains(&format!("tool:call:{name}")));
    }
    Ok(())
}

struct Reader;
impl conversation::ContentResidencyVerifier for Reader {}
pub(super) fn content() -> runtime::ContentBindings {
    runtime::ContentBindings {
        reader: Arc::new(Reader),
        writer: None,
    }
}

#[test]
fn registration_and_grants_follow_available_bindings() -> Result<()> {
    for volume in [false, true] {
        for workspace in [false, true] {
            let mut builder = HarnessBuilder::new().agent_loop(Arc::new(DelegatingLoop));
            if volume {
                builder = builder.content(content());
            }
            if workspace {
                builder = builder.workspace(Arc::new(Echo));
            }
            let harness = builder.build()?;
            for (name, available) in [
                (acyclic_harness::tools::shell::PLATFORM.name, workspace),
                ("fs", workspace),
                ("read", volume),
                ("write", volume),
                ("edit", volume),
                ("spawn", workspace),
                ("send", workspace),
                ("wait", workspace),
                ("archive", workspace),
            ] {
                assert_eq!(harness.runtime().tool(name).is_ok(), available);
                assert_eq!(
                    harness
                        .capabilities()
                        .contains(&format!("tool:call:{name}")),
                    available
                );
            }
        }
    }
    let mut bindings = Bindings::local();
    bindings.content = Some(content());
    bindings.workspace_tools = Some(Arc::new(Echo));
    bindings.scope =
        runtime::RuntimeScope::new(Capabilities::new(["tool:call:read"]), Limits::default())?;
    let harness = HarnessBuilder::new()
        .bindings(bindings)
        .agent_loop(Arc::new(DelegatingLoop))
        .build()?;
    assert_eq!(harness.runtime().tool("read")?.name, "read");
    for name in [
        acyclic_harness::tools::shell::PLATFORM.name,
        "fs",
        "write",
        "edit",
        "spawn",
        "send",
        "wait",
        "archive",
    ] {
        assert!(matches!(
            harness.runtime().tool(name),
            Err(Error::NotFound(_))
        ));
    }
    Ok(())
}

#[test]
fn registry_merge_rejects_duplicates_before_mutating_destination() -> Result<()> {
    let mut destination = acyclic_harness::tools::defaults()?;
    let mut incoming = ToolRegistry::default();
    incoming.register((example_tool(), None), &mut None, None)?;
    incoming.register(
        (acyclic_harness::tool::files::read()?, None),
        &mut None,
        None,
    )?;
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
    let mut combined = acyclic_harness::tools::defaults()?;
    combined.extend(custom)?;
    assert!(combined.get("example.echo", None, None)?.is_none());
    assert!(combined.get("example.echo", Some("1"), None)?.is_some());
    assert!(combined.get("example.echo", Some("2"), None)?.is_some());
    assert_eq!(combined.definitions(None)?.len(), 9);
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
        let tools = acyclic_harness::tools::defaults()?;
        for name in [acyclic_harness::tools::shell::PLATFORM.name, "fs"] {
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
            context: Option<acyclic_harness::runtime::ToolContext>,
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
            _: Option<acyclic_harness::runtime::ToolContext>,
        ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
            Box::pin(async { Ok(None) })
        }
    }
    for workspace in ["checkout", "child-view"] {
        let bundle = HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .workspace(Arc::new(Workspace(workspace)))
            .grant("task:spawn:commands@1")
            .task(TaskDefinition::live(
                "commands",
                "1",
                move |context, (): ()| async move {
                    for (name, command) in [
                        (
                            acyclic_harness::tools::shell::PLATFORM.name,
                            "printf captured",
                        ),
                        ("fs", "diff agents/child"),
                    ] {
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
            acyclic_harness::Outcome::Succeeded(())
        );
    }
    Ok(())
}
