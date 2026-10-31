use super::*;
use crate::executor::records::RecordedCall;
use crate::runtime::{Target, TargetResolver};

struct Devices {
    roster: Arc<crate::agent::Roster>,
    devices: Vec<Arc<Target>>,
}

impl TargetResolver for Devices {
    fn roster(&self) -> &Arc<crate::agent::Roster> {
        &self.roster
    }

    fn resolve(&self, name: Option<&str>) -> Result<Arc<Target>> {
        let selected = self.roster.select(name).map_err(Error::Target)?;
        self.devices
            .iter()
            .find(|device| device.record.name == selected.name)
            .cloned()
            .ok_or_else(|| Error::Invalid("selected device is absent".into()))
    }
}

// Exercise the durable stock claim and real workspace router, with a lost
// reply at the device boundary. Changing a roster cannot change that claim.
#[tokio::test]
async fn started_workspace_call_keeps_its_device_after_home_changes() -> Result<()> {
    for alias in [None, Some("user:ana")] {
        for absent in [false, true] {
            let scope =
                RuntimeScope::new(Capabilities::new(["tool:call:shell"]), Limits::default())?;
            let task = admitted_context(scope.clone())?;
            let original = Arc::new(ContextOnlyTool {
                admission: context_identity(&task).cloned(),
                lost_response: || Error::Storage("device reply lost after effect".into()),
                calls: AtomicUsize::new(0),
                reconciles: AtomicUsize::new(0),
            });
            let other = Arc::new(ContextOnlyTool {
                admission: context_identity(&task).cloned(),
                lost_response: || Error::Storage("unrelated device".into()),
                calls: AtomicUsize::new(0),
                reconciles: AtomicUsize::new(0),
            });
            let device = |name, executor: Arc<dyn ToolExecutor>| {
                Arc::new(Target {
                    record: crate::runtime::InProcessTargets::local_record(name, "ana"),
                    executor,
                })
            };
            let old = device("original", original.clone());
            let new = device("replacement", other.clone());
            let bind = |devices: Vec<Arc<Target>>| -> Result<crate::runtime::TaskContext> {
                let mut task = task.clone();
                task.harness = crate::runtime::Bindings {
                    scope: scope.clone(),
                    targets: Some(Arc::new(Devices {
                        roster: Arc::new(crate::agent::Roster {
                            home: devices[0].record.name.clone(),
                            targets: devices.iter().map(|device| device.record.clone()).collect(),
                        }),
                        devices,
                    })),
                    ..crate::runtime::Bindings::local()
                }
                .build()?;
                Ok(task)
            };
            let defaults = crate::tools::defaults()?;
            let mut tools = ToolRegistry::default();
            tools.register(
                (
                    crate::tool::Tool {
                        skill_source: None,
                        hook_names: &[],
                        definition: crate::tool::ToolDefinition {
                            name: "shell".into(),
                            revision: "1".into(),
                            description: "Shell".into(),
                            input_schema: json!({"type":"object"}),
                            output_schema: json!({"type":"object"}),
                            projection_schema: crate::tool::json_projection_schema(
                                json!({"type":"object"}),
                            ),
                        },
                        executor: defaults
                            .get("shell", None, None)?
                            .expect("stock shell")
                            .executor
                            .clone(),
                        projection: Arc::new(Projection),
                    },
                    None,
                ),
                &mut None,
                None,
            )?;
            let executor = uncompacted_executor(
                Model::new("test".into(), "replay".into(), "1".into())?,
                Arc::new(scripted_model()),
                ContextPipeline::default(),
                tools,
            )
            .with_tool_authority(scope.clone(), allowed()?)?;
            let operation = OperationId::new();
            let mut invocation = ToolInvocation {
                operation_id: operation,
                call_id: "command".into(),
                name: "shell".into(),
                arguments: json!({"command":"echo retained"}),
            };
            if let Some(alias) = alias {
                invocation.arguments["target"] = alias.into();
            }
            let journal = Journal::default();
            let first = executor
                .clone()
                .with_task_context(&bind(vec![old.clone(), new.clone()])?, operation)?;
            let mut messages = vec![call_message(&invocation)];
            assert!(matches!(
                StockExecution::ordinary(&first)
                    .settle_tool_call(
                        &journal,
                        operation,
                        0,
                        invocation.clone(),
                        0,
                        &mut messages,
                        &mut Vec::new(),
                    )
                    .await,
                Err(Error::Indeterminate(_))
            ));
            let (_, records) = StockExecution::ordinary(&first)
                .tool_records(&journal, operation, 0, "command")
                .await?;
            let reference = RecordedCall::classify(&records)
                .expect("valid claim")
                .started()
                .expect("started");
            let saved =
                load_json::<crate::tool::RecordedInvocation>(&journal, reference, None).await?;
            assert_eq!(saved.0.target.as_deref(), Some("original"));
            assert_eq!(
                saved.0.invocation, invocation,
                "model arguments stay unchanged"
            );

            let mut changed = vec![new.clone()];
            if !absent {
                changed.push(old.clone());
            }
            let second = executor
                .clone()
                .with_task_context(&bind(changed)?, operation)?;
            let replay = StockExecution::ordinary(&second)
                .settle_tool_call(
                    &journal,
                    operation,
                    0,
                    invocation.clone(),
                    0,
                    &mut messages,
                    &mut Vec::new(),
                )
                .await;
            if absent {
                assert!(matches!(replay, Err(Error::Indeterminate(id)) if id == operation));
                assert_eq!(original.reconciles.load(Ordering::SeqCst), 0);
                // Restore the original device; only its original effect may settle.
                let restored = executor
                    .clone()
                    .with_task_context(&bind(vec![new, old])?, operation)?;
                StockExecution::ordinary(&restored)
                    .settle_tool_call(
                        &journal,
                        operation,
                        0,
                        invocation,
                        0,
                        &mut messages,
                        &mut Vec::new(),
                    )
                    .await?;
            } else {
                replay?;
            }
            assert_eq!(original.calls.load(Ordering::SeqCst), 1);
            assert_eq!(original.reconciles.load(Ordering::SeqCst), 1);
            assert_eq!(other.calls.load(Ordering::SeqCst), 0);
            assert_eq!(other.reconciles.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}
