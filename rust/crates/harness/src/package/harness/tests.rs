use super::*;
use crate::{
    AgentId, IdempotencyKey, OperationId,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    package::{
        ActorRuntime, EffectHost, Event, Placement, VolumePlacement,
        conformance::{Action, history},
        in_process::{AdmissionAction, AdmissionPolicy, InProcessRuntime, effect_result_operation},
    },
    resources::ProviderRef,
    workflow::{MachineStatus, WorkflowCommand},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::{Value, json};

fn bindings() -> Bindings {
    Bindings {
        streams: "stock-package".into(),
        volume: "project".into(),
        placement: VolumePlacement::Portable,
        model_https: "https://model.example".into(),
        model_secret: "fixture-model".into(),
        compute: "fixture-compute".into(),
    }
}

struct Host;
impl AdmissionPolicy for Host {
    fn authorize<'a>(
        &'a self,
        _: AdmissionAction,
        _: &'a Manifest,
        _: Option<&'a Placement>,
        _: Option<&'a Event>,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
impl EffectHost for Host {
    fn execute(
        &self,
        _: Placement,
        command: WorkflowCommand,
    ) -> BoxProviderFuture<'_, Result<Event>> {
        Box::pin(async move {
            let value = match command.kind.as_str() {
                crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND => json!({"Succeeded":null}),
                crate::filesystem::MODEL_TASK_COMMAND_KIND => {
                    json!({"Succeeded":{"text":"answer","metadata":{},"steps":1}})
                }
                crate::filesystem::TOOL_TASK_COMMAND_KIND => {
                    json!({"Succeeded":{"text":"tool-result"}})
                }
                _ => return Err(crate::Error::Unsupported("unbound fixture effect".into())),
            };
            Ok(Event {
                operation_id: effect_result_operation(command.operation_id),
                idempotency_key: IdempotencyKey::new(format!("result-{}", command.operation_id))?,
                input: json!({"commands":[{"operation_id":command.operation_id,"value":value}]}),
            })
        })
    }
    fn fence(&self, _: Placement) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn command(id: u8, kind: &str) -> Result<WorkflowCommand> {
    Ok(WorkflowCommand {
        operation_id: OperationId::from_bytes([id; 16]),
        kind: kind.into(),
        payload: FileRef::new(
            VolumeRef::new(
                ProviderRef::new("fixture".into(), "filesystem".into(), "1".into(), &mut None)?,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                &mut None,
            )?,
            format!("commands/{id}.json"),
            "immutable".into(),
            FileDescriptor::from_bytes(b"{}", "application/json", &mut None)?,
            format!("{id}.json"),
            &mut None,
        )?,
    })
}

#[test]
fn artifact_declares_all_four_stock_actors_and_typed_definitions() {
    let package = HarnessCode::new().unwrap().bind(bindings()).unwrap();
    assert_eq!(crate::package::validate_package(&package).unwrap().len(), 4);
    let names: std::collections::BTreeSet<_> = package
        .manifest
        .actors
        .iter()
        .map(|identity| identity.name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "acyclic.stock_turn",
            "acyclic.child_agent",
            "acyclic.fork_preparation",
            "acyclic.direct_tool"
        ]
        .into_iter()
        .collect()
    );
    package
        .code
        .tasks
        .get::<Value, crate::executor::TurnOutput>("acyclic.stock_turn")
        .unwrap();
    package
        .code
        .tasks
        .get::<Value, Value>("acyclic.child_agent")
        .unwrap();
    package
        .code
        .tasks
        .get::<crate::resource_preparation::ResourcePreparationRequest, crate::fork::ForkReport>(
            "acyclic.fork_preparation",
        )
        .unwrap();
    package
        .code
        .tasks
        .get::<Value, crate::tool::ToolResult>("acyclic.direct_tool")
        .unwrap();
}

async fn stock_history(reopen_each_step: bool) -> Result<Vec<crate::package::Snapshot>> {
    let directory =
        tempfile::tempdir().map_err(|error| crate::Error::Storage(error.to_string()))?;
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| crate::Error::Storage(error.to_string()))?,
    );
    let package = Arc::new(HarnessCode::new()?.bind(bindings())?);
    let create =
        || InProcessRuntime::new(stream.clone(), bindings(), Arc::new(Host), Arc::new(Host));
    let mut runtime = create();
    runtime.install(package.clone()).await?;
    let root = Placement {
        actor: AgentId::from_bytes([1; 16]),
        node: "local".into(),
        epoch: 1,
    };
    let tool = Placement {
        actor: AgentId::from_bytes([2; 16]),
        ..root.clone()
    };
    let actor = |name: &str| {
        package
            .manifest
            .actors
            .iter()
            .find(|identity| identity.name == name)
            .cloned()
            .ok_or_else(|| crate::Error::NotFound(name.into()))
    };
    let trigger = |id: u8| -> Result<Event> {
        Ok(Event {
            operation_id: OperationId::from_bytes([id; 16]),
            idempotency_key: IdempotencyKey::new(format!("trigger-{id}"))?,
            input: Value::Null,
        })
    };
    let actions = [
        Action::Place {
            placement: root.clone(),
            machine: actor("acyclic.stock_turn")?,
            input: json!({"receive":command(11,crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND)?,"model":command(12,crate::filesystem::MODEL_TASK_COMMAND_KIND)?}),
        },
        Action::Deliver {
            placement: root.clone(),
            event: trigger(3)?,
        },
        Action::Recover(root.clone()),
        Action::Recover(root.clone()),
        Action::Place {
            placement: tool.clone(),
            machine: actor("acyclic.direct_tool")?,
            input: json!({"command":command(13,crate::filesystem::TOOL_TASK_COMMAND_KIND)?}),
        },
        Action::Deliver {
            placement: tool.clone(),
            event: trigger(4)?,
        },
        Action::Recover(tool),
        Action::Recover(root),
    ];
    let mut snapshots = Vec::new();
    for action in actions {
        if reopen_each_step {
            runtime = create();
            runtime.install(package.clone()).await?;
        }
        snapshots.push(
            history(&runtime, &[action])
                .await
                .pop()
                .ok_or_else(|| crate::Error::Invalid("missing history observation".into()))??,
        );
    }
    Ok(snapshots)
}

#[tokio::test]
async fn stock_package_history_matches_reopened_runtime_with_exact_outbox_results() {
    let live = stock_history(false).await.unwrap();
    let reopened = stock_history(true).await.unwrap();
    assert_eq!(live, reopened);
    assert_eq!(
        live[1].pending[0].kind,
        crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
    );
    assert_eq!(
        live[2].pending[0].kind,
        crate::filesystem::MODEL_TASK_COMMAND_KIND
    );
    assert!(
        matches!(&live[3].status, MachineStatus::Completed { value } if value["text"] == "answer")
    );
    assert_eq!(
        live[5].pending[0].kind,
        crate::filesystem::TOOL_TASK_COMMAND_KIND
    );
    assert!(
        matches!(&live[6].status, MachineStatus::Completed { value } if value["value"]["text"] == "tool-result")
    );
    assert_eq!(live[7], live[3]);
}
