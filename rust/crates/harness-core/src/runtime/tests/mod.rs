use super::*;

use crate::model::{ModelContent, ModelContentPart};

use std::sync::atomic::AtomicUsize;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use std::sync::atomic::Ordering;

pub(super) fn dependency_fixture(requirements: &[(&str, &[&str])]) -> Result<Bindings> {
    let mut bindings = Bindings::local();
    for (name, requirements) in requirements {
        let mut definition = TaskDefinition::live(*name, "1", |_context, _input: ()| async {
            Ok::<(), Error>(())
        })?;
        for requirement in *requirements {
            definition = definition.requires(*requirement)?;
        }
        bindings.tasks.register(definition)?;
    }
    Ok(bindings)
}

pub(super) fn admission_fixture() -> Result<TaskAdmissionRecord> {
    Ok(TaskAdmissionRecord::from_canonical_value(
        serde_json::from_str(include_str!(
            "../../../../harness/fixtures/v2/task-admission.json"
        ))
        .map_err(|error| Error::Invalid(error.to_string()))?,
        &mut None,
    )?
    .0)
}

pub(super) fn local_runtime(
    tasks: TaskRegistry,
    grants: &[&str],
    concurrency: usize,
) -> Result<Arc<AgentHarness>> {
    Bindings {
        tasks,
        scope: RuntimeScope::new(Capabilities::new(grants.iter().copied()), Limits::default())?,
        concurrency,
        ..Bindings::local()
    }
    .build(None)
}

pub(super) fn test_context(harness: Arc<AgentHarness>) -> Result<TaskContext> {
    Ok(TaskContext {
        task_id: OperationId::new(),
        durable_task: None,
        descendants: TaskGroup::new(harness.concurrency, None)?,
        scope: harness.scope.clone(),
        harness,
        policy_overrides: Vec::new(),
        owned_interactions: None,
        owned_mailbox: None,
        output_content: None,
        model_steps: Arc::new((AtomicUsize::new(0), None)),
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_purpose: None,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_custody: None,
    })
}

pub(super) fn resumable_fixture(
    name: &str,
    digest: [u8; 32],
) -> Result<TaskDefinition<Value, u32>> {
    TaskDefinition::resumable(
        Arc::new(TestMachine {
            identity: MachineIdentity {
                name: name.into(),
                version: "1".into(),
                digest,
            },
            state_schema: serde_json::json!({"type":"object"}),
        }),
        serde_json::json!({"type":"object"}),
        serde_json::json!({"type":"integer"}),
    )
}

pub(super) fn file_fixture(path: &str, media_type: &str) -> Result<FileRef> {
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../../harness/fixtures/v2/file-ref.json"
    ))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    value["path"] = path.into();
    value["descriptor"]["media_type"] = media_type.into();
    let file: FileRef =
        serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    file.validate(&mut None)?;
    Ok(file)
}

#[derive(Clone)]
pub(super) struct FixedPolicy {
    pub(super) identity: ComponentIdentity,
    pub(super) decision: ToolPolicyDecision,
}

impl ToolPolicy for FixedPolicy {
    fn identity(&self) -> ComponentIdentity {
        self.identity.clone()
    }

    fn evaluate<'a>(
        &'a self,
        _: &'a ToolInvocation,
        _: &'a RuntimeScope,
        _native_io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = task_original_work(_native_io)? {
            return crate::NativeFutureBundle::new(work, || async move {
                use crate::store::history::HistoryIo;
                let mut io = crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                );
                let value = match &self.decision {
                    ToolPolicyDecision::Allow => ToolPolicyDecision::Allow,
                    ToolPolicyDecision::Deny { reason } => ToolPolicyDecision::Deny {
                        reason: io.clone_text(reason)?,
                    },
                    ToolPolicyDecision::RequireApproval { prompt } => {
                        ToolPolicyDecision::RequireApproval {
                            prompt: io.clone_text(prompt)?,
                        }
                    }
                };
                Ok((value, Some(io.into_custody()?)))
            })
            .map(crate::store::HistoryContentFuture::Native);
        }
        let decision = self.decision.clone();
        Ok(crate::store::HistoryContentFuture::Ordinary {
            future: Box::pin(async move { Ok((decision, None)) }),
            frame_bytes: 0,
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        native_fixture_policy_identity(&self.identity, work)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_fixture_policy_identity(
    identity: &ComponentIdentity,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
    use crate::store::history::HistoryIo;
    let mut io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    io.retain_owned(std::mem::size_of::<ComponentIdentity>() as u64)?;
    let name = io.clone_text(&identity.name)?;
    let version = io.clone_text(&identity.version)?;
    Ok((
        ComponentIdentity {
            name,
            version,
            digest: identity.digest,
        },
        io.into_custody()?,
    ))
}

pub(super) fn native_policy_fixture_inputs() -> (FixedPolicy, ToolDefinition, ToolInvocation) {
    let policy = FixedPolicy {
        identity: ComponentIdentity {
            name: "test.policy".into(),
            version: "1".into(),
            digest: [9; 32],
        },
        decision: ToolPolicyDecision::RequireApproval {
            prompt: "Approve \u{03bb} \u{1f980}?".into(),
        },
    };
    let definition = ToolDefinition {
        name: "test.echo".into(),
        revision: "1".into(),
        description: "Echo".into(),
        input_schema: serde_json::json!({"type":"object"}),
        output_schema: serde_json::json!({}),
        projection_schema: crate::tool::json_projection_schema(serde_json::json!({})),
    };
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "provider-call".into(),
        name: definition.name.clone(),
        arguments: serde_json::json!({"z":[true,null],"a":"\u{03bb}"}),
    };
    (policy, definition, invocation)
}

pub(super) struct TestMachine {
    pub(super) identity: MachineIdentity,
    pub(super) state_schema: Value,
}

impl ResumableMachine for TestMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.state_schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(
        &self,
        state: &Value,
        _input: &Value,
    ) -> Result<crate::workflow::MachineTransition> {
        Ok(crate::workflow::MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: crate::workflow::MachineStatus::Suspended,
        })
    }
}

pub(super) struct SplitState {
    pub(super) task_id: TaskId,
    pub(super) record: TaskAdmissionRecord,
}

impl DurableTaskHost for SplitState {}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TaskStateProvider for SplitState {
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        None
    }
    async fn observe_admission(&self, task_id: TaskId) -> Result<TaskAdmissionRecord> {
        if task_id != self.task_id {
            return Err(Error::NotFound("task".into()));
        }
        Ok(self.record.clone())
    }
    async fn resume_scope(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> Result<RuntimeScope> {
        if task_id != self.task_id || operation_id != self.record.operation_id {
            return Err(Error::NotFound("task".into()));
        }
        RuntimeScope::new(self.record.grants.clone(), self.record.limits)?
            .with_run_limits(self.record.run_limits, &mut None)
    }
    async fn outcome(&self, task_id: TaskId) -> Result<Option<Outcome<Value>>> {
        if task_id != self.task_id {
            return Err(Error::NotFound("task".into()));
        }
        Ok(Some(Outcome::Succeeded(serde_json::json!(7))))
    }
    async fn cancel(&self, task_id: TaskId) -> Result<()> {
        if task_id != self.task_id {
            return Err(Error::NotFound("task".into()));
        }
        Ok(())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TaskSpawner for SplitState {
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        None
    }
    async fn admit(&self, _request: TaskAdmissionRecord) -> Result<Admission<TaskId>> {
        Ok(Admission::Accepted(self.task_id))
    }
    async fn reconcile_admission(
        &self,
        _operation_id: OperationId,
    ) -> Result<Option<(TaskId, TaskAdmissionRecord)>> {
        Ok(Some((self.task_id, self.record.clone())))
    }
}

mod admission;
mod groups;
mod native;
mod tasks;
mod tools;
