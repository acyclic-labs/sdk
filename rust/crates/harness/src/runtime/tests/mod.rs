use super::*;

use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};

use crate::model::{ModelContent, ModelContentPart};

use crate::resources::ProviderRef;

use std::collections::{BTreeMap, BTreeSet};

use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) fn generated_interface_fields(source: &str, name: &str) -> BTreeSet<String> {
        let header = format!("export interface {name} {{");
        let bytes = source.as_bytes();
        let Some(header_offset) = bytes
            .windows(header.len())
            .position(|window| window == header.as_bytes())
        else {
            return BTreeSet::new();
        };
        let start = header_offset + header.len();
        let mut depth = 1_u32;
        let Some(end) = bytes[start..]
            .iter()
            .enumerate()
            .find_map(|(offset, byte)| {
                match byte {
                    b'{' => depth += 1,
                    b'}' => depth -= 1,
                    _ => {}
                }
                (depth == 0).then_some(offset)
            })
        else {
            return BTreeSet::new();
        };
        let body = std::str::from_utf8(&bytes[start..start + end]).unwrap_or_default();
        body.lines()
            .filter_map(|line| {
                let field = line.trim().strip_prefix("readonly ")?;
                Some(field.split_once(':')?.0.trim().to_owned())
            })
            .collect()
    }

pub(super) fn serialized_object_fields(value: &Value) -> BTreeSet<String> {
        value
            .as_object()
            .map(|fields| fields.keys().cloned().collect())
            .unwrap_or_default()
    }

pub(super) fn dependency_fixture(
        requirements: &[(&str, &[&str])],
    ) -> BTreeMap<(String, String), BTreeSet<String>> {
        requirements
            .iter()
            .map(|(name, requirements)| {
                (
                    (String::from(*name), String::from("1")),
                    requirements
                        .iter()
                        .map(|value| String::from(*value))
                        .collect(),
                )
            })
            .collect()
    }

pub(super) fn generated_wasm_declarations() -> Result<Option<String>> {
        let workspace_typescript =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../typescript");
        let path =
            workspace_typescript.join("packages/harness/generated/wasm/acyclic_harness_wasm.d.ts");
        match std::fs::read_to_string(path) {
            Ok(source) => Ok(Some(source)),
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && !workspace_typescript.is_dir() =>
            {
                Ok(None)
            }
            Err(error) => Err(Error::Storage(format!(
                "generated WASM declaration is unavailable: {error}"
            ))),
        }
    }

pub(super) struct CompletedModel {
        pub(super) requests: std::sync::Mutex<Vec<(crate::model::ModelDispatch, ModelRequest)>>,
    }

impl ModelProvider for CompletedModel {
        fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
            Ok(crate::context::ModelContextCapacity {
                context_tokens: 131_072,
                output_tokens: 4_096,
            })
        }

        fn count_tokens(
            &self,
            request: &crate::model::PreparedModelRequest,
        ) -> Result<crate::context::ModelTokenCount> {
            Ok(crate::context::ModelTokenCount {
                request_digest: request.request_digest(),
                fixed_tokens: u32::try_from(request.bytes().len())
                    .map_err(|_| Error::Invalid("fixture token count overflow".into()))?,
                message_tokens: vec![0; request.request().messages.len()],
            })
        }

        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            dispatch: crate::model::ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
            assert_eq!(dispatch.request_digest, request.request_digest());
            let request = request.request().clone();
            let Ok(mut requests) = self.requests.lock() else {
                return Box::pin(futures::stream::iter([Err(Error::Storage(
                    "test model lock poisoned".into(),
                ))]));
            };
            requests.push((dispatch, request));
            Box::pin(futures::stream::iter([Ok(ModelEvent::Completed {
                metadata: Value::Null,
            })]))
        }


    }

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
            _invocation: &'a ToolInvocation,
            _scope: &'a RuntimeScope,
        ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
            let decision = self.decision.clone();
            Box::pin(async move { Ok(decision) })
        }

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn supports_native_policy(&self, _work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
            Ok(())
        }

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn identity_with_native_work(
            &self, work: &acyclic_fs::NativeMaterializationWork,
        ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
            native_fixture_policy_identity(&self.identity, work)
        }

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn evaluate_with_native_work<'a>(
            &'a self, _invocation: &'a ToolInvocation, _scope: &'a RuntimeScope,
            work: &'a acyclic_fs::NativeMaterializationWork,
        ) -> Result<crate::NativeFutureBundle<'a, (ToolPolicyDecision, acyclic_fs::NativeAllocation)>> {
            crate::NativeFutureBundle::new(work, || async move {
                use crate::store::history::HistoryIo;
                let mut io = crate::store::history::native_work_io(work)?;
                let value = match &self.decision {
                    ToolPolicyDecision::Allow => ToolPolicyDecision::Allow,
                    ToolPolicyDecision::Deny { reason } => ToolPolicyDecision::Deny {
                        reason: io.clone_text(reason)?,
                    },
                    ToolPolicyDecision::RequireApproval { prompt } => ToolPolicyDecision::RequireApproval {
                        prompt: io.clone_text(prompt)?,
                    },
                };
                Ok((value, io.into_custody()))
            })
        }
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_fixture_policy_identity(
        identity: &ComponentIdentity, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo;
        let mut io = crate::store::history::native_work_io(work)?;
        io.retain_owned(std::mem::size_of::<ComponentIdentity>() as u64)?;
        let name = io.clone_text(&identity.name)?;
        let version = io.clone_text(&identity.version)?;
        Ok((ComponentIdentity { name, version, digest: identity.digest }, io.into_custody()))
    }

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_policy_fixture_inputs() -> (FixedPolicy, ToolDefinition, ToolInvocation) {
        let policy = FixedPolicy {
            identity: ComponentIdentity { name: "test.policy".into(), version: "1".into(), digest: [9; 32] },
            decision: ToolPolicyDecision::RequireApproval { prompt: "Approve Î» ðŸ¦€?".into() },
        };
        let definition = ToolDefinition {
            name: "test.echo".into(), revision: "1".into(), description: "Echo".into(),
            input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({}),
            projection_schema: crate::tool::json_projection_schema(serde_json::json!({})),
        };
        let invocation = ToolInvocation {
            operation_id: OperationId::new(), call_id: "provider-call".into(),
            name: definition.name.clone(),
            arguments: serde_json::json!({"z":[true,null],"a":"Î»"}),
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

pub(super) struct StateOnlyProvider;

impl TaskStateProvider for StateOnlyProvider {
        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }
        fn observe_admission<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
            Box::pin(async { Err(Error::Unsupported("state-only test provider".into())) })
        }
        fn resume_scope<'a>(
            &'a self,
            _task_id: TaskId,
            _operation_id: OperationId,
        ) -> BoxFuture<'a, Result<RuntimeScope>> {
            Box::pin(async { Err(Error::Unsupported("state-only test provider".into())) })
        }
        fn outcome<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
            Box::pin(async { Err(Error::Unsupported("state-only test provider".into())) })
        }
        fn cancel<'a>(&'a self, _task_id: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Err(Error::Unsupported("state-only test provider".into())) })
        }
    }

pub(super) struct IdentityHost {
        pub(super) record: TaskAdmissionRecord,
        pub(super) polls: Arc<AtomicUsize>,
    }

impl DurableTaskHost for IdentityHost {}

impl TaskSpawner for IdentityHost {
        fn policy_identity(&self) -> Option<ComponentIdentity> {
            TaskStateProvider::policy_identity(self)
        }
        fn reconcile_admission<'a>(
            &'a self,
            operation_id: OperationId,
        ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
            Box::pin(async move {
                Ok(Some((
                    TaskId::from_bytes(operation_id.into_bytes()),
                    self.record.clone(),
                )))
            })
        }
        fn admit<'a>(
            &'a self,
            _request: TaskAdmissionRecord,
        ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
            Box::pin(async {
                Ok(Admission::Rejected {
                    reason: "unused".into(),
                })
            })
        }
    }

impl crate::runtime::TaskStateProvider for IdentityHost {
        fn observe_admission<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
            Box::pin(async { Ok(self.record.clone()) })
        }
        fn outcome<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
            Box::pin(async move {
                if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(None)
                } else {
                    Ok(Some(Outcome::Succeeded(serde_json::json!(7))))
                }
            })
        }
        fn cancel<'a>(&'a self, _task_id: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }

        fn resume_scope<'a>(
            &'a self,
            _task_id: TaskId,
            _operation_id: OperationId,
        ) -> BoxFuture<'a, Result<RuntimeScope>> {
            Box::pin(async {
                Err(Error::Unsupported(
                    "durable task context authentication is not bound".into(),
                ))
            })
        }
    }

pub(super) struct SplitState {
        pub(super) task_id: TaskId,
        pub(super) record: TaskAdmissionRecord,
    }

impl TaskStateProvider for SplitState {
        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }
        fn observe_admission<'a>(
            &'a self,
            task_id: TaskId,
        ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
            Box::pin(async move {
                if task_id != self.task_id {
                    return Err(Error::NotFound("task".into()));
                }
                Ok(self.record.clone())
            })
        }
        fn resume_scope<'a>(
            &'a self,
            task_id: TaskId,
            operation_id: OperationId,
        ) -> BoxFuture<'a, Result<RuntimeScope>> {
            Box::pin(async move {
                if task_id != self.task_id || operation_id != self.record.operation_id {
                    return Err(Error::NotFound("task".into()));
                }
                RuntimeScope::new(self.record.grants.clone(), self.record.limits)?
                    .with_run_limits(self.record.run_limits, &mut None)
            })
        }
        fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
            Box::pin(async move {
                if task_id != self.task_id {
                    return Err(Error::NotFound("task".into()));
                }
                Ok(Some(Outcome::Succeeded(serde_json::json!(7))))
            })
        }
        fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                if task_id != self.task_id {
                    return Err(Error::NotFound("task".into()));
                }
                Ok(())
            })
        }
    }

impl TaskSpawner for SplitState {
        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }
        fn admit<'a>(
            &'a self,
            _request: TaskAdmissionRecord,
        ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
            Box::pin(async move { Ok(Admission::Accepted(self.task_id)) })
        }
        fn reconcile_admission<'a>(
            &'a self,
            _operation_id: OperationId,
        ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
            Box::pin(async move { Ok(Some((self.task_id, self.record.clone()))) })
        }
    }

mod native;
mod tasks;
mod admission;
mod tools;
mod groups;
