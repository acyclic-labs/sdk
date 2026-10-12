mod batch;

use super::*;
use crate::runtime::{DurableTaskHost, TaskSpawner};
use crate::wire::ErrorCode;
use crate::wire::ErrorCode;
use crate::{
    AgentId, Capabilities, GroupId,
    conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, AuthorityIssuer},
    resources::ProviderRef,
    runtime::TaskDefinition,
    workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine},
};
use acyclic_stream::{MemoryStream, SystemUnixMillisClock};

fn test_registries<O: 'static>(
    implementation: Arc<dyn ResumableMachine>,
    input: Value,
    output: Value,
) -> Result<(
    ComponentIdentity,
    MachineIdentity,
    TaskRegistry,
    MachineRegistry,
)> {
    let machine = implementation.identity().clone();
    let definition = TaskDefinition::<Value, O>::resumable(implementation.clone(), input, output)?;
    let task = definition.identity().clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(implementation, &mut None)?;
    Ok((task, machine, tasks, machines))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn test_work(
    budget: acyclic_fs::WorkBudget,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<acyclic_fs::NativeMaterializationWork> {
    acyclic_fs::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

#[cfg(feature = "filesystem")]
pub(crate) mod created_resource_access;
#[cfg(feature = "filesystem")]
mod resource_preparation;

struct MemoryPayloads {
    volume: VolumeRef,
    files: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl MemoryPayloads {
    fn new() -> Result<Self> {
        Ok(Self {
            volume: VolumeRef::new(
                ProviderRef::new(
                    "batch-test".into(),
                    "filesystem".into(),
                    "2".into(),
                    &mut None,
                )?,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                &mut None,
            )?,
            files: Mutex::new(BTreeMap::new()),
        })
    }
}

impl SchedulerPayloadStore for MemoryPayloads {
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "scheduler payload fixture has no finite observer".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let path = format!(
                "scheduler/{operation_id}/{}.json",
                blake3::hash(idempotency_key.as_bytes()).to_hex(),
            );
            let mut files = self.files.lock().await;
            if let Some(existing) = files.get(&path) {
                if existing != bytes {
                    return Err(Error::Conflict("staged identity changed bytes".into()));
                }
            } else {
                files.insert(path.clone(), bytes.to_vec());
            }
            FileRef::new(
                self.volume.clone(),
                path,
                "immutable-1".into(),
                FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
                "payload.json".into(),
                &mut None,
            )
        })
    }
}

impl ContentResidencyVerifier for MemoryPayloads {
    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, OwnedBytes>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "payload fixture has no finite observer".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if reference.volume() != &self.volume {
                return Err(Error::Unauthorized(
                    "payload belongs to another owner".into(),
                ));
            }
            let files = self.files.lock().await;
            let bytes = files
                .get(reference.path())
                .ok_or_else(|| Error::NotFound("payload".into()))?;
            if bytes.len() as u64 > maximum_bytes {
                return Err(Error::Invalid("payload exceeds read bound".into()));
            }
            reference.descriptor().verify(bytes, &mut None)?;
            Ok(OwnedBytes::copy_from_slice(bytes))
        })
    }
}

struct BatchMachine {
    identity: MachineIdentity,
    schema: Value,
}

impl ResumableMachine for BatchMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work<'a>(
        &'a self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        work.remaining()?;
        Ok(&self.identity)
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let number = input
            .as_i64()
            .map(serde_json::Number::from)
            .or_else(|| input.as_u64().map(serde_json::Number::from))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native fixture requires a fixed-width integer input",
            })?;
        let custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        Ok((Value::Number(number), custody))
    }
    fn transition(&self, state: &Value, _input: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Suspended,
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_work(&self) -> bool {
        true
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn transition_with_native_work(
        &self,
        state: &Value,
        _input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let number = state
            .as_i64()
            .map(serde_json::Number::from)
            .or_else(|| state.as_u64().map(serde_json::Number::from))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native fixture requires a fixed-width integer state",
            })?;
        let custody = work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        Ok((
            MachineTransition {
                state: Value::Number(number),
                commands: Vec::new(),
                status: MachineStatus::Suspended,
            },
            custody,
        ))
    }
}

#[cfg(feature = "filesystem")]
struct EffectTaskFixture<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    binding: TaskJournalOwner<P>,
    payloads: Arc<MemoryPayloads>,
    runtime_scope: RuntimeScope,
}

#[cfg(feature = "filesystem")]
async fn effect_task_fixture<P: StreamProvider>(
    stream: std::sync::Arc<P>,
    provider: &str,
    additional: Vec<String>,
) -> Result<EffectTaskFixture<P>> {
    let payloads = Arc::new(MemoryPayloads::new()?);
    let (host, binding, runtime_scope) = effect_task_fixture_with_storage(
        stream,
        provider,
        additional,
        payloads.clone(),
        payloads.clone(),
        &payloads.volume,
    )
    .await?;
    Ok(EffectTaskFixture {
        host,
        binding,
        payloads,
        runtime_scope,
    })
}

#[cfg(feature = "filesystem")]
#[allow(clippy::type_complexity)]
async fn effect_task_fixture_with_storage<P: StreamProvider>(
    stream: std::sync::Arc<P>,
    provider: &str,
    additional: Vec<String>,
    payloads: Arc<dyn SchedulerPayloadStore>,
    reader: Arc<dyn ContentResidencyVerifier>,
    payload_volume: &VolumeRef,
) -> Result<(
    Arc<CoordinatorTaskHost<P>>,
    TaskJournalOwner<P>,
    RuntimeScope,
)> {
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "effect-task-owner".into(),
    };
    let issuer = AuthorityIssuer::new("effect-task", [51; 32], authority.clone());
    let mut grants = vec![
        "operation:declare".into(),
        "operation:observe".into(),
        "operation:cancel".into(),
        "task:spawn:test.effect@1".into(),
        "effect:run".into(),
        "effect:plan".into(),
        capability::effect_provider(provider),
        payload_volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
    ];
    grants.extend(additional);
    let grants = Capabilities::new(grants);
    let signed = issuer
        .issue("owner", &grants, None, None, &mut None)
        .expect("scope proof");
    let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
    let implementation = Arc::new(BatchMachine {
        identity: MachineIdentity {
            name: "test.effect".into(),
            version: "1".into(),
            digest: [52; 32],
        },
        schema: serde_json::json!({"type":"integer"}),
    });
    let (task_identity, machine, tasks, machines) = test_registries::<Value>(
        implementation,
        serde_json::json!({"type":"integer"}),
        serde_json::json!({"type":"integer"}),
    )?;
    let host = Arc::new(CoordinatorTaskHost::new(
        DistributedCoordinator::open(&stream, reader.clone(), None, &mut None).await?,
        stream.clone(),
        payloads.clone(),
        reader,
        &(authority),
        signed,
        issuer.verifier(&mut None).expect("verifier copy"),
        runtime_scope.clone(),
        tasks,
        machines,
        Arc::new(SystemUnixMillisClock),
        None,
    )?);
    let operation = OperationId::from_bytes([53; 16]);
    let task = TaskId::from_bytes(operation.into_bytes());
    host.admit(TaskAdmissionRecord {
        resource_proofs: Vec::new(),
        operation_id: operation,
        task: task_identity,
        machine,
        input: serde_json::json!(0),
        input_schema: serde_json::json!({"type":"integer"}),
        output_schema: serde_json::json!({"type":"integer"}),
        parent: None,
        grants: runtime_scope.grants().clone(),
        limits: runtime_scope.limits(),
        run_limits: runtime_scope.run_limits(),
        policy: None,
        extensions: None,
        execution: None,
    })
    .await?;
    let crate::distributed::WorkPull::Claimed(lease) = host
        .pull_work(&crate::distributed::Worker {
            id: "effect-worker".into(),
            available: Default::default(),
            labels: BTreeMap::new(),
        })
        .await?
    else {
        return Err(Error::NotFound("effect lease".into()));
    };
    host.start_task(&lease, None).await?;
    let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
    let binding = host.journal_owner(task, fence.clone()).await?;
    Ok((host, binding, runtime_scope))
}

#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_process_tests;

mod admission;
mod journal;
mod mail;
#[cfg(feature = "filesystem-local")]
mod model_recovery;
mod models;
mod observation;
mod recovery;
use models::model_claim_host;
