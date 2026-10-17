//! Task and machine admission for the approved process example.
use super::*;
use acyclic_harness::runtime::TaskSpawner as _;
use acyclic_harness::store::{HistoryIo as _, HistoryWork};
use std::mem::size_of;

#[cfg(test)]
#[path = "native_properties.rs"]
pub(super) mod properties;

struct Machine {
    identity: MachineIdentity,
    schema: Value,
}
impl ResumableMachine for Machine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, _: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: vec![],
            status: MachineStatus::Suspended,
        })
    }
    fn supports_native_work(&self) -> bool {
        true
    }
    fn identity_with_native_work<'a>(
        &'a self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        Ok(&self.identity)
    }
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        copy_state(input, work, size_of::<Value>())
    }
    fn transition_with_native_work(
        &self,
        state: &Value,
        _: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let (state, custody) = copy_state(state, work, size_of::<MachineTransition>())?;
        Ok((
            MachineTransition {
                state,
                commands: Vec::new(),
                status: MachineStatus::Suspended,
            },
            custody,
        ))
    }
}

fn copy_state(
    state: &Value,
    work: &acyclic_fs::NativeMaterializationWork,
    header_bytes: usize,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    let remaining = work.remaining()?;
    let mut io =
        HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
    io.retain_owned(header_bytes as u64)?;
    let state = acyclic_harness::executor::json::json_value_copy_bounded(
        state,
        acyclic_harness::executor::json::JsonBounds {
            maximum_bytes: remaining.bytes_encoded,
            maximum_items: remaining.items_examined,
            maximum_depth: 256,
        },
        Some(acyclic_harness::wire::ErrorCode::Invalid),
        &mut |charge| io.codec_charge(charge),
    )?;
    Ok((state, io.into_custody()?))
}

pub(super) async fn open<P, A, O>(
    stream: std::sync::Arc<P>,
    files: Arc<Files<A, O>>,
    volume: VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
    work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> Result<Runtime<P, A, O>>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let machine = Arc::new(Machine {
        identity: MachineIdentity {
            name: "example.native".into(),
            version: "1".into(),
            digest: [7; 32],
        },
        schema: json!({"type":"integer"}),
    });
    let definition = TaskDefinition::<Value, Value>::resumable(
        machine.clone(),
        json!({"type":"integer"}),
        json!({"type":"object"}),
    )?;
    let definition = match work {
        Some(work) => definition.publish(work)?,
        None => definition,
    };
    let identity = definition.identity().clone();
    let machine_identity = machine.identity.clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let (runtime_scope, _scope_custody) = match work {
        Some(work) => {
            let (scope, custody) = RuntimeScope::new_with_native_work(
                Capabilities::copy(scope.capabilities().iter(), work)?,
                Limits::default(),
                work,
            )?;
            (scope, Some(custody))
        }
        None => (
            RuntimeScope::new(scope.capabilities().clone(), Limits::default())?,
            None,
        ),
    };
    let io = work
        .map(acyclic_harness::filesystem::PreparationIo::from_native_work)
        .transpose()?;
    let runtime = Runtime::open(
        stream,
        files,
        &volume,
        None,
        &issuer.verifier(&mut None)?,
        scope,
        &runtime_scope,
        &tasks,
        &machines,
        &Default::default(),
        SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        },
        1,
        1_000_000,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        None,
        &[],
        &Default::default(),
        io.as_ref(),
    )
    .await?;
    runtime
        .task_host()
        .admit(TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: OperationId::from_bytes([3; 16]),
            task: identity,
            machine: machine_identity,
            input: json!(0),
            input_schema: json!({"type":"integer"}),
            output_schema: json!({"type":"object"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
    Ok(runtime)
}
