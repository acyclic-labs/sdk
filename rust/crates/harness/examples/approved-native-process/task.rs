//! Task and machine admission for the approved process example.
use super::*;
use acyclic_harness::runtime::TaskSpawner as _;

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
}

pub(super) async fn open<P, A, O>(
    stream: std::sync::Arc<P>,
    files: Arc<Files<A, O>>,
    volume: VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
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
    let identity = definition.identity().clone();
    let machine_identity = machine.identity.clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let runtime_scope = RuntimeScope::new(scope.capabilities().clone(), Limits::default())?;
    let runtime = Runtime::open(
        stream,
        files,
        &volume,
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
        None,
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
