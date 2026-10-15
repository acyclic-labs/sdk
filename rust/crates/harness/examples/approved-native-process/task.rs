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
    fn supports_native_work(&self) -> bool {
        true
    }
    fn identity_with_native_work<'a>(
        &'a self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        work.remaining()
            .map_err(|cause| acyclic_harness::Error::NativeWork {
                cause,
                custody: None,
            })?;
        Ok(&self.identity)
    }
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let custody = work.claim(
            std::mem::size_of::<Value>() as u64,
            1,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let Value::Number(number) = input else {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "example state must be an integer",
            });
        };
        Ok((Value::Number(number.clone()), custody))
    }
    fn transition_with_native_work(
        &self,
        state: &Value,
        _: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let (state, mut custody) = self.initialize_with_native_work(state, work)?;
        custody.grow(
            (std::mem::size_of::<MachineTransition>() - std::mem::size_of::<Value>()) as u64,
        )?;
        Ok((
            MachineTransition {
                state,
                commands: vec![],
                status: MachineStatus::Suspended,
            },
            custody,
        ))
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
    let runtime_scope = RuntimeScope::new(scope.capabilities().clone(), Limits::default())?;
    let io = work
        .map(acyclic_harness::filesystem::PreparationIo::from_native_work)
        .transpose()?;
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

#[cfg(test)]
pub(super) fn verify_native_state(work: &acyclic_fs::NativeMaterializationWork) {
    use proptest::prelude::*;
    let machine = Machine {
        identity: MachineIdentity {
            name: "example.native".into(),
            version: "1".into(),
            digest: [7; 32],
        },
        schema: json!({"type":"integer"}),
    };
    // Every integer state is preserved, with custody for exactly the returned value.
    let integer = prop_oneof![
        any::<i64>().prop_map(Value::from),
        any::<u64>().prop_map(Value::from),
    ];
    proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: 32,
        failure_persistence: None,
        ..Default::default()
    })
    .run(&(integer.clone(), integer), |(state, input)| {
        let (initialized, held) = machine.initialize_with_native_work(&state, work).unwrap();
        prop_assert_eq!(initialized, machine.initialize(&state).unwrap());
        prop_assert_eq!(held.bytes(), std::mem::size_of::<Value>() as u64);
        drop(held);
        let (transition, held) = machine
            .transition_with_native_work(&state, input.clone(), work)
            .unwrap();
        let ordinary = machine.transition(&state, &input).unwrap();
        prop_assert_eq!(transition.state, ordinary.state);
        prop_assert_eq!(transition.commands, ordinary.commands);
        prop_assert_eq!(transition.status, ordinary.status);
        prop_assert_eq!(
            held.bytes(),
            std::mem::size_of::<MachineTransition>() as u64
        );
        Ok(())
    })
    .unwrap();
}
