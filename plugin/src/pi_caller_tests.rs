use super::*;
use crate::pi_fork::tests::work;
use acyclic_harness::{
    Capabilities, TaskId,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    runtime::TaskRunLimits,
    scheduler::SessionLimits,
};

fn fixture() -> (pi_owner::PiOwnerConfig, AdapterState) {
    let mut binding = crate::pi_binding::tests::binding("native-root", 71);
    binding.task_id = TaskId::from_bytes([72; 16]);
    binding.scratch.allocation = PiScratchAllocation::Preparing;
    let journal = VolumeRef::new(
        binding.scratch.volume.provider().clone(),
        "journal".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(binding.agent_id),
        &mut None,
    )
    .unwrap();
    let owner = pi_owner::PiOwnerConfig::new(
        &pi_owner::PiOwnerPolicy {
            root_task: binding.task_id,
            agent: binding.agent_id,
            journal,
            grants: Capabilities::new(["operation:declare", "operation:observe"]),
            limits: Limits::default(),
            run_limits: TaskRunLimits::default(),
            session_limits: SessionLimits::default(),
            concurrency: 1,
            maximum_payload_bytes: 4096,
        },
        None,
        &work(),
    )
    .unwrap();
    let state = AdapterState {
        root_session_id: binding.native_session_id.clone(),
        root_agent_id: binding.native_session_id.clone(),
        task_binding: PluginTaskBinding::Pi(Box::new(binding)),
        ..AdapterState::default()
    };
    (owner, state)
}

#[test]
fn native_caller_preserves_claims_and_environment_and_rejects_substitution() {
    let (owner, mut state) = fixture();
    state.root_session_id = "native-root-\"\\\n-🙂".to_owned();
    let PluginTaskBinding::Pi(binding) = &mut state.task_binding else {
        unreachable!()
    };
    binding.native_session_id = state.root_session_id.clone();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!()
    };
    let work = work();
    let caller = issue(&owner, &state.root_session_id, binding, &work).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(caller.0.id()).unwrap(),
        json!({
            "version":1,"root_session":state.root_session_id,"native_session":binding.native_session_id,"task":binding.task_id
        })
    );
    assert_eq!(routing_session(&caller.0).unwrap(), state.root_session_id);
    assert_eq!(
        caller.0.capabilities(),
        &Capabilities::new([CALLER_CAPABILITY])
    );
    assert_eq!(
        verify(&owner, &state, &caller.0, &binding.native_session_id, &work)
            .unwrap()
            .task_id,
        binding.task_id
    );
    let environment = issued_environment(&owner, &state.root_session_id, binding, &work).unwrap();
    assert_eq!(environment.0.len(), 1);
    let decoded: Scope =
        serde_json::from_str(environment.0.get(CALLER_ENVIRONMENT).unwrap()).unwrap();
    assert_eq!(decoded, caller.0);
    drop(decoded);
    drop(environment);
    let expanded = owner
        .authority()
        .unwrap()
        .issuer
        .issue(
            &caller.0.id(),
            &Capabilities::new([CALLER_CAPABILITY, "operation:declare"]),
            Some(binding.agent_id),
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(verify(&owner, &state, &expanded, &binding.native_session_id, &work).is_err());
    let mut wire = serde_json::to_value(&caller.0).unwrap();
    let mut claims: serde_json::Value = serde_json::from_str(caller.0.id()).unwrap();
    claims["root_session"] = json!("another-root");
    wire["id"] = json!(serde_json::to_string(&claims).unwrap());
    let substituted: Scope = serde_json::from_value(wire).unwrap();
    assert!(
        verify(
            &owner,
            &state,
            &substituted,
            &binding.native_session_id,
            &work
        )
        .is_err()
    );
    let (foreign, _) = fixture();
    assert!(
        verify(
            &foreign,
            &state,
            &caller.0,
            &binding.native_session_id,
            &work
        )
        .is_err()
    );
    assert!(verify(&owner, &state, &caller.0, "another-session", &work).is_err());
    let mut replaced = state.clone();
    let PluginTaskBinding::Pi(replaced_binding) = &mut replaced.task_binding else {
        unreachable!()
    };
    replaced_binding.task_id = TaskId::from_bytes([73; 16]);
    assert!(
        verify(
            &owner,
            &replaced,
            &caller.0,
            &binding.native_session_id,
            &work
        )
        .is_err()
    );
    drop(caller);
}

#[test]
fn native_caller_admission_denies_cancellation_and_exhausted_quota() {
    let (owner, state) = fixture();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!()
    };
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        pi_root_provider::allocation_budget(),
        &cancellation,
    )
    .unwrap();
    let caller = issue(&owner, &state.root_session_id, binding, &work).unwrap();
    cancellation.cancel();
    assert!(matches!(
        issue(&owner, &state.root_session_id, binding, &work),
        Err(acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        verify(&owner, &state, &caller.0, &binding.native_session_id, &work),
        Err(acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                + 1,
            ..pi_root_provider::allocation_budget()
        },
        &acyclic_fs::CancellationToken::new(),
    )
    .unwrap();
    assert!(matches!(
        issued_environment(&owner, &state.root_session_id, binding, &work),
        Err(acyclic_harness::Error::NativeWork { .. })
    ));
    assert!(matches!(
        issue(&owner, &state.root_session_id, binding, &work),
        Err(acyclic_harness::Error::NativeWork { .. })
    ));
}
