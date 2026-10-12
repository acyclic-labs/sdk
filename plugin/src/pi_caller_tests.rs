use super::*;
use acyclic_harness::{AgentId, Capabilities, TaskId,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef, runtime::TaskRunLimits, scheduler::SessionLimits};

fn fixture() -> (pi_owner::PiOwnerConfig, AdapterState) {
    let provider = ProviderRef::new("pi-caller-test", "filesystem", "1").unwrap();
    let agent = AgentId::from_bytes([71; 16]);
    let task = TaskId::from_bytes([72; 16]);
    let journal = VolumeRef::new(provider.clone(), "journal", VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent)).unwrap();
    let scratch = VolumeRef::new(provider, "scratch", VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent)).unwrap();
    let owner = pi_owner::PiOwnerConfig::new(&pi_owner::PiOwnerPolicy {
        root_task: task, agent, journal, grants: Capabilities::new(["operation:declare", "operation:observe"]),
        limits: Limits::default(), run_limits: TaskRunLimits::default(),
        session_limits: SessionLimits::default(), concurrency: 1, maximum_payload_bytes: 4096,
    }, None, &acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap()).unwrap();
    let state = AdapterState { root_session_id: "native-root".to_owned(),
        root_agent_id: "native-root".to_owned(), task_binding: PluginTaskBinding::Pi(Box::new(PiTaskBinding {
            native_session_id: "native-root".to_owned(), task_id: task, agent_id: agent,
            results: None,
            project_volumes: BTreeMap::new(), scratch: PiScratchBinding {
                volume: scratch, allocation: PiScratchAllocation::Preparing,
            },
        })), ..AdapterState::default() };
    (owner, state)
}

#[test]
fn native_caller_proof_is_original_owner_and_identity_only() {
    let (owner, state) = fixture();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else { unreachable!() };
    let caller = issue(&owner, &state.root_session_id, binding).unwrap();
    assert_eq!(verify(&owner, &state, &caller, "native-root").unwrap().task_id, binding.task_id);
    assert_eq!(routing_session(&caller).unwrap(), "native-root");
    assert_eq!(caller.capabilities(), &Capabilities::new([CALLER_CAPABILITY]));
    assert!(!caller.capabilities().contains("operation:declare"));
    assert!(verify(&owner, &state, &caller, "another-native-session").is_err());
    let (foreign, _) = fixture();
    assert!(verify(&foreign, &state, &caller, "native-root").is_err());
    let mut replaced = state.clone();
    let PluginTaskBinding::Pi(binding) = &mut replaced.task_binding else { unreachable!() };
    binding.task_id = TaskId::from_bytes([73; 16]);
    assert!(verify(&owner, &replaced, &caller, "native-root").is_err());
}

#[test]
fn extra_grants_and_scope_claim_substitution_are_rejected() {
    let (owner, state) = fixture();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else { unreachable!() };
    let caller = issue(&owner, &state.root_session_id, binding).unwrap();
    let expanded = owner.authority().unwrap().issuer.root_for_agent(binding.agent_id,
        caller.id(), Capabilities::new([CALLER_CAPABILITY, "operation:declare"]));
    assert!(verify(&owner, &state, &expanded, "native-root").is_err());
    let mut wire = serde_json::to_value(&caller).unwrap();
    let mut claims: serde_json::Value = serde_json::from_str(caller.id()).unwrap();
    claims["root_session"] = json!("another-root");
    wire["id"] = json!(serde_json::to_string(&claims).unwrap());
    let substituted: Scope = serde_json::from_value(wire).unwrap();
    assert!(verify(&owner, &state, &substituted, "native-root").is_err());
}

fn caller_work() -> acyclic_fs::NativeMaterializationWork {
    acyclic_fs::NativeMaterializationWork::new(
        pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).unwrap()
}

#[test]
fn native_caller_uses_original_claim_encoding_and_original_owner() {
    let (owner, mut state) = fixture();
    state.root_session_id = "native-root-\"\\\n-🙂".to_owned();
    let PluginTaskBinding::Pi(binding) = &mut state.task_binding else { unreachable!() };
    binding.native_session_id = state.root_session_id.clone();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else { unreachable!() };
    let ordinary = issue(&owner, &state.root_session_id, binding).unwrap();
    let work = caller_work();
    let caller = issue_with_work(&owner, &state.root_session_id, binding, &work).unwrap();
    assert_eq!(caller.0, ordinary);
    assert_eq!(verify_with_work(&owner, &state, &caller.0,
        &binding.native_session_id, &work).unwrap().task_id, binding.task_id);
    let (foreign, _) = fixture();
    assert!(verify_with_work(&foreign, &state, &caller.0,
        &binding.native_session_id, &work).is_err());
    assert!(verify_with_work(&owner, &state, &caller.0, "another-session", &work).is_err());
    let mut replaced = state.clone();
    let PluginTaskBinding::Pi(replaced_binding) = &mut replaced.task_binding else { unreachable!() };
    replaced_binding.task_id = TaskId::from_bytes([73; 16]);
    assert!(verify_with_work(&owner, &replaced, &caller.0,
        &binding.native_session_id, &work).is_err());
    drop(caller.0);
    drop(caller.1);
}

#[test]
fn native_caller_environment_retains_only_the_original_caller_proof() {
    let (owner, state) = fixture();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else { unreachable!() };
    let work = caller_work();
    let environment = issued_environment_with_work(&owner, &state.root_session_id, binding, &work).unwrap();
    assert_eq!(environment.0.len(), 1);
    let caller: Scope = serde_json::from_str(environment.0.get(CALLER_ENVIRONMENT).unwrap()).unwrap();
    assert_eq!(verify_with_work(&owner, &state, &caller,
        &binding.native_session_id, &work).unwrap().task_id, binding.task_id);
    assert_eq!(caller.capabilities(), &Capabilities::new([CALLER_CAPABILITY]));
    drop(caller);
    drop(environment.0);
    drop(environment.1);
}

#[test]
fn native_caller_cancellation_precedes_authority_copy_and_claim_encoding() {
    let (owner, state) = fixture();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else { unreachable!() };
    let caller = issue(&owner, &state.root_session_id, binding).unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        pi_root_provider::allocation_budget(), &cancellation,
    ).unwrap();
    cancellation.cancel();
    let before = work.receipt();
    assert!(matches!(issue_with_work(&owner, &state.root_session_id, binding, &work),
        Err(acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        })));
    assert!(matches!(verify_with_work(&owner, &state, &caller,
        &binding.native_session_id, &work),
        Err(acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        })));
    assert_eq!(work.receipt(), before);
}

#[test]
fn native_caller_cannot_reconstruct_an_allowance_after_original_quota_denial() {
    let (owner, state) = fixture();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else { unreachable!() };
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 1,
            ..pi_root_provider::allocation_budget()
        }, &acyclic_fs::CancellationToken::new(),
    ).unwrap();
    assert!(matches!(issued_environment_with_work(&owner, &state.root_session_id, binding, &work),
        Err(acyclic_harness::Error::NativeWork { .. })));
    assert!(matches!(issue_with_work(&owner, &state.root_session_id, binding, &work),
        Err(acyclic_harness::Error::NativeWork { .. })));
}
