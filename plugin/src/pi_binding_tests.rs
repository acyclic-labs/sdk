use super::*;

fn binding(session: &str, identity: u8) -> PiTaskBinding {
    let provider = ProviderRef::new("acyclic", "filesystem", "1").unwrap();
    let agent_id = AgentId::from_bytes([identity; 16]);
    PiTaskBinding {
        native_session_id: session.to_owned(),
        task_id: TaskId::from_bytes([identity; 16]),
        agent_id,
        project_volumes: BTreeMap::new(),
        results: None,
        scratch: PiScratchBinding {
            volume: VolumeRef::new(
                provider.clone(),
                "scratch",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent_id),
            )
            .unwrap(),
            allocation: PiScratchAllocation::Created(PiScratchCreation {
                workspace_id: [identity; 16],
                generation: GenerationRef::new(provider, [3; 32], None).unwrap(),
            }),
        },
    }
}

#[test]
fn roles_providers_and_original_creation_are_checked() {
    let original = binding("native-session", 1);
    original
        .validate(Some("native-session"), vec![], false)
        .unwrap();
    assert!(
        original
            .validate(Some("other-session"), vec![], false)
            .is_err()
    );
    assert!(
        original
            .validate(None, vec!["missing-root"], false)
            .is_err()
    );
    let mut changed = original.clone();
    changed.scratch.volume = VolumeRef::new(
        original.scratch.volume.provider().clone(),
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
    )
    .unwrap();
    assert!(changed.validate(None, vec![], false).is_err());
    let mut changed = original.clone();
    let PiScratchAllocation::Created(creation) = &mut changed.scratch.allocation else {
        panic!("fixture must have a created scratch volume");
    };
    creation.generation = GenerationRef::new(
        ProviderRef::new("other", "filesystem", "1").unwrap(),
        [3; 32],
        None,
    )
    .unwrap();
    assert!(changed.validate(None, vec![], false).is_err());
    let mut changed = original;
    let PiScratchAllocation::Created(creation) = &mut changed.scratch.allocation else {
        panic!("fixture must have a created scratch volume");
    };
    creation.workspace_id = [0; 16];
    assert!(changed.validate(None, vec![], false).is_err());
}

#[test]
fn scratch_allocation_requires_an_explicit_current_variant() {
    let original = binding("native-session", 1);
    let current = serde_json::to_value(&original).unwrap();
    let mut preparing = original;
    preparing.scratch.allocation = PiScratchAllocation::Preparing;
    let prepared = serde_json::to_value(&preparing).unwrap();
    let decoded: PiTaskBinding = serde_json::from_value(prepared).unwrap();
    decoded.validate(None, vec![], true).unwrap();
    assert!(decoded.validate(None, vec![], false).is_err());
    for replacement in [
        serde_json::Value::Null,
        json!({}),
        json!({"state":"unknown"}),
        json!({"state":"created"}),
    ] {
        let mut invalid = current.clone();
        *invalid.pointer_mut("/scratch/allocation").unwrap() = replacement;
        assert!(serde_json::from_value::<PiTaskBinding>(invalid).is_err());
    }
    let mut omitted = current;
    omitted
        .get_mut("scratch")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove("allocation");
    assert!(serde_json::from_value::<PiTaskBinding>(omitted).is_err());
}

#[test]
fn project_selection_rejects_aliases_and_non_project_roles() {
    let mut selected = binding("native-session", 1);
    let project = VolumeRef::new(
        selected.scratch.volume.provider().clone(),
        "project",
        VolumeClass::Project,
        VolumeOwner::Project("project-owner".to_owned()),
    )
    .unwrap();
    selected
        .project_volumes
        .insert("a".to_owned(), project.clone());
    selected.validate(None, vec!["a"], false).unwrap();
    selected.project_volumes.insert("b".to_owned(), project);
    assert!(selected.validate(None, vec!["a", "b"], false).is_err());
    selected.project_volumes.remove("b");
    selected
        .project_volumes
        .insert("a".to_owned(), selected.scratch.volume.clone());
    assert!(selected.validate(None, vec!["a"], false).is_err());
}

#[test]
fn retained_bindings_reject_mixed_hosts_and_duplicate_task_identities() {
    let root = binding("root-session", 1);
    let mut state = AdapterState {
        task_binding: PluginTaskBinding::Pi(Box::new(root.clone())),
        root_session_id: "root-session".to_owned(),
        root_agent_id: "root:root-session".to_owned(),
        ..AdapterState::default()
    };
    validate_pi_bindings(&state).unwrap();
    let route = Route {
        task_binding: PluginTaskBinding::HostManaged,
        agent_id: "child-session".to_owned(),
        turn_id: "turn".to_owned(),
        context_id: [2; 16],
        root_id: [2; 16],
        parent_agent_id: state.root_agent_id.clone(),
        roots: BTreeMap::new(),
        mount_path: PathBuf::new(),
        lifecycle: RouteLifecycle::Active,
    };
    state.routes.insert("child-session".to_owned(), route);
    assert!(validate_pi_bindings(&state).is_err());
    let mut child = binding("child-session", 2);
    state.routes.get_mut("child-session").unwrap().task_binding =
        PluginTaskBinding::Pi(Box::new(child.clone()));
    validate_pi_bindings(&state).unwrap();
    child.task_id = root.task_id;
    state.routes.get_mut("child-session").unwrap().task_binding =
        PluginTaskBinding::Pi(Box::new(child));
    assert!(validate_pi_bindings(&state).is_err());
    state.task_binding = PluginTaskBinding::HostManaged;
    assert!(validate_pi_bindings(&state).is_err());
}

#[test]
fn native_retained_binding_validation_uses_original_work_and_retires_keys() {
    let state = AdapterState {
        task_binding: PluginTaskBinding::Pi(Box::new(binding("root-session", 1))),
        root_session_id: "root-session".to_owned(),
        root_agent_id: "root:root-session".to_owned(),
        ..AdapterState::default()
    };
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let live = work.live_allocation_bytes();
    validate_pi_bindings_with_native_work(&state, &work).unwrap();
    assert_eq!(work.live_allocation_bytes(), live);
    let mut substituted = state.clone();
    let PluginTaskBinding::Pi(root) = &mut substituted.task_binding else { unreachable!() };
    root.task_id = TaskId::from_bytes([0; 16]);
    assert!(matches!(validate_pi_bindings_with_native_work(&substituted, &work),
        Err(acyclic_harness::Error::NativeBoundary { .. })));
    assert_eq!(work.live_allocation_bytes(), live);
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(validate_pi_bindings_with_native_work(&state, &work),
        Err(acyclic_harness::Error::NativeWork { .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), live);
}

#[test]
fn native_binding_validation_denies_zero_visit_budget_before_key_retention() {
    let state = AdapterState::default();
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget {
        items_examined: 0, ..crate::pi_root_provider::allocation_budget()
    }, &cancellation).unwrap();
    let live = work.live_allocation_bytes();
    assert!(matches!(validate_pi_bindings_with_native_work(&state, &work),
        Err(acyclic_harness::Error::NativeWork { .. })));
    assert_eq!(work.live_allocation_bytes(), live);
    assert_eq!(work.receipt().items_examined, 0);
}
