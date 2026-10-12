use super::*;
pub(crate) fn binding(session: &str, identity: u8) -> PiTaskBinding {
    let provider =
        ProviderRef::new("acyclic".into(), "filesystem".into(), "1".into(), &mut None).unwrap();
    let agent_id = AgentId::from_bytes([identity; 16]);
    let scratch = PiScratchBinding {
        volume: VolumeRef::new(
            provider.clone(),
            "scratch".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent_id),
            &mut None,
        )
        .unwrap(),
        allocation: PiScratchAllocation::Created(PiScratchCreation {
            workspace_id: [identity; 16],
            generation: GenerationRef::new(&(provider), &([3; 32]), None, None).unwrap().0,
        }),
    };
    PiTaskBinding {
        native_session_id: session.into(),
        task_id: TaskId::from_bytes([identity; 16]),
        agent_id,
        project_volumes: BTreeMap::new(),
        results: None,
        scratch,
    }
}
#[test]
fn allocations_and_selected_roles_are_explicit_and_bound() {
    let original = binding("native-session", 1);
    assert!(original.validate(Some("other-session"), [], false).is_err());
    assert!(original.validate(None, ["missing-root"], false).is_err());
    for mutation in 0..3 {
        let mut changed = original.clone();
        match mutation {
            0 => {
                changed.scratch.volume = VolumeRef::new(
                    original.scratch.volume.provider().clone(),
                    "scratch".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
                    &mut None,
                )
                .unwrap()
            }
            _ => {
                let PiScratchAllocation::Created(creation) = &mut changed.scratch.allocation else {
                    unreachable!()
                };
                if mutation == 1 {
                    creation.generation = GenerationRef::new(&(ProviderRef::new(
                            "other".into(),
                            "filesystem".into(),
                            "1".into(),
                            &mut None,
                        )
                        .unwrap()), &([3; 32]), None, None).map(|(value, _)| value)
                    .unwrap();
                } else {
                    creation.workspace_id = [0; 16];
                }
            }
        }
        assert!(changed.validate(None, [], false).is_err());
    }
    let current = serde_json::to_value(&original).unwrap();
    for allocation in [
        Value::Null,
        json!({}),
        json!({"state":"unknown"}),
        json!({"state":"created"}),
    ] {
        let mut invalid = current.clone();
        invalid["scratch"]["allocation"] = allocation;
        assert!(serde_json::from_value::<PiTaskBinding>(invalid).is_err());
    }
    let mut omitted = current;
    omitted["scratch"]
        .as_object_mut()
        .unwrap()
        .remove("allocation");
    assert!(serde_json::from_value::<PiTaskBinding>(omitted).is_err());
    let mut selected = original;
    selected.scratch.allocation = PiScratchAllocation::Preparing;
    let mut selected: PiTaskBinding =
        serde_json::from_value(serde_json::to_value(selected).unwrap()).unwrap();
    selected.validate(None, [], true).unwrap();
    assert!(selected.validate(None, [], false).is_err());
    selected.scratch.allocation = binding("native-session", 1).scratch.allocation;
    let project = VolumeRef::new(
        selected.scratch.volume.provider().clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project-owner".into()),
        &mut None,
    )
    .unwrap();
    selected.project_volumes.insert("a".into(), project.clone());
    selected.validate(None, ["a"], false).unwrap();
    selected.project_volumes.insert("b".into(), project);
    assert!(selected.validate(None, ["a", "b"], false).is_err());
    selected.project_volumes.remove("b");
    selected
        .project_volumes
        .insert("a".into(), selected.scratch.volume.clone());
    assert!(selected.validate(None, ["a"], false).is_err());
}
#[test]
fn retained_bindings_reject_mixed_hosts_and_reused_task_ids() {
    let root = binding("root-session", 1);
    let mut state = AdapterState {
        task_binding: PluginTaskBinding::Pi(Box::new(root.clone())),
        root_session_id: "root-session".into(),
        root_agent_id: "root:root-session".into(),
        ..AdapterState::default()
    };
    validate_pi_bindings(&state).unwrap();
    state.routes.insert(
        "child-session".into(),
        Route {
            task_binding: PluginTaskBinding::HostManaged,
            agent_id: "child-session".into(),
            turn_id: "turn".into(),
            context_id: [2; 16],
            root_id: [2; 16],
            parent_agent_id: state.root_agent_id.clone(),
            roots: BTreeMap::new(),
            mount_path: PathBuf::new(),
            lifecycle: RouteLifecycle::Active,
        },
    );
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
