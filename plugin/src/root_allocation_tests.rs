use super::*;
use acyclic_harness::{
    AgentId, TaskId,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};

fn preparing_state(path: PathBuf) -> AdapterState {
    let root_id = [61; 16];
    let key = root_key(WorkspaceRootId::from_bytes(root_id));
    let provider = ProviderRef::new("pi-root-intent-test", "filesystem", "1").unwrap();
    let agent = AgentId::from_bytes([62; 16]);
    AdapterState {
        version: ADAPTER_STATE_VERSION,
        active: true,
        root_session_id: "pi-root".to_owned(),
        root_agent_id: "pi-root".to_owned(),
        root_context_id: [63; 16],
        root_id,
        pending_root_registration: true,
        task_binding: PluginTaskBinding::Pi(Box::new(PiTaskBinding {
            native_session_id: "pi-root".to_owned(),
            task_id: TaskId::from_bytes([64; 16]),
            agent_id: agent,
            project_volumes: BTreeMap::from([(
                key.clone(),
                VolumeRef::new(
                    provider.clone(),
                    "project",
                    VolumeClass::Project,
                    VolumeOwner::Project("project".to_owned()),
                )
                .unwrap(),
            )]),
            results: None,
            scratch: PiScratchBinding {
                volume: VolumeRef::new(
                    provider,
                    "scratch",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(agent),
                )
                .unwrap(),
                allocation: PiScratchAllocation::Preparing,
            },
        })),
        roots: BTreeMap::from([(
            key,
            RootBinding {
                root_id,
                path,
                allocation: RootAllocation::Preparing,
            },
        )]),
        ..AdapterState::default()
    }
}

#[test]
fn root_allocation_requires_complete_explicit_shape() {
    let root = RootBinding {
        root_id: [1; 16],
        path: std::env::temp_dir().join("selected-root"),
        allocation: RootAllocation::Created(RootCreation {
            repository_workspace_id: [2; 16],
            source_identity: [3; 16],
            source_epoch: 0,
            native_root_identity: [4; 16],
        }),
    };
    let original = serde_json::to_value(&root).unwrap();
    for field in ["root_id", "path", "allocation"] {
        let mut changed = original.clone();
        changed.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<RootBinding>(changed).is_err(),
            "{field}"
        );
    }
    for field in [
        "repository_workspace_id",
        "source_identity",
        "source_epoch",
        "native_root_identity",
    ] {
        let mut changed = original.clone();
        changed
            .get_mut("allocation")
            .unwrap()
            .get_mut("creation")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            serde_json::from_value::<RootBinding>(changed).is_err(),
            "{field}"
        );
    }
    for allocation in [
        Value::Null,
        json!({}),
        json!({"state":"unknown"}),
        json!({"state":"created"}),
        json!({"state":"preparing","unexpected":true}),
        json!({"state":"preparing","creation":{"repository_workspace_id":[1]}}),
    ] {
        let mut changed = original.clone();
        *changed.get_mut("allocation").unwrap() = allocation;
        assert!(serde_json::from_value::<RootBinding>(changed).is_err());
    }
    let mut changed = original.clone();
    changed
        .get_mut("allocation")
        .unwrap()
        .get_mut("creation")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("unexpected".to_owned(), json!(true));
    assert!(serde_json::from_value::<RootBinding>(changed).is_err());
    let mut decoded: RootBinding = serde_json::from_value(original).unwrap();
    // A watcher observation changes neither allocation nor source identity.
    decoded.created_mut().unwrap().source_epoch = 9;
    assert_eq!(decoded.created().unwrap().source_identity, [3; 16]);
    assert_eq!(decoded.created().unwrap().source_epoch, 9);
}

#[test]
fn preparing_root_intent_survives_reopen_without_workspace_authority() {
    let directory = tempfile::tempdir().unwrap();
    let selected = directory.path().join("unopened-selected-root");
    let state = preparing_state(selected.clone());
    validate_state_bounds(&state).unwrap();
    save_state(directory.path(), &state, Survives::PowerLoss).unwrap();
    let mut reopened = load_saved_state(directory.path()).unwrap();
    assert_eq!(
        serde_json::to_value(&reopened).unwrap(),
        serde_json::to_value(&state).unwrap()
    );
    assert!(reopened.require_created_roots().is_err());
    let root = reopened.roots.values_mut().next().unwrap();
    assert!(root.created().is_err());
    assert!(root.created_mut().is_err());
    assert!(!selected.exists());

    let mut changed = state.clone();
    changed.pending_root_registration = false;
    assert!(validate_state_bounds(&changed).is_err());
    let mut changed = state.clone();
    changed.task_binding = PluginTaskBinding::HostManaged;
    assert!(validate_state_bounds(&changed).is_err());
    let mut changed = state.clone();
    changed
        .pending_root_adoptions
        .insert(changed.roots.keys().next().unwrap().clone());
    assert!(validate_state_bounds(&changed).is_err());
    let mut changed = state;
    changed.roots.values_mut().next().unwrap().allocation = RootAllocation::Created(RootCreation {
        repository_workspace_id: [0; 16],
        source_identity: [2; 16],
        source_epoch: 0,
        native_root_identity: [3; 16],
    });
    assert!(validate_state_bounds(&changed).is_err());
}

#[tokio::test]
async fn preparing_root_cannot_route_or_resolve_workspace() {
    let directory = tempfile::tempdir().unwrap();
    let mut control = ControlPlane::open(directory.path().join("service"))
        .await
        .unwrap();
    let selected = directory.path().join("unopened-selected-root");
    control.state = preparing_state(selected.clone());
    let root_id = WorkspaceRootId::from_bytes(control.state.root_id);
    assert!(
        control
            .route_root_from_cwd(&selected)
            .unwrap_err()
            .contains("preparing")
    );
    assert!(
        control
            .ensure_agent_idle(&control.state.root_agent_id)
            .unwrap_err()
            .contains("preparing")
    );
    assert!(
        control
            .parent_repository_id(
                &control.state.root_agent_id,
                root_id,
                acyclic_fs::WorkspaceId::from_bytes([65; 16]),
            )
            .unwrap_err()
            .contains("preparing")
    );
    let route = Route {
        task_binding: control.state.task_binding.clone(),
        agent_id: "child".to_owned(),
        turn_id: "turn".to_owned(),
        context_id: [66; 16],
        root_id: control.state.root_id,
        parent_agent_id: control.state.root_agent_id.clone(),
        roots: BTreeMap::new(),
        mount_path: directory.path().join("unopened-mount"),
        lifecycle: RouteLifecycle::Active,
    };
    let Err(error) = control.workspace_root(&route, root_id).await else {
        panic!("preparing root resolved a workspace");
    };
    assert!(error.contains("preparing"));
    let Err(error) = control.lazy_workspace_root(&route, root_id).await else {
        panic!("preparing root resolved a lazy workspace");
    };
    assert!(error.contains("preparing"));
    assert!(!selected.exists());
    assert!(
        control
            .restore_root()
            .await
            .unwrap_err()
            .contains("preparing")
    );
    assert!(
        control
            .refresh_native_root(root_id)
            .await
            .unwrap_err()
            .contains("preparing")
    );
    assert!(
        control
            .session_start(json!({"session_id":"pi-root","cwd":selected}))
            .await
            .unwrap_err()
            .contains("preparing")
    );
    fs::create_dir(&selected).unwrap();
    let retained = selected.join("retained.txt");
    fs::write(&retained, b"preserve concurrent user content").unwrap();
    assert!(
        control
            .apply_source_deletions(
                "pi-root",
                BTreeMap::from([(root_id, vec!["/retained.txt".to_owned()]),])
            )
            .await
            .unwrap_err()
            .contains("preparing")
    );
    assert_eq!(
        fs::read(&retained).unwrap(),
        b"preserve concurrent user content"
    );
}

#[tokio::test]
async fn both_service_catalogs_reject_preparing_root_routes() {
    let directory = tempfile::tempdir().unwrap();
    let selected = directory.path().join("selected-root");
    fs::create_dir(&selected).unwrap();
    let data = directory.path().join("service");
    let (_service_lock, configured, private_root, work) =
        crate::tests::configured_test_service_bootstrap(&data).await.unwrap();
    let mut service = crate::service::ServiceControl::open(data, configured, private_root, &work)
        .await.unwrap();
    let mut control = service.open_session("pi-root").await.unwrap();
    control.state = preparing_state(selected.clone());
    service.sessions.insert("pi-root".to_owned(), control);
    assert!(
        service
            .session_for_cwd(&selected)
            .unwrap_err()
            .contains("preparing")
    );
    let sessions = service
        .sessions
        .into_iter()
        .map(|(id, control)| (id, control.into()))
        .collect();
    let concurrent = ConcurrentServiceControl::from_parts(service.resources, sessions, &work).unwrap();
    assert!(concurrent.route_session(&selected).await.unwrap().is_none());
    assert!(
        ConcurrentControlRequestDispatcher::dispatch_request(
            &concurrent,
            ControlRequest {
                version: 1,
                command: ControlCommand::Agents,
                cwd: selected,
                argv: Vec::new(),
                name: String::new(),
                arguments: Value::Null,
            },
            &mut None,
        )
        .await
        .err().expect("preparing Root must not dispatch")
        .to_string().contains("outside every registered")
    );
    assert_eq!(concurrent.drain_sessions(false).await.unwrap(), SessionCloseState::Closed);
}
