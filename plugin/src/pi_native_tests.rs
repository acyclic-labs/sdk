use super::*;
use acyclic_harness::{
    AgentId, TaskId,
    conversation::{VolumeClass, VolumeOwner},
    filesystem::{NativeViewManifest, NativeVolumeMapping, NativeVolumeNamespace},
    resources::{GenerationRef, ProviderRef},
};

fn root_path() -> PathBuf { std::env::temp_dir().join("acyclic-pi-native-test") }

fn work() -> NativeMaterializationWork {
    NativeMaterializationWork::new(super::super::native_root_source::budget(), &CancellationToken::new()).unwrap()
}

fn source_and_session() -> (PiNativeSourceCut, Vec<u8>) {
    let entry = json!({"type":"message","id":"first","parentId":null,"timestamp":"2026-10-09T00:00:00.000Z",
        "message":{"role":"user","content":"original instruction","timestamp":1}});
    let entries_json = serde_json::to_string(&vec![&entry]).unwrap();
    let source = PiNativeSourceCut { mode: PiNativeSourceMode::CallerTool, parent_session_id: "parent".into(), parent_session_file: None,
        parent_cwd: root_path().to_str().unwrap().into(), source_leaf_id: Some("spawn".into()),
        owner_entry_id: Some("spawn".into()), parent_leaf_id: Some("first".into()), captured_at: "2026-10-09T00:00:01.000Z".into(),
        sha256: hex::encode(Sha256::digest(entries_json.as_bytes())), entries_json };
    let header = json!({"type":"session","version":3,"id":"child","timestamp":source.captured_at,"cwd":root_path()});
    let bytes = format!("{header}\n{entry}\n").into_bytes();
    (source, bytes)
}

fn binding_and_manifest() -> (PiTaskBinding, NativeViewManifest) {
    let provider = ProviderRef::new("pi-native-test".into(), "filesystem".into(), "1".into(), &mut None).unwrap();
    let agent = AgentId::from_bytes([31; 16]);
    let scratch = VolumeRef::new(provider.clone(), "scratch", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).unwrap();
    let generation = GenerationRef::new(provider.clone(), [32; 32].into(), None).unwrap();
    let binding = PiTaskBinding { native_session_id: "child".into(), task_id: TaskId::from_bytes([33; 16]), agent_id: agent,
        project_volumes: BTreeMap::new(),
        scratch: super::super::PiScratchBinding { volume: scratch.clone(), allocation: PiScratchAllocation::Created(super::super::PiScratchCreation {
            workspace_id: [34; 16], generation: generation.clone(),
        }) },
        results: Some(super::super::PiScratchBinding { volume: VolumeRef::new(provider, "results", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).unwrap(),
            allocation: PiScratchAllocation::Created(super::super::PiScratchCreation { workspace_id: [38; 16], generation: generation.clone() }) }),
    };
    let manifest = NativeViewManifest { options: NativeViewOptions { root: root_path(), maximum_volumes: 2,
        work_per_volume: super::super::native_root_source::budget() }, volumes: vec![
        NativeVolumeMapping { volume: scratch, generation, path: root_path().join("scratch"), writable: true,
            authority_revision: [36; 32], namespace: NativeVolumeNamespace::MaterializedDirectory },
    ] };
    (binding, manifest)
}

fn invocation() -> PiNativeInvocation {
    let capture = capture_limits(acyclic_harness::conversation::Limits { file_bytes: 524_288, ..Default::default() }, 524_288).unwrap();
    PiNativeInvocation { version: PI_NATIVE_VERSION.into(), executable: root_path().join("node"),
        executable_prefix: vec![root_path().join("pi-cli.js").to_str().unwrap().into()], extension: root_path().join("acyclic.ts"),
        provider_extension: Some(root_path().join("provider.ts")), session_file: root_path().join("scratch/child.jsonl"),
        cwd: root_path().join("scratch"), provider: "pi-native-test".into(), model: "probe".into(), prompt: "child instruction".into(),
        builtin_tools: Vec::new(),
        environment: BTreeMap::from([("HOME".into(), root_path().to_str().unwrap().into()), ("ACYCLIC_PI_CALLER".into(), "protected-child-route".into())]),
        timeout_ms: capture.timeout_ms, control_timeout_ms: capture.control_timeout_ms, cancellation_poll_ms: capture.cancellation_poll_ms,
        maximum_output_bytes: capture.maximum_output_bytes, maximum_result_bytes: capture.maximum_result_bytes }
}

#[test]
fn original_native_capture_policy_is_finite_root_bounded_and_sdk_admissible() {
    let work = work();
    let limits = acyclic_harness::conversation::Limits { file_bytes: 65_536, ..Default::default() };
    let capture = capture_limits(limits, 32_768).unwrap();
    assert_eq!(capture.maximum_result_bytes, 32_768);
    let mut input = invocation();
    input.timeout_ms = capture.timeout_ms;
    input.control_timeout_ms = capture.control_timeout_ms;
    input.cancellation_poll_ms = capture.cancellation_poll_ms;
    input.maximum_output_bytes = capture.maximum_output_bytes;
    input.maximum_result_bytes = capture.maximum_result_bytes;
    let (binding, manifest) = binding_and_manifest();
    assert!(input.request_with_work(&binding, &manifest, &work).unwrap().0.has_valid_capture_allowances());
    assert!(capture_limits(limits, 0).is_err());
    assert!(capture_limits(acyclic_harness::conversation::Limits { file_bytes: 0, ..limits }, 32_768).is_err());
}

#[test]
fn source_bound_session_preserves_caller_modes_and_rejects_forged_cuts_headers_or_extra_lines() {
    let work = work();
    let (source, bytes) = source_and_session();
    source.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).unwrap();
    assert!(source.validate_session("parent", &root_path(), &bytes, 16, 65_536, &work).is_err());
    assert!(source.validate_session("child", &root_path(), &bytes, 0, 65_536, &work).is_err());
    assert!(source.validate_session("child", &root_path(), &bytes, 16, 1, &work).is_err());
    let text = String::from_utf8(bytes.clone()).unwrap();
    for changed in [text.replace("\"version\":3", "\"version\":2"), text.replace("\"child\"", "\"foreign\""),
        text.replace("original instruction", "forged instruction"), format!("{text}{{\"type\":\"session\"}}\n"),
        format!("{text}not-json\n"), text.trim_end().to_owned()] {
        assert!(source.validate_session("child", &root_path(), changed.as_bytes(), 16, 65_536, &work).is_err(), "accepted {changed}");
    }
    let mut forged = source.clone();
    forged.parent_leaf_id = Some("another-leaf".into());
    assert!(forged.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).is_err());
    forged = source.clone(); forged.entries_json = "[]".into();
    assert!(forged.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).is_err());
    {
        assert!(serde_json::from_value::<PiNativeSourceMode>(json!("selected_branch")).is_err());
        let mut source = source.clone();
        source.source_leaf_id = Some("actual-sibling-tool-result".into());
        source.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).unwrap();
        source.mode = PiNativeSourceMode::Scratch;
        source.owner_entry_id = None;
        source.source_leaf_id = None;
        source.parent_leaf_id = None;
        assert!(source.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).is_err());
        source.entries_json = "[]".into();
        source.sha256 = hex::encode(Sha256::digest(source.entries_json.as_bytes()));
        let header = format!("{}\n", json!({"type":"session","version":3,"id":"child","timestamp":source.captured_at,"cwd":root_path()}));
        source.validate_session("child", &root_path(), header.as_bytes(), 16, 65_536, &work).unwrap();
        source.owner_entry_id = Some("fabricated-tool-owner".into());
        assert!(source.validate_session("child", &root_path(), header.as_bytes(), 16, 65_536, &work).is_err());
    }
    work.cancellation().cancel();
    assert!(matches!(source.validate_session("child", &root_path(), &bytes, 16, 65_536, &work),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
}

#[test]
fn native_invocation_is_exact_absolute_and_discovery_free() {
    let work = work();
    let (binding, manifest) = binding_and_manifest();
    let input = invocation();
    let owned_request = input.request_with_work(&binding, &manifest, &work).unwrap();
    let request = &owned_request.0;
    assert_eq!(request.executable, input.executable);
    assert_eq!(request.environment, input.environment);
    assert_eq!(request.cwd, input.cwd);
    for flag in ["--print", "--no-extensions", "--no-mcp", "--no-builtin-tools"] { assert!(request.argv.iter().any(|arg| arg == flag)); }
    assert_eq!(request.argv.iter().filter(|arg| *arg == "--session").count(), 1);
    assert_eq!(request.standard_input.as_deref(), Some(input.prompt.as_bytes()));
    assert!(!request.argv.contains(&input.prompt));
    assert_eq!(request.view, manifest);
    let digest = request.approval_digest_with_native_work(binding.task_id, OperationId::from_bytes([41; 16]), &work).unwrap();
    let mut changed = (*request).clone(); changed.environment.insert("ACYCLIC_PI_CALLER".into(), "foreign-route".into());
    assert_ne!(digest, changed.approval_digest_with_native_work(binding.task_id, OperationId::from_bytes([41; 16]), &work).unwrap());
    changed = (*request).clone(); changed.argv.push("--resume".into());
    assert_ne!(digest, changed.approval_digest_with_native_work(binding.task_id, OperationId::from_bytes([41; 16]), &work).unwrap());
    for prompt in ["--model attacker --session original-root", "@source-file-must-not-be-opened", "literal native prompt\nwith another line"] {
        let mut literal = input.clone();
        literal.prompt = prompt.into();
        let request = literal.request_with_work(&binding, &manifest, &work).unwrap();
        assert_eq!(request.0.standard_input.as_deref(), Some(prompt.as_bytes()));
        assert_eq!(request.0.argv.iter().filter(|arg| *arg == "--model").count(), 1);
        assert_eq!(request.0.argv.iter().filter(|arg| *arg == "--session").count(), 1);
        assert!(!request.0.argv.contains(&literal.prompt));
    }
    work.cancellation().cancel();
    assert!(matches!(input.request_with_work(&binding, &manifest, &work),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
}

#[test]
fn changed_session_grants_paths_models_and_executable_never_rebind() {
    let work = work();
    let (binding, manifest) = binding_and_manifest();
    let input = invocation();
    for mutation in 0..9 {
        let mut changed = input.clone();
        match mutation {
            0 => changed.version = "1.0.0".into(),
            1 => changed.executable = PathBuf::from("pi"),
            2 => changed.executable_prefix = vec!["pi-cli.js".into()],
            3 => changed.session_file = root_path().join("project/child.jsonl"),
            4 => changed.cwd = root_path().join("../foreign"),
            5 => changed.extension = PathBuf::from("index.ts"),
            6 => changed.environment.clear(),
            7 => { changed.environment.insert("BAD=KEY".into(), "value".into()); },
            _ => changed.timeout_ms = 0,
        }
        assert!(changed.request_with_work(&binding, &manifest, &work).is_err());
    }
    let mut changed = binding.clone(); changed.scratch.allocation = PiScratchAllocation::Preparing;
    assert!(input.request_with_work(&changed, &manifest, &work).is_err());
    let mut missing = manifest.clone(); missing.volumes.remove(0);
    assert!(input.request_with_work(&binding, &missing, &work).is_err());
    let mut foreign = manifest.clone();
    foreign.volumes[0].volume = VolumeRef::new(binding.scratch.volume.provider().clone(), "foreign", VolumeClass::AgentPrivate, VolumeOwner::Agent(binding.agent_id)).unwrap();
    assert!(input.request_with_work(&binding, &foreign, &work).is_err());
    let mut binding = binding;
    let mut manifest = manifest;
    let project = VolumeRef::new(binding.scratch.volume.provider().clone(), "project", VolumeClass::Project, VolumeOwner::Project("project".into())).unwrap();
    binding.project_volumes.insert("project".into(), project.clone());
    manifest.volumes.push(NativeVolumeMapping { volume: project, generation: manifest.volumes[0].generation.clone(),
        path: root_path().join("project"), writable: true, authority_revision: [71; 32], namespace: NativeVolumeNamespace::MaterializedDirectory });
    assert!(invocation().request_with_work(&binding, &manifest, &work).is_err());
}

#[test]
fn original_host_settings_strip_recursive_credentials_and_cannot_be_overridden_by_a_child() {
    let work = work();
    let input = invocation();
    let mut environment = input.environment.clone();
    environment.insert("acyclic_pi_token".into(), "must-not-inherit".into());
    environment.insert("ACYCLIC_PI_PROVIDER_EXTENSION".into(), input.provider_extension.as_ref().unwrap().to_str().unwrap().into());
    let raw = PiNativeHostRuntime { version: input.version.clone(), executable: input.executable.clone(),
        executable_prefix: input.executable_prefix.clone(), extension: input.extension.clone(),
        provider_extension: input.provider_extension.clone(), environment };
    assert!(raw.validate(&work).is_err());
    assert!(matches!(raw.validate(&work),
        Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, .. })));
    let host = raw.sanitize(&work).unwrap();
    assert!(host.environment.contains_key("ACYCLIC_PI_PROVIDER_EXTENSION"));
    assert!(!host.environment.keys().any(|key| is_caller_environment(key)));
    let baseline = work.live_allocation_bytes();
    let copied = host.clone_with_native_work(&work).unwrap();
    assert!(copied.0 == host);
    assert!(copied.1.work().shares_ledger(&work));
    drop(copied);
    assert_eq!(work.live_allocation_bytes(), baseline);
    let limits = PiNativeCaptureLimits { timeout_ms: input.timeout_ms, control_timeout_ms: input.control_timeout_ms,
        cancellation_poll_ms: input.cancellation_poll_ms, maximum_output_bytes: input.maximum_output_bytes, maximum_result_bytes: input.maximum_result_bytes };
    let (source, bytes) = source_and_session();
    let seed = host.intent_invocation(&input.session_file, &root_path(), &input.provider, &input.model, &input.prompt, limits, &work).unwrap();
    let intent = PiNativeLaunchIntent { agent_alias: "child".into(), child_session_id: "child".into(), source,
        invocation: seed.0, session_path: "child.jsonl".into(), session_bytes: String::from_utf8(bytes).unwrap(),
        maximum_entries: 16, maximum_bytes: 65_536 };
    let child = host.bind_invocation(&intent,
        &BTreeMap::from([("ACYCLIC_PI_CALLER".into(), "actual-created-child-caller".into())]), &work).unwrap();
    assert_eq!(child.0.executable, host.executable);
    assert_eq!(child.0.provider_extension, host.provider_extension);
    assert_eq!(child.0.environment.get("ACYCLIC_PI_CALLER").map(String::as_str), Some("actual-created-child-caller"));
    assert_eq!(child.0.builtin_tools, ["read", "write", "edit"]);
    assert!(child.1.work().shares_ledger(&work));
    assert!(host.bind_invocation(&intent,
        &BTreeMap::from([("HOME".into(), "forged-host-setting".into())]), &work).is_err());
    assert!(host.bind_invocation(&intent, &BTreeMap::new(), &work).is_err());
    assert!(!intent.invocation.environment.contains_key("ACYCLIC_PI_CALLER"));
    let caller = BTreeMap::from([("ACYCLIC_PI_CALLER".into(), "actual-original-child-current-lease-proof".into())]);
    let mut changed = intent.clone();
    changed.invocation.executable = root_path().join("foreign-node");
    assert!(host.bind_invocation(&changed, &caller, &work).is_err());
    changed = intent.clone();
    changed.invocation.environment.insert("HOME".into(), "foreign-host".into());
    assert!(host.bind_invocation(&changed, &caller, &work).is_err());
    changed = intent.clone();
    changed.invocation.environment.extend(caller.clone());
    assert!(host.bind_invocation(&changed, &caller, &work).is_err(), "immutable seed must never retain a stale caller lease proof");
    changed = intent.clone();
    changed.invocation.builtin_tools.push("bash".into());
    assert!(host.bind_invocation(&changed, &caller, &work).is_err());
    changed = intent.clone();
    changed.child_session_id = changed.source.parent_session_id.clone();
    assert!(host.bind_invocation(&changed, &caller, &work).is_err(), "new child native ID remains separate from its actual source");
    for alias in ["".into(), "non-ASCII-子".into(), "control\nalias".into(), "slash/alias".into(), "backslash\\alias".into(), "space alias".into(), "a".repeat(129)] {
        changed = intent.clone();
        changed.agent_alias = alias;
        assert!(changed.validate(&work).is_err());
    }
    changed = intent.clone();
    changed.agent_alias = "a".repeat(128);
    changed.validate(&work).unwrap();
    assert_eq!(changed.child_session_id, intent.child_session_id, "alias must not replace original native identity");
    let mut missing_alias = serde_json::to_value(&intent).unwrap();
    missing_alias.as_object_mut().unwrap().remove("agent_alias");
    assert!(serde_json::from_value::<PiNativeLaunchIntent>(missing_alias).is_err());
}

#[test]
fn actual_native_namespace_references_reject_wrong_original_task_root_and_descendant() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &cancellation).unwrap();
    let data = root_path().join("owned-data");
    let task = acyclic_harness::TaskId::from_bytes([101; 16]);
    let service = super::super::service::session_directory(&data, "original-root");
    let view = service.join("agents").join(blake3::hash(&task.into_bytes()).to_hex().as_str()).join("view");
    assert_eq!(native_service_root(&data, "original-root", task, &view, &work).unwrap(), service);
    assert!(native_service_root(&data, "different-root", task, &view, &work).is_err());
    assert!(native_service_root(&data, "original-root", acyclic_harness::TaskId::from_bytes([102; 16]), &view, &work).is_err());
    assert!(native_service_root(&data, "original-root", task, &view.join("scratch"), &work).is_err());
    assert!(native_service_root(&data, "original-root", task, &root_path().join("unowned/view"), &work).is_err());
    assert_eq!(work.receipt().backend_write_operations, 0, "planned references do not allocate or publish a native root");
}
