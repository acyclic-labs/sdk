use super::*;
use acyclic_harness::{TaskId, conversation::{VolumeClass, VolumeOwner}, filesystem::{NativeViewManifest, NativeVolumeMapping, NativeVolumeNamespace}};

fn root_path() -> PathBuf { std::env::temp_dir().join("acyclic-pi-native-test") }
fn work() -> NativeMaterializationWork {
    NativeMaterializationWork::new(super::super::native_root_source::budget(), &CancellationToken::new()).unwrap()
}

fn fixture() -> (PiNativeInvocation, PiTaskBinding, NativeViewManifest) {
    let mut binding = crate::pi_binding::tests::binding("child", 31);
    binding.task_id = TaskId::from_bytes([33; 16]);
    let generation = acyclic_harness::resources::GenerationRef::new(binding.scratch.volume.provider().clone(), [32; 32].into(), None).unwrap();
    binding.scratch.allocation = PiScratchAllocation::Created(crate::PiScratchCreation { workspace_id: [34; 16], generation: generation.clone() });
    binding.results = Some(crate::PiScratchBinding {
        volume: VolumeRef::new(binding.scratch.volume.provider().clone(), "results", VolumeClass::AgentPrivate, VolumeOwner::Agent(binding.agent_id)).unwrap(),
        allocation: PiScratchAllocation::Created(crate::PiScratchCreation { workspace_id: [38; 16], generation: generation.clone() }),
    });
    let manifest = NativeViewManifest { options: NativeViewOptions { root: root_path(), maximum_volumes: 2,
        work_per_volume: super::super::native_root_source::budget() }, volumes: vec![NativeVolumeMapping {
        volume: binding.scratch.volume.clone(), generation, path: root_path().join("scratch"), writable: true,
        authority_revision: [36; 32], namespace: NativeVolumeNamespace::MaterializedDirectory,
    }] };
    let limits = capture_limits(acyclic_harness::conversation::Limits { file_bytes: 524_288, ..Default::default() }, 524_288).unwrap();
    let invocation = PiNativeInvocation { version: PI_NATIVE_VERSION.into(), executable: root_path().join("node"),
        executable_prefix: vec![root_path().join("pi-cli.js").to_str().unwrap().into()], extension: root_path().join("acyclic.ts"),
        provider_extension: Some(root_path().join("provider.ts")), session_file: root_path().join("scratch/child.jsonl"), cwd: root_path().join("scratch"),
        provider: "pi-native-test".into(), model: "probe".into(), prompt: "child instruction".into(), builtin_tools: Vec::new(),
        environment: BTreeMap::from([("HOME".into(), root_path().to_str().unwrap().into()), ("ACYCLIC_PI_CALLER".into(), "protected-child-route".into())]),
        timeout_ms: limits.timeout_ms, control_timeout_ms: limits.control_timeout_ms, cancellation_poll_ms: limits.cancellation_poll_ms,
        maximum_output_bytes: limits.maximum_output_bytes, maximum_result_bytes: limits.maximum_result_bytes };
    (invocation, binding, manifest)
}

fn source_and_session() -> (PiNativeSourceCut, Vec<u8>) {
    let entry = json!({"type":"message","id":"first","parentId":null,"timestamp":"2026-10-09T00:00:00.000Z",
        "message":{"role":"user","content":"original instruction","timestamp":1}});
    let entries_json = serde_json::to_string(&[&entry]).unwrap();
    let source = PiNativeSourceCut { mode: PiNativeSourceMode::CallerTool, parent_session_id: "parent".into(), parent_session_file: None,
        parent_cwd: root_path().to_str().unwrap().into(), source_leaf_id: Some("spawn".into()), owner_entry_id: Some("spawn".into()),
        parent_leaf_id: Some("first".into()), captured_at: "2026-10-09T00:00:01.000Z".into(), sha256: hex::encode(Sha256::digest(entries_json.as_bytes())), entries_json };
    let header = json!({"type":"session","version":3,"id":"child","timestamp":source.captured_at,"cwd":root_path()});
    (source, format!("{header}\n{entry}\n").into_bytes())
}

#[test]
fn original_native_capture_policy_is_finite_root_bounded_and_sdk_admissible() {
    let limits = acyclic_harness::conversation::Limits { file_bytes: 65_536, ..Default::default() };
    let capture = capture_limits(limits, 32_768).unwrap(); assert_eq!(capture.maximum_result_bytes, 32_768);
    let (mut input, binding, manifest) = fixture();
    input.timeout_ms = capture.timeout_ms; input.control_timeout_ms = capture.control_timeout_ms;
    input.cancellation_poll_ms = capture.cancellation_poll_ms; input.maximum_output_bytes = capture.maximum_output_bytes;
    input.maximum_result_bytes = capture.maximum_result_bytes;
    assert!(input.request_with_work(&binding, &manifest, &work()).unwrap().0.has_valid_capture_allowances());
    assert!(capture_limits(limits, 0).is_err());
    assert!(capture_limits(acyclic_harness::conversation::Limits { file_bytes: 0, ..limits }, 32_768).is_err());
}

#[test]
fn source_bound_session_preserves_caller_modes_and_rejects_forged_cuts_headers_or_extra_lines() {
    let work = work(); let (source, bytes) = source_and_session();
    source.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).unwrap();
    assert!(source.validate_session("parent", &root_path(), &bytes, 16, 65_536, &work).is_err());
    assert!(source.validate_session("child", &root_path(), &bytes, 0, 65_536, &work).is_err());
    assert!(source.validate_session("child", &root_path(), &bytes, 16, 1, &work).is_err());
    let text = String::from_utf8(bytes.clone()).unwrap();
    for changed in [text.replace("\"version\":3", "\"version\":2"), text.replace("\"child\"", "\"foreign\""),
        text.replace("original instruction", "forged instruction"), format!("{text}{{\"type\":\"session\"}}\n"),
        format!("{text}not-json\n"), text.trim_end().to_owned()] {
        assert!(source.validate_session("child", &root_path(), changed.as_bytes(), 16, 65_536, &work).is_err());
    }
    let mut forged = source.clone(); forged.parent_leaf_id = Some("another-leaf".into());
    assert!(forged.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).is_err());
    forged = source.clone(); forged.entries_json = "[]".into();
    assert!(forged.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).is_err());
    assert!(serde_json::from_value::<PiNativeSourceMode>(json!("selected_branch")).is_err());
    let mut scratch = source.clone(); scratch.source_leaf_id = Some("actual-sibling-tool-result".into());
    scratch.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).unwrap();
    scratch.mode = PiNativeSourceMode::Scratch; scratch.owner_entry_id = None; scratch.source_leaf_id = None; scratch.parent_leaf_id = None;
    assert!(scratch.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).is_err());
    scratch.entries_json = "[]".into(); scratch.sha256 = hex::encode(Sha256::digest(scratch.entries_json.as_bytes()));
    let header = format!("{}\n", json!({"type":"session","version":3,"id":"child","timestamp":scratch.captured_at,"cwd":root_path()}));
    scratch.validate_session("child", &root_path(), header.as_bytes(), 16, 65_536, &work).unwrap();
    scratch.owner_entry_id = Some("fabricated-tool-owner".into());
    assert!(scratch.validate_session("child", &root_path(), header.as_bytes(), 16, 65_536, &work).is_err());
    work.cancellation().cancel(); assert!(matches!(source.validate_session("child", &root_path(), &bytes, 16, 65_536, &work),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
}

#[test]
fn native_invocation_is_exact_absolute_and_discovery_free() {
    let work = work(); let (input, binding, manifest) = fixture();
    let request = input.request_with_work(&binding, &manifest, &work).unwrap();
    assert_eq!(request.0.executable, input.executable); assert_eq!(request.0.environment, input.environment); assert_eq!(request.0.cwd, input.cwd);
    assert_eq!(request.0.view, manifest);
    for flag in ["--print", "--no-extensions", "--no-mcp", "--no-builtin-tools"] { assert!(request.0.argv.iter().any(|arg| arg == flag)); }
    for prompt in [input.prompt.as_str(), "--model attacker --session original-root", "@source-file-must-not-be-opened", "literal native prompt\nwith another line"] {
        let mut literal = input.clone(); literal.prompt = prompt.into();
        let request = literal.request_with_work(&binding, &manifest, &work).unwrap();
        assert_eq!(request.0.standard_input.as_deref(), Some(prompt.as_bytes())); assert!(!request.0.argv.contains(&literal.prompt));
        for flag in ["--model", "--session"] { assert_eq!(request.0.argv.iter().filter(|arg| *arg == flag).count(), 1); }
    }
    work.cancellation().cancel(); assert!(matches!(input.request_with_work(&binding, &manifest, &work),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
}

#[test]
fn changed_session_grants_paths_models_and_executable_never_rebind() {
    let work = work(); let (input, mut binding, mut manifest) = fixture();
    for mutation in 0..9 {
        let mut changed = input.clone();
        match mutation {
            0 => changed.version = "1.0.0".into(), 1 => changed.executable = PathBuf::from("pi"),
            2 => changed.executable_prefix = vec!["pi-cli.js".into()], 3 => changed.session_file = root_path().join("project/child.jsonl"),
            4 => changed.cwd = root_path().join("../foreign"), 5 => changed.extension = PathBuf::from("index.ts"),
            6 => changed.environment.clear(), 7 => { changed.environment.insert("BAD=KEY".into(), "value".into()); }, _ => changed.timeout_ms = 0,
        }
        assert!(changed.request_with_work(&binding, &manifest, &work).is_err());
    }
    let mut changed = binding.clone(); changed.scratch.allocation = PiScratchAllocation::Preparing;
    assert!(input.request_with_work(&changed, &manifest, &work).is_err());
    let mut missing = manifest.clone(); missing.volumes.clear(); assert!(input.request_with_work(&binding, &missing, &work).is_err());
    let mut foreign = manifest.clone();
    foreign.volumes[0].volume = VolumeRef::new(binding.scratch.volume.provider().clone(), "foreign", VolumeClass::AgentPrivate, VolumeOwner::Agent(binding.agent_id)).unwrap();
    assert!(input.request_with_work(&binding, &foreign, &work).is_err());
    let project = VolumeRef::new(binding.scratch.volume.provider().clone(), "project", VolumeClass::Project, VolumeOwner::Project("project".into())).unwrap();
    binding.project_volumes.insert("project".into(), project.clone());
    manifest.volumes.push(NativeVolumeMapping { volume: project, generation: manifest.volumes[0].generation.clone(), path: root_path().join("project"),
        writable: true, authority_revision: [71; 32], namespace: NativeVolumeNamespace::MaterializedDirectory });
    assert!(input.request_with_work(&binding, &manifest, &work).is_err());
}

#[test]
fn original_host_settings_strip_recursive_credentials_and_cannot_be_overridden_by_a_child() {
    let work = work(); let (input, _, _) = fixture();
    let mut environment = input.environment.clone(); environment.insert("acyclic_pi_token".into(), "must-not-inherit".into());
    environment.insert("ACYCLIC_PI_PROVIDER_EXTENSION".into(), input.provider_extension.as_ref().unwrap().to_str().unwrap().into());
    let raw = PiNativeHostRuntime { version: input.version.clone(), executable: input.executable.clone(), executable_prefix: input.executable_prefix.clone(),
        extension: input.extension.clone(), provider_extension: input.provider_extension.clone(), environment };
    assert!(matches!(raw.validate(&work), Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, .. })));
    let host = raw.sanitize(&work).unwrap();
    assert!(host.environment.contains_key("ACYCLIC_PI_PROVIDER_EXTENSION")); assert!(!host.environment.keys().any(|key| is_caller_environment(key)));
    let limits = PiNativeCaptureLimits { timeout_ms: input.timeout_ms, control_timeout_ms: input.control_timeout_ms, cancellation_poll_ms: input.cancellation_poll_ms,
        maximum_output_bytes: input.maximum_output_bytes, maximum_result_bytes: input.maximum_result_bytes };
    let (source, bytes) = source_and_session();
    let seed = host.intent_invocation(&input.session_file, &root_path(), &input.provider, &input.model, &input.prompt, limits, &work).unwrap();
    let intent = PiNativeLaunchIntent { agent_alias: "child".into(), child_session_id: "child".into(), source, invocation: seed.0,
        session_path: "child.jsonl".into(), session_bytes: String::from_utf8(bytes).unwrap(), maximum_entries: 16, maximum_bytes: 65_536 };
    let caller = BTreeMap::from([("ACYCLIC_PI_CALLER".into(), "actual-created-child-caller".into())]);
    let child = host.bind_invocation(&intent, &caller, &work).unwrap();
    assert_eq!(child.0.executable, host.executable); assert_eq!(child.0.provider_extension, host.provider_extension);
    assert_eq!(child.0.environment.get("ACYCLIC_PI_CALLER").map(String::as_str), Some("actual-created-child-caller")); assert_eq!(child.0.builtin_tools, ["read", "write", "edit"]);
    assert!(!intent.invocation.environment.contains_key("ACYCLIC_PI_CALLER"));
    assert!(host.bind_invocation(&intent, &BTreeMap::from([("HOME".into(), "forged-host-setting".into())]), &work).is_err());
    assert!(host.bind_invocation(&intent, &BTreeMap::new(), &work).is_err());
    for mutation in 0..5 {
        let mut changed = intent.clone();
        match mutation {
            0 => changed.invocation.executable = root_path().join("foreign-node"),
            1 => { changed.invocation.environment.insert("HOME".into(), "foreign-host".into()); },
            2 => changed.invocation.environment.extend(caller.clone()), 3 => changed.invocation.builtin_tools.push("bash".into()),
            _ => changed.child_session_id = changed.source.parent_session_id.clone(),
        }
        assert!(host.bind_invocation(&changed, &caller, &work).is_err());
    }
    for alias in ["".into(), "non-ASCII-子".into(), "control\nalias".into(), "slash/alias".into(), "backslash\\alias".into(), "space alias".into(), "a".repeat(129)] {
        let mut changed = intent.clone(); changed.agent_alias = alias; assert!(changed.validate(&work).is_err());
    }
    let mut maximum_alias = intent.clone(); maximum_alias.agent_alias = "a".repeat(128); maximum_alias.validate(&work).unwrap();
    assert_eq!(maximum_alias.child_session_id, intent.child_session_id);
    let mut missing = serde_json::to_value(&intent).unwrap(); missing.as_object_mut().unwrap().remove("agent_alias");
    assert!(serde_json::from_value::<PiNativeLaunchIntent>(missing).is_err());
}

#[test]
fn actual_native_namespace_references_reject_wrong_original_task_root_and_descendant() {
    let work = work(); let data = root_path().join("owned-data"); let task = TaskId::from_bytes([101; 16]);
    let service = super::super::service::session_directory(&data, "original-root");
    let view = service.join("agents").join(blake3::hash(&task.into_bytes()).to_hex().as_str()).join("view");
    assert_eq!(native_service_root(&data, "original-root", task, &view, &work).unwrap(), service);
    assert!(native_service_root(&data, "different-root", task, &view, &work).is_err());
    assert!(native_service_root(&data, "original-root", TaskId::from_bytes([102; 16]), &view, &work).is_err());
    assert!(native_service_root(&data, "original-root", task, &view.join("scratch"), &work).is_err());
    assert!(native_service_root(&data, "original-root", task, &root_path().join("unowned/view"), &work).is_err());
}
