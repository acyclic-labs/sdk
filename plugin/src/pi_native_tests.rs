use super::*;
use acyclic_harness::{
    AgentId, TaskId,
    conversation::{VolumeClass, VolumeOwner},
    filesystem::{NativeViewManifest, NativeVolumeMapping, NativeVolumeNamespace},
    resources::{GenerationRef, ProviderRef},
};

fn root_path() -> PathBuf { std::env::temp_dir().join("acyclic-pi-native-test") }

fn work() -> NativeMaterializationWork {
    pi_meter(super::super::native_root_source::budget(), &CancellationToken::new()).unwrap()
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
    let provider = ProviderRef::new("pi-native-test", "filesystem", "1").unwrap();
    let agent = AgentId::from_bytes([31; 16]);
    let scratch = VolumeRef::new(provider.clone(), "scratch", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).unwrap();
    let generation = GenerationRef::new(provider.clone(), [32; 32], None).unwrap();
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
    PiNativeInvocation { version: PI_NATIVE_VERSION.into(), executable: root_path().join(if cfg!(windows) { "node.exe" } else { "node" }),
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
fn source_bound_session_rejects_forged_cuts_headers_and_extra_lines() {
    let work = work();
    let (source, bytes) = source_and_session();
    source.validate_session("child", &root_path(), &bytes, 16, 65_536, &work).unwrap();
    let unchanged = source.clone();
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
    assert_eq!(source, unchanged);
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
    let serialized = serde_json::to_vec(&request).unwrap();
    let roundtrip: NativeProcessRequest = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(roundtrip, *request);
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
    let mutations: [fn(&mut PiNativeInvocation); 9] = [
        |value: &mut PiNativeInvocation| value.version = "1.0.0".into(),
        |value: &mut PiNativeInvocation| value.executable = PathBuf::from("pi"),
        |value: &mut PiNativeInvocation| value.executable_prefix = vec!["pi-cli.js".into()],
        |value: &mut PiNativeInvocation| value.session_file = root_path().join("project/child.jsonl"),
        |value: &mut PiNativeInvocation| value.cwd = root_path().join("../foreign"),
        |value: &mut PiNativeInvocation| value.extension = PathBuf::from("index.ts"),
        |value: &mut PiNativeInvocation| value.environment.clear(),
        |value: &mut PiNativeInvocation| { value.environment.insert("BAD=KEY".into(), "value".into()); },
        |value: &mut PiNativeInvocation| value.timeout_ms = 0,
    ];
    for mutate in mutations {
        let mut changed = input.clone(); mutate(&mut changed);
        assert!(changed.request_with_work(&binding, &manifest, &work).is_err());
    }
    let mut changed = binding.clone(); changed.scratch.allocation = PiScratchAllocation::Preparing;
    assert!(input.request_with_work(&changed, &manifest, &work).is_err());
    let mut missing = manifest.clone(); missing.volumes.remove(0);
    assert!(input.request_with_work(&binding, &missing, &work).is_err());
    let mut foreign = manifest.clone();
    foreign.volumes[0].volume = VolumeRef::new(binding.scratch.volume.provider().clone(), "foreign", VolumeClass::AgentPrivate, VolumeOwner::Agent(binding.agent_id)).unwrap();
    assert!(input.request_with_work(&binding, &foreign, &work).is_err());
}

#[test]
fn materialized_project_cannot_replace_the_existing_lazy_mount() {
    let work = work();
    let (mut binding, mut manifest) = binding_and_manifest();
    let project = VolumeRef::new(binding.scratch.volume.provider().clone(), "project", VolumeClass::Project, VolumeOwner::Project("project".into())).unwrap();
    binding.project_volumes.insert("project".into(), project.clone());
    manifest.volumes.push(NativeVolumeMapping { volume: project, generation: manifest.volumes[0].generation.clone(),
        path: root_path().join("project"), writable: true, authority_revision: [71; 32], namespace: NativeVolumeNamespace::MaterializedDirectory });
    assert!(invocation().request_with_work(&binding, &manifest, &work).is_err());
}

#[test]
fn exact_native_receipt_schema_preserves_cancellation_uncertainty() {
    use acyclic_harness::{filesystem::{NativeProcessResult, NativeProcessStop, NativeProcessStopKind}, tool::validate_value};
    for kind in [NativeProcessStopKind::Timeout, NativeProcessStopKind::OutputLimit, NativeProcessStopKind::ControlStopped, NativeProcessStopKind::CaptureFailed] {
        let result = NativeProcessResult { success: false, exit_code: None, stdout: vec![0, 255], stderr: vec![],
            stop: Some(NativeProcessStop { kind, message: "original supervisor outcome".into(), cleanup_completed: false }) };
        let schema: Value = serde_json::from_str(result_schema().unwrap().get()).unwrap();
        validate_value(&schema, &serde_json::to_value(&result).unwrap(), "native result").unwrap();
        assert!(!result.stop.unwrap().cleanup_completed);
    }
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
    assert!(raw.validate().is_err());
    let before = work.receipt();
    assert!(matches!(raw.validate_with_native_work(&work),
        Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, .. })));
    assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
    assert!(work.receipt().items_examined > before.items_examined);
    let original = raw.sanitize().unwrap();
    assert!(original.environment.contains_key("ACYCLIC_PI_PROVIDER_EXTENSION"));
    assert!(!original.environment.keys().any(|key| is_caller_environment(key)));
    let baseline = work.live_allocation_bytes();
    let copied = original.clone_with_native_work(&work).unwrap();
    assert!(copied.0 == original);
    assert!(copied.1.work().shares_ledger(&work));
    assert_eq!(work.live_allocation_bytes(), baseline + copied.1.bytes());
    drop(copied);
    assert_eq!(work.live_allocation_bytes(), baseline);
    let limits = PiNativeCaptureLimits { timeout_ms: input.timeout_ms, control_timeout_ms: input.control_timeout_ms,
        cancellation_poll_ms: input.cancellation_poll_ms, maximum_output_bytes: input.maximum_output_bytes, maximum_result_bytes: input.maximum_result_bytes };
    let (source, bytes) = source_and_session();
    let seed = original.intent_invocation_with_work(&input.session_file, &root_path(), &input.provider, &input.model, &input.prompt, limits, &work).unwrap();
    let intent = PiNativeLaunchIntent { agent_alias: "child".into(), child_session_id: "child".into(), source,
        invocation: seed.0, session_path: "child.jsonl".into(), session_bytes: String::from_utf8(bytes).unwrap(),
        maximum_entries: 16, maximum_bytes: 65_536 };
    let child = original.bind_invocation_with_work(&intent,
        &BTreeMap::from([("ACYCLIC_PI_CALLER".into(), "actual-created-child-caller".into())]), &work).unwrap();
    assert_eq!(child.0.executable, original.executable);
    assert_eq!(child.0.provider_extension, original.provider_extension);
    assert_eq!(child.0.environment.get("ACYCLIC_PI_CALLER").map(String::as_str), Some("actual-created-child-caller"));
    assert_eq!(child.0.builtin_tools, ["read", "write", "edit"]);
    assert!(child.1.work().shares_ledger(&work));
    assert!(original.bind_invocation_with_work(&intent,
        &BTreeMap::from([("HOME".into(), "forged-host-setting".into())]), &work).is_err());
    assert!(original.bind_invocation_with_work(&intent, &BTreeMap::new(), &work).is_err());
}

#[test]
fn original_immutable_native_seed_binds_only_the_created_child_current_caller() {
    let work = work();
    let input = invocation();
    let host = PiNativeHostRuntime { version: input.version.clone(), executable: input.executable.clone(),
        executable_prefix: input.executable_prefix.clone(), extension: input.extension.clone(),
        provider_extension: input.provider_extension.clone(), environment: input.environment.clone() }.sanitize().unwrap();
    let capture = PiNativeCaptureLimits { timeout_ms: input.timeout_ms, control_timeout_ms: input.control_timeout_ms,
        cancellation_poll_ms: input.cancellation_poll_ms, maximum_output_bytes: input.maximum_output_bytes, maximum_result_bytes: input.maximum_result_bytes };
    let (source, bytes) = source_and_session();
    let seed = host.intent_invocation_with_work(&root_path().join("scratch/session.jsonl"), &root_path(),
        &input.provider, &input.model, &input.prompt, capture, &work).unwrap();
    let original = PiNativeLaunchIntent { agent_alias: "child-alias".into(), child_session_id: "child".into(), source,
        invocation: seed.0,
        session_path: "session.jsonl".into(), session_bytes: String::from_utf8(bytes).unwrap(),
        maximum_entries: 16, maximum_bytes: 65_536 };
    let caller = || BTreeMap::from([("ACYCLIC_PI_CALLER".into(), "actual-original-child-current-lease-proof".into())]);
    let bound = host.bind_invocation_with_work(&original, &caller(), &work).unwrap();
    assert_eq!(bound.0.environment.get("ACYCLIC_PI_CALLER").map(String::as_str), Some("actual-original-child-current-lease-proof"));
    assert!(!original.invocation.environment.contains_key("ACYCLIC_PI_CALLER"));
    let mut changed = original.clone();
    changed.invocation.executable = root_path().join("foreign-node");
    assert!(host.bind_invocation_with_work(&changed, &caller(), &work).is_err());
    changed = original.clone();
    changed.invocation.environment.insert("HOME".into(), "foreign-host".into());
    assert!(host.bind_invocation_with_work(&changed, &caller(), &work).is_err());
    changed = original.clone();
    changed.invocation.environment.extend(caller());
    assert!(host.bind_invocation_with_work(&changed, &caller(), &work).is_err(), "immutable seed must never retain a stale caller lease proof");
    changed = original.clone();
    changed.invocation.builtin_tools.push("bash".into());
    assert!(host.bind_invocation_with_work(&changed, &caller(), &work).is_err());
    changed = original.clone();
    changed.child_session_id = changed.source.parent_session_id.clone();
    assert!(host.bind_invocation_with_work(&changed, &caller(), &work).is_err(), "new child native ID remains separate from its actual source");
    assert!(host.bind_invocation_with_work(&original, &BTreeMap::new(), &work).is_err());
    for alias in ["".into(), "non-ASCII-子".into(), "control\nalias".into(), "slash/alias".into(), "backslash\\alias".into(), "space alias".into(), "a".repeat(129)] {
        changed = original.clone();
        changed.agent_alias = alias;
        assert!(changed.validate(&work).is_err());
    }
    changed = original.clone();
    changed.agent_alias = "a".repeat(128);
    changed.validate(&work).unwrap();
    assert_eq!(changed.child_session_id, original.child_session_id, "alias must not replace original native identity");
    let mut missing_alias = serde_json::to_value(&original).unwrap();
    missing_alias.as_object_mut().unwrap().remove("agent_alias");
    assert!(serde_json::from_value::<PiNativeLaunchIntent>(missing_alias).is_err());
}

#[test]
fn scratch_and_parallel_caller_modes_preserve_only_the_actual_sdk_caller_branch() {
    assert!(serde_json::from_value::<PiNativeSourceMode>(json!("selected_branch")).is_err());
    let work = work();
    let (mut source, bytes) = source_and_session();
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

#[test]
fn native_process_cleanup_is_an_explicit_observed_result_not_success_or_cancellation_ack() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &cancellation).unwrap();
    for success in [false, true] {
        let completed = json!({"success":success,"exit_code":1,"stdout":[],"stderr":[],"stop":null});
        assert!(process_cleanup_completed(&serde_json::to_vec(&completed).unwrap(), 4096, &work).unwrap());
        for cleaned in [false, true] {
            let stopped = json!({"success":false,"exit_code":null,"stdout":[],"stderr":[],
                "stop":{"kind":"control_stopped","message":"actual platform observation","cleanup_completed":cleaned}});
            assert_eq!(process_cleanup_completed(&serde_json::to_vec(&stopped).unwrap(), 4096, &work).unwrap(), cleaned);
        }
    }
    assert!(process_cleanup_completed(br#"{"cancelled":true,"cwd":"planned-native-path"}"#, 4096, &work).is_err());
    assert!(process_cleanup_completed(br#"{"success":true,"exit_code":0,"stdout":[],"stderr":[],"stop":{"kind":"control_stopped","message":"missing observed cleanup"}}"#, 4096, &work).is_err());
    assert!(process_cleanup_completed(br#"{"success":true,"exit_code":0,"stdout":[],"stderr":[]}"#, 4096, &work).is_err());
    assert!(process_cleanup_completed(br#"{"success":true,"stdout":[],"stderr":[],"stop":null}"#, 4096, &work).is_err());
}

#[tokio::test]
async fn actual_owned_native_scratch_release_denies_guard_without_deleting_data() {
    let directory = tempfile::tempdir().unwrap();
    let cancellation = CancellationToken::new();
    let meter = pi_meter(super::super::native_root_source::budget(), &cancellation).unwrap();
    let service = meter.root_identity(directory.path()).unwrap();
    let relative = Path::new("agents/task/view/scratch");
    let leaf = meter.create_owned_directory(directory.path(), service, relative, true).unwrap();
    let path = directory.path().join(relative);
    std::fs::write(path.join("original.jsonl"), "native-owned-original\n").unwrap();
    let result = meter.release_owned_directory_guarded(directory.path(), service, relative, leaf, || async {
        Err(acyclic_fs::NativeTreeMaterializationError::UnverifiedSourceMetadata("original creator/current task WRITE denied".into()))
    }).await;
    assert!(result.is_err());
    assert_eq!(std::fs::read_to_string(path.join("original.jsonl")).unwrap(), "native-owned-original\n");
    assert!(meter.receipt().items_examined > 0);
}

#[tokio::test]
async fn actual_owned_native_scratch_release_rechecks_replacement_after_async_guard() {
    let directory = tempfile::tempdir().unwrap();
    let cancellation = CancellationToken::new();
    let meter = pi_meter(super::super::native_root_source::budget(), &cancellation).unwrap();
    let service = meter.root_identity(directory.path()).unwrap();
    let relative = Path::new("scratch");
    let leaf = meter.create_owned_directory(directory.path(), service, relative, true).unwrap();
    let path = directory.path().join(relative);
    std::fs::write(path.join("original.jsonl"), "original physical allocation\n").unwrap();
    let moved = directory.path().join("retained-original");
    let mut swapped = false;
    let result = meter.release_owned_directory_guarded(directory.path(), service, relative, leaf, || {
        if !swapped {
            std::fs::rename(&path, &moved).unwrap();
            std::fs::create_dir(&path).unwrap();
            std::fs::write(path.join("user.txt"), "unrelated replacement data\n").unwrap();
            swapped = true;
        }
        std::future::ready(Ok(()))
    }).await;
    assert!(result.is_err());
    assert_eq!(std::fs::read_to_string(path.join("user.txt")).unwrap(), "unrelated replacement data\n");
    assert_eq!(std::fs::read_to_string(moved.join("original.jsonl")).unwrap(), "original physical allocation\n");
}

#[test]
fn actual_owned_native_factory_rejects_unknown_existing_leaf_and_zero_allowance() {
    let directory = tempfile::tempdir().unwrap();
    let cancellation = CancellationToken::new();
    let meter = pi_meter(super::super::native_root_source::budget(), &cancellation).unwrap();
    let service = meter.root_identity(directory.path()).unwrap();
    let unknown = directory.path().join("unknown-prejournal-scratch");
    std::fs::create_dir(&unknown).unwrap();
    std::fs::write(unknown.join("user.txt"), "preserve pre-journal uncertainty\n").unwrap();
    assert!(meter.create_owned_directory(directory.path(), service, Path::new("unknown-prejournal-scratch"), true).is_err());
    assert_eq!(std::fs::read_to_string(unknown.join("user.txt")).unwrap(), "preserve pre-journal uncertainty\n");
    assert!(pi_meter(WorkBudget::default(), &cancellation).is_err());
    let denied = pi_meter(WorkBudget { allocation_operations: 1,
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(), ..WorkBudget::default() }, &cancellation).unwrap();
    assert!(denied.create_owned_directory(directory.path(), service, Path::new("new-private-scratch"), true).is_err());
    assert!(!directory.path().join("new-private-scratch").exists());
}

#[tokio::test]
async fn actual_original_native_approval_ledger_requires_exact_conversation_ticket_and_digest() -> Result<()> {
    use acyclic_harness::{
        core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry},
        filesystem::{FilesystemExecutionJournal, FilesystemHost},
        tool::ToolApprovalVerifier,
        store::StreamAggregate,
    };
    use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
    let directory = tempfile::tempdir().map_err(|error| invalid(&error.to_string()))?;
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.path().join("filesystem"))).await
        .map_err(|error| invalid(&error.to_string()))?;
    let provider = ProviderRef::new("actual-native-approval-test", "filesystem", "1")?;
    let files = Arc::new(FilesystemHost::new(filesystem, provider.clone())?);
    let stream = StreamClient::new(Arc::new(LocalStream::open(directory.path().join("stream"), LocalStreamLimits::default()).await
        .map_err(|error| invalid(&error.to_string()))?));
    for (index, conversation) in ["original-root-task", "original-child-task"].into_iter().enumerate() {
        let agent = AgentId::from_bytes([71 + index as u8; 16]);
        let private = VolumeRef::new(provider.clone(), conversation, VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
        files.create_volume(&private).await?;
        let command = OperationId::from_bytes([73 + index as u8; 16]);
        let approval = task_interaction_id(TaskId::from_bytes([75 + index as u8; 16]), command);
        let digest = [77 + index as u8; 32];
        let audience = Authority { kind: AggregateKind::Conversation, id: conversation.into() };
        let issuer = AuthorityIssuer::new("original-native-policy", [79; 32], audience.clone());
        let grants = Capabilities::new(["conversation:bind".into(), "interaction:open".into(), "interaction:resolve".into(),
            format!("interaction:respond:{approval}"), private.capability(VolumeOperation::Read)?, private.capability(VolumeOperation::Write)?]);
        let scope = issuer.root_for_agent(agent, "original-conversation", grants.clone());
        let mut aggregate = StreamAggregate::open(&stream, audience, issuer.verifier(), SchemaRegistry::new()).await?;
        aggregate.execute(Command { operation_id: OperationId::from_bytes([81 + index as u8; 16]),
            idempotency_key: IdempotencyKey::new(format!("bind-{conversation}"))?, expected_revision: 0, scope: scope.clone(),
            causal_parent: None, action: Action::BindConversation { agent } }).await?;
        let journal = FilesystemExecutionJournal::new(stream.clone(), files.clone(), private, issuer.verifier(), scope.clone(), 1_048_576)?;
        journal.open_interaction(approval, Interaction::approval("Run exact pinned native child", command, digest)?).await?;
        let wrong_owner = AuthorityIssuer::new("original-native-policy", [82; 32], issuer.verifier().audience().clone())
            .root_for_agent(agent, "forged-owner", grants.clone());
        assert!(journal.resolve_interaction(approval, InteractionResponse::Approval { approved: true, reason: None }, &wrong_owner).await.is_err());
        let wrong_audience = AuthorityIssuer::new("original-native-policy", [79; 32],
            Authority { kind: AggregateKind::Conversation, id: "unrelated-conversation".into() })
            .root_for_agent(agent, "wrong-audience", grants.clone());
        assert!(journal.resolve_interaction(approval, InteractionResponse::Approval { approved: true, reason: None }, &wrong_audience).await.is_err());
        assert!(journal.resolve_interaction(InteractionId::from_bytes([83; 16]), InteractionResponse::Approval { approved: true, reason: None }, &scope).await.is_err());
        journal.resolve_interaction(approval, InteractionResponse::Approval { approved: true, reason: None }, &scope).await?;
        ToolApprovalVerifier::verify(&journal, approval, command, digest).await?;
        assert!(ToolApprovalVerifier::verify(&journal, approval, command, [84; 32]).await.is_err());
        assert!(ToolApprovalVerifier::verify(&journal, approval, OperationId::from_bytes([85; 16]), digest).await.is_err());
        assert!(journal.open_interaction(approval, Interaction::approval("Changed reused request", command, digest)?).await.is_err());
    }
    Ok(())
}

#[test]
fn actual_native_artifact_codec_keeps_original_ledger_and_stable_upload_identity() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &cancellation).unwrap();
    let value = json!({"literal":"@/not-a-file\n\"--not-an-option\"","entries":[1,2,3]});
    let encoded = guarded_native_bytes(&value, 4096, &work, false).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&encoded.bytes).unwrap(), value);
    let original = work.retain_owned(0).unwrap();
    assert!(encoded._custody.shares_ledger(&original));
    assert!(encoded._custody.bytes() >= encoded.bytes.len() as u64);
    assert!(work.receipt().bytes_encoded >= encoded.bytes.len() as u64 * 2);
    assert!(work.receipt().bytes_copied >= encoded.bytes.len() as u64);
    let alias = work.native_mount_work().native_materialization_work();
    assert!(alias.retain_owned(0).unwrap().shares_ledger(&original));
    let command = OperationId::from_bytes([91; 16]);
    for role in ["request", "schema", "plan"] {
        assert_eq!(native_artifact_operation_with_work(command, role, &work).unwrap(),
            native_artifact_operation(command, role).unwrap());
    }
    assert_eq!(native_artifact_operation_with_work(command, "plan", &work).unwrap(), native_plan_operation(command).unwrap());
    assert!(work.receipt().bytes_hashed > 0);
}

#[test]
fn actual_native_artifact_codec_retains_exact_quota_and_cancellation_causes() {
    let cancellation = CancellationToken::new();
    let mut budget = super::super::native_root_source::budget();
    budget.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes() + 32;
    let work = NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let value = "x".repeat(1024);
    assert!(matches!(guarded_native_bytes(&value, 4096, &work, false),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }), custody: Some(_) })));
    cancellation.cancel();
    assert!(matches!(guarded_native_bytes(&value, 4096, &work, false),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
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
    assert!(work.receipt().bytes_hashed > 0);
    assert_eq!(work.receipt().backend_write_operations, 0, "planned references do not allocate or publish a native root");
}

#[test]
fn canonical_native_creator_packet_admits_pi_sibling_layout_without_creating_or_adopting_paths() {
    use acyclic_fs::materializer::{NativeHostPrivateStateRoot, NativeOwnedRootAllocation, NativeOwnedRootStatus};
    let directory = tempfile::tempdir().unwrap();
    let cancellation = CancellationToken::new();
    let work = pi_meter(super::super::native_root_source::budget(), &cancellation).unwrap();
    let root = NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(directory.path(), &work).unwrap();
    let relative = [
        Path::new("sessions/root/agents/task/view"),
        Path::new("sessions/root/agents/task/preparation"),
        Path::new("sessions/root/agents/task/preparation/target"),
        Path::new("sessions/root/agents/task/preparation/backup"),
    ];
    let before = work.receipt();
    let baseline = work.live_allocation_bytes();
    let packet = NativeOwnedRootAllocation::new_with_native_work(
        acyclic_fs::OperationId::from_bytes([0xc1; 16]), &root, relative, &work).unwrap();
    assert_eq!(work.receipt().backend_write_operations, before.backend_write_operations);
    assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
    assert!(work.live_allocation_bytes() > baseline);
    assert!(packet.parent_count() > 0);
    for index in 0..4 {
        assert_eq!(packet.status(index), Some(NativeOwnedRootStatus::Pending));
        assert!(!directory.path().join(relative[index]).exists());
    }
    assert_eq!(packet.known_identities(), None, "planned paths are not native creation receipts");
    drop(packet);
    assert_eq!(work.live_allocation_bytes(), baseline);
    cancellation.cancel();
    let before = work.receipt();
    assert!(matches!(NativeOwnedRootAllocation::new_with_native_work(
        acyclic_fs::OperationId::from_bytes([0xc1; 16]), &root, relative, &work),
        Err(acyclic_fs::NativeTreeMaterializationError::Cancelled)));
    assert_eq!(work.receipt(), before);
    assert!(!directory.path().join("sessions").exists());
}
