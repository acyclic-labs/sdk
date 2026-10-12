use super::*;

fn root_path() -> PathBuf {
    std::env::temp_dir().join("acyclic-pi-native-test")
}
fn work() -> NativeMaterializationWork {
    NativeMaterializationWork::new(
        super::super::native_root_source::budget(),
        &CancellationToken::new(),
    )
    .unwrap()
}

fn source_and_session() -> (PiNativeSourceCut, Vec<u8>) {
    let entry = json!({"type":"message","id":"first","parentId":null,"timestamp":"2026-10-09T00:00:00.000Z",
        "message":{"role":"user","content":"original instruction","timestamp":1}});
    let entries_json = serde_json::to_string(&[&entry]).unwrap();
    let source = PiNativeSourceCut {
        mode: PiNativeSourceMode::CallerTool,
        parent_session_id: "parent".into(),
        parent_session_file: None,
        parent_cwd: root_path().to_str().unwrap().into(),
        source_leaf_id: Some("spawn".into()),
        owner_entry_id: Some("spawn".into()),
        parent_leaf_id: Some("first".into()),
        captured_at: "2026-10-09T00:00:01.000Z".into(),
        sha256: hex::encode(Sha256::digest(entries_json.as_bytes())),
        entries_json,
    };
    let header = json!({"type":"session","version":3,"id":"child","timestamp":source.captured_at,"cwd":root_path()});
    (source, format!("{header}\n{entry}\n").into_bytes())
}

#[test]
fn source_bound_session_preserves_caller_modes_and_rejects_forged_cuts_headers_or_extra_lines() {
    let work = work();
    let (source, bytes) = source_and_session();
    source
        .validate_session("child", &root_path(), &bytes, 16, 65_536, &work)
        .unwrap();
    assert!(
        source
            .validate_session("parent", &root_path(), &bytes, 16, 65_536, &work)
            .is_err()
    );
    assert!(
        source
            .validate_session("child", &root_path(), &bytes, 0, 65_536, &work)
            .is_err()
    );
    assert!(
        source
            .validate_session("child", &root_path(), &bytes, 16, 1, &work)
            .is_err()
    );
    let text = String::from_utf8(bytes.clone()).unwrap();
    for changed in [
        text.replace("\"version\":3", "\"version\":2"),
        text.replace("\"child\"", "\"foreign\""),
        text.replace("original instruction", "forged instruction"),
        format!("{text}{{\"type\":\"session\"}}\n"),
        format!("{text}not-json\n"),
        text.trim_end().to_owned(),
    ] {
        assert!(
            source
                .validate_session("child", &root_path(), changed.as_bytes(), 16, 65_536, &work)
                .is_err()
        );
    }
    let mut forged = source.clone();
    forged.parent_leaf_id = Some("another-leaf".into());
    assert!(
        forged
            .validate_session("child", &root_path(), &bytes, 16, 65_536, &work)
            .is_err()
    );
    forged = source.clone();
    forged.entries_json = "[]".into();
    assert!(
        forged
            .validate_session("child", &root_path(), &bytes, 16, 65_536, &work)
            .is_err()
    );
    assert!(serde_json::from_value::<PiNativeSourceMode>(json!("selected_branch")).is_err());
    let mut scratch = source.clone();
    scratch.source_leaf_id = Some("actual-sibling-tool-result".into());
    scratch
        .validate_session("child", &root_path(), &bytes, 16, 65_536, &work)
        .unwrap();
    scratch.mode = PiNativeSourceMode::Scratch;
    scratch.owner_entry_id = None;
    scratch.source_leaf_id = None;
    scratch.parent_leaf_id = None;
    assert!(
        scratch
            .validate_session("child", &root_path(), &bytes, 16, 65_536, &work)
            .is_err()
    );
    scratch.entries_json = "[]".into();
    scratch.sha256 = hex::encode(Sha256::digest(scratch.entries_json.as_bytes()));
    let header = format!(
        "{}\n",
        json!({"type":"session","version":3,"id":"child","timestamp":scratch.captured_at,"cwd":root_path()})
    );
    scratch
        .validate_session("child", &root_path(), header.as_bytes(), 16, 65_536, &work)
        .unwrap();
    scratch.owner_entry_id = Some("fabricated-tool-owner".into());
    assert!(
        scratch
            .validate_session("child", &root_path(), header.as_bytes(), 16, 65_536, &work)
            .is_err()
    );
    work.cancellation().cancel();
    assert!(matches!(
        source.validate_session("child", &root_path(), &bytes, 16, 65_536, &work),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
}

#[test]
fn original_host_settings_reject_child_substitution_and_strip_caller_credentials() {
    let work = work();
    let root = root_path();
    let provider_extension = root.join("provider.ts");
    let raw = PiNativeHostRuntime {
        version: PI_NATIVE_VERSION.into(),
        executable: root.join("node"),
        executable_prefix: vec![root.join("pi-cli.js").to_str().unwrap().into()],
        extension: root.join("acyclic.ts"),
        provider_extension: Some(provider_extension.clone()),
        environment: BTreeMap::from([
            ("HOME".into(), root.to_str().unwrap().into()),
            ("ACYCLIC_PI_CALLER".into(), "protected-child-route".into()),
            ("acyclic_pi_token".into(), "must-not-inherit".into()),
            (
                "ACYCLIC_PI_PROVIDER_EXTENSION".into(),
                provider_extension.to_str().unwrap().into(),
            ),
        ]),
    };
    assert!(matches!(
        raw.validate(&work),
        Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            ..
        })
    ));
    let host = raw.sanitize(&work).unwrap();
    assert!(
        host.environment
            .contains_key("ACYCLIC_PI_PROVIDER_EXTENSION")
    );
    assert!(
        !host
            .environment
            .keys()
            .any(|key| is_caller_environment(key))
    );
    let limits = capture_limits(
        acyclic_harness::conversation::Limits {
            file_bytes: 524_288,
            ..Default::default()
        },
        524_288,
    )
    .unwrap();
    let (source, bytes) = source_and_session();
    let seed = host
        .intent_invocation(
            &root.join("scratch/child.jsonl"),
            &root,
            "pi-native-test",
            "probe",
            "child instruction",
            limits,
            &work,
        )
        .unwrap();
    let intent = PiNativeLaunchIntent {
        agent_alias: "child".into(),
        child_session_id: "child".into(),
        source,
        invocation: seed.0,
        session_path: "child.jsonl".into(),
        session_bytes: String::from_utf8(bytes).unwrap(),
        maximum_entries: 16,
        maximum_bytes: 65_536,
    };
    let caller = BTreeMap::from([(
        "ACYCLIC_PI_CALLER".into(),
        "actual-created-child-caller".into(),
    )]);
    let child = host.bind_invocation(&intent, &caller, &work).unwrap();
    assert_eq!(child.0.executable, host.executable);
    assert_eq!(child.0.provider_extension, host.provider_extension);
    assert_eq!(
        child
            .0
            .environment
            .get("ACYCLIC_PI_CALLER")
            .map(String::as_str),
        Some("actual-created-child-caller")
    );
    assert_eq!(child.0.builtin_tools, ["read", "write", "edit"]);
    assert!(
        !intent
            .invocation
            .environment
            .contains_key("ACYCLIC_PI_CALLER")
    );
    assert!(
        host.bind_invocation(
            &intent,
            &BTreeMap::from([("HOME".into(), "forged-host-setting".into())]),
            &work
        )
        .is_err()
    );
    assert!(
        host.bind_invocation(&intent, &BTreeMap::new(), &work)
            .is_err()
    );
    for mutation in 0..5 {
        let mut changed = intent.clone();
        match mutation {
            0 => changed.invocation.executable = root.join("foreign-node"),
            1 => {
                changed
                    .invocation
                    .environment
                    .insert("HOME".into(), "foreign-host".into());
            }
            2 => changed.invocation.environment.extend(caller.clone()),
            3 => changed.invocation.builtin_tools.push("bash".into()),
            _ => changed.child_session_id = changed.source.parent_session_id.clone(),
        }
        assert!(host.bind_invocation(&changed, &caller, &work).is_err());
    }
    for alias in [
        "".into(),
        "non-ASCII-子".into(),
        "control\nalias".into(),
        "slash/alias".into(),
        "backslash\\alias".into(),
        "space alias".into(),
        "a".repeat(129),
    ] {
        let mut changed = intent.clone();
        changed.agent_alias = alias;
        assert!(changed.validate(&work).is_err());
    }
    let mut maximum_alias = intent.clone();
    maximum_alias.agent_alias = "a".repeat(128);
    maximum_alias.validate(&work).unwrap();
    assert_eq!(maximum_alias.child_session_id, intent.child_session_id);
    let mut missing = serde_json::to_value(&intent).unwrap();
    missing.as_object_mut().unwrap().remove("agent_alias");
    assert!(serde_json::from_value::<PiNativeLaunchIntent>(missing).is_err());
}

#[test]
fn capture_limits_respect_caller_byte_limits_and_deny_empty_allowances() {
    let limits = acyclic_harness::conversation::Limits {
        file_bytes: 65_536,
        ..Default::default()
    };
    assert_eq!(
        capture_limits(limits, 32_768).unwrap().maximum_result_bytes,
        32_768
    );
    assert!(capture_limits(limits, 0).is_err());
    assert!(
        capture_limits(
            acyclic_harness::conversation::Limits {
                file_bytes: 0,
                ..limits
            },
            32_768
        )
        .is_err()
    );
}
