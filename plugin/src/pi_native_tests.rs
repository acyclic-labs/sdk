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
            .any(|key| pi_caller::is_environment_key(key))
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
    assert_eq!(child.0.runtime_digest, intent.invocation.runtime_digest);
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
            0 => changed.invocation.runtime_digest[0] ^= 1,
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

#[test]
fn windows_environment_keys_that_differ_only_in_case_are_one_variable() {
    let root = root_path();
    let runtime = |environment: BTreeMap<String, String>| PiNativeHostRuntime {
        version: PI_NATIVE_VERSION.into(),
        executable: root.join("node"),
        executable_prefix: vec![root.join("pi-cli.js").to_str().unwrap().into()],
        extension: root.join("acyclic.ts"),
        provider_extension: None,
        environment,
    };
    const KEYS: u64 = 512;
    let distinct: BTreeMap<String, String> = (0..KEYS)
        .map(|index| (format!("KEY_{index:04}"), "v".into()))
        .collect();
    let work = NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            items_examined: 1 << 20,
            ..super::super::native_root_source::budget()
        },
        &CancellationToken::new(),
    )
    .unwrap();
    assert!(
        !runtime(distinct.clone())
            .case_folded_duplicate(&work)
            .unwrap()
    );
    let examined = work.receipt().items_examined;
    // Hashing reads each key once; nine merge passes and the final scan
    // compare at most one pair per key each. Negative control: the pairwise
    // scan it replaces examines n(n-1)/2 pairs.
    let sorted = KEYS * 9 + KEYS * (9 + 1);
    let pairwise = KEYS * (KEYS - 1) / 2 * 9;
    assert!(
        examined == sorted && sorted < pairwise,
        "{examined} items for {KEYS} keys"
    );
    let mut folded = distinct;
    folded.insert("key_0007".into(), "shadow".into());
    assert!(
        runtime(folded.clone())
            .case_folded_duplicate(&work)
            .unwrap()
    );
    // Only Windows reads environment keys case-insensitively.
    let small: BTreeMap<String, String> = [("KEY", "v"), ("key", "shadow")]
        .map(|(key, value)| (key.into(), value.into()))
        .into();
    assert_eq!(runtime(small).validate(&work).is_err(), cfg!(windows));
}

#[test]
fn an_unpaid_case_folded_check_is_refused_before_it_folds_any_key() {
    let root = root_path();
    let environment: BTreeMap<String, String> = (0..64)
        .map(|index| (format!("KEY_{index:04}"), "v".into()))
        .collect();
    // 64 keys of 8 bytes are hashed once, then six merge passes and one scan.
    let needed = 64 * 9 + 64 * (6 + 1);
    assert_eq!(
        super::runtime::case_folded_check_cost(environment.keys())
            .unwrap()
            .0,
        needed
    );
    let runtime = PiNativeHostRuntime {
        version: PI_NATIVE_VERSION.into(),
        executable: root.join("node"),
        executable_prefix: vec![root.join("pi-cli.js").to_str().unwrap().into()],
        extension: root.join("acyclic.ts"),
        provider_extension: None,
        environment,
    };
    let budget = |items_examined| {
        NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                items_examined,
                ..super::super::native_root_source::budget()
            },
            &CancellationToken::new(),
        )
        .unwrap()
    };
    let short = budget(needed - 1);
    assert!(runtime.case_folded_duplicate(&short).is_err());
    // The refusal charged nothing: no key was folded before the check was admitted.
    assert_eq!(short.receipt().items_examined, 0);
    // Negative control: exactly the admitted cost completes the check.
    let paid = budget(needed);
    assert!(!runtime.case_folded_duplicate(&paid).unwrap());
    assert_eq!(paid.receipt().items_examined, needed);
}

#[test]
fn the_case_folded_check_holds_exactly_the_bytes_it_claims() {
    let root = root_path();
    for keys in 1..=3_u64 {
        let environment: BTreeMap<String, String> = (0..keys)
            .map(|index| (format!("KEY_{index}"), "v".into()))
            .collect();
        let (_, claimed) = super::runtime::case_folded_check_cost(environment.keys()).unwrap();
        let runtime = PiNativeHostRuntime {
            version: PI_NATIVE_VERSION.into(),
            executable: root.join("node"),
            executable_prefix: vec![root.join("pi-cli.js").to_str().unwrap().into()],
            extension: root.join("acyclic.ts"),
            provider_extension: None,
            environment,
        };
        let budget = work();
        // The ledger records its first charge once per budget.
        budget
            .charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })
            .unwrap();
        let live = budget.live_allocation_bytes();
        let mut duplicate = None;
        let measured = acyclic_test_support::measure(|| {
            duplicate = Some(runtime.case_folded_duplicate(&budget).unwrap());
        });
        assert_eq!(duplicate, Some(false));
        // Two vectors of exactly one 32-byte digest per key, and nothing else.
        assert_eq!(claimed, keys * 64);
        assert_eq!(measured.bytes_max, claimed, "{keys} keys");
        assert_eq!(budget.live_allocation_bytes(), live);
        // Negative control: the hash table it replaces charged 50 bytes per
        // key, but any table under four keys allocates four buckets and their
        // control bytes, so one or two keys overran the charge.
        let table = acyclic_test_support::measure(|| {
            let set: std::collections::HashSet<(u64, &str)> =
                std::collections::HashSet::with_capacity(keys as usize);
            drop(set);
        });
        assert_eq!(
            table.bytes_max > keys * 50,
            keys < 3,
            "{keys} keys: table {} bytes",
            table.bytes_max
        );
    }
}
