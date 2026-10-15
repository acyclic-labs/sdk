use super::*;

#[test]
fn torn_saves_recover_the_last_completed_state() {
    let fixture = Fixture::new();
    let named = |name: &str| AdapterState {
        version: ADAPTER_STATE_VERSION,
        root_session_id: name.into(),
        ..AdapterState::default()
    };
    assert_eq!(load_saved_state(&fixture.data).unwrap().root_session_id, "");
    for name in ["first", "second", "third", "fourth"] {
        save_state(&fixture.data, &named(name), Survives::PowerLoss).unwrap();
        assert_eq!(
            load_saved_state(&fixture.data).unwrap().root_session_id,
            name
        );
    }
    let target = fixture.data.join(ADAPTER_STATE_SLOTS[0]);
    let written = fs::read(&target).unwrap();
    let mut flipped = written.clone();
    *flipped.last_mut().unwrap() ^= 1;
    let mut stale_tail = written.clone();
    stale_tail.extend_from_slice(b"}}");
    let prefixes = [
        0,
        1,
        ADAPTER_STATE_HEADER_BYTES - 1,
        ADAPTER_STATE_HEADER_BYTES,
        written.len() - 1,
    ]
    .map(|length| written[..length].to_vec());
    for torn in prefixes.into_iter().chain([flipped, stale_tail]) {
        fs::write(&target, torn).unwrap();
        assert_eq!(
            load_saved_state(&fixture.data).unwrap().root_session_id,
            "third"
        );
        save_state(&fixture.data, &named("fifth"), Survives::PowerLoss).unwrap();
        assert_eq!(
            load_saved_state(&fixture.data).unwrap().root_session_id,
            "fifth"
        );
    }
    for slot in ADAPTER_STATE_SLOTS {
        fs::write(fixture.data.join(slot), b"torn").unwrap();
    }
    assert!(load_saved_state(&fixture.data).is_err());
}

#[test]
fn failed_save_never_overwrites_the_durable_state() {
    let fixture = Fixture::new();
    let named = |name: &str| AdapterState {
        version: ADAPTER_STATE_VERSION,
        root_session_id: name.into(),
        ..AdapterState::default()
    };
    let (_, mut slots) = load_state(&fixture.data).unwrap();
    for (index, name) in ["a", "b", "c", "d", "e", "f", "g", "h"]
        .into_iter()
        .enumerate()
    {
        let survives = if index % 3 == 1 {
            Survives::ServiceCrash
        } else {
            Survives::PowerLoss
        };
        slots.save(&fixture.data, &named(name), survives).unwrap();
        assert_eq!(
            load_saved_state(&fixture.data).unwrap().root_session_id,
            name
        );
    }
    let newest = fixture
        .data
        .join(ADAPTER_STATE_SLOTS[slots.newest_flushed()]);
    let target = fixture
        .data
        .join(ADAPTER_STATE_SLOTS[1 - slots.newest_flushed()]);
    let durable = fs::read(&newest).unwrap();
    fs::remove_file(&target).unwrap();
    fs::create_dir(&target).unwrap();
    assert!(
        slots
            .save(&fixture.data, &named("failed"), Survives::PowerLoss)
            .is_err()
    );
    fs::remove_dir(&target).unwrap();
    slots
        .save(&fixture.data, &named("retried"), Survives::PowerLoss)
        .unwrap();
    assert_eq!(fs::read(&newest).unwrap(), durable);
    assert_eq!(
        load_saved_state(&fixture.data).unwrap().root_session_id,
        "retried"
    );
}

#[test]
fn persisted_state_rejects_oversize_and_ambiguous_bindings() {
    let fixture = Fixture::new();
    let oversized = fixture.data.join("oversized");
    fs::File::create(&oversized)
        .unwrap()
        .set_len(MAXIMUM_ADAPTER_STATE_BYTES + ADAPTER_STATE_HEADER_BYTES as u64 + 1)
        .unwrap();
    assert!(read_state_slot(&oversized).is_err());
    let mut state = AdapterState {
        version: ADAPTER_STATE_VERSION,
        ..AdapterState::default()
    };
    for index in 0..=MAXIMUM_ADAPTER_TURNS {
        state.turns.insert(index.to_string(), "root".into());
    }
    assert!(validate_state_bounds(&state).is_err());
    state.turns.clear();
    for index in 0..=MAXIMUM_ADAPTER_ROOT_TURNS {
        state.root_turns.insert(index.to_string());
    }
    assert!(validate_state_bounds(&state).is_err());
    state.root_turns.clear();
    state.roots.insert(
        "wrong-key".into(),
        RootBinding {
            root_id: [1; 16],
            path: fixture.root.clone(),
            allocation: RootAllocation::Created(RootCreation {
                repository_workspace_id: [2; 16],
                source_identity: [3; 16],
                source_epoch: 0,
                native_root_identity: [4; 16],
            }),
        },
    );
    assert!(validate_state_bounds(&state).is_err());
    state.roots.clear();
    state.root_session_id = "x".repeat(usize::try_from(MAXIMUM_ADAPTER_STATE_BYTES).unwrap());
    assert!(save_state(&fixture.data, &state, Survives::PowerLoss).is_err());
    assert!(
        ADAPTER_STATE_SLOTS
            .iter()
            .all(|slot| !fixture.data.join(slot).exists())
    );
}

#[test]
fn adapter_schema_requires_explicit_task_ownership() {
    let state = AdapterState {
        version: ADAPTER_STATE_VERSION,
        ..AdapterState::default()
    };
    validate_state_version(&state).unwrap();
    let mut value = serde_json::to_value(&state).unwrap();
    assert_eq!(value["task_binding"]["kind"], "host_managed");
    value.as_object_mut().unwrap().remove("task_binding");
    assert!(serde_json::from_value::<AdapterState>(value).is_err());
    for version in [4, 5] {
        let mut historical = state.clone();
        historical.version = version;
        assert!(validate_state_version(&historical).is_err());
    }
}
