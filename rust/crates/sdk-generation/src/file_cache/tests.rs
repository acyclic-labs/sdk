use super::*;
use proptest::prelude::*;
use std::cell::Cell;
use std::fs::{File, FileTimes};

proptest! {
    #[test]
    fn metadata_retains_only_adjacent_commands_and_rechecks_observations(
        commands in prop::collection::vec(prop::collection::vec((0u8..12, any::<u8>()), 0..12), 1..24),
    ) {
        let values = Values::default();
        let recent = RefCell::new(BTreeSet::new());
        let mut previous = BTreeSet::new();
        let reads = Cell::new(0);
        let mut reference = BTreeMap::new();
        for command in commands {
            retain_metadata(&values, &recent);
            reference.retain(|key, _| previous.contains(key));
            prop_assert_eq!(values.borrow().len(), reference.len());
            let mut current = BTreeSet::new();
            for (context, stamp) in command {
                let key = (PathBuf::from("cargo"), context.to_string());
                let expected_reads = reads.get()
                    + usize::from(reference.get(&key).is_none_or(|(old, _)| *old != stamp));
                let value = observed_metadata(&values, &recent, key.clone(), || Ok(stamp), || {
                    reads.set(reads.get() + 1);
                    Ok(format!("{context}:{stamp}"))
                }).unwrap();
                prop_assert_eq!(&value, &format!("{context}:{stamp}"));
                prop_assert_eq!(reads.get(), expected_reads);
                reference.insert(key.clone(), (stamp, value));
                current.insert(key);
                prop_assert_eq!(&*values.borrow(), &reference);
                prop_assert!(values.borrow().keys().all(|key| previous.contains(key) || current.contains(key)));
            }
            finish_metadata(&values, &recent);
            reference.retain(|key, _| current.contains(key));
            prop_assert_eq!(&*values.borrow(), &reference);
            prop_assert_eq!(&*recent.borrow(), &current);
            previous = current;
        }
        retain_metadata(&values, &recent);
        retain_metadata(&values, &recent);
        prop_assert!(values.borrow().is_empty(), "an unobserved command must retire old contexts");
    }

    #[test]
    fn source_map_identity_matches_the_independent_sha2_oracle(
        entries in proptest::collection::btree_map(".{0,24}", ".{0,80}", 0..16),
    ) {
        use sha2::{Digest, Sha256};
        let mut oracle = Sha256::new();
        for (path, value) in &entries {
            oracle.update(path.as_bytes()); oracle.update([0]);
            oracle.update(value.as_bytes()); oracle.update([0]);
        }
        prop_assert_eq!(crate::digest_map(&entries), format!("sha256:{:x}", oracle.finalize()));
    }

    // The real memoization transition runs over generated observations, without
    // creating thousands of OS files. Native stamp mapping has separate controls.
    #[test]
    fn follows_file_and_context_changes(
        initial in proptest::collection::vec(any::<u8>(), 1..128),
        operations in proptest::collection::vec((0u8..5, any::<u8>()), 1..24),
    ) {
        let values = Values::default();
        let mut bytes = initial;
        let mut generation = 0u64;
        let mut context = 0;
        let reads = Cell::new(0);
        for (operation, byte) in operations {
            match operation {
                0 => {},
                1 | 2 => { bytes[0] ^= byte | 1; generation += 1; },
                3 => context = byte,
                4 => {
                    prop_assert!(observed(&values, ("input".into(), context.to_string()),
                        || Err::<(usize,u64),_>(CliError("missing file".into())), || panic!("missing file was read")).is_err());
                    bytes.push(byte); generation += 1;
                },
                _ => unreachable!(),
            }
            let key = ("input".into(), context.to_string());
            let observe = || Ok((bytes.len(), generation));
            let load = || { reads.set(reads.get() + 1); Ok(format!("{}:{context}", crate::sha256_bytes(&bytes))) };
            let expected = format!("{}:{context}", crate::sha256_bytes(&bytes));
            prop_assert_eq!(observed(&values, key.clone(), observe, load).unwrap(), expected.clone());
            let before = reads.get();
            prop_assert_eq!(observed(&values, key, observe, load).unwrap(), expected);
            prop_assert_eq!(reads.get(), before);
        }
    }

    #[test]
    fn selection_is_reused_only_within_a_command(commands in proptest::collection::vec(any::<bool>(), 1..24)) {
        let values = Values::default();
        let lookups = Cell::new(0);
        let mut expected = 0;
        for (index, next_command) in commands.into_iter().enumerate() {
            if index == 0 || next_command { clear(&values); expected += 1; }
            let value = observed(&values, ("toolchain".into(), "selection".into()), || Ok(0u8), || {
                lookups.set(lookups.get() + 1); Ok("cargo".into())
            }).unwrap();
            prop_assert_eq!(value, "cargo");
            prop_assert_eq!(lookups.get(), expected);
        }
    }

    #[test]
    fn cargo_probe_context_is_complete_and_order_independent(
        inputs in proptest::collection::btree_map("[A-Z]{1,8}", ".{0,16}", 0..12),
        root in "[a-z]{1,8}", discovery in ".{0,16}",
    ) {
        use crate::toolchains::cargo_context;
        let environment = || inputs.iter().map(|(key, value)| (key.into(), value.into()));
        let path = PathBuf::from(root);
        let context = cargo_context(&path, &discovery, environment());
        prop_assert_eq!(&context, &cargo_context(&path, &discovery, environment().rev()));
        prop_assert_ne!(&context, &cargo_context(&path.join("different"), &discovery, environment()));
        prop_assert_ne!(&context, &cargo_context(&path, &format!("{discovery}!"), environment()));
        prop_assert_ne!(context, cargo_context(&path, &discovery, environment().chain([("NEW_INPUT".into(), "value".into())])));
    }

    #[test]
    fn shared_record_round_trip_binds_every_digest_byte(key in any::<[u8;32]>(), digest in any::<[u8;32]>()) {
        let key = crate::sha256_bytes(&key);
        let digest = crate::sha256_bytes(&digest);
        let bytes = crate::cache_record::encode(&key, &digest);
        prop_assert_eq!(crate::cache_record::decode(&bytes, &key), Some(digest.clone()));
        let different_key = format!("{key}0");
        prop_assert!(crate::cache_record::decode(&bytes, &different_key).is_none());
        let mut changed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        changed["digest"] = serde_json::json!(crate::sha256_bytes(digest.as_bytes()));
        prop_assert!(crate::cache_record::decode(&serde_json::to_vec(&changed).unwrap(), &key).is_none());
    }
}

#[test]
fn cargo_probe_key_frames_environment_names_and_values() {
    let context = |key: &str, value: &str| {
        crate::toolchains::cargo_context(Path::new("root"), "", [(key.into(), value.into())])
    };
    assert_ne!(context("A", "BC"), context("AB", "C"));
}

#[test]
fn closed_owner_metadata_rechecks_both_concrete_tools_context_and_wrappers() {
    let directory = tempfile::tempdir().unwrap();
    let cargo = directory.path().join("cargo");
    let rustc = directory
        .path()
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" });
    fs::write(&cargo, "concrete Cargo").unwrap();
    fs::write(&rustc, "concrete compiler").unwrap();
    let mut command = std::process::Command::new(&cargo);
    command
        .env("RUSTC", &rustc)
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER");
    let calls = Cell::new(0);
    sdk_docs::with_command_environment(&command, || {
        let query = |context: &str| {
            with_run(|| {
                resolved_cargo(&cargo);
                owner_metadata(&cargo, context, || {
                    calls.set(calls.get() + 1);
                    Ok(calls.get().to_string())
                })
                .unwrap()
            })
        };
        assert_eq!(query("source/config/environment-a"), "1");
        assert_eq!(query("source/config/environment-a"), "1");
        fs::write(&rustc, "changed compiler").unwrap();
        assert_eq!(query("source/config/environment-a"), "2");
        fs::write(&cargo, "changed Cargo").unwrap();
        assert_eq!(query("source/config/environment-a"), "3");
        assert_eq!(query("source/config/environment-b"), "4");
        METADATA.with_borrow(|values| assert_eq!(values.len(), 1));
        assert_eq!(query("source/config/environment-b"), "4");
        METADATA.with_borrow(|values| assert_eq!(values.len(), 1));
        crate::run(["help".into()], &mut Vec::new()).unwrap();
        crate::run(["help".into()], &mut Vec::new()).unwrap();
        METADATA.with_borrow(|values| assert!(values.is_empty()));
        for compiler in [
            directory.path().join("missing-wrapper"),
            directory.path().to_owned(),
        ] {
            let mut command = std::process::Command::new(&cargo);
            command.env("RUSTC", compiler);
            sdk_docs::with_command_environment(&command, || {
                assert_eq!(query("source/config/environment-a"), "5");
                assert_eq!(query("source/config/environment-a"), "6");
            });
            calls.set(4);
        }
        for key in [
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
            "CARGO_BUILD_RUSTC_WRAPPER",
            "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
        ] {
            let mut command = std::process::Command::new(&cargo);
            command.env(key, "external-wrapper");
            sdk_docs::with_command_environment(&command, || {
                assert_eq!(query("source/config/environment-a"), "5");
                // A wrapper must query afresh even within the same command.
                assert_eq!(
                    owner_metadata(&cargo, "source/config/environment-a", || {
                        calls.set(calls.get() + 1);
                        Ok(calls.get().to_string())
                    })
                    .unwrap(),
                    "6"
                );
                assert_eq!(query("source/config/environment-a"), "7");
            });
            calls.set(4);
        }
    });
}

#[test]
fn artifact_hashes_share_receipt_bytes_and_detect_same_length_rewrites() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("receipt.json");
    proptest::test_runner::TestRunner::default()
        .run(&any::<u64>(), |value| {
            let original = value.to_le_bytes();
            let changed = (!value).to_le_bytes();
            fs::write(&path, original).unwrap();
            let modified = fs::metadata(&path).unwrap().modified().unwrap();
            begin_run();
            sdk_docs::with_rustdoc_cache(|| {
                let snapshot = sdk_docs::read_rustdoc(&path).unwrap();
                prop_assert_eq!(sha256_file(&path).unwrap(), crate::sha256_bytes(&original));
                fs::write(&path, changed).unwrap();
                File::options()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_times(FileTimes::new().set_modified(modified))
                    .unwrap();
                prop_assert_eq!(sha256_file(&path).unwrap(), crate::sha256_bytes(&changed));
                prop_assert_eq!(&*snapshot, &original);
                prop_assert_eq!(&*sdk_docs::read_rustdoc(&path).unwrap(), &changed);
                fs::remove_file(&path).unwrap();
                prop_assert!(sha256_file(&path).is_err());
                Ok(())
            })
        })
        .unwrap();
}

#[test]
fn each_command_discards_all_cached_values_and_resolved_selections() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("toolchain");
    fs::write(&path, "channel").unwrap();
    get(&path, "cargo-vV", || Ok("old version".into())).unwrap();
    resolved_cargo(&path);
    crate::run(["help".into()], &mut Vec::new()).unwrap();
    assert!(!is_resolved_cargo(&path));
    assert_eq!(
        get(&path, "cargo-vV", || Ok("fresh version".into())).unwrap(),
        "fresh version"
    );
}

#[test]
fn changes_during_reads_and_failed_reads_are_not_cached() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input");
    fs::write(&path, b"before").unwrap();
    assert!(get(&path, "mutation", || {
        let value = crate::sha256_file(&path)?;
        fs::write(&path, b"after with different length").map_err(io_error)?;
        Ok(value)
    })
    .unwrap_err()
    .0
    .contains("file changed while reading identity"));
    assert_eq!(
        sha256_file(&path).unwrap(),
        crate::sha256_file(&path).unwrap()
    );
    assert!(get(&path, "failure", || Err(CliError("unavailable".into()))).is_err());
    assert_eq!(
        get(&path, "failure", || Ok("retried".into())).unwrap(),
        "retried"
    );
}

#[test]
fn same_length_edit_with_restored_mtime_invalidates_identity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tool");
    let cache = directory.path().join("cache");
    fs::write(&path, b"before").unwrap();
    let modified = fs::metadata(&path).unwrap().modified().unwrap();
    let original = shared_identity(&path, &cache).unwrap();
    fs::write(&path, b"edited").unwrap();
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
    assert_ne!(shared_identity(&path, &cache).unwrap(), original);
    assert_eq!(
        shared_identity(&path, &cache).unwrap(),
        crate::sha256_file(&path).unwrap()
    );
}

#[test]
fn shared_identity_reads_once_and_recovers_corrupt_records() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tool");
    let cache = directory.path().join("cache");
    fs::write(&path, b"identity").unwrap();
    let before = crate::artifacts::HASH_BYTES.get();
    assert_eq!(
        shared_identity(&path, &cache).unwrap(),
        crate::sha256_bytes(b"identity")
    );
    assert_eq!(
        shared_identity(&path, &cache).unwrap(),
        crate::sha256_bytes(b"identity")
    );
    assert_eq!(crate::artifacts::HASH_BYTES.get() - before, 8);
    // A warm hit must work without opening the writer lock at all.
    let lock = fs::read_dir(&cache)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "lock")
        })
        .unwrap();
    fs::remove_file(&lock).unwrap();
    fs::create_dir(&lock).unwrap();
    assert_eq!(
        shared_identity(&path, &cache).unwrap(),
        crate::sha256_bytes(b"identity")
    );
    assert_eq!(
        crate::artifacts::HASH_BYTES.get() - before,
        8,
        "a cached reader opened the unusable writer lock and rehashed"
    );
    fs::remove_dir(&lock).unwrap();
    for entry in fs::read_dir(&cache).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none() {
            fs::write(path, b"corrupt record").unwrap();
        }
    }
    assert_eq!(
        shared_identity(&path, &cache).unwrap(),
        crate::sha256_bytes(b"identity")
    );
    assert_eq!(crate::artifacts::HASH_BYTES.get() - before, 16);
}

#[test]
fn normalized_receipt_identity_ignores_source_existence() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.rs");
    let receipt = directory.path().join("receipt.json");
    fs::write(&source, "source").unwrap();
    fs::write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({"filename": source})).unwrap(),
    )
    .unwrap();
    begin_run();
    let original = rustdoc_digest(&receipt, directory.path()).unwrap();
    fs::remove_file(source).unwrap();
    begin_run();
    assert_eq!(
        rustdoc_digest(&receipt, directory.path()).unwrap(),
        original
    );
    assert_eq!(
        sdk_docs::rustdoc_digest(&receipt, directory.path()).unwrap(),
        original
    );
}

#[cfg(unix)]
fn wrapper_probe(broken: bool) {
    let directory = tempfile::tempdir().unwrap();
    // Use an immutable executable: concurrent fork can inherit a freshly
    // written fixture's writable descriptor until exec, causing ETXTBSY.
    let wrapper = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/toolchain-wrapper.sh");
    fs::write(directory.path().join("selected"), "version A\n").unwrap();
    begin_run();
    if broken {
        resolved_cargo(&wrapper);
    }
    let first = crate::toolchains::cargo_version(&wrapper, directory.path()).unwrap();
    let retained = VALUES.with(|values| values.borrow().clone());
    fs::write(directory.path().join("selected"), "version B\n").unwrap();
    begin_run();
    if broken {
        VALUES.with_borrow_mut(|values| *values = retained);
        resolved_cargo(&wrapper);
    }
    let actual = crate::toolchains::cargo_version(&wrapper, directory.path()).unwrap();
    let fresh = std::process::Command::new(&wrapper)
        .current_dir(directory.path())
        .arg("-vV")
        .output()
        .unwrap();
    assert_ne!(first.as_bytes(), fresh.stdout.as_slice());
    assert_eq!(
        actual.as_bytes(),
        fresh.stdout.as_slice(),
        "stale toolchain version"
    );
}

#[cfg(unix)]
#[test]
fn unchanged_wrapper_refreshes_the_selected_toolchain() {
    wrapper_probe(false);
}

#[cfg(unix)]
#[test]
#[should_panic(expected = "stale toolchain version")]
fn negative_control_detects_retained_proxy_versions() {
    wrapper_probe(true);
}

#[test]
fn operation_boundaries_retire_metadata_on_error_and_unwind() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("input");
    fs::write(&file, b"input").unwrap();
    let stamp = sdk_file_identity::stamp(&file).unwrap();
    for unwind in [false, true] {
        with_run(|| {
            METADATA.with(|values| {
                METADATA_OBSERVED.with(|recent| {
                    observed_metadata(
                        values,
                        recent,
                        (file.clone(), "closed inventory".into()),
                        || Ok((stamp.clone(), stamp.clone())),
                        || Ok("metadata".into()),
                    )
                    .unwrap();
                })
            })
        });
        assert_eq!(METADATA.with_borrow(BTreeMap::len), 1);
        if unwind {
            assert!(std::panic::catch_unwind(|| with_run(|| panic!("operation unwind"))).is_err());
        } else {
            assert!(with_run(|| Err::<(), _>(CliError("operation error".into()))).is_err());
        }
        assert!(METADATA.with_borrow(BTreeMap::is_empty));
        assert!(METADATA_OBSERVED.with_borrow(BTreeSet::is_empty));
    }
}
