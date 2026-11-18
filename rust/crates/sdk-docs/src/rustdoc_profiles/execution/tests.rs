use super::*;
use proptest::prelude::*;
use std::cell::Cell;

#[test]
fn declared_workspace_metadata_round_trips_to_a_new_root() {
    let source = acyclic_test_support::source_path("workspace-metadata/workspace-metadata.json");
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let metadata = load_workspace_metadata(first.path(), &source).unwrap();
    let serialized = workspace_metadata_bytes(&metadata).unwrap();
    let graph = first.path().join("graph.json");
    fs::write(&graph, serialized).unwrap();
    let relocated = load_workspace_metadata(second.path(), &graph).unwrap();
    assert_eq!(metadata.packages.len(), relocated.packages.len());
    for (before, after) in metadata.packages.iter().zip(&relocated.packages) {
        assert_eq!(before.id, after.id);
        assert_eq!(
            before
                .manifest_path
                .as_std_path()
                .strip_prefix(first.path())
                .unwrap(),
            after
                .manifest_path
                .as_std_path()
                .strip_prefix(second.path())
                .unwrap(),
        );
    }
}

#[test]
fn workspace_metadata_rebases_percent_encoded_package_ids() {
    let source = acyclic_test_support::source_path("workspace-metadata/workspace-metadata.json");
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(source).unwrap()).unwrap();
    metadata["workspace_root"] = "/work/sdk checkout".into();
    metadata["packages"][0]["id"] =
        "path+file:///work/sdk%20checkout/crate%23one#crate@1.0.0".into();
    metadata["packages"][0]["manifest_path"] = "/work/sdk checkout/crate#one/Cargo.toml".into();
    let temp = tempfile::tempdir().unwrap();
    let graph = temp.path().join("graph.json");
    fs::write(&graph, serde_json::to_vec(&metadata).unwrap()).unwrap();

    let destination = temp.path().join("sdk #%");
    let loaded = load_workspace_metadata(&destination, &graph).unwrap();
    assert_eq!(
        loaded.packages[0].manifest_path.as_std_path(),
        destination.join("crate#one").join("Cargo.toml")
    );
    assert!(loaded.packages[0]
        .id
        .repr
        .ends_with("/sdk%20%23%25/crate%23one#crate@1.0.0"));
    let serialized: serde_json::Value =
        serde_json::from_slice(&workspace_metadata_bytes(&loaded).unwrap()).unwrap();
    assert_eq!(
        serialized["packages"][0]["id"],
        "path+file://./crate%23one#crate@1.0.0"
    );
}

#[test]
fn workspace_metadata_relocates_windows_path_components() {
    let mut graph = serde_json::json!({
        "manifest": r"\\?\C:\BUILD\SDK\crate\Cargo.toml",
        "target": "c:/build/sdk/crate/src/lib.rs",
        "sibling": "C:/build/sdk-other/Cargo.toml",
    });
    relocate_metadata(&mut graph, r"C:\build\sdk", ".");
    assert_eq!(graph["manifest"], "./crate/Cargo.toml");
    assert_eq!(graph["target"], "./crate/src/lib.rs");
    assert_eq!(graph["sibling"], "C:/build/sdk-other/Cargo.toml");

    let source = acyclic_test_support::source_path("workspace-metadata/workspace-metadata.json");
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(source).unwrap()).unwrap();
    metadata["workspace_root"] = "C:/build/sdk".into();
    metadata["packages"][0]["manifest_path"] = "C:/build/sdk-other/Cargo.toml".into();
    metadata["packages"][0]["dependencies"] = serde_json::json!([{
        "name": "external", "source": null, "req": "*", "kind": null,
        "rename": null, "optional": false, "uses_default_features": true,
        "features": [], "target": null, "registry": null, "path": "C:/build/sdk-other"
    }]);
    let temp = tempfile::tempdir().unwrap();
    let graph_path = temp.path().join("graph.json");
    fs::write(&graph_path, serde_json::to_vec(&metadata).unwrap()).unwrap();
    let loaded = load_workspace_metadata(temp.path(), &graph_path).unwrap();
    assert_eq!(
        loaded.packages[0].manifest_path.as_str(),
        "C:/build/sdk-other/Cargo.toml"
    );
    assert_eq!(
        loaded.packages[0].dependencies[0]
            .path
            .as_ref()
            .unwrap()
            .as_str(),
        "C:/build/sdk-other"
    );
    metadata["workspace_root"] = "Q:/sdk".into();
    metadata["packages"][0]["id"] =
        "path+file:///Q:/sdk/examples/sdk-services#sdk-service-examples@0.0.0".into();
    let metadata: Metadata = serde_json::from_value(metadata).unwrap();
    let encoded: serde_json::Value =
        serde_json::from_slice(&workspace_metadata_bytes(&metadata).unwrap()).unwrap();
    assert_eq!(
        encoded["packages"][0]["id"],
        "path+file://./examples/sdk-services#sdk-service-examples@0.0.0"
    );

    let mut posix = serde_json::json!({"manifest": r"/source/back\slash/Cargo.toml"});
    relocate_metadata(&mut posix, "/source", r"/destination/back\slash");
    assert_eq!(
        posix["manifest"],
        r"/destination/back\slash/back\slash/Cargo.toml"
    );
    let mut ids = serde_json::json!({
        "inside": "path+file:///work/sdk/dep#dep@1.0.0",
        "outside": "path+file:///archive/work/sdk/dep#dep@1.0.0"
    });
    relocate_metadata(&mut ids, "/work/sdk", "/checkout");
    assert_eq!(ids["inside"], "path+file:///checkout/dep#dep@1.0.0");
    assert_eq!(
        ids["outside"],
        "path+file:///archive/work/sdk/dep#dep@1.0.0"
    );
    let mut windows_id = serde_json::json!("path+file:///Q:/sdk/examples/pkg#pkg@1.0.0");
    relocate_metadata(&mut windows_id, "Q:/sdk", "D:/checkout");
    assert_eq!(
        windows_id,
        "path+file:///D:/checkout/examples/pkg#pkg@1.0.0"
    );
}
proptest! {
    #[test]
    fn producer_snapshots_follow_each_environment_and_end_with_the_operation(
        homes in prop::collection::vec("[a-z]{1,6}", 1..12),
    ) {
        let calls = Cell::new(0usize);
        let select = |home: &str| {
            let mut command = Command::new("rustup");
            command.env("RUSTUP_HOME", home);
            crate::with_command_environment(&command, || crate::render_cache::producer(|| {
                calls.set(calls.get()+1);
                Ok(RustdocProducer {
                    cargo: home.into(), targets: BTreeSet::from([home.into()]),
                    compiler: Compiler { rustc: home.into(), host: home.into(), cfgs: (home.into(), home.into()) },
                })
            }).unwrap())
        };
        crate::with_rustdoc_cache(|| {
            for home in &homes {
                let first = select(home);
                let second = select(home);
                prop_assert!(std::rc::Rc::ptr_eq(&first, &second));
                prop_assert_eq!(&first.cargo, &PathBuf::from(home));
                prop_assert_eq!(&first.compiler.host, home);
                prop_assert!(first.targets.contains(home));
            }
            prop_assert_eq!(calls.get(), homes.iter().collect::<BTreeSet<_>>().len());
            Ok(())
        })?;
        let before = calls.get();
        crate::with_rustdoc_cache(|| { select(&homes[0]); });
        prop_assert_eq!(calls.get(), before+1);
    }
}

#[cfg(unix)]
#[test]
fn scoped_rustup_homes_select_their_own_commands_targets_and_cfgs() {
    let fixture = acyclic_test_support::crate_path!("tests/fixtures/rustup");
    let rustup = fixture.join("rustup");
    crate::with_rustdoc_cache(|| {
        for name in ["a", "b", "a"] {
            let home = fixture.join(name);
            let mut command = Command::new(&rustup);
            command.env("PATH", &fixture).env("RUSTUP_HOME", &home);
            crate::with_command_environment(&command, || {
                let host = format!("fixture-{name}");
                assert_eq!(rustdoc_host().unwrap(), host);
                assert!(rustdoc_targets(&[&host]).is_ok());
                assert_eq!(
                    nightly_rustdoc_command(&host).unwrap().get_program(),
                    home.join("bin/cargo").canonicalize().unwrap()
                );
                assert!(rustdoc_platform_cfgs()
                    .unwrap()
                    .0
                    .contains(&format!("fixture_{name}")));
                let other = if name == "a" {
                    "fixture-b"
                } else {
                    "fixture-a"
                };
                assert!(matches!(
                    rustdoc_targets(&[other]),
                    Err(ProfileError::UnsupportedTarget { .. })
                ));
            });
        }
    });
}

#[cfg(unix)]
#[test]
fn a_new_operation_refreshes_installed_targets_in_the_same_home() {
    let fixtures = acyclic_test_support::crate_path!("tests/fixtures/rustup");
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir(home.path().join("bin")).unwrap();
    std::fs::write(home.path().join("bin/cargo"), "fixture Cargo").unwrap();
    std::os::unix::fs::symlink(fixtures.join("a/bin/rustc"), home.path().join("bin/rustc"))
        .unwrap();
    std::fs::write(home.path().join("targets"), "fixture-a\nfixture-added\n").unwrap();
    let mut command = Command::new(fixtures.join("rustup"));
    command
        .env("PATH", &fixtures)
        .env("RUSTUP_HOME", home.path());
    crate::with_command_environment(&command, || {
        crate::with_rustdoc_cache(|| {
            assert!(rustdoc_targets(&["fixture-added"]).is_ok());
        });
        std::fs::write(home.path().join("targets"), "fixture-a\n").unwrap();
        crate::with_rustdoc_cache(|| {
            assert!(matches!(
                rustdoc_targets(&["fixture-added"]),
                Err(ProfileError::UnsupportedTarget { .. })
            ));
        });
    });
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn worker_inheritance_is_environment_bound_and_does_not_escape_into_new_operations(
        homes in prop::collection::vec("[a-z]{1,6}", 1..8),
    ) {
        for home in homes {
            let mut environment = Command::new("rustup");
            environment.env("RUSTUP_HOME", &home);
            let select = || crate::render_cache::producer(|| Ok(RustdocProducer {
                cargo: home.clone().into(), targets: BTreeSet::from([home.clone()]),
                compiler: Compiler { rustc: home.clone().into(), host: home.clone(), cfgs: (home.clone(), home.clone()) },
            })).unwrap();
            crate::with_command_environment(&environment, || crate::with_rustdoc_cache(|| {
                select();
                let worker = crate::rustdoc_worker(|_| {
                    let cached = crate::render_cache::producer(|| panic!("worker repeated selection")).unwrap();
                    assert_eq!(cached.cargo, PathBuf::from(&home));
                    assert!(cached.targets.contains(&home));
                    let mut other = Command::new("rustup");
                    other.env("RUSTUP_HOME", format!("other-{home}"));
                    let calls = Cell::new(0);
                    crate::with_command_environment(&other, || {
                        crate::render_cache::producer(|| { calls.set(calls.get()+1); Ok((*cached).clone()) }).unwrap();
                    });
                    assert_eq!(calls.get(), 1, "another environment must resolve afresh");
                });
                // Move the immutable snapshot through the actual production worker path.
                std::thread::scope(|scope| {
                    let worker = &worker;
                    let a = scope.spawn(move || worker(()));
                    let b = scope.spawn(move || worker(()));
                    a.join().unwrap(); b.join().unwrap();
                });
            }));
            crate::with_command_environment(&environment, || crate::with_rustdoc_cache(|| {
                prop_assert!(crate::render_cache::producer(|| Err(ProfileError::InvalidRustdoc("fresh selection".into()))).is_err());
                Ok(())
            }))?;
        }
    }
}
