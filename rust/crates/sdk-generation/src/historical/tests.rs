// Controlled protocol bytes exercise the production reader; this is not a parser model.
fn registry_owner_source_inner(
    repository: &Path,
    root: &Path,
    revision: &str,
    cargo: &Path,
    capture: bool,
    authority: Option<&BTreeSet<String>>,
) -> Result<RegistryOwnerSource, String> {
    let (source, _) = registry_source(repository, root, revision, capture)?;
    discover_owner(root, cargo, source, authority)
}

fn batch_protocol_fixture(
    response: &[u8],
    hold: bool,
    consume: impl FnMut(&str, &[u8]) -> Result<(), String>,
) -> (Result<(), String>, std::process::ExitStatus) {
    #[cfg(not(windows))]
    let fixture = Fixture::new();
    let object = "a".repeat(40);
    let listing = format!("100644 blob {object}\toriginal.bin\0");
    #[cfg(windows)]
    let mut command = {
        let bytes = response
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let script = format!(
            "$ErrorActionPreference = 'Stop'\nif ([Console]::ReadLine() -ne '{object}') {{ exit 42 }}\n[byte[]]$bytes = @({bytes})\n$out = [Console]::OpenStandardOutput()\n$out.Write($bytes, 0, $bytes.Length)\n$out.Flush()\n{}\nexit 0\n",
            if hold { "Start-Sleep -Seconds 10" } else { "" }
        );
        let executable =
            std::path::PathBuf::from(std::env::var_os("SystemRoot").expect("Windows system root"))
                .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let mut command = Command::new(executable);
        command
            .args(["-NoLogo", "-NoProfile", "-NonInteractive", "-Command"])
            .arg(script);
        command
    };
    #[cfg(not(windows))]
    let mut command = {
        let script = fixture.0.join("batch.sh");
        // POSIX shell builtin printf writes arbitrary binary bytes without an extra process.
        let bytes = response
            .iter()
            .map(|byte| format!("\\0{byte:03o}"))
            .collect::<String>();
        fs::write(&script, format!(
            "IFS= read -r object\n[ \"$object\" = '{object}' ] || exit 42\nprintf '%b' '{bytes}'\n{}\n",
            if hold { "exec sleep 10" } else { "exit 0" }
        )).unwrap();
        let mut command = Command::new("/bin/sh");
        command.arg(script);
        command
    };
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let result = read_git_source_blobs(&mut child, listing.as_bytes(), consume);
    // Retain the actual child handle: a missing drain cannot masquerade as a returned parser error.
    let status = child
        .try_wait()
        .unwrap()
        .expect("batch child must be terminated before returning");
    (result, status)
}

fn fixture_blob_response(body: &[u8]) -> Vec<u8> {
    let mut response = format!("{} blob {}\n", "a".repeat(40), body.len()).into_bytes();
    response.extend_from_slice(body);
    response.push(b'\n');
    response
}

#[test]
fn git_batch_reader_preserves_binary_body_and_waits_for_successful_child() {
    let body = [0, 255, b'\n', 0];
    let mut received = None;
    let (result, status) =
        batch_protocol_fixture(&fixture_blob_response(&body), false, |path, bytes| {
            assert_eq!(path, "original.bin");
            received = Some(bytes.to_vec());
            Ok(())
        });
    assert_eq!(result, Ok(()));
    assert!(status.success());
    assert_eq!(received.as_deref(), Some(body.as_slice()));
}

#[test]
fn git_batch_reader_rejects_malformed_protocol_and_kills_waiting_child() {
    let cases = [
        (
            b"malformed header\n".to_vec(),
            "invalid Git batch object header",
        ),
        (
            format!("{} blob 0\n\n", "b".repeat(40)).into_bytes(),
            "object identity differs",
        ),
        (
            format!("{} tree 0\n\n", "a".repeat(40)).into_bytes(),
            "object identity differs",
        ),
        (
            format!("{} blob not-a-size\n", "a".repeat(40)).into_bytes(),
            "invalid digit",
        ),
        (
            format!("{} blob {}\n", "a".repeat(40), "9".repeat(100)).into_bytes(),
            "number too large",
        ),
        (
            {
                let mut bytes = format!("{} blob 1\n", "a".repeat(40)).into_bytes();
                bytes.extend_from_slice(&[0, b'!']);
                bytes
            },
            "invalid Git batch object boundary",
        ),
    ];
    for (response, expected) in cases {
        let mut called = false;
        let (result, status) = batch_protocol_fixture(&response, true, |_, _| {
            called = true;
            Ok(())
        });
        assert!(
            result.unwrap_err().contains(expected),
            "wrong protocol failure branch"
        );
        assert!(!called, "malformed body reached consumer");
        // The fixture would exit successfully after its hold; failure proves forced termination.
        assert!(
            !status.success(),
            "reader waited for normal fixture exit instead of killing it"
        );
    }
}

#[test]
fn git_batch_reader_rejects_truncated_eof_and_drains_exited_child() {
    let mut response = format!("{} blob 4\n", "a".repeat(40)).into_bytes();
    response.extend_from_slice(&[0, 255]);
    let (result, status) = batch_protocol_fixture(&response, false, |_, _| {
        panic!("truncated body reached consumer")
    });
    assert!(result.is_err());
    assert!(status.success()); // Protocol failure is distinct from the producer exit status.
}

#[test]
fn git_batch_reader_propagates_callback_failure_and_terminates_child() {
    let body = [0, 255, b'\n', 0];
    let (result, status) =
        batch_protocol_fixture(&fixture_blob_response(&body), true, |_, bytes| {
            assert_eq!(bytes, body);
            Err("callback refused fixture".into())
        });
    assert_eq!(result, Err("callback refused fixture".into()));
    assert!(!status.success());
}

#[cfg(unix)]
#[test]
fn registry_frontier_rejects_redirect_before_first_export_write() {
    let fixture = Fixture::new();
    let repository = fixture.0.join("repository");
    let foreign = fixture.0.join("foreign");
    fs::create_dir(&repository).unwrap();
    fs::create_dir(&foreign).unwrap();
    fs::write(foreign.join("sentinel"), "preserve").unwrap();
    let linked = fixture.0.join("frontier");
    std::os::unix::fs::symlink(&foreign, &linked).unwrap();
    let error = capture_registry_corpus(
        &repository,
        BTreeSet::from(["a".repeat(40)]),
        &linked.join("child"),
        Path::new("absent-cargo"),
    )
    .unwrap_err();
    assert!(error.contains("linked"));
    assert_eq!(fs::read(foreign.join("sentinel")).unwrap(), b"preserve");
    assert!(!foreign.join("child").exists());
}

#[test]
fn original_git_links_are_opaque_and_cannot_supply_cargo_inputs() {
    let fixture = Fixture::new();
    let repository = fixture.0.join("repository");
    fs::create_dir_all(repository.join("src")).unwrap();
    fs::create_dir_all(repository.join("release")).unwrap();
    fs::write(
        repository.join("Cargo.toml"),
        "[package]\nname='demo'\nversion='0.1.0'\nedition='2021'\n[workspace]\n",
    )
    .unwrap();
    fs::write(repository.join("src/lib.rs"), "pub fn demo() {}\n").unwrap();
    fs::write(repository.join("release/cargo-crates.json"), "[\"demo\"]").unwrap();
    let git = |args: &[&str]| {
        command_output(
            Command::new(frontier_git().unwrap())
                .current_dir(&repository)
                .args(args),
        )
        .unwrap()
    };
    git(&["init", "--quiet"]);
    git(&["add", "."]);
    let blob_file = fixture.0.join("link-blob");
    let link_bytes = b"../../../../foreign-outside-workspace";
    fs::write(&blob_file, link_bytes).unwrap();
    let object =
        String::from_utf8(git(&["hash-object", "-w", blob_file.to_str().unwrap()])).unwrap();
    let object = object.trim();
    let link_path = "typescript/node_modules/typescript";
    git(&[
        "update-index",
        "--add",
        "--cacheinfo",
        &format!("120000,{object},{link_path}"),
    ]);
    let commit = || {
        git(&[
            "-c",
            "commit.gpgsign=false",
            "-c",
            "user.name=Source Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "Git link fixture",
        ]);
        String::from_utf8(git(&["rev-parse", "HEAD"]))
            .unwrap()
            .trim()
            .to_owned()
    };
    let revision = commit();
    let export = fixture.0.join("export");
    fs::create_dir(&export).unwrap();
    let captured =
        registry_owner_source_inner(&repository, &export, &revision, &cargo(), true, None).unwrap();
    assert_eq!(captured.symlink_paths, BTreeSet::from([link_path.into()]));
    assert_eq!(captured.owners, BTreeSet::from(["demo".into()]));
    assert_eq!(fs::read(export.join(link_path)).unwrap(), link_bytes);
    assert!(fs::symlink_metadata(export.join(link_path))
        .unwrap()
        .is_file());
    assert_eq!(
        registry_owner_source_inner(&repository, &export, &revision, &cargo(), false, None)
            .unwrap(),
        captured
    );
    fs::write(export.join(link_path), "tampered link bytes").unwrap();
    assert!(
        registry_owner_source_inner(&repository, &export, &revision, &cargo(), false, None)
            .unwrap_err()
            .contains("differs from declared Git source")
    );
    for (index, linked_input) in ["src/lib.rs", ".cargo/config.toml"].iter().enumerate() {
        git(&[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("120000,{object},{linked_input}"),
        ]);
        let revision = commit();
        let export = fixture.0.join(format!("rejected-{index}"));
        fs::create_dir(&export).unwrap();
        let error =
            registry_owner_source_inner(&repository, &export, &revision, &cargo(), true, None)
                .unwrap_err();
        assert!(error.contains("original Git link is a Cargo"), "{error}");
    }
}

#[test]
fn original_release_history_retains_retired_owner_and_rejects_missing_frontier() {
    let fixture = Fixture::new();
    let repository = fixture.0.join("repository");
    fs::create_dir_all(repository.join("release")).unwrap();
    fs::write(
        repository.join(".gitattributes"),
        "original.bin export-ignore\n",
    )
    .unwrap();
    fs::write(repository.join("original.bin"), [0, 255, b'\n', 0]).unwrap();
    for package in ["current-owner", "retired-owner"] {
        let root = repository.join(package);
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(
            root.join("Cargo.toml"),
            format!("[package]\nname = {package:?}\nversion = \"0.1.1\"\nedition = \"2021\"\n"),
        )
        .unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn fixture() {}\n").unwrap();
    }
    fs::write(
        repository.join("Cargo.toml"),
        "[workspace]\nmembers = [\"current-owner\", \"retired-owner\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    command_output(
        Command::new(frontier_git().unwrap())
            .current_dir(&repository)
            .arg("init"),
    )
    .unwrap();
    let commit = || {
        command_output(
            Command::new(frontier_git().unwrap())
                .current_dir(&repository)
                .args(["add", "."]),
        )
        .unwrap();
        command_output(
            Command::new(frontier_git().unwrap())
                .current_dir(&repository)
                .args([
                    "-c",
                    "commit.gpgsign=false",
                    "-c",
                    "user.name=Source Fixture",
                    "-c",
                    "user.email=fixture@example.invalid",
                    "commit",
                    "-m",
                    "Source fixture",
                ]),
        )
        .unwrap();
        String::from_utf8(
            command_output(
                Command::new(frontier_git().unwrap())
                    .current_dir(&repository)
                    .args(["rev-parse", "HEAD"]),
            )
            .unwrap(),
        )
        .unwrap()
        .trim()
        .to_owned()
    };
    let manifest = repository.join("retired-owner/Cargo.toml");
    let source = fs::read_to_string(&manifest).unwrap();
    let before_index = commit();
    fs::write(&manifest, format!("{source}publish = false\n")).unwrap();
    fs::write(
        repository.join("release/cargo-crates.json"),
        "[\"current-owner\", \"retired-owner\"]",
    )
    .unwrap();
    let original = commit();
    fs::write(&manifest, &source).unwrap();
    let public_again = commit();
    fs::write(&manifest, format!("{source}publish = false\n")).unwrap();
    let private = commit();
    fs::write(
        repository.join("Cargo.toml"),
        "[workspace]\nmembers = [\"current-owner\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    fs::remove_dir_all(repository.join("retired-owner")).unwrap();
    let current = commit();
    let anchors = BTreeSet::from([current.clone()]);
    let revisions = frontier_revisions(&repository, &anchors).unwrap();
    assert_eq!(
        revisions,
        BTreeSet::from([
            before_index.clone(),
            original,
            private,
            public_again.clone(),
            current.clone()
        ])
    );
    let (sources, authority) = original_sources(&repository, &fixture.0, &revisions, true).unwrap();
    assert_eq!(
        authority,
        BTreeSet::from(["current-owner".into(), "retired-owner".into()])
    );
    for source in sources {
        let revision = source.revision.clone();
        let export = fixture.0.join(&revision);
        let captured = discover_owner(&export, &cargo(), source, Some(&authority)).unwrap();
        assert_eq!(captured.uses_frontier_authority, revision == before_index);
        assert_eq!(
            captured.owners.contains("retired-owner"),
            revision == before_index || revision == public_again
        );
        assert_eq!(
            captured.absent_authority_members.contains("retired-owner"),
            revision == current
        );
        assert_eq!(
            fs::read(export.join("original.bin")).unwrap(),
            [0, 255, b'\n', 0]
        );
        assert_eq!(
            registry_owner_source_inner(
                &repository,
                &export,
                &revision,
                &cargo(),
                false,
                Some(&authority),
            )
            .unwrap(),
            captured
        );
    }
    // The complete corpus source vector cannot substitute current-only
    // authority for the history computed from these immutable anchors.
    let corpus = RegistryCorpus {
        repository: crate::path_string(&repository),
        declared_anchors: anchors.clone(),
        anchors,
        original_release_authority: BTreeSet::new(),
        frontier: vec![],
        responses: BTreeMap::new(),
        publisher_anchors: BTreeMap::new(),
    };
    assert!(verify_registry_corpus(
        &corpus,
        &fixture.0.join("frontier"),
        Path::new("absent-cargo"),
        "0.1.1",
        &fixture.0
    )
    .unwrap_err()
    .contains("empty"));
}

#[test]
fn cohort_publisher_anchors_require_immutable_archive_checksum_lineage() {
    let fixture = Fixture::new();
    let publisher = "a".repeat(40);
    let (release, archive) = fixture.archive_with_dirty(&publisher, "original-owner", true);
    let archives = fixture.0.join("cohort");
    fs::create_dir(&archives).unwrap();
    let name = "historical-fixture-0.1.0.crate";
    let retained = archives.join(name);
    fs::copy(&archive, &retained).unwrap();
    let inventory = BTreeMap::from([(
        "historical-fixture".into(),
        RegistryOwnerVersions {
            response_sha256: None,
            versions: vec![release],
        },
    )]);
    let facts = cohort_publishers(&inventory, "0.1.0", &archives).unwrap();
    let vcs = facts[name].as_ref().unwrap();
    assert!(vcs.dirty);
    assert_eq!(vcs.path_in_vcs, "original-owner");
    let declared = BTreeSet::from(["b".repeat(40)]);
    assert_eq!(
        publisher_anchor_union(&declared, &facts),
        BTreeSet::from([publisher, "b".repeat(40)])
    );
    fs::write(&retained, "different published bytes").unwrap();
    assert!(cohort_publishers(&inventory, "0.1.0", &archives)
        .unwrap_err()
        .contains("checksum"));
}

#[test]
fn registry_frontier_rejects_missing_or_nonimmutable_history_before_git() {
    assert!(frontier_revisions(Path::new("absent"), &BTreeSet::new())
        .unwrap_err()
        .contains("immutable Git"));
    assert!(
        frontier_revisions(Path::new("absent"), &BTreeSet::from(["HEAD".into()]))
            .unwrap_err()
            .contains("immutable Git")
    );
    let corpus = RegistryCorpus {
        repository: "absent-repository".into(),
        declared_anchors: BTreeSet::new(),
        anchors: BTreeSet::from(["a".repeat(40)]),
        original_release_authority: BTreeSet::new(),
        frontier: vec![RegistryOwnerSource {
            revision: "a".repeat(40),
            input_sha256: BTreeMap::new(),
            symlink_paths: BTreeSet::new(),
            discovery: CargoDiscoveryInputs::default(),
            owners: BTreeSet::new(),
            outside_release_authority: BTreeSet::new(),
            uses_frontier_authority: true,
            absent_authority_members: BTreeSet::new(),
        }],
        responses: BTreeMap::new(),
        publisher_anchors: BTreeMap::new(),
    };
    assert!(verify_registry_corpus(
        &corpus,
        Path::new("absent"),
        Path::new("absent-cargo"),
        "0.1.0",
        Path::new("absent")
    )
    .unwrap_err()
    .contains("declared original history"));
}
use super::*;

#[test]
fn windows_discovery_environment_rejects_conflicting_case_aliases() {
    let values = [
        ("PATH".into(), "first-toolchain".into()),
        ("Path".into(), "second-toolchain".into()),
    ];
    for inputs in [values.clone(), [values[1].clone(), values[0].clone()]] {
        let error = discovery_environment_inputs(inputs, true).unwrap_err();
        assert!(error.contains("conflicting case aliases"));
    }
}

#[test]
fn windows_discovery_environment_retains_equal_path_aliases() {
    let inputs = [
        ("Path".into(), "same-toolchain".into()),
        ("PATH".into(), "same-toolchain".into()),
    ];
    let retained = discovery_environment_inputs(inputs, true).unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained["PATH"], crate::sha256_bytes(b"same-toolchain"));
}

#[test]
fn posix_discovery_environment_keeps_distinct_case_sensitive_names() {
    let inputs = [
        ("CARGO_TEST_VALUE".into(), "upper".into()),
        ("CARGO_test_value".into(), "lower".into()),
    ];
    let retained = discovery_environment_inputs(inputs, false).unwrap();
    assert_eq!(retained.len(), 2);
    assert_ne!(retained["CARGO_TEST_VALUE"], retained["CARGO_test_value"]);
}

#[test]
fn crates_io_publish_eligibility_respects_the_registry_allowlist() {
    assert!(crates_io_eligible(None));
    assert!(!crates_io_eligible(Some(&[])));
    assert!(crates_io_eligible(Some(&["crates-io".into()])));
    assert!(!crates_io_eligible(Some(&["private".into()])));
    assert!(crates_io_eligible(Some(&[
        "private".into(),
        "crates-io".into()
    ])));
    assert!(!crates_io_eligible(Some(&["crates-io-mirror".into()])));
}

#[test]
fn complete_registry_inventory_keeps_yanked_prereleases_and_rejects_partial_sets() {
    let response = serde_json::json!({
        "versions": [
            {"crate":"historical-fixture","num":"0.1.0","checksum":"a".repeat(64),"yanked":false},
            {"crate":"historical-fixture","num":"0.1.1","checksum":"b".repeat(64),"yanked":true},
            {"crate":"historical-fixture","num":"1.0.0-rc.3","checksum":"c".repeat(64),"yanked":true}
        ],
        "meta":{"total":3,"next_page":null}
    });
    let encode = |v: &serde_json::Value| serde_json::to_vec(v).unwrap();
    let versions = decode_registry_versions("historical-fixture", &encode(&response)).unwrap();
    assert_eq!(versions.len(), 3);
    assert_eq!(versions.iter().filter(|v| v.yanked).count(), 2);
    let mut truncated = response.clone();
    truncated["versions"].as_array_mut().unwrap().pop();
    assert!(
        decode_registry_versions("historical-fixture", &encode(&truncated))
            .unwrap_err()
            .contains("incomplete")
    );
    let mut paged = response.clone();
    paged["meta"]["next_page"] = "?seek=retained-next".into();
    assert!(decode_registry_versions("historical-fixture", &encode(&paged)).is_err());
    let mut duplicate = response.clone();
    duplicate["versions"][1] = duplicate["versions"][0].clone();
    assert!(
        decode_registry_versions("historical-fixture", &encode(&duplicate))
            .unwrap_err()
            .contains("duplicate")
    );
    let mut wrong_owner = response.clone();
    wrong_owner["versions"][0]["crate"] = "different-owner".into();
    assert!(decode_registry_versions("historical-fixture", &encode(&wrong_owner)).is_err());
    let mut checksum = response.clone();
    checksum["versions"][0]["checksum"] = "not-a-checksum".into();
    assert!(decode_registry_versions("historical-fixture", &encode(&checksum)).is_err());
}

#[test]
fn complete_registry_cohort_rejects_missing_retired_owner_before_import() {
    let fixture = Fixture::new();
    let archive_dir = fixture.0.join("inventory-inputs");
    fs::create_dir(&archive_dir).unwrap();
    let mut inventory = BTreeMap::new();
    for package in ["current-owner", "retired-owner"] {
        inventory.insert(
            package.into(),
            RegistryOwnerVersions {
                response_sha256: Some(format!("sha256:{}", "a".repeat(64))),
                versions: vec![RegistryVersion {
                    package: package.into(),
                    num: "0.1.1".into(),
                    checksum: "b".repeat(64),
                    yanked: true,
                }],
            },
        );
    }
    fs::write(
        archive_dir.join("current-owner-0.1.1.crate"),
        b"not imported by this gate",
    )
    .unwrap();
    assert!(validate_registry_cohort("0.1.1", &inventory, &archive_dir).is_err());
    fs::write(
        archive_dir.join("retired-owner-0.1.1.crate"),
        b"not imported by this gate",
    )
    .unwrap();
    validate_registry_cohort("0.1.1", &inventory, &archive_dir).unwrap();
    fs::write(archive_dir.join("unregistered-owner-0.1.1.crate"), b"extra").unwrap();
    assert!(validate_registry_cohort("0.1.1", &inventory, &archive_dir).is_err());
    assert!(!archive_dir.join("sources").exists());
}

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "sdk-historical-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(root.join("examples")).unwrap();
        fs::write(root.join("Cargo.toml"), "[package]\nname='historical-fixture'\nversion='0.1.0'\nedition='2021'\n[workspace]\n[features]\nold-feature=[]\n[[example]]\nname='old-example'\nrequired-features=['old-feature']\n").unwrap();
        fs::write(
            root.join("src/lib.rs"),
            "//! Historical source.\npub fn value() -> u8 { 7 }\n",
        )
        .unwrap();
        fs::write(
            root.join("examples/old-example.rs"),
            "fn main() { println!(\"{}\", historical_fixture::value()); }\n",
        )
        .unwrap();
        fs::write(root.join("Cargo.lock"), "# This file is automatically @generated by Cargo.\nversion = 4\n[[package]]\nname = \"historical-fixture\"\nversion = \"0.1.0\"\n").unwrap();
        Self(root)
    }
    fn metadata(&self) -> Metadata {
        sdk_docs::rustdoc_profiles::load_metadata(
            self.0.join("Cargo.toml"),
            sdk_docs::rustdoc_profiles::CargoExecutionContext {
                cargo_path: Some(&cargo()),
                lock_selection: None,
            },
        )
        .unwrap()
    }
    fn archive(&self, revision: &str, vcs_path: &str) -> (RegistryVersion, PathBuf) {
        self.archive_with_dirty(revision, vcs_path, false)
    }
    fn archive_with_dirty(
        &self,
        revision: &str,
        vcs_path: &str,
        dirty: bool,
    ) -> (RegistryVersion, PathBuf) {
        let entry = self.0.join("historical-fixture-0.1.0");
        fs::create_dir_all(&entry).unwrap();
        fs::write(
            entry.join(".cargo_vcs_info.json"),
            serde_json::to_vec(
                &serde_json::json!({"git":{"sha1": revision,"dirty":dirty}, "path_in_vcs": vcs_path}),
            )
            .unwrap(),
        )
        .unwrap();
        fs::create_dir_all(entry.join("src")).unwrap();
        fs::create_dir_all(entry.join("examples")).unwrap();
        fs::copy(self.0.join("src/lib.rs"), entry.join("src/lib.rs")).unwrap();
        fs::copy(
            self.0.join("examples/old-example.rs"),
            entry.join("examples/old-example.rs"),
        )
        .unwrap();
        fs::copy(self.0.join("Cargo.toml"), entry.join("Cargo.toml.orig")).unwrap();
        let archive = self.0.join("release.crate");
        command_output(
            Command::new("tar")
                .arg("-cf")
                .arg(&archive)
                .arg("-C")
                .arg(&self.0)
                .arg("historical-fixture-0.1.0"),
        )
        .unwrap();
        let checksum = crate::sha256_file(&archive)
            .unwrap()
            .trim_start_matches("sha256:")
            .to_owned();
        (
            RegistryVersion {
                package: "historical-fixture".into(),
                num: "0.1.0".into(),
                checksum,
                yanked: true,
            },
            archive,
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn cargo() -> PathBuf {
    PathBuf::from(env!("CARGO"))
}
#[test]
fn dependency_archive_initial_import_uses_verified_registry_facts() {
    if let Some(root) = std::env::var_os("SDK_DOCS_DEPENDENCY_IMPORT_ROOT") {
        let root = PathBuf::from(root);
        let (dependencies, files) =
            producer_dependencies(&root, "historical-fixture-0.1.0", None).unwrap();
        fs::write(
            root.join("transport/import.json"),
            serde_json::to_vec(&(dependencies, files)).unwrap(),
        )
        .unwrap();
    }
}

mod archives;
mod dependency;

mod archive_codec;
