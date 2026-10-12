use super::*;
use crate::pi_fork::tests::{policy, work};
use crate::pi_root::PiRootIntent;
use crate::pi_root_inputs::{
    PiRootRequest, ROOT_INPUT_DIRECTORY, stage_root_command, stage_root_request,
};
use acyclic_harness::{
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOwner},
    filesystem::FilesystemHost,
};
use serde_json::json;
use std::fs;
#[path = "pi_root_preparation_tests.rs"]
mod root_preparation;
#[path = "pi_service_tests.rs"]
mod service;

fn root_policy() -> PiOwnerPolicy {
    let mut policy = policy();
    let project = VolumeRef::new(
        policy.journal.provider().clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )
    .unwrap();
    policy.grants = Capabilities::new(
        policy.grants.iter().map(str::to_owned).chain([
            policy
                .journal
                .directory_read_capability(ROOT_INPUT_DIRECTORY, &mut None)
                .unwrap(),
            project
                .capability(VolumeOperation::Read, &mut None)
                .unwrap(),
        ]),
    );
    policy
}

async fn root_input_fixture(
    session: &Path,
    raw: acyclic_fs::LocalFs,
    policy: PiOwnerPolicy,
) -> (
    PiOwnerConfig,
    FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    PiRootRequest,
    acyclic_fs::LocalFs,
) {
    let project = VolumeRef::new(
        policy.journal.provider().clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )
    .unwrap();
    let request = PiRootRequest {
        binding: crate::PiTaskBinding {
            native_session_id: "pi-root-input".into(),
            task_id: policy.root_task,
            agent_id: policy.agent,
            project_volumes: std::collections::BTreeMap::from([(hex::encode([83; 16]), project)]),
            results: None,
            scratch: crate::PiScratchBinding {
                volume: VolumeRef::new(
                    policy.journal.provider().clone(),
                    "scratch".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(policy.agent),
                    &mut None,
                )
                .unwrap(),
                allocation: crate::PiScratchAllocation::Preparing,
            },
        },
        root: PiRootIntent::Project {
            context_id: [84; 16],
            root_id: [83; 16],
            paths: std::collections::BTreeMap::from([(
                hex::encode([83; 16]),
                session
                    .join("unopened-selected-root")
                    .to_str()
                    .unwrap()
                    .into(),
            )]),
        },
    };
    let owner = PiOwnerConfig::new(&policy, None, &work()).unwrap();
    publish_owner(&owner, session, &work()).unwrap();
    let filesystem =
        FilesystemHost::new(raw.clone(), owner.policy.journal.provider().clone()).unwrap();
    filesystem
        .create_volume(&owner.policy.journal)
        .await
        .unwrap();
    (owner, filesystem, request, raw)
}

pub(crate) fn publish_owner(
    owner: &PiOwnerConfig,
    path: &Path,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let root =
        acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(path, work)
            .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    owner.save_new_with_native_work(&root, Path::new(""), work)
}

fn credential_fixture(policy: &PiOwnerPolicy) -> (tempfile::TempDir, PiOwnerConfig) {
    let directory = tempfile::tempdir().unwrap();
    let owner = PiOwnerConfig::new(policy, None, &work()).unwrap();
    publish_owner(&owner, directory.path(), &work()).unwrap();
    (directory, owner)
}

#[tokio::test]
async fn root_inputs_retain_original_artifacts_and_narrow_read_authority() {
    let session = tempfile::tempdir().unwrap();
    let raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
        session.path().join("filesystem"),
    ))
    .await
    .unwrap();
    let (owner, filesystem, request, _) =
        root_input_fixture(session.path(), raw, root_policy()).await;
    let work = work();
    let original = stage_root_request(&owner, &filesystem, &request, &work)
        .await
        .unwrap();
    let command = stage_root_command(&owner, &filesystem, &request, &work)
        .await
        .unwrap();
    let authority = owner.authority().unwrap();
    let issuer = AuthorityIssuer::new(ISSUER, owner.key, authority.verifier.audience().clone());
    let signed = issuer
        .issue(
            &owner.policy.root_task.to_string(),
            &owner.policy.grants,
            Some(owner.policy.agent),
            None,
            &mut None,
        )
        .unwrap();
    let read = ContentGrant::verify_read(&authority.verifier, &signed, &original.0).unwrap();
    assert_eq!(
        filesystem
            .read_content(&original.0, &read, owner.policy.maximum_payload_bytes, None)
            .await
            .unwrap()
            .as_ref(),
        serde_json::to_vec(&request).unwrap()
    );
    assert!(
        ContentGrant::verify(
            &authority.verifier,
            &signed,
            &owner.policy.journal,
            VolumeOperation::Write,
            u64::MAX,
            &mut None
        )
        .is_err()
    );
    for path in [
        "pi-inputs-sibling/root.json",
        ".system/harness-uploads/receipt.json",
    ] {
        let foreign = FileRef::new(
            original.0.volume().clone(),
            path.to_owned(),
            original.0.version().to_owned(),
            original.0.descriptor().clone(),
            "root.json".into(),
            &mut None,
        )
        .unwrap();
        assert!(ContentGrant::verify_read(&authority.verifier, &signed, &foreign).is_err());
    }
    let mut changed = request.clone();
    let PiRootIntent::Project { paths, .. } = &mut changed.root else {
        unreachable!()
    };
    *paths.values_mut().next().unwrap() = session
        .path()
        .join("changed-selected-root")
        .to_str()
        .unwrap()
        .into();
    assert!(
        stage_root_request(&owner, &filesystem, &changed, &work)
            .await
            .is_err()
    );
    drop(filesystem);
    let raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
        session.path().join("filesystem"),
    ))
    .await
    .unwrap();
    let filesystem = FilesystemHost::new(raw, owner.policy.journal.provider().clone()).unwrap();
    assert_eq!(
        stage_root_request(&owner, &filesystem, &request, &work)
            .await
            .unwrap()
            .0,
        original.0
    );
    assert_eq!(
        stage_root_command(&owner, &filesystem, &request, &work)
            .await
            .unwrap()
            .0,
        command.0
    );
    assert!(!session.path().join("unopened-selected-root").exists());
    assert!(!session.path().join("changed-selected-root").exists());
}

#[tokio::test]
async fn root_input_denial_preserves_storage_and_selected_paths() {
    for failure in [
        "owner",
        "grant",
        "bytes",
        "path",
        "foreign",
        "allocation",
        "hash",
        "cancel",
    ] {
        let session = tempfile::tempdir().unwrap();
        let mut policy = root_policy();
        if failure == "grant" {
            policy.grants = Capabilities::new(
                policy
                    .grants
                    .iter()
                    .filter(|grant| {
                        *grant
                            != policy
                                .journal
                                .directory_read_capability(ROOT_INPUT_DIRECTORY, &mut None)
                                .unwrap()
                    })
                    .map(str::to_owned),
            );
        }
        if failure == "bytes" {
            policy.maximum_payload_bytes = 16;
        }
        if failure == "path" {
            policy.limits.path_bytes = 1;
        }
        let raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
            session.path().join("filesystem"),
        ))
        .await
        .unwrap();
        let (owner, filesystem, mut request, raw) =
            root_input_fixture(session.path(), raw, policy).await;
        let observation = work();
        let workspace = raw
            .open_workspace_with_native_work(
                &owner.policy.journal.storage_name(&mut None).unwrap(),
                &observation,
                observation.cancellation(),
            )
            .await
            .unwrap();
        let head = workspace
            .head(&observation, observation.cancellation())
            .await
            .unwrap()
            .id();
        if failure == "owner" {
            fs::remove_file(session.path().join(OWNER_FILE)).unwrap();
        }
        if failure == "foreign" {
            request.binding.task_id = TaskId::from_bytes([85; 16]);
        }
        let mut budget = crate::pi_root_provider::allocation_budget();
        if failure == "allocation" {
            budget.peak_allocation_bytes =
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 1;
        }
        if failure == "hash" {
            budget.bytes_hashed = 0;
        }
        let query = acyclic_fs::NativeMaterializationWork::new(
            budget,
            &acyclic_fs::CancellationToken::new(),
        )
        .unwrap();
        if failure == "cancel" {
            query.cancellation().cancel();
        }
        let denied = match PiOwnerConfig::load(session.path()) {
            Ok(owner) => stage_root_command(&owner, &filesystem, &request, &query).await,
            Err(error) => Err(error),
        };
        assert!(denied.is_err(), "accepted {failure}");
        assert_eq!(
            workspace
                .head(&observation, observation.cancellation())
                .await
                .unwrap()
                .id(),
            head
        );
        assert!(!session.path().join("unopened-selected-root").exists());
    }
}

#[test]
fn owner_reopen_preserves_expired_policy_and_original_authority() {
    let mut policy = policy();
    policy.run_limits.deadline_epoch_ms = Some(1);
    let (directory, owner) = credential_fixture(&policy);
    let original = owner.authority().unwrap();
    let bytes = fs::read(directory.path().join(OWNER_FILE)).unwrap();
    for _ in 0..2 {
        let reopened = PiOwnerConfig::load(directory.path()).unwrap();
        assert!(reopened == owner);
        let authority = reopened.authority().unwrap();
        authority
            .verifier
            .verify(&original.signed, &mut None)
            .unwrap();
        assert_eq!(authority.signed, original.signed);
        assert_eq!(authority.scope.run_limits(), original.scope.run_limits());
    }
    let replacement = PiOwnerConfig::new(&policy, None, &work()).unwrap();
    assert!(
        replacement
            .authority()
            .unwrap()
            .verifier
            .verify(&original.signed, &mut None)
            .is_err()
    );
    assert!(publish_owner(&replacement, directory.path(), &work()).is_err());
    assert_eq!(fs::read(directory.path().join(OWNER_FILE)).unwrap(), bytes);
}

#[test]
fn concurrent_initialization_publishes_one_owner() {
    let directory = tempfile::tempdir().unwrap();
    let owners = [
        PiOwnerConfig::new(&policy(), None, &work()).unwrap(),
        PiOwnerConfig::new(&policy(), None, &work()).unwrap(),
    ];
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let accepted = std::thread::scope(|threads| {
        owners
            .each_ref()
            .map(|owner| {
                let barrier = barrier.clone();
                let path = directory.path();
                threads.spawn(move || {
                    barrier.wait();
                    publish_owner(owner, path, &work()).is_ok()
                })
            })
            .map(|handle| handle.join().unwrap())
    });
    assert_ne!(accepted[0], accepted[1]);
    assert!(PiOwnerConfig::load(directory.path()).unwrap() == owners[usize::from(accepted[1])]);
}

#[test]
fn missing_or_corrupt_credentials_are_never_regenerated() {
    let directory = tempfile::tempdir().unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    assert!(!directory.path().join(OWNER_FILE).exists());
    let owner = PiOwnerConfig::new(&policy(), None, &work()).unwrap();
    publish_owner(&owner, directory.path(), &work()).unwrap();
    let path = directory.path().join(OWNER_FILE);
    let bytes = fs::read(&path).unwrap();
    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    for invalid in [
        Vec::new(),
        bytes[..7].to_vec(),
        bytes[..39].to_vec(),
        corrupt,
    ] {
        fs::write(&path, &invalid).unwrap();
        assert!(PiOwnerConfig::load(directory.path()).is_err());
        assert!(publish_owner(&owner, directory.path(), &work()).is_err());
        assert_eq!(fs::read(&path).unwrap(), invalid);
    }
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(MAXIMUM_OWNER_BYTES + 1)
        .unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    assert_eq!(fs::metadata(path).unwrap().len(), MAXIMUM_OWNER_BYTES + 1);
}

#[test]
fn protected_owner_policy_rejects_malformed_or_widened_credentials() {
    let (directory, owner) = credential_fixture(&policy());
    let original = serde_json::to_value(&owner).unwrap();
    for mutation in 0..11 {
        let mut value = original.clone();
        match mutation {
            0..=2 => value["version"] = json!([0, 4, 5][mutation]),
            3 => value["key"] = json!(vec![0u8; 32]),
            4 => value["policy"]["concurrency"] = json!(0),
            5 => value["policy"]["maximum_payload_bytes"] = json!(65_537),
            6 => {
                value.as_object_mut().unwrap().remove("version");
            }
            7 => {
                value
                    .as_object_mut()
                    .unwrap()
                    .insert("unknown".into(), json!(true));
            }
            8 => {
                value["policy"]
                    .as_object_mut()
                    .unwrap()
                    .insert("unknown".into(), json!(true));
            }
            9 => value["policy"]["grants"]
                .as_array_mut()
                .unwrap()
                .push(json!(
                    owner
                        .policy
                        .journal
                        .directory_read_capability("", &mut None)
                        .unwrap()
                )),
            _ => value["policy"]["grants"]
                .as_array_mut()
                .unwrap()
                .push(json!(
                    owner
                        .policy
                        .journal
                        .capability(VolumeOperation::Read, &mut None)
                        .unwrap()
                )),
        }
        let protected =
            protection::protect_with_copy(&serde_json::to_vec(&value).unwrap(), |bytes| {
                Ok::<_, std::io::Error>(bytes.to_vec())
            })
            .unwrap();
        let mut bytes = Vec::from(OWNER_MAGIC.as_slice());
        bytes.extend_from_slice(blake3::hash(&protected).as_bytes());
        bytes.extend_from_slice(&protected);
        fs::write(directory.path().join(OWNER_FILE), &bytes).unwrap();
        assert!(PiOwnerConfig::load(directory.path()).is_err());
        assert_eq!(fs::read(directory.path().join(OWNER_FILE)).unwrap(), bytes);
    }
}

#[cfg(windows)]
#[test]
fn windows_owner_requires_protected_payload() {
    let (directory, owner) = credential_fixture(&policy());
    let plain = serde_json::to_vec(&owner).unwrap();
    assert!(
        !fs::read(directory.path().join(OWNER_FILE))
            .unwrap()
            .windows(plain.len())
            .any(|bytes| bytes == plain)
    );
    let mut bytes = Vec::from(OWNER_MAGIC.as_slice());
    bytes.extend_from_slice(blake3::hash(&plain).as_bytes());
    bytes.extend_from_slice(&plain);
    fs::write(directory.path().join(OWNER_FILE), &bytes).unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    assert_eq!(fs::read(directory.path().join(OWNER_FILE)).unwrap(), bytes);
}

#[cfg(unix)]
#[test]
fn unix_owner_rejects_public_permissions_links_and_nonregular_files() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let (directory, _) = credential_fixture(&policy());
    let path = directory.path().join(OWNER_FILE);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let link = directory.path().join("hardlink");
    fs::hard_link(&path, &link).unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    fs::remove_file(link).unwrap();
    let target = directory.path().join("original");
    fs::rename(&path, &target).unwrap();
    symlink(&target, &path).unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
}
