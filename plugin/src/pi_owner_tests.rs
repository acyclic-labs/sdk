use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use crate::pi_fork::tests::{policy, work};
use crate::pi_root::PiRootIntent;
use crate::pi_root_inputs::{
    PiRootRequest, ROOT_ALLOCATION_EFFECT, ROOT_ALLOCATION_PROVIDER, ROOT_INPUT_DIRECTORY,
    stage_root_command, stage_root_request,
};
use acyclic_harness::conversation::{ContentGrant, FileRef};
use acyclic_harness::filesystem::FilesystemHost;
use acyclic_harness::{
    conversation::{VolumeClass, VolumeOwner},
    resources::ProviderRef,
};
use serde_json::json;

#[path = "pi_root_preparation_tests.rs"]
mod root_preparation;

#[path = "pi_service_tests.rs"]
mod service;

async fn root_input_fixture(
    session: &Path,
    raw: Option<acyclic_fs::LocalFs>,
    configure: impl FnOnce(&mut PiOwnerPolicy),
) -> (
    PiOwnerConfig,
    FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    PiRootRequest,
    acyclic_fs::LocalFs,
) {
    let raw = match raw {
        Some(raw) => raw,
        None => acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(session.join("filesystem"))).await.unwrap(),
    };
    let mut policy = policy();
    let root_id = [83; 16];
    let project = VolumeRef::new(
        policy.journal.provider().clone(),
        "project",
        VolumeClass::Project,
        VolumeOwner::Project("project".to_owned()),
    )
    .unwrap();
    policy.grants = Capabilities::new(
        policy.grants.iter().map(str::to_owned).chain([
            policy
                .journal
                .directory_read_capability(ROOT_INPUT_DIRECTORY)
                .unwrap(),
            project.capability(VolumeOperation::Read).unwrap(),
        ]),
    );
    configure(&mut policy);
    let request = PiRootRequest {
        binding: crate::PiTaskBinding {
            native_session_id: "pi-root-input".to_owned(),
            task_id: policy.root_task,
            agent_id: policy.agent,
            project_volumes: std::collections::BTreeMap::from([(hex::encode(root_id), project)]),
            results: None,
            scratch: crate::PiScratchBinding {
                volume: VolumeRef::new(
                    policy.journal.provider().clone(),
                    "scratch",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(policy.agent),
                )
                .unwrap(),
                allocation: crate::PiScratchAllocation::Preparing,
            },
        },
        root: PiRootIntent::Project {
            context_id: [84; 16],
            root_id,
            paths: std::collections::BTreeMap::from([(
                hex::encode(root_id),
                session
                    .join("unopened-selected-root")
                    .to_str()
                    .unwrap()
                    .to_owned(),
            )]),
        },
    };
    let owner = PiOwnerConfig::new(&policy, None, &crate::pi_fork::tests::work()).unwrap();
    owner.save_new(session).unwrap();
    let filesystem =
        FilesystemHost::new(raw.clone(), owner.policy.journal.provider().clone()).unwrap();
    filesystem
        .create_volume(&owner.policy.journal)
        .await
        .unwrap();
    (owner, filesystem, request, raw)
}

#[tokio::test]
async fn pi_root_input_staging_reuses_original_upload_and_narrow_read_authority() {
    let session = tempfile::tempdir().unwrap();
    let (owner, filesystem, request, raw) = root_input_fixture(session.path(), None, |_| {}).await;
    let work = work();
    let _reference_custody;
    let reference;
    (reference, _reference_custody) = stage_root_request(&owner, &filesystem, &request, &work).await.unwrap();
    let repeated = stage_root_request(&owner, &filesystem, &request, &work).await.unwrap();
    assert_eq!(
        repeated.0,
        reference
    );
    let authority = owner.authority().unwrap();
    let issuer = AuthorityIssuer::new(ISSUER, owner.key, authority.verifier.audience().clone());
    let task_scope = issuer.issue(&owner.policy.root_task.to_string(), &owner.policy.grants, Some(owner.policy.agent), None, &mut None).expect("scope proof");
    let read = ContentGrant::verify_read(&authority.verifier, &task_scope, &reference).unwrap();
    let original = filesystem
        .read_content(&reference, &read, owner.policy.maximum_payload_bytes)
        .await
        .unwrap();
    assert_eq!(original.as_ref(), serde_json::to_vec(&request).unwrap());
    assert!(
        ContentGrant::verify(
            &authority.verifier,
            &task_scope,
            &owner.policy.journal,
            VolumeOperation::Write
        )
        .is_err()
    );
    for path in [
        "pi-inputs-sibling/root.json",
        ".system/harness-uploads/receipt.json",
    ] {
        let foreign = FileRef::new(
            reference.volume().clone(),
            path,
            reference.version(),
            reference.descriptor().clone(),
            "root.json",
        )
        .unwrap();
        assert!(ContentGrant::verify_read(&authority.verifier, &task_scope, &foreign).is_err());
    }
    let mut changed = request.clone();
    let observation = crate::pi_fork::tests::work();
    let workspace = raw
        .open_workspace_with_native_work(&owner.policy.journal.storage_name().unwrap(), &observation, observation.cancellation())
        .await
        .unwrap();
    let original_head = workspace.head(&observation, observation.cancellation()).await.unwrap().id();
    let PiRootIntent::Project { paths, .. } = &mut changed.root else {
        unreachable!()
    };
    *paths.values_mut().next().unwrap() = session
        .path()
        .join("changed-selected-root")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        stage_root_request(&owner, &filesystem, &changed, &work)
            .await
            .is_err()
    );
    assert_eq!(workspace.head(&observation, observation.cancellation()).await.unwrap().id(), original_head);
    assert_eq!(
        filesystem
            .read_content(&reference, &read, owner.policy.maximum_payload_bytes)
            .await
            .unwrap(),
        original
    );
    drop(workspace);
    drop(raw);
    drop(filesystem);
    let filesystem = FilesystemHost::new(
        acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
            session.path().join("filesystem"),
        ))
        .await
        .unwrap(),
        owner.policy.journal.provider().clone(),
    )
    .unwrap();
    let reopened = stage_root_request(&owner, &filesystem, &request, &work).await.unwrap();
    assert_eq!(
        reopened.0,
        reference
    );
    assert!(!session.path().join("unopened-selected-root").exists());
    assert!(!session.path().join("changed-selected-root").exists());
}

#[tokio::test]
async fn pi_root_command_pins_actual_artifacts_in_original_admission_and_reopen() {
    use acyclic_harness::{Admission, OperationId, effect_host::TaskEffectPlan};
    use acyclic_stream::{LocalStream, LocalStreamLimits};
    use std::sync::Arc;
    let session = tempfile::tempdir().unwrap();
    let (owner, filesystem, request, raw) = root_input_fixture(session.path(), None, |_| {}).await;
    let work = work();
    let authority = owner.authority().unwrap();
    let _command_custody;
    let command;
    (command, _command_custody) = stage_root_command(&owner, &filesystem, &request, &work).await.unwrap();
    let repeated = stage_root_command(&owner, &filesystem, &request, &work).await.unwrap();
    assert_eq!(
        repeated.0,
        command
    );
    let read = ContentGrant::verify_directory_read(
        &authority.verifier,
        &authority.signed,
        &owner.policy.journal,
        ROOT_INPUT_DIRECTORY,
    )
    .unwrap();
    let plan: TaskEffectPlan = serde_json::from_slice(
        &filesystem
            .read_content(&command.payload, &read, owner.policy.maximum_payload_bytes)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(plan.provider, ROOT_ALLOCATION_PROVIDER);
    assert_eq!(plan.effect_kind, ROOT_ALLOCATION_EFFECT);
    assert_eq!(
        plan.guarantee,
        acyclic_harness::core::EffectGuarantee::AtMostOnce
    );
    assert_eq!(
        plan.result_schema.path(),
        "pi-inputs/allocation-schema.json"
    );
    assert_eq!(
        plan.result_schema.descriptor().media_type(),
        "application/schema+json"
    );
    let schema: serde_json::Value = serde_json::from_slice(
        &filesystem
            .read_content(
                &plan.result_schema,
                &read,
                owner.policy.maximum_payload_bytes,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    let expected_schema = crate::pi_root_allocation::PiRootAllocation::schema(&request.binding, &work).unwrap();
    assert_eq!(schema, expected_schema.0);
    drop(expected_schema);
    let original_request = stage_root_request(&owner, &filesystem, &request, &work).await.unwrap();
    assert_eq!(
        plan.request,
        original_request.0
    );
    let retained: PiRootRequest = serde_json::from_slice(
        &filesystem
            .read_content(&plan.request, &read, owner.policy.maximum_payload_bytes)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(retained).unwrap(),
        serde_json::to_value(&request).unwrap()
    );
    let filesystem = Arc::new(filesystem);
    let stream = Arc::new(
        LocalStream::open(session.path().join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    );
    let runtime = crate::pi_runtime::PiTaskRuntime::open_with_native_work(
        stream, filesystem.clone(), &owner.policy.journal,
        owner.authority_with_native_work(&work).unwrap(), &work,
    )
    .await
    .unwrap();
    let admission = runtime.admit_root_with_native_work(
        request.binding.clone(), request.root.clone(), command.clone(), &work,
    ).await.unwrap();
    assert!(matches!(admission.0, Admission::Accepted(_)));
    drop(admission.0);
    drop(admission.1);
    let mut changed = command.clone();
    changed.operation_id = OperationId::from_bytes([91; 16]);
    assert!(
        runtime
            .admit_root_with_native_work(request.binding.clone(), request.root.clone(), changed, &work)
            .await
            .is_err()
    );
    use acyclic_harness::runtime::DurableTaskHost;
    let admitted = runtime
        .runtime
        .task_host()
        .observe_admission(request.binding.task_id)
        .await
        .unwrap();
    assert_eq!(
        admitted.input.pointer("/execution/preparation").unwrap(),
        &serde_json::to_value(&command).unwrap()
    );
    drop(runtime);
    drop(filesystem);
    drop(raw);
    let filesystem = FilesystemHost::new(
        acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
            session.path().join("filesystem"),
        ))
        .await
        .unwrap(),
        owner.policy.journal.provider().clone(),
    )
    .unwrap();
    let reopened = stage_root_command(&owner, &filesystem, &request, &work).await.unwrap();
    assert_eq!(
        reopened.0,
        command
    );
    assert!(!session.path().join("unopened-selected-root").exists());
}

#[tokio::test]
async fn pi_root_input_staging_requires_original_owner_explicit_read_and_finite_bytes() {
    for failure in ["owner", "grant", "bytes", "path", "foreign"] {
        let work = work();
        let session = tempfile::tempdir().unwrap();
        let (owner, filesystem, mut request, raw) = root_input_fixture(session.path(), None, |policy| {
            if failure == "grant" {
                policy.grants = Capabilities::new(
                    policy
                        .grants
                        .iter()
                        .filter(|grant| {
                            *grant
                                != policy
                                    .journal
                                    .directory_read_capability(ROOT_INPUT_DIRECTORY)
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
        })
        .await;
        let observation = crate::pi_fork::tests::work();
        let workspace = raw
            .open_workspace_with_native_work(&owner.policy.journal.storage_name().unwrap(), &observation, observation.cancellation())
            .await
            .unwrap();
        let original_head = workspace.head(&observation, observation.cancellation()).await.unwrap().id();
        if failure == "owner" {
            fs::remove_file(session.path().join(OWNER_FILE)).unwrap();
        }
        if failure == "foreign" {
            request.binding.task_id = TaskId::from_bytes([85; 16]);
        }
        let result = match PiOwnerConfig::load(session.path()) {
            Ok(original_owner) => stage_root_request(&original_owner, &filesystem, &request, &work).await,
            Err(error) => Err(error),
        };
        assert!(result.is_err(), "accepted {failure}");
        assert!(!session.path().join("unopened-selected-root").exists());
        assert_eq!(
            workspace.head(&observation, observation.cancellation()).await.unwrap().id(),
            original_head,
            "mutated storage for {failure}"
        );
    }
}

#[test]
fn pi_owner_reopen_retains_original_key_policy_and_signed_authority() {
    let directory = tempfile::tempdir().unwrap();
    let owner = PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work()).unwrap();
    let original = owner.authority().unwrap();
    owner.save_new(directory.path()).unwrap();
    let saved = fs::read(directory.path().join(OWNER_FILE)).unwrap();
    #[cfg(windows)] {
        let plain = serde_json::to_vec(&owner).unwrap();
        assert!(!saved.windows(plain.len()).any(|bytes| bytes == plain));
    }
    drop(owner);
    for _ in 0..2 {
        let reopened = PiOwnerConfig::load(directory.path()).unwrap();
        let authority = reopened.authority().unwrap();
        assert!(reopened.policy == policy());
        assert_eq!(authority.signed, original.signed);
        authority.verifier.verify(&original.signed).unwrap();
        assert!(authority.scope.grants() == original.scope.grants());
        assert_eq!(authority.scope.limits(), original.scope.limits());
        assert_eq!(authority.scope.run_limits(), original.scope.run_limits());
        assert!(fs::read(directory.path().join(OWNER_FILE)).unwrap() == saved);
    }
    let replacement = PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work()).unwrap();
    assert!(
        replacement
            .authority()
            .unwrap()
            .verifier
            .verify(&original.signed)
            .is_err()
    );
    assert!(replacement.save_new(directory.path()).is_err());
    assert!(fs::read(directory.path().join(OWNER_FILE)).unwrap() == saved);
}

#[test]
fn pi_owner_concurrent_initialization_publishes_one_original_owner() {
    use std::sync::{Arc, Barrier};
    let directory = tempfile::tempdir().unwrap();
    let owners = [
        PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work()).unwrap(),
        PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work()).unwrap(),
    ];
    let barrier = Arc::new(Barrier::new(2));
    let accepted = std::thread::scope(|threads| {
        owners.each_ref().map(|owner| {
            let barrier = barrier.clone();
            let session = directory.path();
            threads.spawn(move || { barrier.wait(); owner.save_new(session).is_ok() })
        }).map(|handle| handle.join().unwrap())
    });
    assert_ne!(accepted[0], accepted[1]);
    let saved = PiOwnerConfig::load(directory.path()).unwrap();
    assert!(saved == owners[usize::from(accepted[1])]);
}

#[test]
fn pi_owner_reopen_retains_expired_bounds_for_original_attempt_reconciliation() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = policy();
    policy.run_limits.deadline_epoch_ms = Some(1);
    let owner = PiOwnerConfig::new(&policy, None, &crate::pi_fork::tests::work()).unwrap();
    owner.save_new(directory.path()).unwrap();
    let reopened = PiOwnerConfig::load(directory.path()).unwrap();
    assert!(reopened == owner);
    assert_eq!(
        reopened
            .authority()
            .unwrap()
            .scope
            .run_limits()
            .deadline_epoch_ms,
        Some(1)
    );
}

#[test]
fn pi_owner_missing_torn_corrupt_or_oversized_file_never_regenerates_credentials() {
    let directory = tempfile::tempdir().unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    assert!(!directory.path().join(OWNER_FILE).exists());
    let owner = PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work()).unwrap();
    owner.save_new(directory.path()).unwrap();
    let path = directory.path().join(OWNER_FILE);
    let valid = fs::read(&path).unwrap();
    let mut corrupt = valid.clone();
    let last = corrupt.last_mut().unwrap();
    *last ^= 1;
    for invalid in [
        Vec::new(),
        valid.get(..7).unwrap().to_vec(),
        valid.get(..39).unwrap().to_vec(),
        corrupt,
    ] {
        fs::write(&path, &invalid).unwrap();
        assert!(PiOwnerConfig::load(directory.path()).is_err());
        assert!(fs::read(&path).unwrap() == invalid);
        assert!(owner.save_new(directory.path()).is_err());
        assert!(fs::read(&path).unwrap() == invalid);
    }
    let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(MAXIMUM_OWNER_BYTES + 1).unwrap();
    drop(file);
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    assert_eq!(fs::metadata(&path).unwrap().len(), MAXIMUM_OWNER_BYTES + 1);
}

#[test]
fn pi_owner_rejects_non_v1_unknown_fields_empty_keys_and_widened_journal_grants() {
    let directory = tempfile::tempdir().unwrap();
    let owner = PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work()).unwrap();
    owner.save_new(directory.path()).unwrap();
    let original = serde_json::to_value(&owner).unwrap();
    let mut invalids = Vec::new();
    for (pointer, replacement) in [
        ("/version", json!(0)), ("/version", json!(4)), ("/version", json!(5)),
        ("/key", json!(vec![0u8; 32])), ("/policy/concurrency", json!(0)),
        ("/policy/maximum_payload_bytes", json!(65_537)),
    ] {
        let mut value = original.clone();
        *value.pointer_mut(pointer).unwrap() = replacement;
        invalids.push(value);
    }
    let mut value = original.clone();
    value.as_object_mut().unwrap().remove("version");
    invalids.push(value);
    for pointer in ["", "/policy"] {
        let mut value = original.clone();
        value.pointer_mut(pointer).unwrap().as_object_mut().unwrap().insert("unknown".into(), json!(true));
        invalids.push(value);
    }
    for grant in [
        owner.policy.journal.directory_read_capability("").unwrap(),
        owner.policy.journal.capability(VolumeOperation::Read).unwrap(),
    ] {
        let mut value = original.clone();
        value["policy"]["grants"].as_array_mut().unwrap().push(json!(grant));
        invalids.push(value);
    }
    for value in invalids {
        let plain = serde_json::to_vec(&value).unwrap();
        let protected = protection::protect(&plain).unwrap();
        let mut bytes = Vec::from(OWNER_MAGIC.as_slice());
        bytes.extend_from_slice(blake3::hash(&protected).as_bytes());
        bytes.extend_from_slice(&protected);
        fs::write(directory.path().join(OWNER_FILE), bytes).unwrap();
        let before = fs::read(directory.path().join(OWNER_FILE)).unwrap();
        assert!(PiOwnerConfig::load(directory.path()).is_err());
        assert!(fs::read(directory.path().join(OWNER_FILE)).unwrap() == before);
    }
}

#[cfg(windows)]
#[test]
fn pi_owner_windows_dpapi_rejects_unprotected_payload_even_with_matching_envelope_digest() {
    let directory = tempfile::tempdir().unwrap();
    let owner = PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work()).unwrap();
    owner.save_new(directory.path()).unwrap();
    let plain = serde_json::to_vec(&owner).unwrap();
    let mut bytes = Vec::from(OWNER_MAGIC.as_slice());
    bytes.extend_from_slice(blake3::hash(&plain).as_bytes());
    bytes.extend_from_slice(&plain);
    fs::write(directory.path().join(OWNER_FILE), &bytes).unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
    assert!(fs::read(directory.path().join(OWNER_FILE)).unwrap() == bytes);
}

#[cfg(unix)]
#[test]
fn pi_owner_unix_rejects_public_permissions_links_and_nonregular_files() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let directory = tempfile::tempdir().unwrap();
    PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work())
        .unwrap()
        .save_new(directory.path())
        .unwrap();
    let path = directory.path().join(OWNER_FILE);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
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
    fs::remove_file(path).unwrap();
    fs::create_dir(directory.path().join(OWNER_FILE)).unwrap();
    assert!(PiOwnerConfig::load(directory.path()).is_err());
}

#[tokio::test]
async fn pi_root_startup_quota_and_cancellation_do_not_publish_inputs() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError,
        WorkBudget};
    let session = tempfile::tempdir().unwrap();
    let (owner, filesystem, request, raw) = root_input_fixture(session.path(), None, |_| {}).await;
    let observation = crate::pi_fork::tests::work();
    let workspace = raw.open_workspace_with_native_work(&owner.policy.journal.storage_name().unwrap(), &observation, observation.cancellation()).await.unwrap();
    let original = workspace.head(&observation, observation.cancellation()).await.unwrap().id();
    let limited = NativeMaterializationWork::new(WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 1,
        ..crate::pi_root_provider::allocation_budget()
    }, &CancellationToken::new()).unwrap();
    let baseline = limited.live_allocation_bytes();
    assert!(matches!(stage_root_command(&owner, &filesystem, &request, &limited).await,
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(_), .. })));
    assert_eq!(limited.live_allocation_bytes(), baseline);
    assert_eq!(workspace.head(&observation, observation.cancellation()).await.unwrap().id(), original);
    let unhashed = NativeMaterializationWork::new(WorkBudget {
        bytes_hashed: 0, ..crate::pi_root_provider::allocation_budget()
    }, &CancellationToken::new()).unwrap();
    let before = unhashed.receipt();
    assert!(matches!(stage_root_command(&owner, &filesystem, &request, &unhashed).await,
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(_), .. })));
    assert_eq!(unhashed.receipt(), before);
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(stage_root_command(&owner, &filesystem, &request, &work).await,
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(workspace.head(&observation, observation.cancellation()).await.unwrap().id(), original);
    assert!(!session.path().join("unopened-selected-root").exists());
}

#[test]
fn native_owner_authority_preserves_original_grants_and_scope_names() {
    let work = work();
    let owner = PiOwnerConfig::new(&policy(), None, &work).unwrap();
    let baseline = work.live_allocation_bytes();
    let ordinary = owner.authority().unwrap();
    let native = owner.authority_with_native_work(&work).unwrap();
    assert!(native.0.signed == ordinary.signed);
    assert_eq!(native.0.scope.grants(), ordinary.scope.grants());
    native.0.validate_with_native_work(&owner.policy.journal, &work).unwrap();
    drop(native.0);
    drop(native.1);
    let ordinary_conversation = owner.conversation_authority(
        owner.policy.root_task, owner.policy.agent, owner.policy.grants.clone(),
    ).unwrap();
    let conversation = owner.conversation_authority_with_native_work(
        owner.policy.root_task, owner.policy.agent, &owner.policy.grants, &work,
    ).unwrap();
    assert!(conversation.0.0 == ordinary_conversation.0);
    assert!(conversation.0.2 == ordinary_conversation.2);
    drop(conversation.0);
    drop(conversation.1);
    let ordinary_issuer = owner.effect_issuer();
    let ordinary_effect_scope = ordinary_issuer.issue(&owner.policy.root_task.to_string(), &owner.policy.grants, Some(owner.policy.agent), None, &mut None).expect("scope proof");
    let effect = owner.effect_authority_with_native_work(&work).unwrap();
    assert!(effect.0.2 == ordinary_effect_scope);
    let verifier = owner.effect_verifier_with_native_work(&work).unwrap();
    verifier.0.verify_with_native_work(&effect.0.2, &work).unwrap();
    drop(verifier.0);
    drop(verifier.1);
    drop(effect.0);
    drop(effect.1);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[test]
fn native_owner_authority_denial_releases_original_ledger() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let owner = PiOwnerConfig::new(&policy(), None, &work()).unwrap();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &cancellation,
    ).unwrap();
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(owner.authority_with_native_work(&work),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    let limited = NativeMaterializationWork::new(WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 1,
        ..crate::pi_root_provider::allocation_budget()
    }, &CancellationToken::new()).unwrap();
    let baseline = limited.live_allocation_bytes();
    assert!(owner.authority_with_native_work(&limited).is_err());
    assert_eq!(limited.live_allocation_bytes(), baseline);
}

#[test]
fn native_owner_rooted_readonly_releases_record_and_cancellation_precedes_io() {
    let directory = tempfile::tempdir().unwrap();
    let owner = PiOwnerConfig::new(&policy(), None, &crate::pi_fork::tests::work()).unwrap();
    owner.save_new(directory.path()).unwrap();
    let original = fs::read(directory.path().join(OWNER_FILE)).unwrap();
    let work = work();
    let root = acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
        directory.path(), &work,
    ).unwrap();
    let before = work.receipt();
    let loaded = PiOwnerConfig::load_readonly_with_native_work(&root, Path::new(""), &work).unwrap();
    assert!(loaded == owner);
    assert_eq!(work.receipt().backend_write_operations, before.backend_write_operations);
    assert_eq!(work.receipt().durability_operations, before.durability_operations);
    assert_eq!(fs::read(directory.path().join(OWNER_FILE)).unwrap(), original);
    let retained = work.live_allocation_bytes();
    drop(loaded);
    assert!(work.live_allocation_bytes() < retained);
    let before = work.receipt();
    work.cancellation().cancel();
    assert!(matches!(PiOwnerConfig::load_readonly_with_native_work(&root, Path::new(""), &work),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert!(matches!(owner.save_new_with_native_work(&root, Path::new(""), &work),
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(fs::read(directory.path().join(OWNER_FILE)).unwrap(), original);
}
