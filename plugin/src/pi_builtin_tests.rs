use super::*;
use acyclic_fs::CancellationToken;
use acyclic_harness::conversation::{VolumeClass, VolumeOwner, VolumeRef};

fn request(io: PiBuiltinIo) -> PiBuiltinRequest {
    PiBuiltinRequest { caller: TaskId::from_bytes([1; 16]), sdk_operation: "pi-original-tool-call".into(),
        path: "file.txt".into(), io }
}

#[test]
fn builtin_shared_executor_charges_its_real_arc_attempt_before_allocation() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let before = work.receipt().allocation_operations;
    let executor = builtin_tool_executor_with_native_work(&work).unwrap();
    assert_eq!(work.receipt().allocation_operations, before + 1);
    drop(executor);
    let remaining = work.remaining().unwrap().allocation_operations;
    work.charge(WorkCounters { allocation_operations: remaining, ..WorkCounters::default() }).unwrap();
    let before = work.receipt().allocation_operations;
    assert!(matches!(builtin_tool_executor_with_native_work(&work), Err(Error::NativeWork { .. })));
    assert_eq!(work.receipt().allocation_operations, before);

    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    cancellation.cancel();
    let before = work.receipt().allocation_operations;
    assert!(matches!(builtin_tool_executor_with_native_work(&work), Err(Error::NativeWork { .. })));
    assert_eq!(work.receipt().allocation_operations, before);
}

async fn original_native_actor(
    fixture: &crate::pi_fork::tests::Fixture, work: &NativeMaterializationWork,
) -> (std::sync::Arc<acyclic_harness::runtime::NativeTaskContext>, std::sync::Arc<Owner>) {
    let task = fixture.owner.policy().root_task;
    let context = fixture.runtime.runtime.task_context_with_native_work(task, &fixture.fence, work)
        .await.unwrap();
    let owner = fixture.runtime.runtime.task_host().journal_owner_with_native_work(task, &fixture.fence, work)
        .await.unwrap().with_invocation_cancellation(work.cancellation()).into_native_shared(work).unwrap();
    let context = crate::pi_root_provider::bind_root_output_context_with_native_work(
        context, &owner, std::sync::Arc::clone(&fixture.owner), &fixture.resources,
        std::sync::Arc::clone(&fixture.filesystem), fixture.stream.clone(), &fixture.root_request, work)
        .await.unwrap();
    (context, owner)
}

#[test]
fn original_builtin_operation_binds_caller_tool_and_phase() {
    let root = TaskId::from_bytes([2; 16]);
    let read = request(PiBuiltinIo::EditRead { maximum_bytes: MAXIMUM_BUILTIN_BYTES });
    let write = request(PiBuiltinIo::EditWrite { content: "new".into(), expected_sha256: "00".repeat(32) });
    assert_ne!(read.operation(root), write.operation(root));
    let mut other = read.clone();
    other.caller = TaskId::from_bytes([3; 16]);
    assert_ne!(read.operation(root), other.operation(root));
    other = read.clone();
    other.sdk_operation.push_str("-other");
    assert_ne!(read.operation(root), other.operation(root));
}

#[test]
fn model_path_gate_rejects_parent_unc_drive_and_outside_marker() {
    let temporary = tempfile::tempdir().unwrap();
    let view = temporary.path().join("view");
    std::fs::create_dir(&view).unwrap();
    let marker = temporary.path().join("harmless-outside-marker.txt");
    std::fs::write(&marker, b"not a secret, still outside authority").unwrap();
    assert!(relative_path(marker.to_str().unwrap(), &view).is_err());
    for path in ["../harmless-outside-marker.txt", "directory/../../file", "directory\\..\\file",
        "\\\\server\\share\\file", "//server/share/file", "C:relative.txt", "", "file\0.txt"] {
        assert!(relative_path(path, &view).is_err(), "{path:?}");
    }
    assert_eq!(relative_path("directory/file.txt", &view).unwrap(), Path::new("directory/file.txt"));
    assert_eq!(std::fs::read(&marker).unwrap(), b"not a secret, still outside authority");
}

#[cfg(windows)]
#[test]
fn windows_original_drive_and_verbatim_mapping_are_equivalent_not_other_roots() {
    let root = Path::new(r"\\?\C:\original-source");
    assert_eq!(relative_path(r"C:\original-source\file.txt", root).unwrap(), Path::new("file.txt"));
    assert_eq!(relative_path(r"\\?\C:\original-source\file.txt", root).unwrap(), Path::new("file.txt"));
    for outside in [r"D:\original-source\file.txt", r"C:\other-source\file.txt",
        r"\\server\share\file.txt", r"\\?\UNC\server\share\file.txt", r"\\.\GLOBALROOT\device\file",
        r"C:\original-source\..\other-source\file"] {
        assert!(relative_path(outside, root).is_err(), "{outside}");
    }
}

#[test]
fn bounded_preimage_and_variants_cannot_launder_an_unbounded_read() {
    assert!(request(PiBuiltinIo::Read { maximum_bytes: MAXIMUM_BUILTIN_BYTES + 1, variants: vec![] }).validate().is_err());
    assert!(request(PiBuiltinIo::Read { maximum_bytes: 0, variants: vec![] }).validate().is_err());
    assert!(request(PiBuiltinIo::Read { maximum_bytes: 1, variants: vec!["file".into(); 5] }).validate().is_err());
    assert!(request(PiBuiltinIo::EditWrite { content: "x".into(), expected_sha256: "not-a-preimage".into() }).validate().is_err());
    assert!(request(PiBuiltinIo::Write { content: "x".repeat(MAXIMUM_BUILTIN_BYTES as usize + 1) }).validate().is_err());
}

#[test]
fn read_fallback_cannot_launder_owner_denials_or_outside_variants() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("view");
    let outside = temporary.path().join("harmless-marker");
    let variants = vec!["allowed-name".into(), outside.to_str().unwrap().into()];
    assert!(scoped_variants(&variants, &root).is_err());
    let native_absence = Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Io(
        std::io::Error::from(std::io::ErrorKind::NotFound)), custody: None };
    assert!(is_absent(&native_absence));
    let denied = Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Io(
        std::io::Error::from(std::io::ErrorKind::PermissionDenied)), custody: None };
    assert!(!is_absent(&denied));
    assert!(!is_absent(&Error::Unauthorized("original stale owner".into())));
    assert!(!is_absent(&Error::Conflict("source changed".into())));
}

#[tokio::test]
async fn real_original_owner_grant_and_cancel_checks_are_used_by_builtins() {
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (context, owner) = original_native_actor(&fixture, &work).await;
    authorize_owner(context.context(), &owner, owner.task_id(), &work).await.unwrap();
    assert!(authorize_owner(context.context(), &owner, TaskId::from_bytes([41; 16]), &work).await.is_err());
    let project = fixture.root_request.binding.project_volumes.values().next().unwrap();
    authorize_volume(&owner, project, true,
        &PiBuiltinIo::Write { content: "real owner WRITE required".into() }, &work).unwrap();
    // Even a genuine Root WRITE grant is reserved for the existing operator
    // publication engine, never an ambient model builtin mutation.
    for io in [PiBuiltinIo::Write { content: "no physical Root write".into() },
        PiBuiltinIo::EditRead { maximum_bytes: 16 },
        PiBuiltinIo::EditWrite { content: "no physical Root edit".into(), expected_sha256: "00".repeat(32) }] {
        let mut original_root = request(io);
        original_root.caller = owner.task_id();
        assert!(original_root.require_root_source_read(owner.task_id()).is_err());
    }
    assert!(authorize_volume(&owner, project, false,
        &PiBuiltinIo::EditRead { maximum_bytes: 16 }, &work).is_err());
    let unrelated = VolumeRef::new(project.provider().clone(), "outside-project", VolumeClass::Project,
        VolumeOwner::Project("outside".into())).unwrap();
    assert!(authorize_volume(&owner, &unrelated, true,
        &PiBuiltinIo::Read { maximum_bytes: 16, variants: vec![] }, &work).is_err());
    cancellation.cancel();
    assert!(authorize_owner(context.context(), &owner, owner.task_id(), &work).await.is_err());
}

#[tokio::test]
async fn genuine_held_sdk_leaf_preserves_allowed_read_write_and_exact_edit() {
    use sha2::{Digest as _, Sha256};
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let temporary = tempfile::tempdir().unwrap();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (context, owner) = original_native_actor(&fixture, &work).await;
    // The native producer acquires and holds this real directory identity; the
    // production Pi facade instead obtains it from the original view creator.
    let identity = work.root_identity(temporary.path()).unwrap();
    let guard = || authorize_owner(context.context(), &owner, owner.task_id(), &work);
    work.write_working_file_guarded(temporary.path(), identity, Path::new("nested/file.txt"),
        "before", true, None, guard).await.unwrap();
    let bytes = work.read_working_file_guarded(temporary.path(), identity,
        Path::new("nested/file.txt"), 32, guard).await.unwrap();
    assert_eq!(bytes.as_ref(), b"before");
    let expected: [u8; 32] = Sha256::digest(bytes.as_ref()).into();
    drop(bytes);
    work.write_working_file_guarded(temporary.path(), identity, Path::new("nested/file.txt"),
        "after", false, Some(expected), guard).await.unwrap();
    assert!(work.write_working_file_guarded(temporary.path(), identity, Path::new("nested/file.txt"),
        "must not overwrite", false, Some(expected), guard).await.is_err());
    let bytes = work.read_working_file_guarded(temporary.path(), identity,
        Path::new("nested/file.txt"), 32, guard).await.unwrap();
    assert_eq!(bytes.as_ref(), b"after");
    drop(bytes);
    assert!(work.read_working_file_guarded(temporary.path(), identity,
        Path::new("nested/file.txt"), 2, guard).await.is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn genuine_held_sdk_leaf_rejects_symlink_outside_before_read_or_write() {
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("view");
    std::fs::create_dir(&root).unwrap();
    let marker = temporary.path().join("harmless-marker.txt");
    std::fs::write(&marker, b"outside").unwrap();
    std::os::unix::fs::symlink(&marker, root.join("escape")).unwrap();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (context, owner) = original_native_actor(&fixture, &work).await;
    let identity = work.root_identity(&root).unwrap();
    let guard = || authorize_owner(context.context(), &owner, owner.task_id(), &work);
    assert!(work.read_working_file_guarded(&root, identity, Path::new("escape"), 32, guard).await.is_err());
    assert!(work.write_working_file_guarded(&root, identity, Path::new("escape"), "overwrite", false,
        None, guard).await.is_err());
    assert_eq!(std::fs::read(marker).unwrap(), b"outside");
}

#[tokio::test]
async fn original_task_cancellation_and_replaced_root_fail_before_leaf_io() {
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("view");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("file"), b"original").unwrap();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (context, owner) = original_native_actor(&fixture, &work).await;
    let identity = work.root_identity(&root).unwrap();
    std::fs::rename(&root, temporary.path().join("original-view")).unwrap();
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("file"), b"replacement").unwrap();
    let guard = || authorize_owner(context.context(), &owner, owner.task_id(), &work);
    assert!(work.read_working_file_guarded(&root, identity, Path::new("file"), 32, guard).await.is_err());
    assert!(work.write_working_file_guarded(&root, identity, Path::new("file"), "must not overwrite",
        false, None, guard).await.is_err());
    assert_eq!(std::fs::read(root.join("file")).unwrap(), b"replacement");
    fixture.runtime.runtime.task_host().cancel(owner.task_id()).await.unwrap();
    assert!(authorize_owner(context.context(), &owner, owner.task_id(), &work).await.is_err());
}

#[tokio::test]
async fn failed_whole_file_checks_spend_the_original_quota_without_reading_payload() {
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(temporary.path().join("file"), b"larger than request").unwrap();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (context, owner) = original_native_actor(&fixture, &work).await;
    let identity = work.root_identity(temporary.path()).unwrap();
    let before = work.receipt();
    let guard = || authorize_owner(context.context(), &owner, owner.task_id(), &work);
    assert!(work.read_working_file_guarded(temporary.path(), identity, Path::new("file"), 1, guard).await.is_err());
    let after = work.receipt();
    assert!(after.source_path_components > before.source_path_components);
    assert_eq!(after.source_bytes_read, before.source_bytes_read);
    assert!(work.read_working_file_guarded(temporary.path(), identity, Path::new("file"), 1, guard).await.is_err());
    assert!(work.receipt().source_path_components > after.source_path_components);
    assert_eq!(work.receipt().source_bytes_read, before.source_bytes_read);
    let denied_work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (denied_context, denied_owner) = original_native_actor(&fixture, &denied_work).await;
    denied_work.charge(WorkCounters {
        source_bytes_read: denied_work.remaining().unwrap().source_bytes_read,
        ..WorkCounters::default()
    }).unwrap();
    let denied_guard = || authorize_owner(denied_context.context(), &denied_owner, denied_owner.task_id(), &denied_work);
    assert!(denied_work.read_working_file_guarded(temporary.path(), identity, Path::new("file"), 32, denied_guard).await.is_err());
    assert_eq!(std::fs::read(temporary.path().join("file")).unwrap(), b"larger than request");
}

#[tokio::test]
async fn actual_child_result_identity_cannot_be_labeled_as_a_native_builtin_volume() {
    use acyclic_harness::filesystem::{NativeViewManifest, NativeViewOptions, NativeVolumeMapping, NativeVolumeNamespace};
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let plan = fixture.plan().await;
    let prepared = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(),
        CancellationToken::new()).await.unwrap().unwrap();
    let binding = &prepared.binding;
    let temporary = tempfile::tempdir().unwrap();
    let generation = fixture.root_allocation.scratch.generation.clone();
    let project = binding.project_volumes.values().next().unwrap().clone();
    let result = binding.results.as_ref().unwrap().volume.clone();
    // Deliberately forged metadata tests only the selection rejection; it is
    // never accepted as a held-root proof or passed to real filesystem IO.
    let mapping = |volume, role: &str| NativeVolumeMapping { volume, generation: generation.clone(),
        namespace: NativeVolumeNamespace::MaterializedDirectory, path: temporary.path().join(role),
        writable: true, authority_revision: [1; 32] };
    let manifest = NativeViewManifest { options: NativeViewOptions { root: temporary.path().to_path_buf(),
        maximum_volumes: 3, work_per_volume: crate::pi_root_provider::allocation_budget() },
        volumes: vec![mapping(project, "project"), mapping(binding.scratch.volume.clone(), "scratch"),
            mapping(result, "result")] };
    assert!(validate_declared_working_roles(binding, &manifest).is_err());
    let declared = NativeViewManifest {
        options: manifest.options.clone(),
        volumes: manifest.volumes[..2].to_vec(),
    };
    validate_declared_working_roles(binding, &declared).unwrap();
    let mut substituted = declared.clone();
    substituted.volumes[1].volume = binding.results.as_ref().unwrap().volume.clone();
    assert!(validate_declared_working_roles(binding, &substituted).is_err());
    substituted = declared.clone();
    substituted.volumes[0].volume = fixture.root_request.binding.project_volumes.values().next().unwrap().clone();
    assert!(validate_declared_working_roles(binding, &substituted).is_err());
    let result_path = temporary.path().join("result/output.txt");
    assert!(select_native_path(result_path.to_str().unwrap(), binding, &manifest).is_err());
    let outside_path = temporary.path().join("outside-marker.txt");
    assert!(select_native_path(outside_path.to_str().unwrap(), binding, &manifest).is_err());
}
