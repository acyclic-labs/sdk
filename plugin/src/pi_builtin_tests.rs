use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_fs::CancellationToken;
use acyclic_harness::conversation::{VolumeClass, VolumeOwner, VolumeRef};

fn request(io: PiBuiltinIo) -> PiBuiltinRequest {
    PiBuiltinRequest { caller: TaskId::from_bytes([1; 16]), sdk_operation: "pi-original-tool-call".into(),
        path: "file.txt".into(), io }
}

async fn original_native_actor(
    fixture: &crate::pi_fork::tests::Fixture, work: &NativeMaterializationWork,
) -> (std::sync::Arc<acyclic_harness::runtime::NativeTaskContext>, std::sync::Arc<Owner>) {
    let task = fixture.owner.policy().root_task;
    let context = fixture.runtime.runtime.task_context_with_native_work(task, &fixture.fence, work)
        .await.unwrap();
    let owner = fixture.runtime.runtime.task_host().journal_owner_with_native_work(task, &fixture.fence, work)
        .await.unwrap().with_invocation_cancellation(work.cancellation()).into_native_shared(work).unwrap();
    let context = crate::pi_root_provider::bind_root_output_context_with_native_work(context, &owner, std::sync::Arc::clone(&fixture.owner), &fixture.resources, std::sync::Arc::clone(&fixture.filesystem), fixture.stream.clone(), &(fixture.root_request).binding, &(fixture.root_request).root, work)
        .await.unwrap();
    (context, owner)
}

#[tokio::test]
async fn builtin_arguments_cannot_substitute_the_original_sdk_invocation_actor() {
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (context, _owner) = original_native_actor(&fixture, &work).await;
    let operation = OperationId::from_bytes([42; 16]);
    let tool = acyclic_harness::runtime::ToolContext::new_with_native_work(
        context, operation, "original-call", &work).unwrap();
    let mut input = request(PiBuiltinIo::Read { maximum_bytes: 32, variants: vec![] });
    input.caller = fixture.owner.policy().root_task;
    let value = crate::pi_root_provider::guarded_pi_value(&input, MAXIMUM_BUILTIN_BYTES * 2, &work).unwrap();
    let _value_custody = value.1;
    let mut invocation = acyclic_harness::tool::ToolInvocation {
        operation_id: operation, call_id: "original-call".into(),
        name: PI_BUILTIN_COMMAND_KIND.into(), arguments: value.0,
    };
    assert!(decoded_tool_request(&tool, &invocation, &work).is_ok());
    let executor = builtin_tool_executor_with_native_work(&work).unwrap();
    executor.authorize_with_native_context(&tool, &invocation, &work).unwrap();
    assert!(executor.execute_with_native_context(&tool, &invocation, &work).unwrap().await.is_err());
    assert!(executor.reconcile_with_native_context(&tool, &invocation, &work).unwrap().await.is_err());
    invocation.operation_id = OperationId::from_bytes([43; 16]);
    assert!(decoded_tool_request(&tool, &invocation, &work).is_err());
    invocation.operation_id = operation;
    invocation.call_id = "other-call".into();
    assert!(decoded_tool_request(&tool, &invocation, &work).is_err());
    invocation.call_id = "original-call".into();
    input.caller = TaskId::from_bytes([44; 16]);
    let value = crate::pi_root_provider::guarded_pi_value(&input, MAXIMUM_BUILTIN_BYTES * 2, &work).unwrap();
    let _other_value_custody = value.1;
    invocation.arguments = value.0;
    assert!(decoded_tool_request(&tool, &invocation, &work).is_err());
}

#[test]
fn original_builtin_operation_binds_caller_tool_and_phase() {
    let root = TaskId::from_bytes([2; 16]);
    let read = request(PiBuiltinIo::EditRead { maximum_bytes: MAXIMUM_BUILTIN_BYTES });
    let write = request(PiBuiltinIo::EditWrite { content: "new".into(), expected_sha256: "00".repeat(32) });
    assert_ne!(read.operation(root, &crate::pi_fork::tests::work()).unwrap(), write.operation(root, &crate::pi_fork::tests::work()).unwrap());
    let mut other = read.clone();
    other.caller = TaskId::from_bytes([3; 16]);
    assert_ne!(read.operation(root, &crate::pi_fork::tests::work()).unwrap(), other.operation(root, &crate::pi_fork::tests::work()).unwrap());
    other = read.clone();
    other.sdk_operation.push_str("-other");
    assert_ne!(read.operation(root, &crate::pi_fork::tests::work()).unwrap(), other.operation(root, &crate::pi_fork::tests::work()).unwrap());
}

#[test]
fn model_path_gate_rejects_parent_unc_drive_and_outside_marker() {
    let temporary = std::env::temp_dir();
    let view = temporary.join("view");
    let marker = temporary.join("harmless-outside-marker.txt");
    assert!(relative_path(marker.to_str().unwrap(), &view).is_err());
    for path in ["../harmless-outside-marker.txt", "directory/../../file", "directory\\..\\file",
        "\\\\server\\share\\file", "//server/share/file", "C:relative.txt", "", "file\0.txt"] {
        assert!(relative_path(path, &view).is_err(), "{path:?}");
    }
    assert_eq!(relative_path("directory/file.txt", &view).unwrap(), Path::new("directory/file.txt"));
    #[cfg(windows)] {
        let root = Path::new(r"\\?\C:\original-source");
        assert_eq!(relative_path(r"C:\original-source\file.txt", root).unwrap(), Path::new("file.txt"));
        assert_eq!(relative_path(r"\\?\C:\original-source\file.txt", root).unwrap(), Path::new("file.txt"));
        for outside in [r"D:\original-source\file.txt", r"C:\other-source\file.txt",
            r"\\server\share\file.txt", r"\\?\UNC\server\share\file.txt", r"\\.\GLOBALROOT\device\file",
            r"C:\original-source\..\other-source\file"] {
            assert!(relative_path(outside, root).is_err(), "{outside}");
        }
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
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (context, owner) = original_native_actor(&fixture, &work).await;
    authorize_owner(context.context(), &owner, owner.task_id(), &work).await.unwrap();
    assert!(authorize_owner(context.context(), &owner, TaskId::from_bytes([41; 16]), &work).await.is_err());
    let project = fixture.root_request.binding.project_volumes.values().next().unwrap();
    for operation in [acyclic_harness::conversation::VolumeOperation::Read,
        acyclic_harness::conversation::VolumeOperation::Write] {
        owner.require_volume_grant_with_native_work(project, operation, &work).unwrap();
    }
    for io in [PiBuiltinIo::Write { content: "no physical Root write".into() },
        PiBuiltinIo::EditRead { maximum_bytes: 16 },
        PiBuiltinIo::EditWrite { content: "no physical Root edit".into(), expected_sha256: "00".repeat(32) }] {
        let mut original_root = request(io);
        original_root.caller = owner.task_id();
        assert!(original_root.require_root_source_read(owner.task_id()).is_err());
    }
    let readonly = acyclic_harness::filesystem::NativeVolumeMapping {
        volume: project.clone(), generation: fixture.root_allocation.scratch.generation.clone(),
        namespace: acyclic_harness::filesystem::NativeVolumeNamespace::MaterializedDirectory,
        path: fixture.resources.data.join("readonly-project"), writable: false, authority_revision: [1; 32],
    };
    require_builtin_mapping_access(&readonly, &PiBuiltinIo::Read { maximum_bytes: 16, variants: vec![] }).unwrap();
    for io in [PiBuiltinIo::Write { content: "requires original WRITE".into() },
        PiBuiltinIo::EditRead { maximum_bytes: 16 },
        PiBuiltinIo::EditWrite { content: "requires original WRITE".into(), expected_sha256: "00".repeat(32) }] {
        assert!(require_builtin_mapping_access(&readonly, &io).is_err());
    }
    let unrelated = VolumeRef::new(project.provider().clone(), "outside-project", VolumeClass::Project,
        VolumeOwner::Project("outside".into())).unwrap();
    for operation in [acyclic_harness::conversation::VolumeOperation::Read,
        acyclic_harness::conversation::VolumeOperation::Write] {
        assert!(owner.require_volume_grant_with_native_work(&unrelated, operation, &work).is_err());
    }
    fixture.runtime.runtime.task_host().cancel(owner.task_id()).await.unwrap();
    assert!(authorize_owner(context.context(), &owner, owner.task_id(), &work).await.is_err());
    cancellation.cancel();
    assert!(authorize_owner(context.context(), &owner, owner.task_id(), &work).await.is_err());
}

#[tokio::test]
async fn actual_child_result_identity_cannot_be_labeled_as_a_native_builtin_volume() {
    use acyclic_harness::filesystem::{NativeViewManifest, NativeViewOptions, NativeVolumeMapping, NativeVolumeNamespace};
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let (context, owner) = original_native_actor(&fixture, &work).await;
    let plan = fixture.plan(&work).await;
    let coordinator = crate::pi_fork::PiForkCoordinator::new(
        fixture.runtime.clone(), fixture.filesystem.clone(), fixture.stream.clone(),
        &fixture.resources, fixture.owner.clone(), &work).unwrap();
    let prepared = fixture.advance_with(&coordinator, context.context(), &owner, &fixture.fence,
        &plan, fixture.root_handle.workspace.clone(), work.remaining().unwrap(),
        cancellation).await.unwrap().unwrap();
    let binding = &prepared.binding;
    let temporary = tempfile::tempdir().unwrap();
    let generation = fixture.root_allocation.scratch.generation.clone();
    let project = binding.project_volumes.values().next().unwrap().clone();
    let result = binding.results.as_ref().unwrap().volume.clone();
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
