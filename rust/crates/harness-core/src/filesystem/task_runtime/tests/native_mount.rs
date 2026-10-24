use super::*;
use crate::conversation::ContentGrant;
use crate::filesystem::{
    BorrowedNativeVolumeBinding, NativeMountedVolume, NativePreparationRoot, NativeViewOptions,
    NativeVolumeNamespace, NativeVolumeView,
};
use crate::runtime::TaskDefinition;
use acyclic_fs::{
    CancellationToken, LazyWorkspace, MemoryLazyWorkspaceStore, MountPublication,
    NativeMaterializationWork, PublicationPermit,
};
use std::{
    borrow::Cow,
    os::windows::process::CommandExt,
    process::{Command, Stdio},
    time::Duration,
};

#[tokio::test(flavor = "multi_thread")]
async fn native_command_reads_lazy_snapshot_and_publishes_on_finite_work()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    acyclic_fs::initialize_native_mount_services()?;
    let mut fixture = Fixture::new().await?;
    let project = VolumeRef::new(
        fixture.filesystem.provider.clone(),
        "lazy-project".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(fixture.signed.agent().expect("fixture agent")),
        &mut None,
    )?;
    let mut grants = fixture
        .signed
        .capabilities()
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    grants.extend([
        project.capability(VolumeOperation::Read, &mut None)?,
        project.capability(VolumeOperation::Write, &mut None)?,
    ]);
    fixture.signed = fixture.issuer.issue(
        "owner",
        &Capabilities::new(grants),
        fixture.signed.agent(),
        None,
        &mut None,
    )?;
    fixture.scope = RuntimeScope::new(
        fixture.signed.capabilities().clone(),
        fixture.scope.limits(),
    )?
    .with_run_limits(fixture.scope.run_limits(), &mut None)?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    fixture.filesystem.create_volume(&project).await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let grant = ContentGrant::verify(
        &verifier,
        &fixture.signed,
        &project,
        VolumeOperation::Write,
        128,
        &mut None,
    )?;
    fixture
        .filesystem
        .put_content(
            &project,
            &grant,
            "input.txt",
            b"pinned snapshot",
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new("lazy-native-input")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024);
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, _)| cause)?;
    let io = PreparationIo::from_native_work(&work)?;
    let (input_schema, mut schema_custody) =
        crate::tool::clone_value(&fixture.machine.schema, None, &work)?;
    let (output_schema, mut output_custody) =
        crate::tool::clone_value(&json!({"type":"string"}), None, &work)?;
    schema_custody.absorb(&mut output_custody)?;
    fixture.tasks = TaskRegistry::default();
    fixture
        .tasks
        .register(TaskDefinition::<Value, Value>::resumable_with_native_work(
            fixture.machine.clone(),
            input_schema,
            output_schema,
            schema_custody,
            &work,
        )?)?;
    fixture.machines = MachineRegistry::default();
    let _machine_metadata = fixture
        .machines
        .register_with_native_work(fixture.machine.clone(), &work)?;
    let runtime = FilesystemTaskRuntime::open(
        fixture.stream.clone(),
        fixture.filesystem.clone(),
        &fixture.volume,
        None,
        &verifier,
        &fixture.signed,
        &fixture.scope,
        &fixture.tasks,
        &fixture.machines,
        &fixture.tools,
        SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 1,
            model_steps: 1,
        },
        1,
        65_536,
        fixture.clock.clone(),
        None,
        &[],
        &ResumableToolRegistry::default(),
        Some(&io),
    )
    .await
    .expect("native runtime opens within its allowance");
    let lease = fixture.admit(&runtime).await?;
    runtime
        .task_host()
        .start_task(&lease, &mut Some(&mut io.history_io()))
        .await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, _)| cause)?;
    let context = runtime
        .task_context_with_native_work(task, &fence, &work)
        .await
        .expect("native task context retains its original allowance");
    let owner = context.journal_owner::<MemoryStream>(&fence, &work)?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("input.txt"), b"live source")?;
    let native = acyclic_fs::demand::native::NativeDemandSource::open(
        Cow::Borrowed(source.path()),
        acyclic_fs::FilesystemProfile::Portable,
        acyclic_fs::VolumeLimits::default(),
        None,
        None,
        &work,
        &cancellation,
    )
    .await
    .expect("native source opens within its allowance");
    let workspace = fixture
        .filesystem
        .filesystem
        .open_workspace(project.storage_name(&mut None)?, &work, &cancellation)
        .await?;
    let lazy = LazyWorkspace::bind(
        workspace.clone(),
        Arc::new(native),
        MemoryLazyWorkspaceStore::default(),
        &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        },
    )
    .await
    .expect("lazy workspace binds within its allowance");
    let root = tempfile::tempdir()?;
    let preparation = tempfile::tempdir()?;
    std::fs::create_dir(preparation.path().join("target"))?;
    std::fs::create_dir(preparation.path().join("backup"))?;
    let (allocation, _allocation) =
        NativePreparationRoot::witness(root.path(), preparation.path(), &work)?.await?;
    let callback = context.volume_callback(&owner, &project, true, &work)?;
    std::fs::create_dir(root.path().join("project"))?;
    let head = workspace.head(&work, &cancellation).await?;
    let (head_ref, _head_custody) = crate::resources::GenerationRef::new(
        project.provider(),
        head.id().digest().as_bytes(),
        None,
        Some(&work),
    )?;
    let binding = BorrowedNativeVolumeBinding {
        host: &fixture.filesystem,
        volume: &project,
        generation: &head_ref,
        namespace: &NativeVolumeNamespace::MaterializedDirectory,
        mounted: None,
        path: "project",
        writable: true,
        verifier: &verifier,
        scope: &fixture.signed,
        publication: PublicationPermit::Unrestricted,
    };
    let options = binding
        .mount_options(&workspace, head.id(), &context, &work)
        .await?;
    let mount = Arc::new(
        lazy.mount(
            root.path().join("project"),
            options
                .publication(MountPublication::Manual)
                .native_context(&work, Some(callback), Some(&work)),
        )
        .await
        .expect("lazy mount starts within its allowance"),
    );
    let snapshot = mount.snapshot(&work, &cancellation).await?;
    let (generation, _generation) = crate::resources::GenerationRef::new(
        project.provider(),
        snapshot.authored_generation.digest().as_bytes(),
        None,
        Some(&work),
    )?;
    let mounted: Arc<dyn NativeMountedVolume> = mount.clone();
    let namespace = NativeVolumeNamespace::LazyMount { snapshot };
    let bindings = [BorrowedNativeVolumeBinding {
        host: &fixture.filesystem,
        volume: &project,
        generation: &generation,
        namespace: &namespace,
        mounted: Some(&mounted),
        path: "project",
        writable: true,
        verifier: &verifier,
        scope: &fixture.signed,
        publication: PublicationPermit::Unrestricted,
    }];
    let view = NativeVolumeView::prepare(
        &owner,
        &[context],
        &allocation,
        &NativeViewOptions {
            root: root.path().into(),
            maximum_volumes: 1,
            work_per_volume: budget,
        },
        &bindings,
        &work,
    )?
    .await
    .unwrap_or_else(|error| panic!("mounted preparation: {error:?}; work: {:?}", work.receipt()));
    view.validate_for_dispatch()
        .await
        .expect("mounted view validates within its allowance");
    let before = work.receipt();
    let mut stdout = work.copy_bytes(&[0; 128])?;
    let mut stderr = work.copy_bytes(&[0; 128])?;
    stdout.0.clear();
    stderr.0.clear();
    let executable = std::path::PathBuf::from(
        crate::native_environment::variable("SystemRoot").expect("Windows root"),
    )
    .join("System32")
    .join("cmd.exe");
    let mut command = Command::new(executable);
    command.creation_flags(0x08000000); // CREATE_NO_WINDOW.
    command
        .args(["/d", "/c", "type input.txt"])
        .current_dir(&view.manifest().volumes[0].path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let meter = work.runtime_meter();
    let mut process = None;
    acyclic_native_runtime::ProcessTree::spawn_controlled(
        &mut command,
        Some(meter.as_ref()),
        &mut process,
    )
    .unwrap_or_else(|error| {
        panic!(
            "native process launch: {:?}; before: {before:?}; work: {:?}",
            work.runtime_error(error),
            work.receipt()
        )
    });
    let output = process
        .as_mut()
        .expect("spawned process")
        .wait_with_output(
            Duration::from_secs(10),
            128,
            None,
            || Ok(()),
            |_| Ok(Default::default()),
            Some(meter.as_ref()),
            Some([stdout.0, stderr.0, Vec::new()]),
        )
        .unwrap_or_else(|error| {
            panic!(
                "native process wait: {:?}; stdout: {:?}; stderr: {:?}; work: {:?}",
                work.runtime_error(error.error),
                String::from_utf8_lossy(&error.stdout),
                String::from_utf8_lossy(&error.stderr),
                work.receipt()
            )
        });
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, b"pinned snapshot");
    view.file_io(
        &owner,
        &project,
        VolumeOperation::Write,
        std::path::Path::new("published.txt"),
        true,
        true,
        &work,
        |file, work| {
            use std::io::Write;
            work.charge(acyclic_fs::WorkCounters {
                output_bytes: 7,
                ..Default::default()
            })?;
            file.write_all(b"publish")
                .map_err(|error| work.runtime_error(error))
        },
    )
    .await
    .unwrap_or_else(|error| {
        let mut cause: &dyn std::error::Error = &error;
        while let Some(source) = cause.source() {
            cause = source;
        }
        panic!(
            "mounted write: {error:?}; root cause: {cause:?}; work: {:?}",
            work.receipt()
        )
    });
    let published = view
        .publish(&owner)
        .await
        .expect("mounted publication stays within its allowance");
    assert!(published.0[0].1.is_lazy());
    assert!(published.0[0].1.operation().is_some());
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    let cut = published.0[0]
        .1
        .lazy_snapshot(&mut accounting)?
        .expect("lazy publication retains its exact cut");
    let (content, used_source) = lazy
        .read_at_snapshot(cut, "/published.txt", None, 128, &mut accounting)
        .await?;
    assert_eq!(content.bytes.as_ref(), b"publish");
    assert!(!used_source);
    let receipt = work.receipt();
    receipt.verify(budget)?;
    assert!(receipt.backend_read_operations > before.backend_read_operations);
    assert_eq!(receipt.materializations, 0);
    drop(view);
    mount.retire(&CancellationToken::new())?;
    Ok(())
}
