use super::verify::source_budget;
use super::*;
use crate::conversation::Limits;
use crate::conversation::{VolumeOperation, VolumeOwner};
use crate::core::AggregateKind;
use crate::runtime::{TaskAdmissionRecord, TaskRunLimits};

fn admission(root: crate::OperationId, project: &VolumeRef) -> Result<TaskAdmissionRecord> {
    Ok(TaskAdmissionRecord {
        operation_id: root,
        task: crate::registry::ComponentIdentity {
            name: "test.root-bootstrap".into(),
            version: "1".into(),
            digest: [41; 32],
        },
        machine: crate::workflow::MachineIdentity {
            name: "test.root-bootstrap".into(),
            version: "1".into(),
            digest: [42; 32],
        },
        input: serde_json::json!({}),
        input_schema: serde_json::json!({"type":"object"}),
        output_schema: serde_json::json!({"type":"object"}),
        parent: None,
        resource_proofs: Vec::new(),
        grants: crate::Capabilities::new([project.capability(VolumeOperation::Read, &mut None)?]),
        limits: Limits::default(),
        run_limits: TaskRunLimits::default(),
        policy: None,
        extensions: None,
        execution: None,
    })
}

fn context<'a>(
    owner: &'a Authority,
    admission: &'a TaskAdmissionRecord,
    root: TaskId,
    cancellation: &'a acyclic_fs::CancellationToken,
) -> RootProjectSourceContext<'a> {
    RootProjectSourceContext {
        owner,
        caller: admission,
        source: admission,
        graph_root: root,
        operation_id: crate::OperationId::from_bytes([45; 16]),
        maximum_work: 14_000_000,
        maximum_bytes: 90_000_000,
        cancellation,
        created_project_authenticated: false,
    }
}

#[tokio::test]
async fn real_filesystem_birth_without_protected_root_association_is_not_source_authority()
-> Result<()> {
    let provider = crate::resources::ProviderRef::new(
        "root-birth".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let project = VolumeRef::new(
        provider,
        "root-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("root-project".into()),
        &mut None,
    )?;
    let owner = Authority {
        kind: AggregateKind::Task,
        id: "root-owner".into(),
    };
    let operation = acyclic_fs::OperationId::from_bytes([43; 16]);
    let root_operation = crate::OperationId::from_bytes([44; 16]);
    let root = crate::runtime::task_id_for_admission(root_operation);
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = source_budget(14_000_000, 90_000_000)?;
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .expect("finite original Root bootstrap ledger");
    let config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable);
    let birth = host
        .filesystem
        .create_workspace_with_operation_with_native_work(
            &project.storage_name(&mut None)?,
            config,
            operation,
            &work,
            &cancellation,
        )
        .await
        .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
    let selected = ProjectSourceSnapshot {
        project: project.clone(),
        generation: host.generation_ref_id(birth.id())?,
        native_snapshot: None,
    };
    drop(birth);
    let policy = FsRootProjectSourceProvider::new(
        host,
        &owner,
        root,
        Some(&super::super::PreparationIo::from_native_work(&work)?),
    )?;
    let original = admission(root_operation, &project)?;
    assert!(matches!(
        policy
            .verify_with_native_work(
                context(&owner, &original, root, &cancellation),
                &selected,
                &work
            )?
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    Ok(())
}

#[test]
fn selected_lazy_snapshot_decodes_with_finite_io() -> Result<()> {
    let value = serde_json::json!({
        "id": ([1; 32]), "workspace_id": ([2; 16]), "authored_generation": ([3; 32]),
        "source": { "identity": ([4; 16]), "epoch": 7 },
        "overlay": ([5; 32]), "shadows": ([6; 32])
    });
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = source_budget(14_000_000, 90_000_000)?;
    let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)?;
    let ordinary = crate::filesystem::PreparationIo::from_budget(budget, &cancellation)?;
    let original = crate::filesystem::PreparationIo::from_native_work(&native)?;
    for io in [&ordinary, &original] {
        let (snapshot, _hold) = super::verify::source_snapshot(
            &value,
            JsonBounds {
                maximum_bytes: 4096,
                maximum_items: 4096,
                maximum_depth: 32,
            },
            io,
        )?;
        assert_eq!(snapshot.workspace_id.into_bytes(), [2; 16]);
        assert_eq!(snapshot.source.epoch, 7);
    }
    Ok(())
}
