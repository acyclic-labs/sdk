use acyclic_fs::{
    CancellationToken, Fs, FsError, Lifecycle, VolumeConfig, VolumeInitialization, WorkspaceError,
};
use acyclic_fs_format::identity::OperationId;
use std::error::Error;

use crate::creation_budget::FINITE_BUDGET;

#[tokio::test]
async fn creation_lookup_is_readonly_exact_and_bounded() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let operation = OperationId::from_bytes([0xd4; 16]);
    let cancellation = CancellationToken::new();
    let budget = FINITE_BUDGET;
    let initial = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "creation",
            VolumeInitialization {
                config,
                access_policy: None,
            },
            operation,
            budget,
            &cancellation,
            None,
        )
        .await?
        .value;
    let workspace = unbounded!(fs, open_workspace("creation"))?;
    workspace.write_text("/later", "advanced head").await?;
    let head = unbounded!(workspace, head())?;
    assert_ne!(head.id(), initial.id());
    for selection in [Some(config), None] {
        let original = fs
            .workspace_for_creation_operation(
                "creation",
                selection,
                operation,
                budget,
                &cancellation,
            )
            .await?;
        original.work.verify(budget)?;
        assert_eq!(original.work.backend_write_operations, 0);
        let original = original.value.ok_or("creation missing")?;
        assert_eq!(original.id(), initial.id());
        assert_eq!(original.workspace_id(), workspace.id());
        assert_eq!(original.workspace().volume_config(), config);
    }
    let mut wrong_config = config;
    wrong_config.lifecycle = Lifecycle::Durable;
    for (config, operation) in [
        (Some(config), OperationId::from_bytes([0xd5; 16])),
        (None, OperationId::from_bytes([0xd5; 16])),
        (Some(wrong_config), operation),
    ] {
        let failure = fs
            .workspace_for_creation_operation("creation", config, operation, budget, &cancellation)
            .await
            .err()
            .ok_or("mismatched creation accepted")?;
        failure.work()?.verify(budget)?;
        assert!(matches!(
            failure.error,
            WorkspaceError::Filesystem(FsError::CreationRejected)
        ));
    }
    assert!(
        unbounded!(
            fs,
            workspace_for_creation_operation("missing", config, operation)
        )?
        .is_none()
    );
    assert!(unbounded!(fs, open_workspace("missing")).is_err());
    let mut denied = budget;
    denied.backend_read_operations = 0;
    let failure = fs
        .workspace_for_creation_operation("creation", config, operation, denied, &cancellation)
        .await
        .err()
        .ok_or("read budget ignored")?;
    failure.work()?.verify(denied)?;
    assert_eq!(unbounded!(workspace, head())?.id(), head.id());
    Ok(())
}
