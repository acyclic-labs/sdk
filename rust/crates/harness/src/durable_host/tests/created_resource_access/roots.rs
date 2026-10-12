use super::*;

pub(super) async fn create(
    filesystem: &Arc<MemoryFilesystem>,
    payload: &VolumeRef,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<[OriginalRootFilesystemBirth; 2]> {
    let project = VolumeRef::new(
        payload.provider().clone(),
        "parent-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("created-project".into()),
        &mut None,
    )?;
    let config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable);
    let mut births = Vec::new();
    for (volume, operation, bytes) in [
        (
            project,
            acyclic_fs::OperationId::from_bytes([177; 16]),
            b"original project import".as_slice(),
        ),
        (
            payload.clone(),
            acyclic_fs::OperationId::from_bytes([178; 16]),
            b"original root private".as_slice(),
        ),
    ] {
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let name = volume.storage_name(&mut Some(&mut io))?;
        let name_custody = io.into_custody()?;
        let initial = filesystem
            .filesystem
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                &name,
                config,
                operation,
                work,
                work.cancellation(),
                None,
            )
            .await?;
        let initial_generation = initial.id();
        let initial_ref = GenerationRef::new(
            volume.provider(),
            initial_generation.digest().as_bytes(),
            None,
            None,
        )?
        .0;
        let workspace = workspace_ref(volume.provider().clone(), &name)?;
        let selected = filesystem
            .apply(
                &workspace,
                Some(&initial_ref),
                &[WorkspaceMutation::PutFile {
                    path: "/root.txt".into(),
                    bytes: bytes.to_vec().into(),
                }],
                &IdempotencyKey::new(format!(
                    "original-root-import-{}",
                    uuid::Uuid::from_bytes(operation.into_bytes())
                ))?,
            )
            .await?;
        let selected_generation = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
            selected
                .as_resource()
                .key()
                .try_into()
                .map_err(|_| Error::Invalid("fixture generation size".into()))?,
        ));
        assert_ne!(initial_generation, selected_generation);
        births.push(OriginalRootFilesystemBirth {
            volume,
            operation,
            config,
            initial_generation,
            selected_generation,
        });
        drop((initial, name, name_custody));
    }
    births
        .try_into()
        .map_err(|_| Error::Invalid("fixture root birth count".into()))
}
