//! Restore the original sealed generation through the shipping command.
use acyclic_fs::{
    AuthoredMutation, CancellationToken, Fs, Lifecycle, LivePublicationObservation, LocalOptions,
    OperationId, PublicationPermit, VolumeConfig, WorkBudget,
    kernel::{FileMetadata, LogicalName, NameEncoding, NamespacePath},
    model::{AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, MutationMode},
};
use std::{path::Path, process::Command, time::Duration};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn restore(
    store: &Path,
    volume: &str,
    generation: &str,
    destination: &Path,
) -> Result<std::process::Output> {
    Ok(acyclic_native_runtime::process_output(
        Command::new(env!("CARGO_BIN_EXE_qualify"))
            .arg("restore-gen")
            .arg(store)
            .arg(volume)
            .arg(generation)
            .arg(destination),
        Duration::from_secs(5),
        usize::MAX,
    )?)
}

#[test]
fn restore_gen_preserves_sealed_storage_and_refuses_invalid_references_before_io() -> Result {
    let directory = tempfile::tempdir()?;
    let store = directory.path().join("store");
    let runtime = tokio::runtime::Runtime::new()?;
    let (volume, generation, released) = runtime.block_on(async {
        let fs = Fs::local(LocalOptions::new(&store)).await?;
        let released = fs
            .local_root_release_barrier()
            .ok_or("missing root release event")?;
        let cancel = CancellationToken::new();
        let config = VolumeConfig::portable(Lifecycle::Durable);
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancel)
            .await?
            .value;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await?
            .value;
        checkout
            .create(
                AuthoredMutation::CreateFile {
                    path: NamespacePath::new(
                        vec![LogicalName::new(
                            NameEncoding::Utf8,
                            b"original".to_vec(),
                            config.limits.maximum_component_bytes,
                        )?],
                        config.limits,
                    )?,
                    bytes: acyclic_stream::OwnedBytes::from_static(b"sealed generation contents"),
                    metadata: FileMetadata::default(),
                },
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await?;
        let committed = checkout
            .commit(
                OperationId::new(),
                None,
                None,
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancel,
                |_| async {
                    Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: Default::default(),
                        },
                    )
                },
            )
            .await?
            .value;
        let generation = match committed {
            LivePublicationObservation::Committed { generation_id, .. }
            | LivePublicationObservation::AlreadyCommitted { generation_id, .. } => generation_id,
            other => return Err(format!("fixture publication was not durable: {other:?}").into()),
        };
        Ok::<_, Box<dyn std::error::Error>>((checkout.volume_id(), generation, released))
    })?;
    runtime.block_on(runtime.spawn_blocking(move || {
        released.wait_until(std::time::Instant::now() + Duration::from_secs(5))
    }))??;
    assert!(
        generation.object_id().storage.is_some(),
        "fixture must require the sealed association"
    );
    let volume = uuid::Uuid::from_bytes(volume.into_bytes()).to_string();
    let destination = directory.path().join("restored");
    let output = restore(
        &store,
        &volume,
        &hex::encode(generation.to_bytes().as_slice()),
        &destination,
    )?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(destination.join("original"))?,
        b"sealed generation contents"
    );

    let missing_store = directory.path().join("must-not-open");
    let rejected = directory.path().join("must-not-create");
    let mut wrong_kind = generation.object_id();
    wrong_kind.kind = acyclic_fs::ObjectKind::BlobChunk;
    for malformed in [
        hex::encode(generation.digest().as_bytes()),
        hex::encode(wrong_kind.to_bytes().as_slice()),
        "0中".to_owned(),
    ] {
        let output = restore(&missing_store, &volume, &malformed, &rejected)?;
        assert_eq!(
            output.status.code(),
            Some(1),
            "must refuse without a panic: {output:?}"
        );
        let error = String::from_utf8(output.stderr)?;
        assert!(error.contains("step=restore-gen admission:"), "{error}");
        assert!(error.contains("recovery: qualify restore-gen"), "{error}");
        assert!(!missing_store.exists(), "invalid input opened a store");
        assert!(!rejected.exists(), "invalid input created a destination");
    }
    Ok(())
}
