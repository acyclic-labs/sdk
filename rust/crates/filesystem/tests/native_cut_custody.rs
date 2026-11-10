//! Composite continuations keep their decoded backing on its original meter.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::{
    CancellationToken, Fs, LazyDirectoryCursor, LazyWorkspace, Lifecycle, MemoryLazyWorkspaceStore,
    NativeMaterializationWork, OperationId, VolumeConfig, VolumeInitialization, WorkBudget,
    demand::{Empty, Routed, native::NativeDemandSource},
    kernel::{KernelAccounting, LogicalName, NameEncoding},
};
use std::{borrow::Cow, sync::Arc};

#[tokio::test]
async fn decoded_cut_credit_survives_a_cursor_clone_on_another_meter()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("a"), b"a")?;
    std::fs::write(directory.path().join("b"), b"b")?;
    let cancel = CancellationToken::new();
    let capture = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let native = NativeDemandSource::open(
        Cow::Borrowed(directory.path()),
        config.profile,
        config.limits,
        None,
        None,
        &capture,
        &cancel,
    )
    .await?;
    let source = Arc::new(Routed::new(
        Empty,
        LogicalName::new(
            NameEncoding::Utf8,
            b"ui".to_vec(),
            config.limits.maximum_component_bytes,
        )?,
        native,
        config.limits,
    ));
    let fs = Fs::memory();
    let (view, _) = LazyWorkspace::attach(
        &fs,
        "cursor-credit",
        source,
        MemoryLazyWorkspaceStore::default(),
        VolumeInitialization {
            config,
            access_policy: None,
        },
        OperationId::new(),
        &mut KernelAccounting::native(&capture, &cancel),
    )
    .await?;
    let (mut page, backing) = view
        .list_directory(
            "/ui",
            None,
            1,
            &mut KernelAccounting::native(&capture, &cancel),
        )
        .await?;
    let cursor = page.next.take().ok_or("fixture must have another page")?;
    drop(page);
    drop(backing);
    let (encoded, backing) = cursor.encode(&capture)?;
    cursor.retire(&mut KernelAccounting::native(&capture, &cancel))?;

    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let receiver = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let original_baseline = original.live_allocation_bytes();
    let receiver_baseline = receiver.live_allocation_bytes();
    let decoded = LazyDirectoryCursor::decode(&encoded, config.profile, config.limits, &original)?;
    drop(encoded);
    drop(backing);
    let clone = decoded.clone(&mut KernelAccounting::native(&receiver, &cancel))?;
    decoded.retire(&mut KernelAccounting::native(&original, &cancel))?;
    assert!(
        original.live_allocation_bytes() > original_baseline,
        "the clone still holds decoded source cuts funded by the original meter"
    );
    clone.retire(&mut KernelAccounting::native(&receiver, &cancel))?;
    assert_eq!(original.live_allocation_bytes(), original_baseline);
    assert_eq!(receiver.live_allocation_bytes(), receiver_baseline);
    Ok(())
}
