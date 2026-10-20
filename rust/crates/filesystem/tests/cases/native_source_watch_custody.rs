//! Shared native sources retain watch registry capacity on its original ledger.
#![cfg(any(target_os = "linux", windows))]
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    CancellationToken, NativeMaterializationWork,
    demand::{
        DemandError, DemandSource, SourceChange, SourceChangeSink, native::NativeDemandSource,
    },
    model::{FilesystemProfile, VolumeLimits},
};
use std::{error::Error, sync::Arc};
struct Changes;
impl SourceChangeSink for Changes {
    fn source_changed(
        &self,
        _: &[SourceChange],
        _: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        Ok(())
    }
}

#[tokio::test]
async fn a_shared_source_registers_watches_from_different_original_ledgers()
-> Result<(), Box<dyn Error>> {
    acyclic_fs::initialize_native_mount_services()?;
    acyclic_native_runtime::initialize_native_io_services()?;
    let temp = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let source_work =
        NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .expect("source ledger");
    let first_work =
        NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .expect("first view ledger");
    let second_work =
        NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .expect("second view ledger");
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(temp.path()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        &source_work,
        &cancellation,
    )
    .await?;
    let first = source
        .watch(Arc::new(Changes), &first_work, &cancellation)?
        .ok_or("missing first source watch")?;
    let second = source
        .watch(Arc::new(Changes), &second_work, &cancellation)?
        .ok_or("missing second source watch")?;
    assert!(!first.is_stopped());
    assert!(!second.is_stopped());
    first.stop(&mut (&first_work).accounting(&cancellation))?;
    second.stop(&mut (&second_work).accounting(&cancellation))?;
    let ledgers = [&source_work, &first_work, &second_work];
    let retained = ledgers.map(|work| work.live_allocation_bytes());
    drop((first, second, source));
    for (work, before) in ledgers.into_iter().zip(retained) {
        assert!(
            work.live_allocation_bytes() < before,
            "original payload custody must retire"
        );
    }
    Ok(())
}
