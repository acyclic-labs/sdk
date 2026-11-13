//! Blocking native access must not require the embedder's Tokio executor.

use super::*;

#[test]
fn current_thread_embedder_writes_through_its_own_view() -> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let storage = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut session = runtime.block_on(async {
        let fs = crate::Fs::local(crate::LocalOptions::new(storage.path().join("fs"))).await?;
        let cancellation = crate::CancellationToken::new();
        let config = posix_config();
        let volume = fs
            .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let checkout = volume
            .checkout(
                crate::GenerationSelector::Head,
                crate::CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let source = crate::native_mount::CheckoutMountSource::new(
            Arc::new(crate::native_mount::SharedCheckout::with_publication(
                checkout,
                crate::MountPublication::CloseAndSync,
            )),
            config,
        )?;
        let source =
            crate::native_mount::adapter::tests::access::bind_test_access(source, vec![], None)?;
        let session = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: destination.path().join("view"),
                writable: true,
                shared: true,
            },
            Arc::new(source),
        )?;
        // Deliberately stay on the only runtime thread. No spawn_blocking,
        // block_in_place, executor migration, or second task drives this call.
        let path = destination.path().join("view/file");
        std::fs::write(&path, b"current-thread owner")?;
        assert_eq!(std::fs::read(path)?, b"current-thread owner");
        Ok::<_, Box<dyn std::error::Error>>(session)
    })?;
    assert!(session.stop()?);
    Ok(())
}
