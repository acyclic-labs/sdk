use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    CancellationToken, Fs, Lifecycle, LocalOptions, ObjectId, ObjectKind, VolumeConfig, VolumeId,
};
use acyclic_fs_work::performance::WorkBudget;

use crate::checkout_modes::writable_pinned;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_reopens_durable_volume_and_exact_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let volume_id = VolumeId::from_bytes([48; 16]);
    let cancellation = CancellationToken::new();
    let generation_id;
    let root_released;
    let durable = VolumeConfig::portable(Lifecycle::Durable);
    {
        let fs = Fs::local(LocalOptions::new(directory.path())).await?;
        root_released = fs
            .local_root_release_barrier()
            .ok_or("local root has no release barrier")?;
        let created = fs
            .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        let checkout = created
            .value
            .checkout(
                GenerationSelector::Head,
                writable_pinned(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        generation_id = checkout.value.generation_id();
    }
    root_released.released().await;
    for relative in ["stream/stream.lock", "objects/owner.lock"] {
        drop(acyclic_native_runtime::StoreClaim::acquire(
            &directory.path().join(relative),
        )?);
    }
    let reopened = Fs::local(LocalOptions::new(directory.path())).await?;
    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let exact = volume
        .value
        .checkout(
            GenerationSelector::Exact(generation_id),
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(exact.value.generation_id(), generation_id);
    assert_eq!(exact.value.volume_id(), volume_id);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_shares_bounded_object_acceleration_across_handles()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let durable = VolumeConfig::portable(Lifecycle::Durable);
    let fs = Fs::local(LocalOptions::new(directory.path())).await?;
    let volume = fs
        .create_volume(durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let root = ObjectId {
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: checkout.generation_id().digest(),
    };

    fs.clear_object_cache()?;
    let baseline = fs.object_cache_stats()?;
    assert_eq!(baseline.resident_entries, 0);
    let first = fs
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let cloned = fs.clone();
    let warm = cloned
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;

    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    let stats = fs.object_cache_stats()?;
    assert_eq!(stats.misses - baseline.misses, 1);
    assert_eq!(stats.hits - baseline.hits, 1);
    assert_eq!(stats.resident_entries, 1);
    assert!(stats.resident_bytes > 0);
    Ok(())
}
