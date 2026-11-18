use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    AuthoredMutation, ByteRange, CancellationToken, Fs, FsError, Lifecycle, LocalOptions,
    PublicationPermit, VolumeConfig,
};
use acyclic_fs_format::identity::{OperationId, VolumeId};
use acyclic_fs_format::metadata::FileMetadata;
#[cfg(any(unix, windows))]
use acyclic_fs_format::object_reference::{ObjectId, ObjectKind};
use acyclic_fs_work::performance::WorkBudget;
use acyclic_stream::OwnedBytes;

use crate::allow_publication::allow;
use crate::checkout_fixture::{path, writable_pinned};

#[cfg(any(unix, windows))]
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

#[cfg(any(unix, windows))]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_garbage_collection_keeps_live_state_while_the_root_is_open()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let file = path("retained.txt")?;
    let durable = VolumeConfig::portable(Lifecycle::Durable);
    let fs = Fs::local(options.clone()).await?;
    let volume = fs
        .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .create(
            AuthoredMutation::CreateFile {
                file_id: None,
                path: file.clone(),
                bytes: OwnedBytes::from_static(b"retained"),
                metadata: FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    checkout
        .commit(
            OperationId::from_bytes([49; 16]),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow::<FsError>,
        )
        .await?;
    let pending = path("pending.txt")?;
    checkout
        .create(
            AuthoredMutation::CreateFile {
                file_id: None,
                path: pending.clone(),
                bytes: OwnedBytes::from(vec![b'p'; 256 * 1_024]),
                metadata: FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    checkout
        .commit(
            OperationId::from_bytes([50; 16]),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow::<FsError>,
        )
        .await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    drop(checkout);
    drop(volume);
    drop(fs);

    let orphan_bytes = OwnedBytes::from(vec![b'o'; 64 * 1_024 + 1]);
    let objects = acyclic_objects::local::LocalObjects::open(
        directory.path().join("objects"),
        acyclic_objects::local::LocalObjectsLimits::default(),
        None,
    )
    .await?;
    acyclic_objects::Blobs::put(&objects, orphan_bytes, Vec::new()).await?;
    drop(objects);
    let reopened = Fs::local(options).await?;
    let collected = reopened.collect_local_garbage(None, &cancellation).await?;
    assert!(collected.removed >= 1);

    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let mut checkout = volume
        .value
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let retained = checkout
        .value
        .read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 8,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(retained.value.bytes.as_ref(), b"retained");
    let pending = checkout
        .value
        .read_file_range(
            &pending,
            ByteRange {
                offset: 0,
                length: 256 * 1_024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(
        pending.value.bytes.as_ref(),
        vec![b'p'; 256 * 1_024].as_slice()
    );
    Ok(())
}

/// Counts one local commit of `files` new files: the ordering barriers and
/// the full device drains its durable trace issues.
#[cfg(any(unix, windows))]
async fn local_commit_flushes(files: u8) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    let counted = schedule
        .scope(async {
            let cancellation = CancellationToken::new();
            let durable = VolumeConfig::portable(Lifecycle::Durable);
            let fs = Fs::local(LocalOptions::new(directory.path())).await?;
            let volume = fs
                .create_volume_with_id(
                    VolumeId::from_bytes([51; 16]),
                    durable,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            let mut checkout = volume
                .checkout(
                    GenerationSelector::Head,
                    writable_pinned(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            for file in 0..files {
                checkout
                    .create(
                        AuthoredMutation::CreateFile {
                            file_id: None,
                            path: path(&format!("f{file}.txt"))?,
                            bytes: OwnedBytes::from(vec![file; 4096]),
                            metadata: FileMetadata::default(),
                        },
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await?;
            }
            let before = schedule.durable_write_count();
            checkout
                .commit(
                    OperationId::from_bytes([51; 16]),
                    None,
                    None,
                    None,
                    PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                    allow::<FsError>,
                )
                .await?;
            let trace = schedule.durable_writes();
            let commit = trace.get(before..).ok_or("durable trace lost its prefix")?;
            let barrier = |call: &acyclic_native_runtime::testing::DurableWrite| {
                call.operation.ends_with("_barrier")
            };
            let drain = |call: &acyclic_native_runtime::testing::DurableWrite| {
                matches!(
                    call.operation,
                    "file:sync" | "file:sync_data" | "file:sync_all" | "file:sync_parent"
                )
            };
            let counted = (
                commit.iter().filter(|call| barrier(call)).count(),
                commit.iter().filter(|call| drain(call)).count(),
            );
            assert!(
                commit.iter().rposition(barrier) < commit.iter().rposition(drain),
                "the acknowledging drain must follow every ordering barrier: {commit:?}"
            );
            drop(checkout);
            drop(volume);
            drop(fs);
            Ok::<_, Box<dyn std::error::Error>>(counted)
        })
        .await?;
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    Ok(counted)
}

/// A commit's objects are only ordered ahead of its Stream frame, and the
/// frame's flush is the commit's one device drain, at any commit size.
#[cfg(any(unix, windows))]
#[tokio::test]
async fn a_local_commit_drains_the_device_once_at_any_size()
-> Result<(), Box<dyn std::error::Error>> {
    let small = local_commit_flushes(1).await?;
    let large = local_commit_flushes(64).await?;
    eprintln!("LOCAL_COMMIT_FLUSHES files=1 {small:?} files=64 {large:?}");
    assert_eq!(small, (1, 1), "unexpected flush counts for one file");
    assert_eq!(large, (1, 1), "unexpected flush counts for 64 files");
    Ok(())
}
