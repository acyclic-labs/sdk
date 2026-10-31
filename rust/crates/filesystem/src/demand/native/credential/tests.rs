use super::*;
use futures::FutureExt;
use std::sync::atomic::AtomicUsize;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn bind(
    directory: &std::path::Path,
    entry: &str,
    original: &crate::NativeMaterializationWork,
    reads: &Arc<AtomicUsize>,
) -> TestResult<NativeCredentialSource> {
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(directory),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        original,
        original.cancellation(),
    )
    .await?;
    let portable = crate::path::PortablePath::parse(&format!("/{entry}"), VolumeLimits::default())?;
    let path = NamespacePath::from_portable(&portable, VolumeLimits::default())?;
    let read_work = original.clone();
    let reads = reads.clone();
    Ok(NativeCredentialSource::bind(
        source,
        path,
        original.clone(),
        || 1,
        move |_, revision, node, file| {
            let original = read_work.clone();
            let reads = reads.clone();
            async move {
                // The real file is the fixture's original protected source. There
                // is no provider stream, HTTP request or credential substitute.
                let bytes = spawn_blocking(move || {
                    reads.fetch_add(1, Ordering::Relaxed);
                    let length = node
                        .logical_bytes
                        .ok_or_else(|| error(DemandError::NotRegularFile))?;
                    let bytes = file
                        .read_range(
                            0,
                            length,
                            &mut crate::kernel::KernelAccounting::native(
                                &original,
                                original.cancellation(),
                            ),
                        )
                        .map_err(error)?;
                    String::from_utf8(bytes.bytes.as_ref().to_vec()).map_err(error)
                })
                .await
                .map_err(|_| error(CredentialSourceError::Unavailable))??;
                CredentialSnapshot::new(bytes, revision, 100)
            }
        },
    )
    .await?)
}

#[tokio::test]
async fn native_source_fences_rotation_and_owner_loss_without_warm_reads() -> TestResult {
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("credential"), "first-source-snapshot")?;
    std::fs::write(
        directory.path().join("independent"),
        "independent-source-snapshot",
    )?;
    let stop = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &stop)
            .map_err(|(error, _)| error)?;
    let reads = Arc::new(AtomicUsize::new(0));
    let other_reads = Arc::new(AtomicUsize::new(0));
    let source = bind(directory.path(), "credential", &original, &reads).await?;
    let other = bind(directory.path(), "independent", &original, &other_reads).await?;
    let (first, follower) = tokio::join!(source.resolve(), source.resolve());
    let first = first?;
    let follower = follower?;
    assert_eq!(reads.load(Ordering::Relaxed), 1, "one cold source read");
    assert_eq!(first.binding(), follower.binding());
    first.check().await?;
    let before_warm = original.receipt();
    let warm = source.resolve().await?;
    let after_warm = original.receipt();
    assert_eq!(warm.binding(), first.binding());
    assert_eq!(after_warm.source_bytes_read, before_warm.source_bytes_read);
    assert_eq!(
        after_warm.backend_write_operations,
        before_warm.backend_write_operations
    );
    assert_eq!(
        after_warm.durability_operations,
        before_warm.durability_operations
    );

    assert_eq!(
        reads.load(Ordering::Relaxed),
        1,
        "warm access does not read/parse"
    );
    assert!(
        warm.retained_access().is_none(),
        "a physical fence is asynchronous"
    );

    let independent_request = other.resolve().await?;
    let independent_change = other.watch().await?.ok_or("missing independent watch")?;
    let changed = source.watch().await?.ok_or("missing source watch")?;
    std::fs::write(
        directory.path().join("credential"),
        "rotated-source-snapshot",
    )?;
    // The original inotify fence is the event producer. No timer, sleep,
    // repeated read, readiness probe or polling loop advances this control.
    source.0.watch.clone().fence().await?;
    changed.await?;
    assert!(
        first.check().await.is_err(),
        "old source cannot retain effects"
    );
    let current = source.resolve().await?;
    assert_ne!(current.binding(), first.binding());
    assert_eq!(current.as_str(), "rotated-source-snapshot");
    assert_eq!(reads.load(Ordering::Relaxed), 2);
    assert!(
        !first.reject().await?,
        "old rejection cannot discard replacement"
    );
    current.check().await?;
    independent_request.check().await?;
    assert!(independent_change.now_or_never().is_none());
    assert_eq!(other_reads.load(Ordering::Relaxed), 1);

    let lost = source.watch().await?.ok_or("missing owner-loss watch")?;
    let physical = source.0.watch.clone();
    drop(source);
    assert!(matches!(lost.await, Err(CredentialCacheError::Unavailable)));
    assert!(
        current.check().await.is_err(),
        "a request cannot own the producer"
    );
    physical.physical.stop_accepted_cleanup()?;
    assert!(physical.physical.is_stopped());

    let cancelled = other.watch().await?.ok_or("missing invocation watch")?;
    stop.cancel();
    assert!(matches!(
        cancelled.await,
        Err(CredentialCacheError::Unavailable)
    ));
    assert!(independent_request.check().await.is_err());
    other.0.watch.physical.stop_accepted_cleanup()?;
    assert!(other.0.watch.physical.is_stopped());
    eprintln!(
        "protected source reads: cold=1 warm=0 rotation=1 independent=1; original cleanup joined"
    );
    Ok(())
}
