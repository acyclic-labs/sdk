use super::*;
use futures::FutureExt;
use std::sync::atomic::AtomicUsize;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn native_source(
    directory: &std::path::Path,
    original: &crate::NativeMaterializationWork,
) -> TestResult<NativeDemandSource> {
    Ok(NativeDemandSource::open(
        std::borrow::Cow::Borrowed(directory),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        original,
        original.cancellation(),
    )
    .await?)
}

async fn bind(
    source: NativeDemandSource,
    entry: &str,
    original: &crate::NativeMaterializationWork,
    reads: &Arc<AtomicUsize>,
) -> TestResult<NativeCredentialSource> {
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
                CredentialSnapshot::new(bytes, revision, 100).map(NativeCredentialUpdate::Observed)
            }
        },
    )
    .await?)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PublicationControl {
    Current,
    Altered,
    Replaced,
    Observed,
    ForeignRevision,
}

async fn publication_control(control: PublicationControl) -> TestResult {
    use acyclic_native_runtime::{RenameMode, private_file::StagedFile};
    use std::io::Write;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("credential");
    std::fs::write(&path, "original-protected-source")?;
    let stop = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &stop)
            .map_err(|(error, _)| error)?;
    let root = native_source(directory.path(), &original).await?;
    let portable = crate::path::PortablePath::parse("/credential", VolumeLimits::default())?;
    let selected = NamespacePath::from_portable(&portable, VolumeLimits::default())?;
    let publications = Arc::new(AtomicUsize::new(0));
    let attempts = publications.clone();
    let source = NativeCredentialSource::bind(
        root,
        selected,
        original.clone(),
        || 1,
        move |rejected, revision, _, file| {
            let path = path.clone();
            let attempts = attempts.clone();
            async move {
                drop(file);
                if rejected.is_none() {
                    return CredentialSnapshot::new(
                        "original-protected-source".into(),
                        revision,
                        100,
                    )
                    .map(NativeCredentialUpdate::Observed);
                }
                // Exercise the real original private publisher, not a synthetic
                // SourceChange or a reconstructed receipt. Its descriptor remains
                // open across the original native fence and cache admission.
                spawn_blocking(move || {
                    let number = attempts.fetch_add(1, Ordering::Relaxed);
                    let token = format!("published-protected-source-{number}");
                    let mut staged =
                        StagedFile::create(path.parent().expect("parent"), ".credential-")
                            .map_err(error)?;
                    staged
                        .as_file_mut()
                        .write_all(token.as_bytes())
                        .map_err(error)?;
                    let file = staged
                        .publish_retained(&path, RenameMode::Replace)
                        .map_err(error)?;
                    match control {
                        PublicationControl::Altered => {
                            std::fs::write(&path, "foreign-in-place-value").map_err(error)?
                        }
                        PublicationControl::Replaced => {
                            let mut foreign =
                                StagedFile::create(path.parent().expect("parent"), ".foreign-")
                                    .map_err(error)?;
                            foreign
                                .as_file_mut()
                                .write_all(b"foreign-replacement")
                                .map_err(error)?;
                            foreign.publish(&path, RenameMode::Replace).map_err(error)?;
                        }
                        _ => {}
                    }
                    let revision = if control == PublicationControl::ForeignRevision {
                        CredentialRevision::Bytes([42; 32])
                    } else {
                        revision
                    };
                    let snapshot = CredentialSnapshot::new(token, revision, 100)?;
                    if control == PublicationControl::Observed {
                        drop(file);
                        Ok(NativeCredentialUpdate::Observed(snapshot))
                    } else {
                        Ok(NativeCredentialUpdate::Published { snapshot, file })
                    }
                })
                .await
                .map_err(|_| error(CredentialSourceError::Unavailable))?
            }
        },
    )
    .await?;
    let first = source.resolve().await?;
    assert!(first.reject().await?);
    let (replacement, follower) = tokio::join!(source.resolve(), source.resolve());
    if control == PublicationControl::Current {
        let replacement = replacement?;
        assert_eq!(replacement.as_str(), follower?.as_str());
        assert_eq!(publications.load(Ordering::Relaxed), 1);
        let held = source
            .0
            .state
            .observation
            .lock()
            .map_err(|_| "observation poisoned")?
            .published
            .is_some();
        assert!(
            held,
            "the accepted snapshot retains the original descriptor"
        );
        let warm = source.resolve().await?;
        assert_eq!(warm.binding(), replacement.binding());
        assert_eq!(
            publications.load(Ordering::Relaxed),
            1,
            "no warm publication"
        );
        assert!(
            !first.reject().await?,
            "late 401 cannot reject a replacement"
        );
        assert!(replacement.reject().await?);
        let next = source.resolve().await?;
        next.check().await?;
        assert_ne!(next.binding(), replacement.binding());
        assert_eq!(
            publications.load(Ordering::Relaxed),
            2,
            "one refresh per rejection"
        );
    } else {
        assert!(
            replacement.is_err() && follower.is_err(),
            "foreign, changed or omitted publication evidence must refuse"
        );
        assert_eq!(
            publications.load(Ordering::Relaxed),
            1,
            "no publication redispatch"
        );
    }
    let physical = source.0.watch.clone();
    drop(source);
    physical.physical.stop_accepted_cleanup()?;
    assert!(physical.physical.is_stopped());
    Ok(())
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
    let root = native_source(directory.path(), &original).await?;
    let source = bind(root.clone(), "credential", &original, &reads).await?;
    let other = bind(root, "independent", &original, &other_reads).await?;
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
    assert_ne!(
        independent_request.binding(),
        warm.binding(),
        "a shared native root must not conflate credential partitions"
    );
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
    for control in [
        PublicationControl::Current,
        PublicationControl::Altered,
        PublicationControl::Replaced,
        PublicationControl::Observed,
        PublicationControl::ForeignRevision,
    ] {
        publication_control(control).await?;
    }
    eprintln!(
        "protected source reads: cold=1 warm=0 rotation=1 independent=1; original cleanup joined"
    );
    Ok(())
}
