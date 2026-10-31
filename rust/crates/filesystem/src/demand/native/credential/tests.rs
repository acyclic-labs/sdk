use super::*;
use futures::FutureExt;
use std::sync::atomic::AtomicUsize;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// Synthetic token bytes; the allocation and cache/request owners are real.
#[tokio::test]
async fn retained_token_custody_follows_exact_snapshot_and_request_retirement() -> TestResult {
    use acyclic_auth::credential::{CredentialProvider, CredentialToken};

    fn snapshot(
        work: &crate::NativeMaterializationWork,
        text: &str,
    ) -> Result<CredentialSnapshot, CredentialError> {
        let frame = std::mem::size_of::<CredentialToken<crate::NativeAllocation>>() as u64;
        let mut allocation = work
            .claim(frame + text.len() as u64, 2, crate::WorkCounters::UNCHARGED)
            .map_err(error)?;
        let token = text.to_owned();
        allocation
            .reconcile_capacity(frame + token.capacity() as u64)
            .map_err(error)?;
        let pointer = token.as_ptr();
        let snapshot = CredentialSnapshot::from_retained(
            Box::new(CredentialToken::new(token, allocation)),
            CredentialRevision::Bytes([1; 32]),
            100,
        )?;
        assert_eq!(
            snapshot.token().as_ptr(),
            pointer,
            "token must not be copied"
        );
        Ok(snapshot)
    }

    let stop = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(64, 1024 * 1024),
        &stop,
    )
    .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    assert!(matches!(
        snapshot(&original, "invalid\r\ntoken"),
        Err(CredentialError::Invalid)
    ));
    assert_eq!(original.live_allocation_bytes(), baseline);

    let attempts = Arc::new(AtomicUsize::new(0));
    let refresh_attempts = attempts.clone();
    let refresh_work = original.clone();
    let cache = CredentialCache::new(
        move |_| {
            let attempt = refresh_attempts.fetch_add(1, Ordering::Relaxed);
            futures::future::ready(snapshot(
                &refresh_work,
                if attempt < 2 {
                    "synthetic-original"
                } else {
                    "synthetic-replacement"
                },
            ))
        },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some(CredentialRevision::Bytes([1; 32])),
        },
    );
    let provider = CredentialProvider::from(cache.clone());
    let old = provider.resolve().await?;
    let retained_old = old.clone();
    let first_bytes = original.live_allocation_bytes();
    assert!(first_bytes > baseline);
    let receipt = original.receipt();
    for _ in 0..1024 {
        let warm = provider.resolve().await?;
        assert_eq!(warm.as_str().as_ptr(), old.as_str().as_ptr());
        warm.check().await?;
    }
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
    assert_eq!(original.receipt(), receipt, "warm leases add no token work");

    assert!(old.reject().await?);
    assert!(matches!(
        provider.resolve().await,
        Err(CredentialError::Provider(error))
            if matches!(error.downcast_ref::<CredentialCacheError>(), Some(CredentialCacheError::Rejected))
    ));
    assert_eq!(original.live_allocation_bytes(), first_bytes);
    let current = provider.resolve().await?;
    let both_bytes = original.live_allocation_bytes();
    assert!(both_bytes > first_bytes);
    assert!(old.check().await.is_err());
    assert!(
        !old.reject().await?,
        "late 401 cannot retire the replacement"
    );
    current.check().await?;
    assert!(snapshot(&original, "invalid\r\nreplacement").is_err());
    assert_eq!(original.live_allocation_bytes(), both_bytes);

    cache.invalidate()?;
    stop.cancel();
    drop((cache, provider));
    assert_eq!(original.live_allocation_bytes(), both_bytes);
    drop(old);
    assert_eq!(original.live_allocation_bytes(), both_bytes);
    drop(retained_old);
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + both_bytes - first_bytes,
        "only the final exact original lease retires its claim"
    );
    assert!(current.check().await.is_err());
    drop(current);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

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
    AlteredThenRebound,
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
    let refresh_path = path.clone();
    let source = NativeCredentialSource::bind(
        root,
        selected,
        original.clone(),
        || 1,
        move |rejected, revision, _, file| {
            let path = refresh_path.clone();
            let attempts = attempts.clone();
            async move {
                drop(file);
                if rejected.is_none() && attempts.load(Ordering::Relaxed) == 0 {
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
                        PublicationControl::AlteredThenRebound => {
                            std::fs::write(&path, "foreign-in-place-value").map_err(error)?;
                            let moved = path.with_extension("moved");
                            std::fs::rename(&path, &moved).map_err(error)?;
                            std::fs::rename(&moved, &path).map_err(error)?;
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
        let changed = source.watch().await?.ok_or("missing publication watch")?;
        std::fs::write(&path, "externally-rotated-source")?;
        source.0.watch.clone().fence().await?;
        changed.await?;
        assert!(next.check().await.is_err());
        assert!(
            source
                .0
                .state
                .observation
                .lock()
                .map_err(|_| "observation poisoned")?
                .published
                .is_some(),
            "source notification leaves descriptor retirement with the refresh owner"
        );
        // The real notification already invalidated this lease. Its original
        // publisher must now close and drain before the next writer is armed.
        let after_rotation = source.resolve().await?;
        after_rotation.check().await?;
        assert_ne!(after_rotation.binding(), next.binding());
        assert_eq!(publications.load(Ordering::Relaxed), 3);
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
    // The original native fence is the event producer. No timer, sleep,
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
        PublicationControl::AlteredThenRebound,
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
