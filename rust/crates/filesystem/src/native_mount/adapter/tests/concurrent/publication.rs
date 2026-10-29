use super::*;

struct ReentrantPublication {
    source: Arc<MemorySource>,
    completed: AtomicBool,
}

impl ViewObserver for ReentrantPublication {
    fn view_changed(&self, _: ViewStamp, _: crate::native_mount::ViewOrigin) {
        assert!(
            self.source.checkout.publication_fence.try_write().is_ok(),
            "publication must release its write fence before notifying observers"
        );
        let work =
            capture_meter(BOUNDARY_BUDGET, self.source.cancellation()).expect("observer work");
        self.source
            .runtime
            .wait(|| async {
                let operation = self.source.checkout.lock().await.publication_operation;
                let mut carrier = NativeDemandCarrier::Native(&work);
                drop(
                    self.source
                        .checkout
                        .candidate(operation, &mut carrier, work.cancellation(), None)
                        .await?,
                );
                Ok(())
            })
            .expect("publication observer can start a candidate");
        self.completed.store(true, Ordering::Release);
    }
}

#[test]
fn publication_rebase_notifies_after_unlocking() -> Result<(), Box<dyn std::error::Error>> {
    let source = conflicted_source()?;
    let observer = observe_publication(&source)?;
    source.sync()?;
    assert!(observer.completed.load(Ordering::Acquire));
    assert!(source.lookup(&native_test_path("remote"))?.is_some());
    assert!(source.lookup(&native_test_path("local"))?.is_some());
    Ok(())
}

fn conflicted_source() -> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
    let (source, writer) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::TrackingSafe,
        MountPublication::Manual,
    )?;
    let source = Arc::new(source);
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    source.create_file(&native_test_path("local"), metadata(), None, &work)?;
    let writer_work = capture_meter(BOUNDARY_BUDGET, writer.cancellation())?;
    writer.create_file(&native_test_path("remote"), metadata(), None, &writer_work)?;
    writer.sync()?;
    source.runtime.wait(|| async {
        // Reconcile a retained operation against the newly published head.
        source.checkout.lock().await.retained_operation_id();
        Ok(())
    })?;
    Ok(source)
}

fn observe_publication(
    source: &Arc<MemorySource>,
) -> Result<Arc<ReentrantPublication>, MountSourceError> {
    let observer = Arc::new(ReentrantPublication {
        source: Arc::clone(&source),
        completed: AtomicBool::new(false),
    });
    let weak: Weak<dyn ViewObserver> = Arc::<ReentrantPublication>::downgrade(&observer);
    source.observe_view(weak)?;
    Ok(observer)
}

#[test]
fn rejected_private_composition_notifies_only_after_successful_retry()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::AtomicUsize;
    for abandon in [false, true] {
        let source = conflicted_source()?;
        let observer = observe_publication(&source)?;
        let entered = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&entered);
        let started = Arc::new(tokio::sync::Notify::new());
        let signal = Arc::clone(&started);
        let mut guard = move |_: &crate::NativeMaterializationWork| {
            let calls = Arc::clone(&calls);
            let signal = Arc::clone(&signal);
            async move {
                // Preparation can compose an independent head privately. A
                // rejected seal must not expose that unpublished composition.
                calls.fetch_add(1, Ordering::AcqRel);
                signal.notify_one();
                if abandon {
                    std::future::pending::<()>().await;
                }
                Err(MountSourceError::Invalid(
                    "publication guard rejected".into(),
                ))
            }
        };
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        source.runtime.wait(|| async {
            let mut acknowledged = None;
            let mut publication = Box::pin(source.checkout.publish(
                crate::PublicationPermit::Unrestricted,
                crate::kernel::KernelAccounting::native(&work, work.cancellation()),
                None,
                &mut guard,
                None,
                &mut acknowledged,
            ));
            if abandon {
                tokio::select! {
                    _ = started.notified() => {}
                    result = publication.as_mut() => {
                        panic!("publication completed before abandonment: {result:?}");
                    }
                }
                assert_eq!(entered.load(Ordering::Acquire), 1);
                drop(publication);
            } else {
                assert!(matches!(
                    publication.await,
                    Err((MountSourceError::Invalid(_), _))
                ));
            }
            Ok(())
        })?;
        assert!(
            !observer.completed.load(Ordering::Acquire),
            "an unpublished private composition became visible"
        );
        source.sync()?;
        assert!(observer.completed.load(Ordering::Acquire));
        assert!(source.lookup(&native_test_path("remote"))?.is_some());
        assert!(source.lookup(&native_test_path("local"))?.is_some());
    }
    Ok(())
}
