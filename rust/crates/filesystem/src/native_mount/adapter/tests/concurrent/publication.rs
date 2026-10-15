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
fn publication_rebase_error_and_cancellation_notify_after_unlocking()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::AtomicUsize;
    for abandon in [false, true] {
        let source = conflicted_source()?;
        let observer = observe_publication(&source)?;
        let entered = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&entered);
        let mut guard = move |_: &crate::NativeMaterializationWork| {
            let calls = Arc::clone(&calls);
            async move {
                // Memory authority seals only an append that can succeed, so
                // reaching this guard follows the conflicting-head rebase.
                calls.fetch_add(1, Ordering::AcqRel);
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
                assert!(futures::poll!(publication.as_mut()).is_pending());
                assert_eq!(
                    entered.load(Ordering::Acquire),
                    1,
                    "publication must be rebased before cancellation"
                );
                drop(publication);
            } else {
                assert!(matches!(
                    publication.await,
                    Err((MountSourceError::Invalid(_), _))
                ));
            }
            Ok(())
        })?;
        assert!(observer.completed.load(Ordering::Acquire));
        assert!(source.lookup(&native_test_path("remote"))?.is_some());
        assert!(source.lookup(&native_test_path("local"))?.is_some());
    }
    Ok(())
}
