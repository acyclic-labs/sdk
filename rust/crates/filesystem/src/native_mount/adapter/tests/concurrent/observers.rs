use super::*;

#[test]
fn observer_reenters_candidate_with_a_publisher_queued() -> Result<(), Box<dyn std::error::Error>> {
    struct Reentrant {
        source: Arc<MemorySource>,
        armed: AtomicBool,
        completed: AtomicBool,
    }
    impl ViewObserver for Reentrant {
        fn view_changed(&self, _: ViewStamp, _: crate::native_mount::ViewOrigin) {
            if !self.armed.swap(false, Ordering::AcqRel) {
                return;
            }
            assert!(
                self.source.checkout.publication_fence.try_write().is_ok(),
                "observer must run after the candidate publication lease drops"
            );
            let work =
                capture_meter(BOUNDARY_BUDGET, self.source.cancellation()).expect("observer work");
            self.source
                .runtime
                .wait(|| async {
                    let pinned = candidate_now(&self.source.checkout, &work).await?;
                    let publication = self
                        .source
                        .sync_async(crate::PublicationPermit::Unrestricted);
                    tokio::pin!(publication);
                    assert!(futures::poll!(publication.as_mut()).is_pending());
                    let mut carrier = NativeDemandCarrier::Native(&work);
                    let reentered = self.source.checkout.candidate(
                        None,
                        &mut carrier,
                        work.cancellation(),
                        None,
                    );
                    tokio::pin!(reentered);
                    assert!(futures::poll!(reentered.as_mut()).is_pending());
                    drop(pinned);
                    futures::try_join!(publication, async {
                        drop(reentered.await?);
                        Ok::<_, MountSourceError>(())
                    })?;
                    Ok(())
                })
                .expect("observer and queued publisher finish without an outer read lease");
            self.completed.store(true, Ordering::Release);
        }
    }
    let (source, _) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::Pinned,
        MountPublication::Manual,
    )?;
    let source = Arc::new(source);
    let observer = Arc::new(Reentrant {
        source: Arc::clone(&source),
        armed: AtomicBool::new(true),
        completed: AtomicBool::new(false),
    });
    let weak: Weak<dyn ViewObserver> = Arc::<Reentrant>::downgrade(&observer);
    source.observe_view(weak)?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    source.create_file(&native_test_path("observed"), metadata(), None, &work)?;
    assert!(observer.completed.load(Ordering::Acquire));
    Ok(())
}
