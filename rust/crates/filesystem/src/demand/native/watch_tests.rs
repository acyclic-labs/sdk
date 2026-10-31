use super::*;
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};

#[derive(Default)]
struct Loss(
    CancellationToken,
    AtomicUsize,
    std::sync::Mutex<Option<std::sync::Weak<crate::source_watch::NativeSourceWatch>>>,
);

impl SourceChangeSink for Loss {
    fn source_changed(
        &self,
        changes: &[SourceChange],
        _: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        self.1.fetch_add(changes.len(), Ordering::Relaxed);
        Ok(())
    }

    fn delivery_lost(&self) {
        let physical = self
            .2
            .lock()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        if let Some(physical) = physical {
            assert!(
                matches!(
                    physical.stop_accepted_cleanup(),
                    Err(DemandError::WorkerUnavailable)
                ),
                "reentrant retirement must refuse before acquiring the physical owner"
            );
        }
        self.0.cancel();
    }
}

#[derive(Default)]
struct Wakes(AtomicUsize);

impl futures::task::ArcWake for Wakes {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn source_watch_preserves_fence_and_delivery_loss() -> Result<(), Box<dyn std::error::Error>>
{
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(directory.path()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        &original,
        &cancellation,
    )
    .await?;
    let lost = Arc::new(Loss::default());
    let peer_lost = Arc::new(Loss::default());
    let watch = source
        .watch(lost.clone(), &original, &cancellation)?
        .ok_or("source watch missing")?;
    let peer = source
        .watch(peer_lost.clone(), &original, &cancellation)?
        .ok_or("peer source watch missing")?;
    let physical = source
        .inner
        .watches
        .read()
        .map_err(|_| "watch registry poisoned")?
        .first()
        .and_then(std::sync::Weak::upgrade)
        .ok_or("physical producer missing")?;
    let mut accounting = crate::kernel::KernelAccounting::native(&original, &cancellation);
    std::fs::write(
        directory.path().join("change"),
        b"source observation control",
    )?;
    assert_eq!(watch.fence(&mut accounting)?, SourceFence::Confirmed);
    assert_eq!(peer.fence(&mut accounting)?, SourceFence::Confirmed);
    assert!(
        lost.1.load(Ordering::Relaxed) != 0,
        "real source changes must reach the first sink"
    );
    assert!(
        peer_lost.1.load(Ordering::Relaxed) != 0,
        "real source changes must reach the peer sink"
    );

    let wakes = Arc::new(Wakes::default());
    let waker = futures::task::waker(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut changed = std::pin::pin!(lost.0.cancelled());
    assert!(changed.as_mut().poll(&mut context).is_pending());
    assert!(!peer_lost.0.is_cancelled());

    *lost.2.lock().unwrap() = Some(Arc::downgrade(&physical));
    // Fault injection enters the actual producer's existing failure path.
    // No event loop, timer or subsequent fence supplies the loss notification.
    let before = original.receipt();
    let allocation = acyclic_test_support::measure(|| {
        physical.inject_delivery_failure(DemandError::SourceUnavailable);
    });
    assert_eq!(allocation.count_total, 0, "delivery loss does not allocate");
    assert_eq!(
        original.receipt(),
        before,
        "loss cannot require fresh work credit"
    );
    eprintln!(
        "delivery loss: allocations={} added work={:?}",
        allocation.count_total,
        crate::WorkCounters::UNCHARGED
    );
    assert_eq!(
        wakes.0.load(Ordering::Relaxed),
        1,
        "delivery loss must wake the registered consumer"
    );
    assert!(matches!(
        changed.as_mut().poll(&mut context),
        Poll::Ready(())
    ));
    assert!(!watch.is_exact());
    assert!(matches!(
        watch.fence(&mut accounting),
        Err(DemandError::SourceUnavailable)
    ));
    // Consuming the original cause cannot promote this producer back to exact.
    assert_eq!(watch.fence(&mut accounting)?, SourceFence::Unconfirmed);
    assert!(
        !peer_lost.0.is_cancelled(),
        "one failed subscription cannot retire another"
    );
    assert_eq!(peer.fence(&mut accounting)?, SourceFence::Confirmed);
    let mut peer_changed = std::pin::pin!(peer_lost.0.cancelled());
    assert!(peer_changed.as_mut().poll(&mut context).is_pending());
    drop(peer);
    assert_eq!(
        wakes.0.load(Ordering::Relaxed),
        2,
        "owner loss must wake the registered consumer"
    );
    assert!(peer_changed.as_mut().poll(&mut context).is_ready());
    watch.stop(&mut accounting)?;
    assert!(watch.is_stopped());
    for stop in 0..3 {
        let lost = Arc::new(Loss::default());
        let watch = source
            .watch(lost.clone(), &original, &cancellation)?
            .ok_or("stop control source watch missing")?;
        let mut changed = std::pin::pin!(lost.0.cancelled());
        assert!(changed.as_mut().poll(&mut context).is_pending());
        let before = wakes.0.load(Ordering::Relaxed);
        match stop {
            0 => watch.stop(&mut accounting)?,
            1 => watch.stop_with_cleanup_wait(&cancellation)?,
            _ => watch.stop_accepted_cleanup()?,
        }
        assert_eq!(
            wakes.0.load(Ordering::Relaxed),
            before + 1,
            "successful stop must wake before returning: method {stop}"
        );
        assert!(changed.as_mut().poll(&mut context).is_ready());
        assert!(
            watch.is_stopped(),
            "physical retirement precedes successful stop"
        );
        drop(watch);
        assert_eq!(
            wakes.0.load(Ordering::Relaxed),
            before + 1,
            "Drop cannot supply or duplicate the completed stop wake"
        );
    }
    Ok(())
}
