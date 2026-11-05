//! The public native watch subscription over actual OS file events.
#![cfg(all(feature = "native-watch", any(target_os = "linux", windows)))]

use acyclic_fs::kernel::NamespacePath;
use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs::path::PortablePath;
use acyclic_fs::watch::{
    NativeWatch, NativeWatchError, NativeWatchOptions, WatchBatch, WatchChange,
};
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn native_watch_work() -> Result<NativeMaterializationWork, Box<dyn std::error::Error>> {
    acyclic_fs::watch::initialize_native_watch_services()?;
    NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .map_err(|(error, _)| error.into())
}

#[tokio::test]
async fn native_change_wait_uses_retained_events_and_settles_on_cancel_or_drop()
-> Result<(), Box<dyn std::error::Error>> {
    use std::future::Future as _;
    use std::task::{Context, Poll, Wake, Waker};

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    for recursive in [false, true] {
        let root = tempfile::tempdir()?;
        let mut options = NativeWatchOptions::new(VolumeLimits::default());
        options.recursive = recursive;
        let mut watch =
            NativeWatch::open_with_profile(root.path(), FilesystemProfile::Portable, options)?;
        let scan = watch.begin_rescan()?;
        watch.finish_rescan(scan)?;
        let work = native_watch_work()?;
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        let mut changed = std::pin::pin!(watch.changed_with_native_work(&work));
        assert!(matches!(
            changed.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        let idle = work.receipt();
        assert_eq!(
            wakes.0.load(Ordering::Acquire),
            0,
            "quiet observers do not self-wake"
        );
        assert_eq!(
            idle.backend_read_operations, 0,
            "registration never inspects the root"
        );
        assert_eq!(idle.backend_write_operations, 0);
        std::fs::write(root.path().join("delivered"), b"complete")?;
        changed.as_mut().await?;
        let expected = NamespacePath::from_portable(
            &PortablePath::parse("/delivered", VolumeLimits::default())?,
            VolumeLimits::default(),
        )?;
        loop {
            watch.changed_with_native_work(&work).await?;
            let receipt = watch
                .poll(64, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|failure| failure.error)?;
            match receipt.value {
                WatchBatch::Changes { changes, .. }
                    if changes.iter().any(|change| {
                        matches!(change, WatchChange::Created(path) | WatchChange::Modified(path)
                            if path == &expected)
                    }) =>
                {
                    break;
                }
                WatchBatch::RescanRequired { reason, .. } => {
                    panic!("lost native event: {reason:?}")
                }
                WatchBatch::Changes { .. } => {}
            }
        }

        // Drain the delivered batch before arming cancellation. The future
        // retains only its subscription, so the watcher can be dropped too.
        watch.poll(u32::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
        let cancellation = CancellationToken::new();
        let cancelled_work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut cancelled = std::pin::pin!(watch.changed_with_native_work(&cancelled_work));
        let first = cancelled
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        // A concurrent native hint may already be ready; cancellation still
        // takes priority for a new observer after the signal is cancelled.
        cancellation.cancel();
        if first.is_pending() {
            assert!(matches!(
                cancelled.await,
                Err(NativeWatchError::Cancelled(_))
            ));
        }
        assert!(matches!(
            watch.changed_with_native_work(&cancelled_work).await,
            Err(NativeWatchError::Cancelled(_))
        ));
        let retired = watch.changed_with_native_work(&work);
        drop(watch);
        assert!(matches!(
            retired.await,
            Err(NativeWatchError::SourceUnavailable)
        ));
    }
    Ok(())
}
