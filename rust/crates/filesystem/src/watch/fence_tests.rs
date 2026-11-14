//! The real fence may wait for native delivery without holding an executor.
use super::*;
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

#[test]
fn owned_fence_keeps_inline_executors_available_during_native_contention()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    initialize_native_watch_services()?;
    for withdraw in [false, true] {
        contended_fence(withdraw)?;
    }
    Ok(())
}

fn contended_fence(withdraw: bool) -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"before")?;
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut options = NativeWatchOptions::new(VolumeLimits::default());
    options.permit_fence_writes = false;
    let mut watch = NativeWatch::open_with_profile_with_native_work(
        root.path(),
        FilesystemProfile::Portable,
        options,
        &work,
    )?;
    let NativeWatchDriver::Source(source) = &watch.watcher else {
        return Err("use the actual held native driver".into());
    };
    let (retire, retired) = mpsc::channel();
    source.observe_retirement(retire);
    let (enter, entered) = tokio::sync::oneshot::channel();
    watch.before_fence = Some(Box::new(move || {
        let _ = enter.send(());
    }));
    let identity = watch.root_identity();
    let state = Arc::clone(&watch.shared);
    // This is the original notification state mutex, not a replacement driver
    // or a synthetic future. A fence cannot finish while this guard is held.
    let held = state.state.lock().map_err(|_| "watch state poisoned")?;
    let (progress, progressed) = mpsc::channel();
    let worker = std::thread::spawn(move || -> Result<_, NativeWatchError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let _inline = acyclic_native_runtime::InlineBlocking::enter();
            let started = Instant::now();
            let mut fence = Some(Box::pin(watch.fence_owned()));
            assert!(
                futures::poll!(fence.as_mut().expect("original fence")).is_pending(),
                "a held native fence cannot complete"
            );
            entered.await.expect("original physical fence entered");
            if withdraw {
                fence = None;
            }
            // A separate task runs on this very executor while the original
            // native fence is still unable to settle.
            tokio::spawn(async move {
                progress.send(started.elapsed()).expect("live controller");
            })
            .await
            .expect("independent task settled");
            match fence {
                Some(fence) => {
                    let watch = fence.await?;
                    assert_eq!(watch.root_identity(), identity);
                    Ok(Some(watch))
                }
                None => Ok(None),
            }
        })
    });
    let progress = progressed.recv_timeout(Duration::from_millis(500));
    let premature_retirement = retired.try_recv();
    // Release before inspecting any assertion, including the negative control,
    // so the original native work always settles and its thread is joined.
    drop(held);
    let watch = worker.join().map_err(|_| "fence executor panicked")??;
    let elapsed = progress.expect("native fence parked the inline executor");
    assert!(
        elapsed < Duration::from_millis(500),
        "independent executor progress took {elapsed:?}"
    );
    eprintln!(
        "CONTENDED_FENCE withdraw={withdraw} executor_progress_us={}",
        elapsed.as_micros()
    );
    assert!(
        matches!(premature_retirement, Err(mpsc::TryRecvError::Empty)),
        "withdrawing observation retired a physically active watch"
    );
    if let Some(watch) = watch {
        // Reusing the returned original watch still observes the actual source.
        std::fs::write(root.path().join("file"), b"after")?;
        let watch = futures::executor::block_on(watch.fence_owned())?;
        assert_eq!(watch.root_identity(), identity);
        drop(watch);
    }
    retired
        .recv_timeout(Duration::from_secs(2))
        .expect("original physical driver retired");
    assert!(work.receipt().backend_read_operations > 0);
    Ok(())
}
