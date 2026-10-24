use super::*;
use crate::deadline_test_support::paused_clock_oracle;

async fn cookie_oracle(control: bool) {
    paused_clock_oracle(
        control,
        (false, vec![Duration::from_secs(60)], false, true),
        |clock| async move {
            let directory = tempfile::tempdir().unwrap();
            let external = tempfile::tempdir().unwrap();
            let mut watch = NativeWatch::open(
                directory.path(),
                NativeWatchOptions::new(VolumeLimits::default()),
            )
            .unwrap();
            watch.begin_rescan().unwrap();
            watch.finish_rescan().unwrap();
            // No producer watches this path, so the original cookie wait must reach its
            // deadline. Other watcher activity cannot manufacture a cookie receipt.
            let cookie = external.path().join("unobserved-cookie");
            assert!(watch.write_cookie(&cookie).unwrap());
            let delivered = clock
                .scope(async { watch.await_cookie(Duration::from_secs(60)) })
                .await
                .unwrap();
            let unlocked = watch.shared.try_lock().is_ok();
            (delivered, clock.advances(), cookie.exists(), unlocked)
        },
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn cookie_fence_timeout_uses_virtual_time_without_holding_the_watch_state() {
    cookie_oracle(false).await;
}

#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "host deadline escaped virtual time")]
async fn cookie_wait_restored_host_deadline_negative_control() {
    cookie_oracle(true).await;
}

#[cfg(any(target_os = "linux", windows))]
async fn source_fence_oracle(control: bool, outer: bool) {
    paused_clock_oracle(
        control,
        (true, true, Vec::<Duration>::new()),
        |clock| async move {
            let directory = tempfile::tempdir().unwrap();
            let options = NativeWatchOptions::new(VolumeLimits::default());
            // Linux's recursive default uses notify's cookie adapter. Capture
            // uses the bounded native reader instead, admitting directories on
            // demand. Exercise that same source and its own fence deadline.
            #[cfg(target_os = "linux")]
            let options = NativeWatchOptions {
                recursive: false,
                ..options
            };
            let mut watch = NativeWatch::open(directory.path(), options).unwrap();
            watch.begin_rescan().unwrap();
            assert!(matches!(
                watch.finish_rescan().unwrap(),
                WatchBatch::Changes { .. }
            ));
            let delivered = clock.enter(|| {
                if outer {
                    watch.fence(Duration::from_secs(1)).unwrap();
                    true
                } else {
                    let NativeWatchDriver::Source(source) = &watch.watcher else {
                        panic!("source fence fixture needs the original native reader");
                    };
                    let mut work = crate::native_host::ordinary_work();
                    source
                        .fence_with_work(Duration::from_secs(1), &mut work)
                        .unwrap()
                }
            });
            let valid = watch.shared.lock().unwrap().invalidation.is_none();
            (delivered, valid, clock.advances())
        },
    )
    .await;
}

#[cfg(any(target_os = "linux", windows))]
#[tokio::test(flavor = "current_thread")]
async fn native_fence_and_source_reader_keep_paused_deadlines() {
    source_fence_oracle(false, true).await;
    source_fence_oracle(false, false).await;
}

#[cfg(any(target_os = "linux", windows))]
#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "host deadline escaped virtual time")]
async fn native_fence_restored_host_deadline_negative_control() {
    source_fence_oracle(true, true).await;
}

#[cfg(any(target_os = "linux", windows))]
#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "host deadline escaped virtual time")]
async fn source_reader_restored_host_deadline_negative_control() {
    source_fence_oracle(true, false).await;
}
