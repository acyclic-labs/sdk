//! Mount callbacks must retain the creating seed after leaving its task/thread.
use acyclic_fs::native_mount::capture_mount_clock_callbacks;
use acyclic_native_runtime::testing::Schedule;

fn trace(seed: u64, host_millis: u64, restore_host_callback: bool) -> Vec<u8> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let (schedule, _) = Schedule::seeded(seed);
    let callback = runtime.block_on(async {
        tokio::time::pause();
        schedule.set_unix_millis(1_700_000_000_000 + seed % 1000);
        schedule
            .scope(async { capture_mount_clock_callbacks() })
            .await
    });
    // A driver thread has neither the seed's task-local context nor a runtime.
    // Invoke after scope exit, retaining the runtime only through the provider.
    std::thread::spawn(move || callback(host_millis, restore_host_callback))
        .join()
        .unwrap()
}

#[test]
fn mount_callbacks_replay_identically_despite_host_clock_skew() {
    let equal = |left: &[u8], right: &[u8]| -> Result<(), &'static str> {
        if left == right {
            Ok(())
        } else {
            Err("mount callback trace diverged")
        }
    };
    for seed in [0, 71, u64::MAX] {
        let before = 1_600_000_000_000;
        let after = 1_900_000_000_000;
        let left = trace(seed, before, false);
        let right = trace(seed, after, false);
        equal(&left, &right).expect("same seed, skewed host clock");
        // Restore just the FUSE/NFS now-attribute callback to the host clock.
        // The same byte comparator must reject the broken production path.
        let left = trace(seed, before, true);
        let right = trace(seed, after, true);
        assert_eq!(equal(&left, &right), Err("mount callback trace diverged"));
        assert_eq!(equal(&left, &trace(seed, before, true)), Ok(()));
    }
}

#[test]
fn captured_provider_clock_tracks_simulated_jumps_and_elapsed_time() {
    use acyclic_native_runtime::testing::CapturedClock;
    use std::time::Duration;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let (schedule, _) = Schedule::seeded(71);
    let clock = runtime.block_on(async {
        tokio::time::pause();
        schedule.set_unix_millis(100);
        schedule
            .scope(async { CapturedClock::capture().unwrap() })
            .await
    });
    let read = |clock: CapturedClock| {
        std::thread::spawn(move || (clock.unix_millis(), clock.elapsed()))
            .join()
            .unwrap()
    };
    assert_eq!(read(clock.clone()), (100, Duration::ZERO));
    runtime.block_on(async {
        schedule.advance(Duration::from_millis(10)).await;
        schedule.set_unix_millis(5);
    });
    assert_eq!(read(clock), (5, Duration::from_millis(10)));
}
