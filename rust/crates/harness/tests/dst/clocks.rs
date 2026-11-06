//! Mount callbacks must retain the creating seed after leaving its task/thread.
#[cfg(any(target_os = "linux", target_os = "macos"))]
use acyclic_fs::native_mount::capture_mount_clock_callbacks;
use acyclic_native_runtime::testing::Schedule;

#[test]
fn runtime_seed_controls_original_select_order() -> std::io::Result<()> {
    async fn choices() -> [u8; 128] {
        let mut trace = [0; 128];
        for branch in &mut trace {
            *branch = tokio::select! {
                () = std::future::ready(()) => 0,
                () = std::future::ready(()) => 1,
                () = std::future::ready(()) => 2,
            };
        }
        trace
    }
    fn trace(seed: u64) -> std::io::Result<[[u8; 128]; 2]> {
        acyclic_native_runtime::testing::seeded_runtime(seed)?.block_on(async {
            let driver = choices().await;
            let mut controller = super::driver::Driver::new(seed, None);
            let outcome = controller
                .run(super::entry(async {
                    acyclic_native_runtime::testing::spawn_blocking(|| {
                        assert!(tokio::runtime::Handle::try_current().is_err());
                        let selected = acyclic_native_runtime::testing::blocking_on(async {
                            // The native handoff owns Pending. A cooperative
                            // yield cannot defer its wake until this synchronous
                            // callback returns; reject exhaustion before await.
                            for _ in 0..256 {
                                assert!(
                                    tokio::task::coop::has_budget_remaining(),
                                    "callback exhausted Tokio's cooperative budget"
                                );
                                let (sent, waiting) = tokio::sync::oneshot::channel();
                                sent.send(()).unwrap();
                                waiting.await.unwrap();
                            }
                            choices().await
                        });
                        assert!(tokio::runtime::Handle::try_current().is_err());
                        selected
                    })
                    .await
                }))
                .await;
            let super::driver::Outcome::Completed(worker) = outcome else {
                panic!("uncut worker selection crashed");
            };
            Ok([driver, worker?])
        })
    }
    let original = trace(71)?;
    assert_eq!(
        original,
        trace(71)?,
        "runtime branch order escaped the seed"
    );
    // Change the actual runtime input, retaining the original futures and
    // comparator. A fixed seed or biased selection would miss this control.
    assert_ne!(
        original,
        trace(72)?,
        "runtime seed did not control selection"
    );
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn trace(
    seed: u64,
    host_millis: u64,
    restore_host_callback: bool,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let runtime = acyclic_native_runtime::testing::seeded_runtime(seed)?;
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
    Ok(
        std::thread::spawn(move || callback(host_millis, restore_host_callback))
            .join()
            .map_err(|_| "mount callback thread panicked")??,
    )
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn mount_callbacks_replay_identically_despite_host_clock_skew()
-> Result<(), Box<dyn std::error::Error>> {
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
        let left = trace(seed, before, false)?;
        let right = trace(seed, after, false)?;
        equal(&left, &right)?;
        // Restore just the FUSE/NFS now-attribute callback to the host clock.
        // The same byte comparator must reject the broken production path.
        let left = trace(seed, before, true)?;
        let right = trace(seed, after, true)?;
        assert_eq!(equal(&left, &right), Err("mount callback trace diverged"));
        assert_eq!(equal(&left, &trace(seed, before, true)?), Ok(()));
    }
    Ok(())
}

#[test]
fn captured_provider_clock_tracks_simulated_jumps_and_elapsed_time() {
    use acyclic_native_runtime::testing::CapturedClock;
    use std::time::Duration;
    let runtime = acyclic_native_runtime::testing::seeded_runtime(71).unwrap();
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
