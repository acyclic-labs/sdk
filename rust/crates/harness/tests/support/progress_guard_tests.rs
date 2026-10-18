#[test]
fn accepted_native_io_may_outlive_the_shared_idle_budget() -> crate::TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()?
        .block_on(async {
            let mut guard = crate::progress_guard::Guard::new();
            let io = guard.io.clone();
            let registry = guard.registry.clone();
            let (release, released) = std::sync::mpsc::channel();
            let (entered, ready) = tokio::sync::oneshot::channel();
            let started = tokio::time::Instant::now();
            let work = crate::stall::run(io, registry.clone(), "native-io".into(), async move {
                let operation = acyclic_native_runtime::run_blocking_io(
                    move || {
                        entered.send(()).unwrap();
                        released.recv().unwrap();
                        42
                    },
                    None,
                )
                .unwrap();
                let finish = async move {
                    ready.await.unwrap();
                    // Advance virtual time across several watchdog observations.
                    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                    release.send(()).unwrap();
                };
                crate::stall::waiting("native completion before follow", async {
                    let (value, ()) = tokio::join!(operation, finish);
                    value.unwrap()
                })
                .await
            });
            assert_eq!(guard.run(work).await.unwrap(), 42);
            assert!(started.elapsed() >= std::time::Duration::from_secs(20));
            assert!(registry.snapshot().1.is_empty());
        });
    Ok(())
}

#[test]
fn scoped_blocking_work_uses_a_real_worker_with_an_inline_negative_control() -> crate::TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            // The former implementation ran this exact blocking read on Tokio.
            let broken = std::panic::catch_unwind(|| *tokio::sync::RwLock::new(42).blocking_read());
            assert!(broken.is_err(), "inline blocking control was not detected");
            let mut guard = crate::progress_guard::Guard::new();
            let io = guard.io.clone();
            let value = guard
                .run(io.scope(async {
                    acyclic_native_runtime::testing::spawn_blocking(|| {
                        assert!(tokio::runtime::Handle::try_current().is_err());
                        *tokio::sync::RwLock::new(42).blocking_read()
                    })
                    .await
                }))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(value, 42);
        });
    Ok(())
}
