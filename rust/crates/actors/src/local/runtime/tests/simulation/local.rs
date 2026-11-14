//! Reopen the real journal after losing a delivery publication at physical I/O.
use super::*;
use acyclic_native_runtime::testing::DurableFault;

async fn episode(seed: u64, fault: DurableFault) -> TestResult {
    let directory = tempfile::tempdir()?;
    let address = actor("physical-recovery")?;
    let (schedule, mut boundaries) = Schedule::seeded(seed);
    let alarm_deadline = drive(&schedule, &mut boundaries, async {
        let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let store = Store::new(provider.clone(), root()?);
        let created = spawn_with(&store, "physical-recovery", &[b"emit", b"set 10"]).await?;
        assert_eq!(created, address);
        let runtime = open(&store, std::slice::from_ref(&address)).await?;
        runtime.drain_actor(&address).await?;
        let cut = store.load(&address).await?;
        assert_eq!(cut.state().and_then(|state| state.cursor("inbox")), Some(2));
        let deadline = cut
            .state()
            .and_then(|state| state.alarm().pending())
            .ok_or("alarm was not armed")?;
        provider
            .append(AppendRequest {
                path: StreamPath::new("agents/physical-recovery")?,
                records: vec![Bytes::from_static(b"emit").into()],
                if_tail: Some(2),
                idempotency_key: key("physical-recovery/second-input")?,
            })
            .await?;
        schedule.cut_next(fault);
        assert!(
            runtime.drain_actor(&address).await.is_err(),
            "physical failure was acknowledged as a delivery"
        );
        Ok::<_, Box<dyn std::error::Error>>(deadline)
    })
    .await?;
    let cause = schedule
        .storage_fault()
        .ok_or("selected fault was not reached")?;
    assert_eq!(cause.fault, fault);
    drop(schedule);

    let (recovery, mut boundaries) = Schedule::seeded(seed);
    drive(&recovery, &mut boundaries, async {
        let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let store = Store::new(provider.clone(), root()?);
        let runtime = open(&store, std::slice::from_ref(&address)).await?;
        recovery.set_unix_millis(alarm_deadline);
        runtime.drain_actor(&address).await?;
        let state = store.load(&address).await?;
        let state = state.state().ok_or("recovered actor missing")?;
        assert_eq!(state.cursor("inbox"), Some(3));
        assert_eq!(state.alarm().generation(), 1);
        assert_eq!(state.alarm().delivered_generation, Some(1));
        let output = records(provider.as_ref(), "out/physical-recovery").await?;
        let records = output
            .iter()
            .filter(|record| record.starts_with(b"r"))
            .cloned()
            .collect::<Vec<_>>();
        let alarms = output
            .iter()
            .filter(|record| record.starts_with(b"a"))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            records,
            vec![Bytes::from_static(b"r0"), Bytes::from_static(b"r2")]
        );
        assert_eq!(alarms, vec![Bytes::from_static(b"a1")]);
        runtime.drain_actor(&address).await?;
        assert_eq!(
            output,
            super::records(provider.as_ref(), "out/physical-recovery").await?
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await
}

#[test]
fn seeded_physical_restart_preserves_exactly_once_delivery_and_alarm() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    for seed in [0, 7] {
        for fault in [
            DurableFault::Before,
            DurableFault::After,
            DurableFault::Torn(1),
        ] {
            eprintln!("seed={seed} fault={fault:?}");
            seeded_runtime(seed)?.block_on(async {
                tokio::time::pause();
                episode(seed, fault).await
            })?;
        }
    }
    Ok(())
}
