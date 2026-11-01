//! Outcome controls at the real log's original I/O boundaries.
use super::*;
use acyclic_native_runtime::testing::{DurableFault, DurableWrite, Schedule};

fn bytes(value: &[u8]) -> OwnedBytes {
    OwnedBytes::from(value.to_vec())
}

#[tokio::test]
async fn owned_close_reports_physical_completion_and_failure()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::Blobs as _;

    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let account = LocalObjects::open(root.path(), LocalObjectsLimits::default(), None).await?;
    let digest = account
        .put(bytes(b"retained"), Vec::new())
        .await?
        .verified()
        .ok_or("put outcome unknown")?
        .digest;
    assert_eq!(account.clone().close().await?, LocalClose::StillReferenced);
    assert!(matches!(
        LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await,
        Err(LocalOpenError::AlreadyOwned)
    ));
    assert_eq!(account.close().await?, LocalClose::Released);
    let reopened = LocalObjects::open(root.path(), LocalObjectsLimits::default(), None).await?;
    assert_eq!(
        reopened
            .get(digest, crate::ByteRange::FULL, 8)
            .await?
            .body()
            .as_ref(),
        b"retained"
    );
    assert_eq!(reopened.close().await?, LocalClose::Released);

    // Every original durability barrier has both a before-effect and a lost-
    // acknowledgement control. Drop used to swallow these physical failures.
    let mut barriers = 0;
    for cut in std::iter::once(None).chain((0..2).flat_map(|ordinal| {
        [DurableFault::Failed, DurableFault::After].map(|fault| Some((ordinal, fault)))
    })) {
        let root = tempfile::tempdir()?;
        let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
        assert!(medium.create("key", bytes(b"dirty")).await?);
        let (schedule, mut events) = Schedule::new();
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        if let Some((ordinal, fault)) = cut {
            schedule.cut_at(ordinal, fault);
        }
        let result = schedule.scope(medium.close()).await;
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        let trace = schedule.durable_writes();
        match cut {
            None => {
                assert_eq!(result?, LocalClose::Released);
                barriers = trace.len();
                assert_eq!(barriers, 2, "one data and one directory barrier: {trace:?}");
            }
            Some((ordinal, fault)) => {
                let Err(LocalCloseError::Io(error)) = result else {
                    return Err(format!("close hid {cut:?}: {result:?}").into());
                };
                assert_eq!(
                    error
                        .get_ref()
                        .and_then(|cause| cause
                            .downcast_ref::<acyclic_native_runtime::testing::InjectedStorageFault>(
                        ))
                        .map(|cause| (cause.ordinal, cause.fault)),
                    Some((ordinal, fault))
                );
                assert_eq!(trace.len(), ordinal + 1, "Drop retried the failed close");
                assert!(ordinal < barriers);
            }
        }
        // Both success and error are delivered after the actual lock retires.
        let reopened = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
        assert_eq!(
            reopened.read("key", None).await?.as_deref(),
            Some(&b"dirty"[..])
        );
        if cut.is_some_and(|(ordinal, _)| ordinal == 0) {
            assert!(
                reopened.recovered(),
                "failed data flush lost recovery marker"
            );
        }
        assert_eq!(reopened.close().await?, LocalClose::Released);
    }

    let root = tempfile::tempdir()?;
    let anchor = acyclic_native_runtime::OwnershipAnchor::new(());
    let released = anchor.release_barrier();
    let medium =
        LocalMedium::open(root.path(), LocalObjectsLimits::default(), Some(anchor)).await?;
    assert!(medium.create("key", bytes(b"accepted")).await?);
    let (schedule, mut events) = Schedule::new();
    let mut close = Box::pin(schedule.scope(medium.close()));
    let submitted = tokio::select! {
        biased;
        result = &mut close => return Err(format!("close ended before submission: {result:?}").into()),
        boundary = events.recv() => boundary.ok_or("close submission producer lost")?,
    };
    assert_eq!(submitted.name, "task:submitted");
    submitted.release();
    let submitted = tokio::select! {
        biased;
        result = &mut close => return Err(format!("close ended before native submission: {result:?}").into()),
        boundary = events.recv() => boundary.ok_or("native close submission producer lost")?,
    };
    assert_eq!(submitted.name, "native:submitted");
    drop(close);
    assert!(
        !released.is_released(),
        "cancelled caller released accepted close"
    );
    assert!(matches!(
        LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await,
        Err(LocalOpenError::AlreadyOwned)
    ));
    submitted.release();
    let completed = tokio::select! {
        biased;
        boundary = events.recv() => boundary.ok_or("close completion producer lost")?,
        () = schedule.drained() => return Err("close producer ended without completion".into()),
    };
    assert_eq!(completed.name, "native:completed");
    completed.release();
    let completed = tokio::select! {
        biased;
        boundary = events.recv() => boundary.ok_or("close task terminal producer lost")?,
        () = schedule.drained() => return Err("close task ended without its terminal".into()),
    };
    assert_eq!(completed.name, "task:completed");
    completed.release();
    schedule.drained().await;
    assert!(
        released.is_released(),
        "physical close retained its original anchor"
    );
    let reopened = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
    assert!(!reopened.recovered());
    assert_eq!(
        reopened.read("key", None).await?.as_deref(),
        Some(&b"accepted"[..])
    );
    assert_eq!(reopened.close().await?, LocalClose::Released);
    Ok(())
}

async fn mutation_outcomes_preserve_physical_uncertainty_and_no_effect_refusals() {
    for operation in ["create", "replace", "remove", "remove-if"] {
        for fault in [
            DurableFault::After,
            DurableFault::Torn(1),
            DurableFault::Failed,
        ] {
            let root = tempfile::tempdir().unwrap();
            let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None)
                .await
                .unwrap();
            if operation != "create" {
                assert!(medium.create("key", bytes(b"old")).await.unwrap());
            }
            let observed = if operation == "create" {
                None
            } else {
                Some(observation(&medium, "key").await)
            };
            let (schedule, mut events) = Schedule::new();
            let driver = tokio::spawn(async move {
                while let Some(boundary) = events.recv().await {
                    boundary.release();
                }
            });
            schedule.cut_at(0, fault);
            let result = schedule
                .scope(async {
                    match operation {
                        "create" => medium.create("key", bytes(b"new")).await,
                        "replace" => medium
                            .replace(observed.unwrap(), bytes(b"new"))
                            .await
                            .map(|value| value.is_some()),
                        "remove" => medium.remove("key").await,
                        "remove-if" => medium.remove_if(observed.unwrap()).await,
                        _ => unreachable!(),
                    }
                })
                .await;
            schedule.drained().await;
            driver.abort();
            let _ = driver.await;
            assert!(
                matches!(result, Err(MediumError::Ambiguous(_))),
                "{operation} {fault:?}: {result:?}"
            );
            assert_eq!(schedule.storage_fault().unwrap().fault, fault);
            // The stopped root can prove this later proposal did not run.
            assert!(matches!(
                medium.create("later", bytes(b"later")).await,
                Err(MediumError::Failed(_))
            ));
            assert!(medium.read("later", None).await.unwrap().is_none());
            assert!(matches!(
                medium.flush(&[]).await,
                Err(MediumError::Ambiguous(_))
            ));
            drop(medium);
            let reopened = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None)
                .await
                .unwrap();
            let value = reopened.read("key", None).await.unwrap();
            let expected: Option<&[u8]> = match (operation, fault) {
                ("create" | "replace", DurableFault::After) => Some(b"new"),
                ("remove" | "remove-if", DurableFault::After) | ("create", _) => None,
                _ => Some(b"old"),
            };
            assert_eq!(value.as_deref(), expected, "{operation} {fault:?}");
            let before = std::fs::metadata(root.path().join(LOG)).unwrap().len();
            assert!(matches!(
                reopened.list("invalid-prefix", None, 1).await,
                Err(MediumError::Failed(_))
            ));
            assert_eq!(
                std::fs::metadata(root.path().join(LOG)).unwrap().len(),
                before
            );
        }
    }
}

async fn compacting_mutation(
    remove: bool,
    cut: Option<(usize, DurableFault)>,
) -> Vec<DurableWrite> {
    let root = tempfile::tempdir().unwrap();
    let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None)
        .await
        .unwrap();
    assert!(
        medium
            .create("key", bytes(&vec![7; COMPACTION_CHUNK as usize]))
            .await
            .unwrap()
    );
    assert!(medium.create("kept", bytes(b"kept")).await.unwrap());
    medium.flush(&[]).await.unwrap();
    let observed = observation(&medium, "key").await;
    let (schedule, mut events) = Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    if let Some((ordinal, fault)) = cut {
        schedule.cut_at(ordinal, fault);
    }
    let result = schedule
        .scope(async {
            if remove {
                medium.remove_if(observed).await
            } else {
                medium
                    .replace(observed, bytes(b"new"))
                    .await
                    .map(|value| value.is_some())
            }
        })
        .await;
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    assert!(
        matches!(result, Ok(true)),
        "maintenance {cut:?}: {result:?}"
    );
    let expected = if remove {
        None
    } else {
        Some(b"new".as_slice())
    };
    assert_eq!(medium.read("key", None).await.unwrap().as_deref(), expected);
    assert_eq!(
        medium.read("kept", None).await.unwrap().unwrap().as_ref(),
        b"kept"
    );
    if let Some((ordinal, fault)) = cut {
        let actual = schedule.storage_fault().unwrap();
        assert_eq!((actual.ordinal, actual.fault), (ordinal, fault));
        assert!(matches!(
            medium.flush(&[]).await,
            Err(MediumError::Ambiguous(_))
        ));
        assert!(matches!(
            medium.create("later", bytes(b"later")).await,
            Err(MediumError::Failed(_))
        ));
    }
    let trace = schedule.durable_writes();
    drop(medium);
    let reopened = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None)
        .await
        .unwrap();
    assert_eq!(
        reopened.read("key", None).await.unwrap().as_deref(),
        expected
    );
    assert_eq!(
        reopened.read("kept", None).await.unwrap().unwrap().as_ref(),
        b"kept"
    );
    trace
}

async fn maintenance_failure_cannot_refuse_an_already_visible_mutation() {
    for remove in [false, true] {
        // Capture this OS's actual successful flush/rename trace, then cut its
        // original calls. No invented syscall inventory or replacement medium.
        let trace = compacting_mutation(remove, None).await;
        let cuts: Vec<_> = trace
            .iter()
            .filter(|call| call.operation.contains("sync") || call.operation == "namespace:rename")
            .collect();
        assert!(
            cuts.iter().any(|call| call.operation.contains("sync")),
            "{trace:?}"
        );
        assert!(
            cuts.iter().any(|call| call.operation == "namespace:rename"),
            "{trace:?}"
        );
        for call in cuts {
            for fault in [DurableFault::Before, DurableFault::After] {
                compacting_mutation(remove, Some((call.ordinal, fault))).await;
            }
        }
    }
}

#[tokio::test]
async fn physical_outcomes_preserve_visible_mutations_and_uncertainty() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    mutation_outcomes_preserve_physical_uncertainty_and_no_effect_refusals().await;
    maintenance_failure_cannot_refuse_an_already_visible_mutation().await;
}
