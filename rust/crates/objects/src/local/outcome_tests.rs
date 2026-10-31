//! Outcome controls at the real log's original I/O boundaries.
use super::*;
use acyclic_native_runtime::testing::{DurableFault, DurableWrite, Schedule};

fn bytes(value: &[u8]) -> OwnedBytes {
    OwnedBytes::from(value.to_vec())
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
                        "replace" => medium.replace("key", b"old", bytes(b"new")).await,
                        "remove" => medium.remove("key").await,
                        "remove-if" => medium.remove_if("key", b"old").await,
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
                medium
                    .remove_if("key", &vec![7; COMPACTION_CHUNK as usize])
                    .await
            } else {
                medium
                    .replace("key", &vec![7; COMPACTION_CHUNK as usize], bytes(b"new"))
                    .await
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
