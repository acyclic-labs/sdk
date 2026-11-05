use super::*;

#[tokio::test]
async fn mixed_read_plan_rejects_known_extents_before_backend_submission()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let (spilled, _) = spill_fixture(&store, &token).await?;
    let (resident, bytes) = blob(OwnedBytes::from_static(b"resident preflight"));
    store
        .put(
            resident,
            bytes,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    let (durable, bytes) = blob(OwnedBytes::from_static(b"durable preflight"));
    store
        .inner()
        .put(
            durable,
            bytes,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    for private in [resident, spilled] {
        for native in [false, true] {
            for stored in [false, true] {
                let original = NativeMaterializationWork::new(native_read_budget(), &token)
                    .map_err(|(cause, _)| cause)?;
                let baseline = original.live_allocation_bytes();
                let mut work = if native {
                    AuthorityWork::native(&original, &token)
                } else {
                    AuthorityWork::new(native_read_budget(), &token)
                };
                // The invalid private member follows a durable miss, so all
                // known bounds must be validated before submitting that miss.
                let requests = [
                    ObjectReadRequest {
                        object_id: durable,
                        maximum_bytes: 1024,
                    },
                    ObjectReadRequest {
                        object_id: private,
                        maximum_bytes: 0,
                    },
                ];
                let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
                let driver = tokio::spawn(async move {
                    while let Some(event) = events.recv().await {
                        event.release();
                    }
                });
                let result = schedule
                    .scope(async {
                        if stored {
                            store.read_many_stored(&requests, &mut work).await
                        } else {
                            store.read_many(&requests, &mut work).await
                        }
                    })
                    .await;
                schedule.drained().await;
                driver.abort();
                let _ = driver.await;
                assert!(matches!(
                    result,
                    Err(ObjectStoreError::TooLarge { maximum: 0, .. })
                ));
                assert!(
                    !schedule
                        .trace()
                        .iter()
                        .any(|event| event.starts_with("native:")),
                    "known private extent dispatched backend work"
                );
                assert_eq!(work.spent().backend_read_operations, 0);
                assert_eq!(work.spent().bytes_copied, 0);
                assert_eq!(work.allocations.live_bytes(), 0);
                assert_eq!(original.live_allocation_bytes(), baseline);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn mixed_read_plan_admits_request_copies_before_backend_submission()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (store, _provider) = open_staged(directory.path()).await?;
    let token = CancellationToken::new();
    let (resident, bytes) = blob(OwnedBytes::from_static(b"resident metadata copy"));
    store
        .put(
            resident,
            bytes,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    let (durable, bytes) = blob(OwnedBytes::from_static(b"durable metadata copy"));
    store
        .inner()
        .put(
            durable,
            bytes,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await?;
    for misses in [1, 3] {
        let copied = (misses * std::mem::size_of::<ObjectReadRequest>()) as u64;
        let mut requests = vec![
            ObjectReadRequest {
                object_id: durable,
                maximum_bytes: 1024
            };
            misses
        ];
        requests.insert(
            0,
            ObjectReadRequest {
                object_id: resident,
                maximum_bytes: 1024,
            },
        );
        for native in [false, true] {
            for stored in [false, true] {
                let budget = WorkBudget {
                    bytes_copied: copied - 1,
                    ..native_read_budget()
                };
                let original =
                    NativeMaterializationWork::new(budget, &token).map_err(|(cause, _)| cause)?;
                let baseline = original.live_allocation_bytes();
                let mut work = if native {
                    AuthorityWork::native(&original, &token)
                } else {
                    AuthorityWork::new(budget, &token)
                };
                let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
                let driver = tokio::spawn(async move {
                    while let Some(event) = events.recv().await {
                        event.release();
                    }
                });
                let result = schedule
                    .scope(async {
                        if stored {
                            store.read_many_stored(&requests, &mut work).await
                        } else {
                            store.read_many(&requests, &mut work).await
                        }
                    })
                    .await;
                schedule.drained().await;
                driver.abort();
                let _ = driver.await;
                assert!(
                    matches!(
                        result,
                        Err(ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
                            counter: "bytes_copied",
                            observed,
                            maximum,
                        })) | Err(ObjectStoreError::Native(
                            NativeTreeMaterializationError::Work(
                                crate::WorkError::BudgetExceeded {
                                    counter: "bytes_copied",
                                    observed,
                                    maximum,
                                }
                            )
                        )) if observed == copied && maximum == copied - 1
                    ),
                    "request metadata copies bypassed original admission"
                );
                assert!(
                    !schedule
                        .trace()
                        .iter()
                        .any(|event| event.starts_with("native:")),
                    "unfunded request metadata dispatched backend work"
                );
                assert_eq!(work.spent().backend_read_operations, 0);
                assert_eq!(work.allocations.live_bytes(), 0);
                assert_eq!(original.live_allocation_bytes(), baseline);
            }
        }
    }
    Ok(())
}
