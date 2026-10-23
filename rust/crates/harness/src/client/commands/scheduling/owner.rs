use std::future::Future;

// One race owns both futures until presentation exits. Neither event handling
// nor an awaited file write can stop polling the owner that may unblock it.
// Presentation exit stops admission, then joins settlement before executor exit.
pub(in crate::client::commands) async fn drive_command_owner<T, E: From<crate::Error>>(
    owner: impl Future<Output = crate::Result<()>>,
    presentation: impl Future<Output = Result<T, E>>,
    stop_admission: impl FnOnce(),
) -> Result<T, E> {
    futures::pin_mut!(owner, presentation);
    match futures::future::select(owner, presentation).await {
        futures::future::Either::Left((result, _)) => {
            result.map_err(E::from)?;
            Err(crate::Error::Conflict("local command owner stopped".into()).into())
        }
        futures::future::Either::Right((result, owner)) => {
            stop_admission();
            let settlement = owner.await.map_err(E::from);
            match result {
                Err(error) => Err(error),
                Ok(value) => settlement.map(|()| value),
            }
        }
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests {
    // Model the real race with a publication waiting for owner progress. The
    // channel is a model input, not a replacement for a production lock: this
    // orchestration function adds no synchronization or spawned tasks.
    fn model<const DRIVE: bool>() {
        for readiness in 0..4 {
            shuttle::check_dfs(
                move || {
                    shuttle::future::block_on(async move {
                        let (release, waiting) = futures::channel::oneshot::channel();
                        let (stop, detached) = futures::channel::oneshot::channel();
                        let owner = async move {
                            if readiness & 1 != 0 {
                                shuttle::future::yield_now().await;
                            }
                            release.send(()).unwrap();
                            detached.await.unwrap();
                            Ok(())
                        };
                        let presentation = async move {
                            if readiness & 2 != 0 {
                                shuttle::future::yield_now().await;
                            }
                            waiting.await.unwrap();
                            Ok::<_, crate::Error>(())
                        };
                        if DRIVE {
                            super::drive_command_owner(owner, presentation, || {
                                let _ = stop.send(());
                            })
                            .await
                            .unwrap();
                        } else {
                            // Broken original: retain the owner without polling it
                            // while a presentation-side publication awaits progress.
                            let _retained = owner;
                            presentation.await.unwrap();
                        }
                    })
                },
                None,
            );
        }
    }
    #[test]
    fn owner_progress_survives_awaited_publications() {
        model::<true>();
    }
    #[test]
    fn negative_control_parked_owner_deadlocks() {
        let failure =
            std::panic::catch_unwind(model::<false>).expect_err("parked owner must deadlock");
        let text = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(text.contains("deadlock"), "{text}");
    }
    #[test]
    fn presentation_exit_joins_worker_settlement() {
        for failed in [false, true] {
            futures::executor::block_on(async {
                let (stop, detached) = futures::channel::oneshot::channel();
                let settled = std::cell::Cell::new(false);
                let owner = async {
                    detached.await.unwrap();
                    settled.set(true);
                    Ok(())
                };
                let presentation = async {
                    if failed {
                        Err(crate::Error::Invalid("closed output".into()))
                    } else {
                        Ok(())
                    }
                };
                let result = super::drive_command_owner(owner, presentation, || {
                    let _ = stop.send(());
                })
                .await;
                assert_eq!(result.is_err(), failed);
                assert!(settled.get());
            });
        }
    }

    #[test]
    fn negative_control_returning_on_presentation_discards_settlement() {
        futures::executor::block_on(async {
            let (stop, detached) = futures::channel::oneshot::channel::<()>();
            let settled = std::cell::Cell::new(false);
            let owner = Box::pin(async {
                detached.await.unwrap();
                settled.set(true);
                Ok::<(), crate::Error>(())
            });
            let presentation = Box::pin(async { Ok::<(), crate::Error>(()) });
            match futures::future::select(owner, presentation).await {
                futures::future::Either::Right((Ok(()), owner)) => drop(owner),
                _ => panic!("presentation closes first"),
            }
            assert!(stop.send(()).is_err());
            assert!(!settled.get());
        });
    }
}
