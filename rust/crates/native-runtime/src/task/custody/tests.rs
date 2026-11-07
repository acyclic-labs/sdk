use super::*;
use tokio::sync::oneshot;

struct Released(Option<oneshot::Sender<()>>);

impl Drop for Released {
    fn drop(&mut self) {
        if let Some(released) = self.0.take() {
            let _ = released.send(());
        }
    }
}

async fn scoped_custody() -> Result<(), Box<dyn std::error::Error>> {
    for explicit_abort in [false, true] {
        let (started, starting) = oneshot::channel();
        let (released, releasing) = oneshot::channel();
        let task = spawn_owned(async move {
            let _release = Released(Some(released));
            assert!(started.send(()).is_ok(), "original start observer lost");
            std::future::pending::<()>().await;
        });
        starting.await?;
        if explicit_abort {
            task.abort_handle().abort();
            let error = task.await.err().ok_or("cancelled task completed")?;
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);
            assert_eq!(error.to_string(), "TaskCancelled");
        } else {
            drop(task);
        }
        // The original future's destructor is this event's only producer.
        releasing.await?;
    }

    let bytes = vec![73; 4096];
    let original = bytes.as_ptr() as usize;
    let output = spawn_owned(async move { bytes }).await?;
    assert_eq!(output.as_ptr() as usize, original);
    assert!(output.iter().all(|byte| *byte == 73));

    // Inject the payload through the real scheduler's panic boundary.
    let error = spawn_owned(async { std::panic::panic_any("private task payload") })
        .await
        .err()
        .ok_or("panicking original task completed")?;
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(error.to_string(), "TaskPanicked");
    assert!(!format!("{error:?}").contains("private task payload"));
    let cause = error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<JoinFailure>())
        .ok_or("panic lost its original scheduler cause")?;
    // The original scheduler failure is retained, not replaced by a label.
    match cause {
        JoinFailure::Native(error) => assert!(error.is_panic()),
        #[cfg(feature = "test-support")]
        JoinFailure::Panicked { _payload } => assert_eq!(
            _payload
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .downcast_ref::<&str>(),
            Some(&"private task payload")
        ),
        #[cfg(feature = "test-support")]
        JoinFailure::Cancelled(_) => return Err("panic lost its original cause".into()),
    }
    Ok(())
}

#[test]
fn original_tasks_conform_to_scoped_and_shared_completion_custody() {
    super::super::coordination_runtime()
        .unwrap()
        .block_on(async {
            scoped_custody().await.unwrap();

            let (started, starting) = oneshot::channel();
            let (release, held) = oneshot::channel::<()>();
            let completion = TaskCompletion::new(super::super::spawn(async move {
                started.send(()).unwrap();
                let error = held.await.unwrap_err();
                Err(io::Error::new(io::ErrorKind::BrokenPipe, error))
            }));
            let second = completion.clone();
            let third = completion.clone();
            let mut abandoned_wait = Box::pin(completion.wait());
            assert!(
                abandoned_wait
                    .as_mut()
                    .poll(&mut Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            starting.await.unwrap();
            drop(abandoned_wait);
            drop(completion);
            // Losing this original producer terminates the wait with its typed
            // error; dropping an observer must neither cancel nor replace it.
            drop(release);
            let first_error = second.wait().await.unwrap_err();
            let later_error = third.wait().await.unwrap_err();
            assert_eq!(first_error.kind(), io::ErrorKind::BrokenPipe);
            let retained = |error: &io::Error| {
                error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<Arc<io::Error>>()
                    .unwrap()
                    .clone()
            };
            assert!(Arc::ptr_eq(
                &retained(&first_error),
                &retained(&later_error)
            ));

            let completion = TaskCompletion::new(super::super::spawn(async {
                panic!("private physical task payload")
            }));
            let error = completion.wait().await.unwrap_err();
            assert_eq!(error.to_string(), "TaskPanicked");
            assert!(!format!("{error:?}").contains("private physical task payload"));
        });
}

#[cfg(feature = "test-support")]
#[test]
fn seeded_executor_uses_the_same_scoped_custody_contract() {
    super::super::coordination_runtime().unwrap().block_on(async {
        let (schedule, mut events) = crate::testing::Schedule::seeded(83);
        let work = schedule.scope(scoped_custody());
        tokio::pin!(work);
        loop {
            tokio::select! {
                result = &mut work => { result.unwrap(); break; },
                boundary = events.recv() => boundary.expect("original boundary producer").release(),
            }
        }
        schedule.drained().await;
    });
}
