use super::*;
use futures::FutureExt;

#[tokio::test]
async fn original_alarm_terminal_reaches_active_and_late_observers()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    for fail_before_wait in [true, false] {
        let (producer, wake) = terminal();
        let deadline = GuestDeadline {
            until: None,
            expiry: None,
            wake: Some(wake),
            _alarm: None,
        };
        let cause = Arc::new(std::io::Error::from_raw_os_error(5));
        if fail_before_wait {
            producer.finish(Err(cause.clone()));
        }
        let mut observers = Box::pin(futures::future::join_all(
            (0..64).map(|_| deadline.run(std::future::pending::<()>())),
        ));
        if !fail_before_wait {
            assert!(observers.as_mut().now_or_never().is_none());
            producer.finish(Err(cause.clone()));
        }
        let results = observers
            .as_mut()
            .now_or_never()
            .expect("the original terminal must settle every registered observer");
        for result in results {
            assert!(matches!(result,
                Err(DeadlineStop::SourceLost(actual)) if Arc::ptr_eq(&actual.0, &cause)));
        }
        // A later expiry or callback retirement cannot replace the first cause.
        producer.finish(Ok(()));
        drop(producer);
        let stop = deadline
            .run::<()>(async { panic!("work polled after original source loss") })
            .now_or_never()
            .expect("late observation cannot wait for a consumed notification")
            .expect_err("source loss must refuse admission");
        assert!(matches!(stop.clone().before_dispatch(),
            ProviderError::DeadlineSourceLost { machine: None, cause: actual }
                if Arc::ptr_eq(&actual.0, &cause)));
        assert!(matches!(stop.clone().command(machine),
            ProviderError::DeadlineSourceLost { machine: Some(actual), cause: failure }
                if actual == machine && Arc::ptr_eq(&failure.0, &cause)));
        assert!(matches!(stop.file(machine),
            ProviderError::DeadlineSourceLost { machine: Some(actual), cause: failure }
                if actual == machine && Arc::ptr_eq(&failure.0, &cause)));
    }

    // Losing the callback itself also publishes one retained failure.
    let (producer, wake) = terminal();
    let deadline = GuestDeadline {
        until: None,
        expiry: None,
        wake: Some(wake),
        _alarm: None,
    };
    let mut active = Box::pin(deadline.run(std::future::pending::<()>()));
    assert!(active.as_mut().now_or_never().is_none());
    drop(producer);
    let first = active
        .now_or_never()
        .expect("callback loss must settle the original wait")
        .expect_err("callback loss cannot attest expiry");
    let late = deadline
        .run(std::future::pending::<()>())
        .now_or_never()
        .expect("callback loss must remain observable")
        .expect_err("callback loss cannot expire");
    assert_eq!(first, late);
    assert!(matches!(first, DeadlineStop::SourceLost(_)));

    // Exercise the actual native WallAlarm and the exact production callback.
    let expiry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let mut elapsed = GuestDeadline::new(None, Some(expiry))?;
    let wake = elapsed
        .wake
        .as_mut()
        .expect("authenticated expiry owns its original alarm");
    wake.wait_for(Option::is_some).await?;
    assert_eq!(
        elapsed.run(std::future::pending::<()>()).await,
        Err(DeadlineStop::Expired)
    );
    assert_eq!(
        elapsed.run(std::future::pending::<()>()).await,
        Err(DeadlineStop::Expired)
    );

    let unbounded = GuestDeadline::new(None, None)?;
    assert!(unbounded.wake.is_none() && unbounded._alarm.is_none());
    assert_eq!(unbounded.run(async { 7 }).await?, 7);
    assert!(matches!(
        GuestDeadline::new(Some(Duration::ZERO), None)?
            .run(std::future::pending::<()>())
            .await,
        Err(DeadlineStop::Expired)
    ));
    Ok(())
}
