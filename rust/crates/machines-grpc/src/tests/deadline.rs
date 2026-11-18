use super::*;
use acyclic_machines::deadline::GuestDeadline;
use futures::FutureExt;

#[tokio::test]
async fn original_terminal_settles_dispatch_before_and_after_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    for after_dispatch in [false, true] {
        let (deadline, finish) = GuestDeadline::controlled();
        let cause = Arc::new(std::io::Error::from_raw_os_error(5));
        if !after_dispatch {
            finish(Err(cause.clone()));
        }
        let (entered, entering) = tokio::sync::oneshot::channel();
        let mut dispatch = Box::pin(crate::transport::dispatch(
            Some(&deadline),
            async {
                let _ = entered.send(());
                std::future::pending::<()>().await
            },
            |stop| stop.command(machine),
        ));
        if after_dispatch {
            assert!(dispatch.as_mut().now_or_never().is_none());
            entering.await?;
            finish(Err(cause.clone()));
        }
        assert!(
            matches!(dispatch.now_or_never().expect("original terminal must settle dispatch"),
            Err(ProviderError::DeadlineSourceLost { machine: actual, cause: failure })
                if actual == after_dispatch.then_some(machine) && Arc::ptr_eq(&failure.0, &cause))
        );
    }
    Ok(())
}
