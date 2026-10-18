use crate::{TestResult, notification_history::Projection};
use std::{
    future::{Future, poll_fn},
    process::{Command, Stdio},
    task::Poll,
    time::{Duration, Instant},
};

const CHILD: &str = "ACYCLIC_NOTIFICATION_COOP_CHILD";
const NESTED: &str = "ACYCLIC_NOTIFICATION_NESTED_CONTROL";

// A nested executor never returns Pending to Tokio, so neither an async timeout
// nor the shared progress guard can preempt it. Isolate that exact mutant in a
// child process; always kill and reap it before checking the negative control.
#[test]
fn initialization_yields_after_exhausting_tokios_cooperative_budget() -> TestResult {
    if let Some(marker) = std::env::var_os(CHILD) {
        let nested = std::env::var_os(NESTED).is_some();
        return super::on_runtime(async {
            poll_fn(|cx| {
                while tokio::task::coop::has_budget_remaining() {
                    assert!(
                        std::pin::pin!(tokio::task::coop::consume_budget())
                            .poll(cx)
                            .is_ready()
                    );
                }
                Poll::Ready(())
            })
            .await;
            std::fs::write(marker, b"budget exhausted")?;
            let mut initialization = std::pin::pin!(Projection::new());
            let projection = if nested {
                // The former constructor drove these same operations without
                // returning to Tokio. With no budget it spins indefinitely.
                futures::executor::block_on(initialization)
            } else {
                poll_fn(|cx| {
                    assert!(initialization.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                tokio::task::yield_now().await;
                initialization.await
            };
            assert!(projection.view.files().is_empty());
            Ok(())
        });
    }

    for nested in [false, true] {
        let files = tempfile::tempdir()?;
        let marker = files.path().join("exhausted");
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                concat!(
                    module_path!(),
                    "::initialization_yields_after_exhausting_tokios_cooperative_budget"
                )
                .split_once("::")
                .unwrap()
                .1,
                "--nocapture",
            ])
            .env(CHILD, &marker)
            .env_remove(NESTED)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if nested {
            command.env(NESTED, "1");
        }
        let mut child = command.spawn()?;
        let started = Instant::now();
        let outcome = loop {
            match child.try_wait() {
                Ok(Some(status)) => break Ok(Some(status)),
                Err(error) => break Err(error),
                Ok(None) if started.elapsed() >= Duration::from_secs(2) => break Ok(None),
                Ok(None) => std::thread::sleep(Duration::from_millis(2)),
            }
        };
        if !matches!(outcome, Ok(Some(_))) {
            // Cleanup precedes every assertion, including child startup errors.
            let _ = child.kill();
            child.wait()?;
        }
        let outcome = outcome?;
        assert_eq!(std::fs::read(&marker)?, b"budget exhausted");
        if nested {
            assert!(outcome.is_none(), "nested executor unexpectedly returned");
        } else {
            assert!(
                outcome.is_some_and(|status| status.success()),
                "async initialization did not yield and finish"
            );
        }
    }
    Ok(())
}
