use crate::{TestResult, notification_history::Projection};
use std::{
    future::{Future, poll_fn},
    path::Path,
    process::{Command, Stdio},
    task::Poll,
};

const CHILD: &str = "ACYCLIC_NOTIFICATION_COOP_CHILD";
const NESTED: &str = "ACYCLIC_NOTIFICATION_NESTED_CONTROL";
const NESTED_REJECTED: &str = "from within another executor";

// Every poll of the initialization runs inside an executor context. A nested
// futures executor then panics on entry, and Tokio rejects a nested runtime, so
// a constructor that stops returning Pending to Tokio fails at once instead of
// spinning. No clock bounds the child: its exit is the parent's only event.
fn unnested<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let mut future = Box::pin(future);
    poll_fn(move |cx| {
        let _executor = futures::executor::enter().expect("initialization polled re-entrantly");
        future.as_mut().poll(cx)
    })
}

#[test]
fn initialization_yields_after_exhausting_tokios_cooperative_budget() -> TestResult {
    if let Some(marker) = std::env::var_os(CHILD) {
        let nested = std::env::var_os(NESTED).is_some();
        // The child's own progress guard ends a lost wakeup; it has no deadline.
        return super::on_runtime(crate::stall::guarded(
            Path::new("notification initialization"),
            async {
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
                let mut initialization = std::pin::pin!(unnested(async move {
                    if nested {
                        // The former constructor drove these same operations
                        // without returning to Tokio.
                        futures::executor::block_on(Projection::new())
                    } else {
                        Projection::new().await
                    }
                }));
                poll_fn(|cx| {
                    assert!(initialization.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                tokio::task::yield_now().await;
                let projection = initialization.await;
                assert!(projection.view.files().is_empty());
                TestResult::Ok(())
            },
        ))?;
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
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if nested {
            command.env(NESTED, "1");
        }
        // Waits for the child's exit, however long a busy host delays it.
        let output = command.output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(std::fs::read(&marker)?, b"budget exhausted", "{stderr}");
        if nested {
            assert!(
                !output.status.success() && stderr.contains(NESTED_REJECTED),
                "the nested executor was not rejected: {stderr}"
            );
        } else {
            assert!(
                output.status.success(),
                "async initialization did not yield and finish: {stderr}"
            );
        }
    }
    Ok(())
}
