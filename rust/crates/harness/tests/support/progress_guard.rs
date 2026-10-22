//! One idle guard for the whole runner, separate from scenario outcome semantics.
use super::progress_registry::Registry;
use acyclic_native_runtime::testing::{Boundary, Schedule};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::mpsc::UnboundedReceiver;

pub struct Guard {
    pub registry: Arc<Registry>,
    pub io: Schedule,
    events: UnboundedReceiver<Boundary>,
}

impl Guard {
    pub fn new() -> Self {
        let (io, events) = Schedule::new();
        Self {
            registry: Arc::default(),
            io,
            events,
        }
    }

    pub async fn run<F: Future>(&mut self, future: F) -> Result<F::Output, String> {
        self.run_retiring(future, || Ok(())).await
    }

    /// Quiesce thread-affine caller drivers before publishing shared cancellation.
    /// Retirement only drops callers; this guard still drives all accepted owners.
    pub async fn run_retiring<F: Future>(
        &mut self,
        future: F,
        retire: impl FnOnce() -> Result<(), String>,
    ) -> Result<F::Output, String> {
        let mut future = Box::pin(future);
        let result = self.drive(future.as_mut()).await;
        let result = match retire() {
            Ok(()) => result,
            Err(error) => Err(error),
        };
        if result.is_err() {
            self.io.cancel();
        }
        // Cancel the caller before draining its accepted owners. The runner's
        // abort guards also cancel each actual, tracked scenario task.
        drop(future);
        let io = self.io.clone();
        let cleanup = self.drive(io.drained()).await;
        match result {
            Err(error) => {
                cleanup.map_err(|cleanup| format!("{error}\n{cleanup}"))?;
                Err(error)
            }
            Ok(output) => {
                cleanup?;
                Ok(output)
            }
        }
    }

    async fn drive<F: Future>(&mut self, future: F) -> Result<F::Output, String> {
        // An idle budget, never a deadline on a scenario. Real accepted native
        // work is allowed to finish regardless of machine load or its duration.
        let quiet = Duration::from_secs(5);
        let idle = tokio::time::sleep(quiet);
        tokio::pin!(idle);
        tokio::pin!(future);
        let mut revision = self.registry.snapshot().0;
        loop {
            tokio::select! {
                biased;
                output = &mut future => return Ok(output),
                event = self.events.recv() => {
                    let Some(event) = event else {
                        return Err("native progress driver lost its schedule".into());
                    };
                    event.release();
                    idle.as_mut().reset(tokio::time::Instant::now() + quiet);
                }
                _ = &mut idle => {
                    if let Some(open) = self.registry.blocked_snapshot(revision)
                        && self.io.pending_native_work() == 0 {
                        return Err(if open.is_empty() {
                            "stalled: accepted native/task cleanup made no progress".into()
                        } else { open.iter().map(|(path, on)| {
                            format!("stalled: {} at {on}", path.display())
                        }).collect::<Vec<_>>().join("\n") });
                    }
                    revision = self.registry.snapshot().0;
                    idle.as_mut().reset(tokio::time::Instant::now() + quiet);
                }
            }
        }
    }
}
