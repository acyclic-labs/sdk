//! Tool output is staged on the agent's private volume while the command runs.
use super::*;
use acyclic_native_runtime::ProcessOutput;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, oneshot};

mod writer;

/// The native contract's bound on one host control check.
pub(super) const CONTROL_TIMEOUT_MS: u32 = 1000;
const OUTPUT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);

/// Waits, from a native capture worker, for one output publication. It
/// completes when the volume answers, never on a wall-clock deadline.
pub(super) fn await_publication<T, F: std::future::Future<Output = std::io::Result<T>>>(
    runtime: &tokio::runtime::Handle,
    step: impl FnOnce() -> F,
) -> std::io::Result<T> {
    crate::filesystem::native_process::capture::wait_control(runtime, step)
}

pub(super) struct Output {
    writer: Arc<Mutex<writer::Writer>>,
    ready: Arc<Notify>,
    stop: Option<oneshot::Sender<()>>,
    timer: Option<tokio::task::JoinHandle<()>>,
    runtime: tokio::runtime::Handle,
    finished: bool,
    pub(super) health: Health,
}

#[derive(Clone, Default)]
pub(super) struct Health(Arc<std::sync::Mutex<HealthState>>);

#[derive(Default)]
struct HealthState {
    failure: Option<String>,
    wake: Option<acyclic_native_runtime::ProcessWake>,
}

impl Health {
    pub(super) fn check(&self) -> std::io::Result<()> {
        match self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .as_ref()
        {
            Some(cause) => Err(std::io::Error::other(cause.clone())),
            None => Ok(()),
        }
    }

    pub(super) fn wake_on_failure(
        &self,
        wake: acyclic_native_runtime::ProcessWake,
    ) -> std::io::Result<()> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let failed = state.failure.is_some();
        if failed {
            wake.stop()?;
        }
        state.wake = Some(wake);
        drop(state);
        Ok(())
    }

    fn fail(&self, cause: impl std::fmt::Display) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.failure.get_or_insert_with(|| cause.to_string());
        let wake = state.wake.clone();
        drop(state);
        if let Some(wake) = wake {
            let _ = wake.stop();
        }
    }
}

impl Output {
    pub(super) async fn open(context: &ToolContext, request: Vec<u8>) -> Result<Self> {
        let writer = writer::Writer::open(context, request).await?;
        let writer = Arc::new(Mutex::new(writer));
        let ready = Arc::new(Notify::new());
        let health = Health::default();
        let (stop, stopped) = oneshot::channel();
        let timer = tokio::spawn(flush_ready(
            writer.clone(),
            ready.clone(),
            health.clone(),
            stopped,
        ));
        Ok(Self {
            writer,
            ready,
            stop: Some(stop),
            timer: Some(timer),
            runtime: tokio::runtime::Handle::current(),
            finished: false,
            health,
        })
    }

    // Native capture invokes this callback on its blocking I/O worker. Most
    // chunks only copy into the bounded buffer; full batches publish immediately.
    pub(super) fn observe(&mut self, chunk: ProcessOutput<'_>) -> std::io::Result<()> {
        self.health.check()?;
        let mut writer = self.writer.blocking_lock();
        self.health.check()?;
        let was_empty = !writer.pending();
        if let Err(cause) = writer.observe(chunk) {
            self.health.fail(&cause);
            return Err(cause);
        }
        if was_empty && writer.pending() {
            self.ready.notify_one();
        }
        Ok(())
    }

    pub(super) async fn finish(&mut self, result: &ToolResult) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(timer) = self.timer.take() {
            timer.await.map_err(storage)?;
        }
        // Joining the timer closes its last failure publication. A command
        // that ended before its wake was consumed cannot publish success.
        self.health.check().map_err(storage)?;
        self.writer.lock().await.finish(result).await?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for Output {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let timer = self.timer.take();
        let writer = Arc::clone(&self.writer);
        // Capture owns Output until the physical worker has drained and reaped
        // its process tree. If the awaiting caller disappears, that worker's
        // final drop still stages the accepted bytes.
        self.runtime.spawn(async move {
            if let Some(timer) = timer
                && let Err(cause) = timer.await
            {
                tracing::error!(?cause, "tool output timer cleanup failed");
            }
            let mut writer = writer.lock().await;
            let result = async {
                writer.flush(false).await?;
                writer.flush(true).await
            }
            .await;
            if let Err(cause) = result {
                tracing::error!(?cause, "captured tool output cleanup failed");
            }
        });
    }
}

async fn flush_ready(
    writer: Arc<Mutex<writer::Writer>>,
    ready: Arc<Notify>,
    health: Health,
    mut stopped: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            _ = &mut stopped => return,
            () = ready.notified() => {},
        }
        if !writer.lock().await.pending() {
            continue;
        }
        tokio::select! {
            biased;
            _ = &mut stopped => return,
            () = tokio::time::sleep(OUTPUT_FLUSH_INTERVAL) => {},
        }
        let mut writer = writer.lock().await;
        let result = async {
            writer.flush(false).await?;
            writer.flush(true).await
        }
        .await;
        if let Err(cause) = result {
            health.fail(cause);
            return;
        }
    }
}
