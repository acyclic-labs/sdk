//! Tool output is staged under its host-owned destination while the command runs.
use super::*;
use acyclic_native_runtime::ProcessOutput;
use tokio::sync::{Mutex, Notify, oneshot};

pub mod writer;

pub struct Output {
    writer: Arc<Mutex<writer::Writer>>,
    ready: Arc<Notify>,
    stop: Option<oneshot::Sender<()>>,
    publisher: Option<acyclic_native_runtime::TaskHandle<()>>,
    tasks: acyclic_native_runtime::TaskSpawner,
    runtime: tokio::runtime::Handle,
    finished: bool,
    pub health: Health,
}

#[derive(Clone, Default)]
pub struct Health(Arc<std::sync::Mutex<HealthState>>);

#[derive(Default)]
struct HealthState {
    failure: Option<String>,
    wake: Option<acyclic_native_runtime::ProcessWake>,
}

impl Health {
    pub fn check(&self) -> std::io::Result<()> {
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

    pub fn wake_on_failure(
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
    pub async fn open(context: &ToolContext, request: Vec<u8>) -> Result<Self> {
        let writer = writer::Writer::open(context, request).await?;
        let writer = Arc::new(Mutex::new(writer));
        let ready = Arc::new(Notify::new());
        let health = Health::default();
        let (stop, stopped) = oneshot::channel();
        let tasks = acyclic_native_runtime::TaskSpawner::current();
        let publisher = tasks.spawn(flush_ready(
            writer.clone(),
            ready.clone(),
            health.clone(),
            stopped,
        ));
        Ok(Self {
            writer,
            ready,
            stop: Some(stop),
            publisher: Some(publisher),
            tasks,
            runtime: tokio::runtime::Handle::current(),
            finished: false,
            health,
        })
    }

    // Native capture invokes this callback on its blocking I/O worker. Most
    // chunks only copy into the bounded buffer; full batches publish immediately.
    pub fn observe(&mut self, chunk: ProcessOutput<'_>) -> std::io::Result<()> {
        self.health.check()?;
        // The publisher may hold the writer while waiting for the volume.
        // Use one native handoff for both the lock and any full-batch write.
        crate::filesystem::native_process::capture::wait_control(&self.runtime, || async {
            let mut writer = self.writer.lock().await;
            self.health.check()?;
            let was_empty = !writer.pending();
            if let Err(cause) = writer.observe(chunk).await {
                self.health.fail(&cause);
                return Err(cause);
            }
            if was_empty && writer.pending() {
                self.ready.notify_one();
            }
            Ok(())
        })
    }

    pub async fn finish(&mut self, result: &ToolResult) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(publisher) = self.publisher.take() {
            publisher.await.map_err(storage)?;
        }
        // Joining the publisher closes its last failure publication. A command
        // that ended before its wake was consumed cannot publish success.
        self.health.check().map_err(storage)?;
        self.writer.lock().await.finish(result).await?;
        self.finished = true;
        Ok(())
    }

    pub async fn completed(
        &self,
        outcome: &crate::filesystem::native_process::capture::NativeProcessOutcome,
    ) -> Result<()> {
        crate::filesystem::native_process::capture::finish_output(
            &self.writer.lock().await.output,
            outcome,
        )
        .await
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
        let publisher = self.publisher.take();
        let writer = Arc::clone(&self.writer);
        // Capture owns Output until the physical worker has drained and reaped
        // its process tree. If the awaiting caller disappears, that worker's
        // final drop still stages the accepted bytes.
        self.tasks.spawn(async move {
            if let Some(publisher) = publisher
                && let Err(cause) = publisher.await
            {
                tracing::error!(?cause, "tool output publisher cleanup failed");
            }
            let mut writer = writer.lock().await;
            if let Err(cause) = writer.flush_pending().await {
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
        let mut writer = writer.lock().await;
        if let Err(cause) = writer.flush_pending().await {
            health.fail(cause);
            return;
        }
    }
}
