//! Tool output is staged on the agent's private volume while the command runs.
use super::*;
use acyclic_native_runtime::ProcessOutput;

mod writer;

/// Waits, from a native capture worker, for one output publication. It
/// completes when the volume answers, never on a wall-clock deadline.
pub(super) fn await_publication<T, F: std::future::Future<Output = std::io::Result<T>>>(
    runtime: &tokio::runtime::Handle,
    step: impl FnOnce() -> F,
) -> std::io::Result<T> {
    crate::filesystem::native_process::capture::wait_control(runtime, step)
}

pub(super) struct Output {
    writer: Option<writer::Writer>,
    runtime: tokio::runtime::Handle,
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
        Ok(Self {
            writer: Some(writer::Writer::open(context, request).await?),
            runtime: tokio::runtime::Handle::current(),
            health: Health::default(),
        })
    }

    // The native capture worker publishes every observed chunk before accepting
    // more output. Publication supplies backpressure without a timer or queue.
    pub(super) fn observe(&mut self, chunk: ProcessOutput<'_>) -> std::io::Result<()> {
        self.health.check()?;
        let writer = self
            .writer
            .as_mut()
            .ok_or(std::io::ErrorKind::NotConnected)?;
        if let Err(cause) = writer.observe(chunk) {
            self.health.fail(&cause);
            return Err(cause);
        }
        Ok(())
    }

    pub(super) async fn finish(&mut self, result: &ToolResult) -> Result<()> {
        self.health.check().map_err(storage)?;
        self.writer
            .as_mut()
            .ok_or_else(|| Error::Storage("tool output is closed".into()))?
            .finish(result)
            .await?;
        self.writer.take();
        Ok(())
    }
}

impl Drop for Output {
    fn drop(&mut self) {
        let Some(mut writer) = self.writer.take() else {
            return;
        };
        // Capture owns Output until the physical worker has drained and reaped
        // its process tree. If publication was interrupted, retain its pending
        // bytes through the existing cleanup task.
        self.runtime.spawn(async move {
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

#[cfg(all(test, not(loom)))]
#[tokio::test]
async fn observed_output_is_published_before_callback_returns() -> Result<()> {
    use crate::conversation::Limits;
    use crate::filesystem::MemoryHarnessStorage;
    use crate::runtime::{Bindings, RuntimeScope};

    let files = MemoryHarnessStorage::new(crate::AgentId::new(), 1024).await?;
    let mut bindings = Bindings::local();
    bindings.scope = RuntimeScope::new(
        files.owner_scope().capabilities().clone(),
        Limits {
            file_bytes: Some(1024),
            ..Limits::default()
        },
    )?;
    let task = bindings.build()?.agent_context(
        &crate::Capabilities::new(Vec::<String>::new()),
        files.content_bindings(),
    )?;
    let context = ToolContext::new(Arc::new(task), OperationId::new(), "output")?;
    let directory = format!("tools/{}/{}", context.turn_id(), context.operation_id());
    let mut output = Output::open(&context, b"{}".to_vec()).await?;
    let (mut output, body) = tokio::task::spawn_blocking(move || {
        let body: Vec<_> = (0..2500).map(|index| index as u8).collect();
        output
            .observe(ProcessOutput::Stdout(&body))
            .map_err(storage)?;
        output
            .observe(ProcessOutput::Stderr(b"diagnostic"))
            .map_err(storage)?;
        Ok::<_, Error>((output, body))
    })
    .await
    .map_err(storage)??;
    // No finish, scheduler yield, timer advance or follow is needed for these
    // exact bytes, including the partial final chunk, to be committed.
    let mut observed = Vec::new();
    for sequence in 0..3 {
        let (_, bytes) = files
            .read_delegated_path(
                files.owner_scope(),
                "tools",
                &format!("{directory}/stdout/{sequence:016x}"),
                None,
            )
            .await?;
        observed.extend_from_slice(&bytes);
    }
    assert_eq!(observed, body);
    let (_, stderr) = files
        .read_delegated_path(
            files.owner_scope(),
            "tools",
            &format!("{directory}/stderr/0000000000000000"),
            None,
        )
        .await?;
    assert_eq!(stderr.as_ref(), b"diagnostic");
    let (_, before) = files
        .list_private_directory(&directory, None, None, 16)
        .await?;
    assert!(
        !before
            .entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"result.json")
    );
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success": true}),
        })
        .await?;
    let (_, result) = files
        .read_delegated_path(
            files.owner_scope(),
            "tools",
            &format!("{directory}/result.json"),
            None,
        )
        .await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result).map_err(storage)?,
        serde_json::json!({"success": true})
    );
    Ok(())
}
