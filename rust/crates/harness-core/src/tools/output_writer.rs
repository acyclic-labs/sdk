//! Tool output is files on the agent's private volume, each staged once under
//! an operation derived from the command's own: `tools/{turn}/{operation}/`,
//! under the turn the call belongs to, holds
//! `request.json`, `stdout/{sequence}`, `stderr/{sequence}` and `result.json`.
use crate::{Error, OperationId, Result, runtime::ToolContext, tool::ToolResult};
fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
use acyclic_native_runtime::ProcessOutput;

const OUTPUT_BATCH_BYTES: usize = 64 * 1024;

/// Shared private-volume layout for native and remote shell output.
pub struct ToolOutputWriter {
    context: ToolContext,
    /// `tools/{turn}/{operation}`.
    directory: String,
    operation: OperationId,
    sequence: (u64, u64),
    pending: (Vec<u8>, Vec<u8>),
    maximum: usize,
}

impl ToolOutputWriter {
    /// Whether either stream has bytes awaiting publication.
    pub fn pending(&self) -> bool {
        !self.pending.0.is_empty() || !self.pending.1.is_empty()
    }

    /// Stages the exact request before dispatch.
    pub async fn open(context: &ToolContext, request: Vec<u8>) -> Result<Self> {
        let maximum = usize::try_from(context.effective_scope().limits().file_bytes)
            .unwrap_or(usize::MAX)
            .min(OUTPUT_BATCH_BYTES);
        if maximum == 0 {
            return Err(Error::Storage("tool output has no file budget".into()));
        }
        let writer = Self {
            context: context.clone(),
            directory: format!("tools/{}/{}", context.turn_id(), context.operation_id()),
            operation: context.operation_id(),
            sequence: (0, 0),
            pending: (Vec::new(), Vec::new()),
            maximum,
        };
        writer
            .stage("request.json", &request, "application/json")
            .await?;
        Ok(writer)
    }

    async fn stage(&self, name: &str, bytes: &[u8], media_type: &str) -> Result<()> {
        let path = format!("{}/{name}", self.directory);
        let mut hash = blake3::Hasher::new_derive_key("acyclic.local.tool-output/v1");
        hash.update(&self.operation.into_bytes());
        hash.update(path.as_bytes());
        let mut operation = [0; 16];
        operation.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        self.context
            .stage_file_once(
                OperationId::from_bytes(operation),
                &path,
                bytes,
                media_type,
                // A label is one path component: `stdout`, not `stdout/{seq}`.
                name.split('/').next().unwrap_or(name),
                None,
            )
            .await?;
        Ok(())
    }

    /// Accepts bytes with bounded buffering, publishing each full batch.
    pub async fn observe(&mut self, chunk: ProcessOutput<'_>) -> Result<()> {
        let (stderr, mut bytes) = match chunk {
            ProcessOutput::Stdout(bytes) => (false, bytes),
            ProcessOutput::Stderr(bytes) => (true, bytes),
        };
        while !bytes.is_empty() {
            let maximum = self.maximum;
            let pending = if stderr {
                &mut self.pending.1
            } else {
                &mut self.pending.0
            };
            let amount = bytes.len().min(maximum - pending.len());
            let (prefix, rest) = bytes.split_at(amount);
            pending.try_reserve(amount).map_err(storage)?;
            pending.extend_from_slice(prefix);
            bytes = rest;
            if pending.len() == maximum {
                self.flush(stderr).await?;
            }
        }
        Ok(())
    }

    // Retain the bytes and sequence until their file is staged.
    /// Publishes the pending bytes of one stream once.
    pub async fn flush(&mut self, stderr: bool) -> Result<()> {
        let (pending, sequence, stream) = if stderr {
            (&self.pending.1, self.sequence.1, "stderr")
        } else {
            (&self.pending.0, self.sequence.0, "stdout")
        };
        if pending.is_empty() {
            return Ok(());
        }
        let next = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Storage("tool output sequence overflow".into()))?;
        self.stage(
            &format!("{stream}/{sequence:016x}"),
            pending,
            "application/octet-stream",
        )
        .await?;
        if stderr {
            self.pending.1.clear();
            self.sequence.1 = next;
        } else {
            self.pending.0.clear();
            self.sequence.0 = next;
        }
        Ok(())
    }

    /// Flushes both streams and writes terminal metadata without duplicating output.
    pub async fn finish(&mut self, result: &ToolResult) -> Result<()> {
        self.flush(false).await?;
        self.flush(true).await?;
        let metadata: std::collections::BTreeMap<_, _> = result
            .value
            .as_object()
            .ok_or_else(|| Error::Invalid("process result must be an object".into()))?
            .iter()
            .filter(|(key, _)| !matches!(key.as_str(), "stdout" | "stderr"))
            .collect();
        let outcome = serde_json::to_vec(&metadata).map_err(storage)?;
        self.stage("result.json", &outcome, "application/json")
            .await
    }
}
