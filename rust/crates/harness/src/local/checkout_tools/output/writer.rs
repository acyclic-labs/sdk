//! Tool output is files on the agent's private volume, each staged once under
//! an operation derived from the command's own: `tools/{operation}/` holds
//! `request.json`, `stdout/{sequence}`, `stderr/{sequence}` and `result.json`.
use super::*;
use acyclic_native_runtime::ProcessOutput;

const OUTPUT_BATCH_BYTES: usize = 64 * 1024;

pub(super) struct Writer {
    context: ToolContext,
    operation: OperationId,
    sequence: (u64, u64),
    pending: (Vec<u8>, Vec<u8>),
    maximum: usize,
    runtime: tokio::runtime::Handle,
}

impl Writer {
    pub(super) fn pending(&self) -> bool {
        !self.pending.0.is_empty() || !self.pending.1.is_empty()
    }

    pub(super) async fn open(context: &ToolContext, request: Vec<u8>) -> Result<Self> {
        let maximum = usize::try_from(context.effective_scope().limits().file_bytes)
            .unwrap_or(usize::MAX)
            .min(OUTPUT_BATCH_BYTES);
        if maximum == 0 {
            return Err(Error::Storage("tool output has no file budget".into()));
        }
        let writer = Self {
            context: context.clone(),
            operation: context.operation_id(),
            sequence: (0, 0),
            pending: (Vec::new(), Vec::new()),
            maximum,
            runtime: tokio::runtime::Handle::current(),
        };
        writer
            .stage("request.json", &request, "application/json")
            .await?;
        Ok(writer)
    }

    async fn stage(&self, name: &str, bytes: &[u8], media_type: &str) -> Result<()> {
        let path = format!("tools/{}/{name}", self.operation);
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
                name,
                None,
            )
            .await?;
        Ok(())
    }

    pub(super) fn observe(&mut self, chunk: ProcessOutput<'_>) -> std::io::Result<()> {
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
            pending.try_reserve(amount).map_err(std::io::Error::other)?;
            pending.extend_from_slice(prefix);
            bytes = rest;
            if pending.len() == maximum {
                let runtime = self.runtime.clone();
                await_publication(&runtime, || async {
                    self.flush(stderr).await.map_err(std::io::Error::other)
                })?;
            }
        }
        Ok(())
    }

    // Retain the bytes and sequence until their file is staged.
    pub(super) async fn flush(&mut self, stderr: bool) -> Result<()> {
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

    pub(super) async fn finish(&mut self, result: &ToolResult) -> Result<()> {
        self.flush(false).await?;
        self.flush(true).await?;
        let outcome = crate::local::tool_metadata::encode(
            result
                .value
                .as_object()
                .ok_or_else(|| Error::Invalid("process result must be an object".into()))?,
        )?;
        self.stage("result.json", &outcome, "application/json")
            .await
    }
}
