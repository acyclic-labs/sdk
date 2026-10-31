//! Exact output pages are ordinary immutable files in the caller's volume.
use crate::{
    Error, OperationId, Result, conversation::VolumeRef, runtime::ToolContext, tool::ToolResult,
};

pub(super) fn directory(operation: OperationId) -> String {
    format!("tools/{operation}/device")
}

/// A model preview is bounded by the original render allowance. The complete
/// byte stream is retained in separately named files, never in this collector.
pub(super) struct Output {
    operation: OperationId,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    remaining: u64,
    truncated: bool,
    destination: Option<(VolumeRef, String)>,
}

impl Output {
    pub(super) fn new(context: &ToolContext) -> Self {
        Self {
            operation: context.operation_id(),
            stdout: Vec::new(),
            stderr: Vec::new(),
            remaining: context.effective_scope().limits().render_bytes,
            truncated: false,
            destination: None,
        }
    }

    pub(super) async fn append(
        &mut self,
        context: &ToolContext,
        sequence: u64,
        stderr: bool,
        data: &[u8],
    ) -> Result<()> {
        if data.is_empty() || data.len() > super::records::OUTPUT_CHUNK_BYTES {
            return Err(Error::Invalid(
                "device output exceeds its protective chunk".into(),
            ));
        }
        let channel = if stderr { "stderr" } else { "stdout" };
        let path = format!("{}/{sequence:020}-{channel}.bin", directory(self.operation));
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.devices.output-file.v1");
        hash.update(&self.operation.into_bytes());
        hash.update(&sequence.to_le_bytes());
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        // The immutable original stream position selects one file operation.
        // Reconciliation reuses it even if the first publication reply was lost.
        let (file, _) = context
            .stage_tool_file_once(
                OperationId::from_bytes(identity),
                &path,
                data,
                "application/octet-stream",
                channel,
                None,
            )
            .await?;
        let parent = file
            .path()
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent);
        if let Some((volume, path)) = &self.destination {
            if volume != file.volume() || path != parent {
                return Err(Error::Storage("device output destination changed".into()));
            }
        } else {
            self.destination = Some((file.volume().clone(), parent.to_owned()));
        }
        let take = self.remaining.min(data.len() as u64) as usize;
        self.remaining -= take as u64;
        self.truncated |= take != data.len();
        let output = if stderr {
            &mut self.stderr
        } else {
            &mut self.stdout
        };
        output.extend_from_slice(&data[..take]);
        Ok(())
    }

    pub(super) fn finish(self, mut result: ToolResult) -> Result<ToolResult> {
        let fields = result
            .value
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("device terminal result is not an object".into()))?;
        fields.insert("stdout".into(), preview(self.stdout).into());
        fields.insert("stderr".into(), preview(self.stderr).into());
        fields.insert("truncated".into(), self.truncated.into());
        if let Some((volume, path)) = &self.destination {
            fields.insert("output_directory".into(), path.clone().into());
            fields.insert(
                "output_volume".into(),
                serde_json::to_value(volume).map_err(|error| Error::Invalid(error.to_string()))?,
            );
        }
        Ok(result)
    }
}

fn preview(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

#[cfg(all(test, feature = "filesystem"))]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities,
        conversation::{ContentAccess, Limits},
        filesystem::MemoryHarnessStorage,
        runtime::{Bindings, RuntimeScope},
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn output_replay_retains_original_files_after_visible_edits() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
        let mut bindings = Bindings::local();
        bindings.scope = RuntimeScope::new(
            storage.owner_scope().capabilities().clone(),
            Limits {
                file_bytes: 4096,
                render_bytes: 4,
                ..Limits::default()
            },
        )?;
        let task = Arc::new(
            bindings
                .build()?
                .agent_context(&Capabilities::default(), storage.content_bindings())?,
        );
        let operation = OperationId::new();
        let context = ToolContext::new(Arc::clone(&task), operation, "original-call")?;
        let mut first = Output::new(&context);
        first.append(&context, 1, false, b"out").await?;
        let path = format!("{}/00000000000000000001-stdout.bin", directory(operation));
        // A file is readable before any terminal event arrives.
        let (original, bytes, _) = storage
            .content_reader()
            .read_private_path(
                storage.volume(),
                "",
                &path,
                None,
                4096,
                ContentAccess::default(),
            )?
            .await?;
        assert_eq!(bytes.as_ref(), b"out");
        first.append(&context, 2, true, b"err").await?;
        let terminal = ToolResult {
            value: serde_json::json!({"exit_code": 7}),
        };
        let result = first.finish(terminal.clone())?;
        assert_eq!(result.value["stdout"], "out");
        assert_eq!(result.value["stderr"], "e");
        assert_eq!(result.value["truncated"], true);
        assert_eq!(result.value["output_directory"], directory(operation));
        assert_eq!(
            result.value["output_volume"],
            serde_json::to_value(storage.volume())
                .map_err(|error| Error::Invalid(error.to_string()))?
        );

        let edited = storage
            .stage(
                OperationId::new(),
                &path,
                b"edited",
                "application/octet-stream",
                "stdout",
            )
            .await?;
        let mut replay = Output::new(&context);
        replay.append(&context, 1, false, b"out").await?;
        replay.append(&context, 2, true, b"err").await?;
        assert_eq!(replay.finish(terminal)?, result);
        assert_eq!(task.read_file(&original, None).await?.as_ref(), b"out");
        assert_eq!(task.read_file(&edited, None).await?.as_ref(), b"edited");
        let (_, visible, _) = storage
            .content_reader()
            .read_private_path(
                storage.volume(),
                "",
                &path,
                None,
                4096,
                ContentAccess::default(),
            )?
            .await?;
        assert_eq!(visible.as_ref(), b"edited");
        assert!(
            Output::new(&context)
                .append(&context, 1, false, b"changed")
                .await
                .is_err()
        );
        assert_eq!(task.read_file(&original, None).await?.as_ref(), b"out");
        Ok(())
    }
}
