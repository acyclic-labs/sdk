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

    pub(super) fn finish(&self, mut result: ToolResult) -> Result<ToolResult> {
        let fields = result
            .value
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("device terminal result is not an object".into()))?;
        fields.insert(
            "stdout".into(),
            String::from_utf8_lossy(&self.stdout).into_owned().into(),
        );
        fields.insert(
            "stderr".into(),
            String::from_utf8_lossy(&self.stderr).into_owned().into(),
        );
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
