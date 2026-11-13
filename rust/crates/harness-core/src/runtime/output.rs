//! Host output uses the journal without changing ordinary tool authority.
use super::*;

/// Original output key inside one invocation. Callers validate relative names
/// before publication; deriving this key grants no output authority.
pub fn tool_output_path(turn: OperationId, call: OperationId, name: &str) -> String {
    format!("tools/{turn}/{call}/{name}")
}

impl ToolContext {
    // Only this private, short-lived context carries the output owner's grant.
    // Callers choose a relative name inside this call, never a volume or turn.
    pub(super) fn output_context(&self, name: &str) -> Result<(Self, OperationId, String)> {
        crate::conversation::validate_content_path(name, &mut None)?;
        let path = tool_output_path(self.turn_id(), self.operation_id(), name);
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.tool-output/v1");
        hash.update(&self.operation_id().into_bytes());
        hash.update(path.as_bytes());
        let mut operation = [0; 16];
        operation.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        let mut context = self.clone();
        if let Some(journal) = &self.task.journal {
            let content = journal.output_content(self.turn_id())?.ok_or_else(|| {
                Error::Unsupported("execution journal has no output publication owner".into())
            })?;
            let mut task = self.task().clone();
            task.scope.grants = Capabilities::new(
                [
                    crate::conversation::VolumeOperation::Read,
                    crate::conversation::VolumeOperation::Write,
                ]
                .into_iter()
                .map(|operation| content.volume.capability(operation, &mut None))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                // This ceiling cannot grant a role the bound publisher lacks.
                // Preserve its host role only inside this call's output owner.
                .chain([crate::contract::capability::HARNESS_HOST.to_owned()]),
            );
            task.output_content = content;
            task.journal_output = None;
            context.task = Arc::new(task);
        }
        Ok((context, OperationId::from_bytes(operation), path))
    }

    /// Publishes one host-owned output inside this exact tool call's directory.
    /// Limits remain the invocation's; ordinary tool grants are unchanged.
    pub async fn stage_output_file_once(
        &self,
        name: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        let (context, operation, path) = self.output_context(name)?;
        Ok(context
            .stage_file_once(operation, &path, bytes, media_type, display_name, None)
            .await?
            .0)
    }

    /// Streams a bounded host-owned output into this exact call's directory.
    pub async fn stage_output_stream_once(
        &self,
        name: &str,
        source: &mut dyn crate::conversation::ContentSource,
        metadata: (&str, &str),
    ) -> Result<FileRef> {
        let (context, operation, path) = self.output_context(name)?;
        context
            .stage_file_stream_once(operation, &path, source, metadata)
            .await
    }

    /// Reads only the original output named inside this exact call, for replay.
    pub async fn read_output_file_once(
        &self,
        name: &str,
    ) -> Result<Option<(FileRef, acyclic_stream::OwnedBytes)>> {
        let (context, operation, path) = self.output_context(name)?;
        let (file, _) = context.observe_staged_file_once(operation, None).await?;
        let Some(file) = file else {
            return Ok(None);
        };
        if file.path() != path {
            return Err(Error::Storage(
                "output receipt names another call file".into(),
            ));
        }
        let bytes = context.task().read_file(&file, None).await?;
        Ok(Some((file, bytes)))
    }
}
