//! Exact output pages are ordinary immutable files in the caller's volume.
use crate::{
    Error, OperationId, Result,
    conversation::{ContentChange, VolumeRef},
    resources::GenerationRef,
    runtime::ToolContext,
    tool::ToolResult,
};

pub(super) fn directory(operation: OperationId) -> String {
    format!("tools/{operation}/device/capture-v1")
}

/// A model preview is bounded by the original render allowance. The complete
/// byte stream is retained in separately named files, never in this collector.
pub(super) struct Output {
    operation: OperationId,
    window: crate::tool::OutputWindow,
    maximum: u64,
    destination: Option<(VolumeRef, String, GenerationRef)>,
}

impl Output {
    pub(super) fn new(context: &ToolContext) -> Self {
        let maximum =
            crate::tool::output::view_bytes(context.effective_scope().limits().render_bytes);
        Self {
            operation: context.operation_id(),
            window: crate::tool::OutputWindow::new(usize::try_from(maximum).unwrap_or(usize::MAX)),
            maximum,
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
        // Selected file credit partitions this already bounded source chunk.
        let maximum = context
            .effective_scope()
            .limits()
            .file_bytes
            .and_then(|maximum| usize::try_from(maximum).ok())
            .map_or(data.len(), |maximum| maximum.min(data.len()));
        let channel = if stderr { "stderr" } else { "stdout" };
        let directory = directory(self.operation);
        if maximum == 0 {
            return Err(Error::tool_rejected(
                "device output has no admitted file capacity",
            ));
        }
        let begin = self.window.total();
        begin
            .checked_add(data.len() as u64)
            .ok_or_else(|| Error::Invalid("device output byte offset overflow".into()))?;
        for (index, piece) in data.chunks(maximum).enumerate() {
            let offset = (index * maximum) as u64;
            let start = begin + offset;
            let end = start + piece.len() as u64;
            // End-first names make a namespace cursor after `offset~` select
            // the first file containing that byte, without earlier pages.
            let path = format!("{directory}/{end:020}-{start:020}-{channel}.bin");
            let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.devices.output-page.v1");
            hash.update(&self.operation.into_bytes());
            hash.update(&sequence.to_le_bytes());
            hash.update(&offset.to_le_bytes());
            let mut identity = [0; 16];
            identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
            // The original task's file limit partitions this immutable stream
            // position. Each byte offset retains its own publication identity
            // across partial publication and lost-reply recovery.
            let (volume, selected_path) = context.tool_write_path_selection(&path, None)?;
            let (published, _) = context
                .stage_tool_changes(
                    OperationId::from_bytes(identity),
                    volume,
                    &[ContentChange::CreateAbsent {
                        path: &selected_path,
                        bytes: piece,
                        media_type: "application/octet-stream",
                        display_name: channel,
                    }],
                    None,
                )
                .await?;
            let file = published
                .files()
                .next()
                .ok_or_else(|| Error::Storage("device output publication has no file".into()))?;
            let parent = file
                .path()
                .rsplit_once('/')
                .map_or("", |(parent, _)| parent);
            if let Some((volume, path, _)) = &self.destination {
                if volume != file.volume() || path != parent {
                    return Err(Error::Storage("device output destination changed".into()));
                }
            }
            self.destination = Some((
                file.volume().clone(),
                parent.to_owned(),
                published.generation.clone(),
            ));
        }
        self.window
            .observe(data, stderr)
            .map_err(Error::process_io)?;
        Ok(())
    }

    pub(super) fn finish(self, mut result: ToolResult) -> Result<ToolResult> {
        let fields = result
            .value
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("device terminal result is not an object".into()))?;
        if let Some((volume, path, _)) = &self.destination {
            fields.insert("output_directory".into(), path.clone().into());
            fields.insert(
                "output_volume".into(),
                serde_json::to_value(volume).map_err(|error| Error::Invalid(error.to_string()))?,
            );
        }
        let mut result = self.window.render(result, self.maximum)?;
        if result.value["omitted_bytes"].as_u64().unwrap_or_default() > 0
            && let Some((volume, path, generation)) = &self.destination
        {
            // The original namespace cut is supplied by the same protected
            // publication receipt, never by a later head or a directory scan.
            // Fixed-width decimal boundaries have identical encoded length.
            result.value["output_page_query"] =
                page_query(volume, path, generation, self.operation, 0)?;
            result = self.window.render(result, self.maximum)?;
            let start = result.value["omitted_range"]["start"]
                .as_u64()
                .ok_or_else(|| Error::Storage("device output lost its source range".into()))?;
            result.value["output_page_query"] =
                page_query(volume, path, generation, self.operation, start)?;
        }
        Ok(result)
    }
}

fn page_query(
    volume: &VolumeRef,
    path: &str,
    generation: &GenerationRef,
    operation: OperationId,
    offset: u64,
) -> Result<serde_json::Value> {
    use crate::tool::files::{ReadFileInput, ReadSelection, encode_path_version};
    let boundary = format!("{path}/{offset:020}~");
    let (after, _) = encode_path_version(volume, &boundary, generation, None)?;
    serde_json::to_value(ReadFileInput {
        path: directory(operation),
        wait: false,
        version: None,
        range: None,
        selection: Some(ReadSelection::Directory {
            after: Some(after),
            limit: 1,
        }),
    })
    .map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities,
        conversation::{ContentAccess, Limits},
        filesystem::MemoryHarnessStorage,
        runtime::{Bindings, RuntimeScope},
    };
    use std::sync::Arc;

    async fn read_output(
        task: Arc<crate::runtime::TaskContext>,
        arguments: serde_json::Value,
    ) -> Result<ToolResult> {
        let invocation = crate::tool::ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "read-output".into(),
            name: "read".into(),
            arguments,
        };
        let context = ToolContext::new(task, invocation.operation_id, &invocation.call_id)?;
        crate::tool::files::read()?
            .executor
            .execute(invocation, Some(context))
            .await
    }

    #[tokio::test]
    async fn omitted_device_bytes_are_seekable_with_the_original_read_tool() -> Result<()> {
        use crate::{conversation::VolumeOperation, tool::files::DirectoryReadResult};
        use serde_json::json;

        let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
        let mut grants = vec!["tool:call:read".into()];
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            grants.push(storage.volume().capability(operation, &mut None)?);
        }
        let limits = Limits {
            file_bytes: Some(4096),
            render_bytes: Some(16384),
            ..Limits::default()
        };
        let task = Arc::new(
            Bindings {
                scope: RuntimeScope::new(Capabilities::new(grants), limits.clone())?,
                content: Some(storage.content_bindings()),
                ..Bindings::local()
            }
            .build()?
            .agent_context(&Capabilities::default(), storage.content_bindings())?,
        );
        let operation = OperationId::new();
        let context = ToolContext::new(Arc::clone(&task), operation, "device-command")?;
        let records = (0..32)
            .map(|index| vec![b'a' + index % 26; 897 + usize::from(index % 7)])
            .collect::<Vec<_>>();
        let original = records.concat();
        let mut output = Output::new(&context);
        for (index, record) in records.iter().enumerate() {
            output
                .append(&context, index as u64, index % 2 == 1, record)
                .await?;
        }
        let terminal = ToolResult {
            value: json!({"exit_code": 0}),
        };
        let result = output.finish(terminal.clone())?;
        let start = result.value["omitted_range"]["start"].as_u64().unwrap();
        let end = result.value["omitted_range"]["end"].as_u64().unwrap();
        assert!(start > records[0].len() as u64 && end > start);
        let query = result.value["output_page_query"].clone();
        let invocation = crate::tool::ToolInvocation {
            operation_id: operation,
            call_id: "device-command".into(),
            name: "shell".into(),
            arguments: json!({"command": "capture"}),
        };
        use crate::tool::ToolProjection as _;
        let (projected, _) =
            crate::tools::shell::ShellProjection.project(&invocation, &result, None)?;
        assert!(
            serde_json::to_string(&projected)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .len() as u64
                <= limits.render_bytes.unwrap()
        );
        let page: DirectoryReadResult =
            serde_json::from_value(read_output(Arc::clone(&task), query.clone()).await?.value)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(page.entries.len(), 1);
        let first = &page.entries[0];

        // The former unpositioned directory starts at source byte zero. The
        // advertised cursor must instead select the page containing `start`.
        let mut unpositioned = query.clone();
        unpositioned["selection"]["after"] = serde_json::Value::Null;
        let first_page = read_output(Arc::clone(&task), unpositioned).await?;
        assert_ne!(first_page.value["entries"][0]["name"], first.name);

        storage
            .stage(
                OperationId::new(),
                &first.path,
                b"replacement is not captured output",
                "text/plain",
                "later",
            )
            .await?;
        assert_eq!(
            read_output(Arc::clone(&task), query.clone()).await?.value,
            serde_json::to_value(&page).map_err(|error| Error::Invalid(error.to_string()))?,
            "directory continuation retains its original publication cut",
        );

        let mut next_query = query.clone();
        let mut position = start;
        let mut recovered = Vec::new();
        // This consumes bounded pages, not readiness checks or earlier files.
        while position < end {
            let selected: DirectoryReadResult = serde_json::from_value(
                read_output(Arc::clone(&task), next_query.clone())
                    .await?
                    .value,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(selected.entries.len(), 1);
            let entry = &selected.entries[0];
            let mut coordinates = entry.name.split('-');
            let file_end = coordinates.next().unwrap().parse::<u64>().unwrap();
            let file_start = coordinates.next().unwrap().parse::<u64>().unwrap();
            assert!(file_start <= position && position < file_end);
            let until = end.min(file_end);
            let read = read_output(
                Arc::clone(&task),
                json!({
                    "path": entry.path,
                    "version": entry.version,
                    "range": {"start": position - file_start, "end": until - file_start}
                }),
            )
            .await?;
            let bytes = read.value["text"].as_str().unwrap().as_bytes();
            assert_eq!(bytes, &original[position as usize..until as usize]);
            recovered.extend_from_slice(bytes);
            next_query["selection"]["after"] = entry.version.clone().into();
            position = until;
        }
        assert_eq!(recovered, original[start as usize..end as usize]);

        let mut replay = Output::new(&context);
        for (index, record) in records.iter().enumerate() {
            replay
                .append(&context, index as u64, index % 2 == 1, record)
                .await?;
        }
        assert_eq!(replay.finish(terminal)?, result);

        let denied = Arc::new(task.scoped(Capabilities::new(["tool:call:read"]), limits)?);
        assert_eq!(
            read_output(denied, query.clone()).await.unwrap_err().code(),
            crate::wire::ErrorCode::Unauthorized,
            "an immutable cursor does not grant volume access",
        );
        let mut different_directory = query;
        different_directory["path"] = format!("tools/{}", OperationId::new()).into();
        assert_eq!(
            read_output(task, different_directory)
                .await
                .unwrap_err()
                .code(),
            crate::wire::ErrorCode::Invalid,
            "a continuation cannot be moved to another directory",
        );
        Ok(())
    }

    #[tokio::test]
    async fn output_replay_retains_original_files_after_visible_edits() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
        let mut bindings = Bindings::local();
        bindings.scope = RuntimeScope::new(
            storage.owner_scope().capabilities().clone(),
            Limits {
                file_bytes: Some(2),
                render_bytes: Some(2048),
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
        // Four source bytes exercise elision while the actual model envelope
        // retains enough credit for its source identity and range metadata.
        first.window = crate::tool::OutputWindow::new(4);
        first.append(&context, 1, false, b"out").await?;
        let path = format!(
            "{}/00000000000000000002-00000000000000000000-stdout.bin",
            directory(operation)
        );
        // A file is readable before any terminal event arrives.
        let (original, bytes, _) = storage
            .content_reader()
            .read_private_path(
                storage.volume(),
                "",
                &path,
                None,
                Some(4096),
                ContentAccess::default(),
            )?
            .await?
            .expect("the selected fixture file exists");
        assert_eq!(bytes.as_ref(), b"ou");
        let (_, remainder, _) = storage
            .content_reader()
            .read_private_path(
                storage.volume(),
                "",
                &format!(
                    "{}/00000000000000000003-00000000000000000002-stdout.bin",
                    directory(operation)
                ),
                None,
                Some(2),
                ContentAccess::default(),
            )?
            .await?
            .expect("the selected fixture file exists");
        assert_eq!(remainder.as_ref(), b"t");
        first.append(&context, 2, true, b"err").await?;
        let terminal = ToolResult {
            value: serde_json::json!({"exit_code": 7}),
        };
        let result = first.finish(terminal.clone())?;
        assert_eq!(result.value["stdout"], "ou");
        assert_eq!(result.value["stderr"], "rr");
        assert_eq!(result.value["truncated"], true);
        assert_eq!(result.value["omitted_bytes"], 2);
        assert_eq!(result.value["output_directory"], directory(operation));
        assert_eq!(
            result.value["output_volume"],
            serde_json::to_value(storage.volume())
                .map_err(|error| Error::Invalid(error.to_string()))?
        );
        // Prepared windows retain their original coordinates through the
        // shared model projection; already shortened channels are not recut.
        for maximum in [2048, 1024] {
            let bounded = crate::tools::shell::bounded_result(result.clone(), maximum)?;
            assert_eq!(bounded.value["omitted_bytes"], 2);
            assert_eq!(bounded.value["truncated"], true);
            let invocation = crate::tool::ToolInvocation {
                operation_id: operation,
                call_id: "original-call".into(),
                name: "shell".into(),
                arguments: serde_json::json!({"command":"echo"}),
            };
            use crate::tool::ToolProjection as _;
            let (projected, _) =
                crate::tools::shell::ShellProjection.project(&invocation, &bounded, None)?;
            let encoded = serde_json::to_string(&projected)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert!(encoded.len() as u64 <= maximum);
            assert!(encoded.contains("bytes omitted"));
            assert!(encoded.contains(&directory(operation)));
            assert!(encoded.contains(storage.volume().id()));
        }

        let edited = storage
            .stage(
                OperationId::new(),
                &path,
                b"xx",
                "application/octet-stream",
                "stdout",
            )
            .await?;
        let mut replay = Output::new(&context);
        replay.window = crate::tool::OutputWindow::new(4);
        replay.append(&context, 1, false, b"out").await?;
        replay.append(&context, 2, true, b"err").await?;
        assert_eq!(replay.finish(terminal)?, result);
        assert_eq!(task.read_file(&original, None).await?.as_ref(), b"ou");
        assert_eq!(task.read_file(&edited, None).await?.as_ref(), b"xx");
        let (_, visible, _) = storage
            .content_reader()
            .read_private_path(
                storage.volume(),
                "",
                &path,
                None,
                Some(4096),
                ContentAccess::default(),
            )?
            .await?
            .expect("the selected fixture file exists");
        assert_eq!(visible.as_ref(), b"xx");
        assert!(
            Output::new(&context)
                .append(&context, 1, false, b"changed")
                .await
                .is_err()
        );
        assert_eq!(task.read_file(&original, None).await?.as_ref(), b"ou");
        Ok(())
    }
}
