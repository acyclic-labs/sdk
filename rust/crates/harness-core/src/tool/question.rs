//! Shared file-backed question admission and settlement on every host.
use crate::{
    tool::{
        files::{FileProjection, FileResult, FileTool, WriteFileInput},
        schema,
    },
    tools::{ToolBinding, ToolSpec},
};
use std::sync::Arc;

pub(crate) fn authorize_publication(
    volume: &crate::conversation::VolumeRef,
    path: &str,
    bytes: &[u8],
    scope: &crate::runtime::RuntimeScope,
    io: Option<&crate::conversation::ContentIo>,
) -> crate::Result<()> {
    if volume.class() == crate::conversation::VolumeClass::AgentPrivate
        && path.starts_with("ui/")
        && input_fence(bytes)
        && !scope.grants().contains("tool:call:ask_question")
    {
        return Err(crate::conversation::content_error(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "only the orchestrator role may ask the human",
        )
        .tool_input());
    }
    Ok(())
}

pub(crate) fn valid_path(arguments: &serde_json::Value) -> bool {
    arguments
        .get("path")
        .and_then(serde_json::Value::as_str)
        .and_then(|path| path.strip_prefix(crate::layers::PRIVATE_PATH))
        .and_then(|path| path.strip_prefix("ui/"))
        .is_some_and(|path| {
            !path.is_empty()
                && path
                    .split(['/', '\\'])
                    .all(|part| !matches!(part, "" | "." | ".."))
        })
}

pub(crate) static SPEC: ToolSpec = ToolSpec {
    name: "ask_question",
    revision: "1",
    description: "Ask the human directly by publishing a private UI file. Set path to volume:///private/ui/<name>.md and content to CommonMark containing input fences. End your turn to wait; replies preserve the file generation and asking agent identity.",
    hook_names: &["Write", "write"],
    binding: ToolBinding::Volume,
    input: schema::input::<WriteFileInput>,
    output: schema::output::<FileResult>,
    projection_schema: FileProjection::model_schema,
    executor: |_| Arc::new(FileTool::Write),
    projection: |mode| Arc::new(FileProjection::Publication(mode)),
};

/// Conservative byte scan: no markdown parse, no reads beyond committed writes.
/// Inspect fence runs anywhere on a line, including list/blockquote containers.
/// This deliberately overapproximates markdown: settlement never parses it.
pub(crate) fn input_fence(bytes: &[u8]) -> bool {
    bytes.split(|b| *b == b'\n').any(|line| {
        let mut rest = line;
        while let Some((&marker, tail)) = rest.split_first() {
            rest = tail;
            if !matches!(marker, b'`' | b'~') {
                continue;
            }
            let extra = rest.iter().take_while(|b| **b == marker).count();
            rest = rest.get(extra..).unwrap_or_default();
            if extra >= 2
                && rest
                    .trim_ascii_start()
                    .strip_prefix(b"input")
                    .is_some_and(|tail| tail.first().is_some_and(u8::is_ascii_whitespace))
            {
                return true;
            }
        }
        false
    })
}

#[cfg(test)]
mod tests {
    use super::input_fence;
    #[test]
    fn ui_turn_write_scan_recognises_fences_without_reading_a_tree() {
        for bytes in [
            b"```input choose id=x\n".as_slice(),
            b"   ~~~~ input text\r\n",
            b"```input future id=z\n",
            b"> ```input text id=quote\n",
            b"- ~~~input multi id=list\n",
        ] {
            assert!(input_fence(bytes));
        }
        for bytes in [
            b"prose input text".as_slice(),
            b"```inputs choose",
            b"``input text",
        ] {
            assert!(!input_fence(bytes));
        }
    }
}
