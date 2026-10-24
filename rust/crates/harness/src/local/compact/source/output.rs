//! Summary output comes from the immutable streams at the admitted file cut.
use super::*;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct OutputStreams {
    streams: BTreeMap<FileKey, Vec<u8>>,
    bytes: u64,
}

impl OutputStreams {
    pub(super) fn observe(&mut self, key: &FileKey, bytes: &[u8], limits: Limits) -> Result<()> {
        let FileKey::Task {
            task,
            file: TaskFile::Tool { turn, call, file },
        } = key
        else {
            return Ok(());
        };
        let file = match file {
            ToolFile::Stdout(_) => ToolFile::Stdout(0),
            ToolFile::Stderr(_) => ToolFile::Stderr(0),
            _ => return Ok(()),
        };
        let total = self.bytes.saturating_add(bytes.len() as u64);
        if total > limits.file_bytes {
            return Err(Error::Invalid(
                "compaction output exceeds file bound".into(),
            ));
        }
        let stream = self
            .streams
            .entry(FileKey::Task {
                task: task.clone(),
                file: TaskFile::Tool {
                    turn: *turn,
                    call: call.clone(),
                    file,
                },
            })
            .or_default();
        stream.try_reserve(bytes.len()).map_err(storage)?;
        stream.extend_from_slice(bytes);
        self.bytes = total;
        Ok(())
    }

    pub(super) fn append(self, text: &mut String, limits: Limits) -> Result<()> {
        for (key, bytes) in self.streams {
            let FileKey::Task {
                file: TaskFile::Tool { turn, call, file },
                ..
            } = key
            else {
                return Err(Error::Invalid("summary stream has no tool identity".into()));
            };
            let channel = match file {
                ToolFile::Stdout(_) => "stdout",
                ToolFile::Stderr(_) => "stderr",
                _ => {
                    return Err(Error::Invalid(
                        "summary stream has no output channel".into(),
                    ));
                }
            };
            append_source(
                text,
                &format!("tool {turn}/{} {channel}:", call.as_str()),
                limits,
            )?;
            // Decode the complete selected stream, so a code point split by
            // the 64 KiB frame boundary has the same meaning as the tool result.
            append_source(text, &String::from_utf8_lossy(&bytes), limits)?;
        }
        Ok(())
    }
}
