//! Settlement reads only this turn's durable publication receipts. Replaying
//! the same execution after a crash names the same immutable file versions.
use super::*;
use crate::executor::{ExecutionEvent, ExecutionJournal};
use std::collections::BTreeMap;

#[derive(serde::Deserialize)]
struct Completed {
    value: Publication,
}
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Publication {
    File(crate::tool::files::FileResult),
    Files { files: Vec<FileRef> },
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> AgentInternalFiles<A, O> {
    pub(super) async fn ui_input(
        &self,
        turn: OperationId,
        journal: &dyn ExecutionJournal,
    ) -> Result<Option<String>> {
        let mut after = 0;
        let mut files = BTreeMap::new();
        loop {
            let records = journal
                .replay(
                    turn,
                    after,
                    crate::executor::EXECUTION_REPLAY_PAGE_RECORDS,
                    None,
                )?
                .await?;
            if records.is_empty() {
                break;
            }
            for record in records {
                after = record.sequence;
                let ExecutionEvent::ToolCompleted { result, .. } = record.event else {
                    continue;
                };
                let bytes = journal.load(&result, None)?.await?;
                let Ok(completed) = serde_json::from_slice::<Completed>(&bytes) else {
                    continue;
                };
                let publications = match completed.value {
                    Publication::File(file) => vec![file.file],
                    Publication::Files { files } => files,
                };
                for file in publications {
                    if file.volume() == &self.volume && file.path().starts_with("ui/") {
                        files.insert(file.path().to_owned(), file);
                    }
                }
            }
        }
        if files.is_empty() {
            return Ok(None);
        }
        let head = self.head().await?;
        for (path, file) in files {
            let original = self
                .host
                .read_content(&file, &self.grant, file.descriptor().byte_length(), None)
                .await?;
            if !input_fence(&original) {
                continue;
            }
            // A later overwrite or deletion in the same turn closes the ask.
            // Current bytes are never substituted for this turn's own receipt.
            let Some(current) = Self::read_at(&head, &format!("/{path}"), u64::MAX).await? else {
                continue;
            };
            if original.as_ref() == current.as_ref() {
                return Ok(Some(path.strip_prefix("ui/").unwrap_or(&path).to_owned()));
            }
        }
        Ok(None)
    }
}

/// Conservative byte scan: no markdown parse, no reads beyond committed writes.
/// Inspect fence runs anywhere on a line, including list/blockquote containers.
/// This deliberately overapproximates markdown: settlement never parses it.
fn input_fence(bytes: &[u8]) -> bool {
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
