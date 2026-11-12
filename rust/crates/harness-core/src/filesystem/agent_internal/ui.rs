//! Settlement reads only this turn's durable publication receipts. Replaying
//! the same execution after a crash names the same immutable file versions.
use super::*;
use crate::executor::{ExecutionEvent, ExecutionJournal};
use crate::tool::question::input_fence;
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
                    if file.volume() == &self.public_volume && file.path().starts_with("ui/") {
                        files.insert(file.path().to_owned(), file);
                    }
                }
            }
        }
        if files.is_empty() {
            return Ok(None);
        }
        let head = self
            .volume_head(&self.public_volume, &self.public_grant)
            .await?;
        for (path, file) in files {
            // The journal already owns authenticated reads of this private
            // volume's pinned receipts; the internal file grant is write-only.
            let original = journal.load(&file, None)?.await?;
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
