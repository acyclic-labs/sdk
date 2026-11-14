//! Working files selected at one immutable private-volume cut.
use crate::{
    Error, Result,
    conversation::{
        ContentAccess, ContentResidencyVerifier, FileRef, Limits, PrivateDirectoryEntryKind,
        VolumeRef,
    },
    resources::GenerationRef,
};

/// Captures notes, a working summary and recursively paged pins. The returned
/// refs feed the ordinary manifest and epoch-change path; source failures and
/// admission failures are errors, never an instruction to omit a document.
pub async fn capture_working_documents(
    reader: &dyn ContentResidencyVerifier,
    volume: &VolumeRef,
    limits: Limits,
) -> Result<Vec<FileRef>> {
    limits.validate(&mut None)?;
    let (generation, _hold) = reader
        .select_private_generation(volume, "context", "context", None, ContentAccess::default())?
        .await?;
    let mut capture = Capture {
        reader,
        volume,
        generation,
        limits,
        documents: Vec::new(),
        bytes: 0,
    };
    for path in ["context/notes.md", "context/summary.md"] {
        capture.file(path).await?;
    }
    let mut directories = vec!["context/pins".to_owned()];
    while let Some(directory) = directories.pop() {
        // The opaque position keeps its original page custody until the next
        // request has finished borrowing it.
        let mut after: Option<(Vec<u8>, _)> = None;
        loop {
            let Some((page, hold)) = reader
                .list_private_directory(
                    volume,
                    "context",
                    &directory,
                    Some(&capture.generation),
                    after.as_ref().map(|(cursor, _)| cursor.as_slice()),
                    256,
                    None,
                    None,
                )?
                .await?
            else {
                break;
            };
            page.validate()?;
            if page.generation != capture.generation {
                return Err(Error::Storage(
                    "context directory changed its selected cut".into(),
                ));
            }
            for entry in page.entries {
                let path = format!("{directory}/{}", entry.name);
                match entry.kind {
                    PrivateDirectoryEntryKind::Directory => directories.push(path),
                    PrivateDirectoryEntryKind::File => capture.file(&path).await?,
                }
            }
            after = page.next.map(|cursor| (cursor, hold));
            if after.is_none() {
                break;
            }
        }
    }
    // Directory page boundaries and traversal order cannot change the prompt.
    capture
        .documents
        .sort_unstable_by(|left, right| left.path().cmp(right.path()));
    Ok(capture.documents)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{AgentId, OperationId, conversation::ContentChange};

    #[tokio::test]
    async fn paged_pins_keep_their_immutable_cut_and_refuse_partial_admission() -> Result<()> {
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
        let bindings = storage.content_bindings();
        let writer = bindings
            .writer
            .as_ref()
            .ok_or_else(|| Error::Invalid("private writer is missing".into()))?;
        let paths = (0..257)
            .map(|index| format!("context/pins/{index:04}.md"))
            .collect::<Vec<_>>();
        let changes = paths
            .iter()
            .map(|path| ContentChange::CreateAbsent {
                path,
                bytes: b"old",
                media_type: "text/plain",
                display_name: "pin.md",
            })
            .collect::<Vec<_>>();
        writer
            .stage_changes(
                &bindings.volume,
                OperationId::new(),
                &changes,
                ContentAccess::default(),
            )?
            .await?;
        let limits = Limits::default();
        let captured =
            capture_working_documents(&*bindings.reader, &bindings.volume, limits).await?;
        assert_eq!(captured.len(), paths.len());
        assert!(
            captured
                .iter()
                .map(FileRef::path)
                .eq(paths.iter().map(String::as_str))
        );
        let first = captured
            .first()
            .ok_or_else(|| Error::Invalid("captured pin is missing".into()))?;
        writer
            .stage_changes(
                &bindings.volume,
                OperationId::new(),
                &[ContentChange::UpdateExpected {
                    source: first,
                    bytes: b"new",
                }],
                ContentAccess::default(),
            )?
            .await?;
        assert_eq!(
            bindings
                .reader
                .read(first, 4096, None, None)?
                .await?
                .as_ref(),
            b"old"
        );
        let current =
            capture_working_documents(&*bindings.reader, &bindings.volume, limits).await?;
        let updated = current
            .first()
            .ok_or_else(|| Error::Invalid("updated pin is missing".into()))?;
        assert_eq!(
            bindings
                .reader
                .read(updated, 4096, None, None)?
                .await?
                .as_ref(),
            b"new"
        );
        assert!(matches!(
            capture_working_documents(
                &*bindings.reader,
                &bindings.volume,
                Limits { render_bytes: Some(1), ..limits },
            ).await,
            Err(Error::Invalid(reason)) if reason == "context documents exceed admitted context bounds"
        ));
        Ok(())
    }
}

struct Capture<'a> {
    reader: &'a dyn ContentResidencyVerifier,
    volume: &'a VolumeRef,
    generation: GenerationRef,
    limits: Limits,
    documents: Vec<FileRef>,
    bytes: u64,
}

impl Capture<'_> {
    async fn file(&mut self, path: &str) -> Result<()> {
        let Some((file, _content, _hold)) = self
            .reader
            .read_private_path(
                self.volume,
                "context",
                path,
                Some(&self.generation),
                self.limits.file_bytes,
                ContentAccess::default(),
            )?
            .await?
        else {
            return Ok(());
        };
        self.bytes = self
            .bytes
            .checked_add(file.descriptor().byte_length())
            .ok_or_else(|| Error::Invalid("context document byte count overflows".into()))?;
        if self
            .limits
            .render_bytes
            .is_some_and(|maximum| self.bytes > maximum)
            || self
                .limits
                .context_messages
                .is_some_and(|maximum| self.documents.len() >= maximum.get())
        {
            return Err(Error::Invalid(
                "context documents exceed admitted context bounds".into(),
            ));
        }
        self.documents.push(file);
        Ok(())
    }
}
