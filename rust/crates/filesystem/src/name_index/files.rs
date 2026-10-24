//! Derived membership of the pinned index files. Generation diffs can prove
//! unrelated writes without reverse-resolving paths or reopening checkpoints.
use super::*;
use crate::{FileId, GenerationDiff, kernel::FileKind};

#[derive(Clone)]
pub(super) struct IndexFiles {
    members: imbl::OrdMap<FileId, u64>,
    volume_root: FileId,
    sdk: FileId,
}

impl IndexFiles {
    pub(super) async fn collect<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        generation: &Generation<A, O>,
    ) -> Result<Self, SearchError> {
        let stop = CancellationToken::new();
        let mut files = Self {
            members: Default::default(),
            volume_root: generation
                .stat("/", WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?
                .value
                .file_id,
            sdk: generation
                .stat("/.acyclic-sdk", WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?
                .value
                .file_id,
        };
        // These are the format's directories, not a recursive walk of arbitrary
        // user-created children. All index chunks and their headers live here.
        for directory in [
            ROOT.to_owned(),
            format!("{ROOT}/frontier"),
            format!("{ROOT}/policies"),
            format!("{ROOT}/directories"),
        ] {
            let stat = generation
                .stat(&directory, WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?
                .value;
            files.add(stat.file_id);
            let mut after = None;
            loop {
                let page = generation
                    .list_directory(
                        &directory,
                        after.as_ref(),
                        128,
                        WorkBudget::UNBOUNDED,
                        &stop,
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value
                    .0;
                for entry in &page.entries {
                    files.add(entry.file_id);
                }
                if !page.has_more {
                    break;
                }
                after = page.entries.last().map(|entry| entry.name.clone());
            }
        }
        Ok(files)
    }

    fn rebinds_ancestor(&self, diff: &GenerationDiff) -> bool {
        diff.bindings.iter().any(|binding| {
            let expected = if binding.directory_id == self.volume_root {
                ".acyclic-sdk"
            } else if binding.directory_id == self.sdk {
                "name-index"
            } else {
                return false;
            };
            let name = &binding.name;
            if name.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le {
                name.as_bytes()
                    .iter()
                    .copied()
                    .eq(expected.encode_utf16().flat_map(u16::to_le_bytes))
            } else {
                name.as_bytes() == expected.as_bytes()
            }
        }) || diff.files.iter().any(|file| {
            matches!(file.file_id, id if id == self.volume_root || id == self.sdk)
                && file
                    .after
                    .is_none_or(|record| record.kind != FileKind::Directory)
        })
    }

    fn add(&mut self, id: FileId) {
        let count = self.members.get(&id).copied().unwrap_or(0);
        self.members.insert(id, count + 1);
    }

    pub(super) fn unchanged(&self, diff: &GenerationDiff) -> bool {
        !diff.truncated
            && !self.rebinds_ancestor(diff)
            && diff
                .files
                .iter()
                .all(|change| !self.members.contains_key(&change.file_id))
            && diff
                .bindings
                .iter()
                .all(|binding| !self.members.contains_key(&binding.directory_id))
    }

    pub(super) async fn after_publication<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        generation: &Generation<A, O>,
        previous: Option<&NameIndex<A, O>>,
    ) -> Result<Self, SearchError> {
        if let Some(previous) = previous
            && let Some(diff) = bounded_diff(&previous.generation, generation).await?
            && !previous.files.rebinds_ancestor(&diff)
            && !diff.bindings.iter().any(|binding| {
                binding.before.as_ref().is_some_and(|entry| {
                    entry.kind == FileKind::Directory
                        && previous.files.members.contains_key(&entry.file_id)
                })
            })
        {
            let mut files = previous.files.clone();
            // A publication can introduce both a directory and its files in
            // one transaction. Fold bindings until those parent IDs are known.
            loop {
                let mut added = false;
                for binding in &diff.bindings {
                    if files.members.contains_key(&binding.directory_id)
                        && let Some(entry) = &binding.after
                        && !files.members.contains_key(&entry.file_id)
                    {
                        files.members.insert(entry.file_id, 0);
                        added = true;
                    }
                }
                if !added {
                    break;
                }
            }
            // Count protected bindings, including aliases. Moving the last
            // binding out of the index must not retain historical file IDs.
            for binding in &diff.bindings {
                if files.members.contains_key(&binding.directory_id) {
                    if let Some(entry) = &binding.before {
                        let count = files.members.get(&entry.file_id).copied().unwrap_or(0);
                        files.members.insert(entry.file_id, count.saturating_sub(1));
                    }
                    if let Some(entry) = &binding.after {
                        files.add(entry.file_id);
                    }
                }
            }
            for binding in &diff.bindings {
                for entry in binding.before.iter().chain(binding.after.iter()) {
                    if files.members.get(&entry.file_id) == Some(&0) {
                        files.members.remove(&entry.file_id);
                    }
                }
            }
            return Ok(files);
        }
        Self::collect(generation).await
    }
}

pub(super) async fn bounded_diff<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    before: &Generation<A, O>,
    after: &Generation<A, O>,
) -> Result<Option<GenerationDiff>, SearchError> {
    if before.workspace_id() != after.workspace_id()
        || !after
            .workspace()
            .volume
            .fs
            .owns_workspace(&before.workspace())
    {
        return Ok(None);
    }
    // This is a fast-path work bound. Larger deltas use the ordinary cold file
    // query; they never reject an otherwise valid index or search request.
    match before
        .diff_to(after, 512, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
    {
        Ok(receipt) if !receipt.value.0.changes().truncated => {
            Ok(Some(receipt.value.0.changes().clone()))
        }
        Ok(_) => Ok(None),
        Err(failure) if matches!(failure.error, WorkspaceError::JoinLimit) => Ok(None),
        Err(failure) => Err(failure.error.into()),
    }
}
