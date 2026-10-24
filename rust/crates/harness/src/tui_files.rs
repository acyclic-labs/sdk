//! Completion is a disposable fold of the ordinary task-volume live query.
use crate::client::{FileUpdate, file_follow::FileFollow};
use crate::{
    task_volume::{Cursor, Query, TaskVolume},
    tui::PromptInput,
};
use acyclic_stream::StreamProvider;

/// Cursor-follow state; no background task, callback or additional authoritative store.
pub struct CompletionFiles {
    /// Last fully applied publication cut; persist alongside the host's view cursor.
    pub cursor: Cursor,
    follow: FileFollow,
}

impl CompletionFiles {
    /// Load command/workspace files, or resume a previously folded publication cut.
    /// Pass a cursor only with the matching retained input fold. Lost history
    /// replaces the fold using the query primitive's explicit reset snapshot.
    pub async fn observe<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        input: &mut PromptInput,
        cursor: Option<Cursor>,
        maximum_files: usize,
    ) -> crate::Result<Self> {
        Self::observe_source(volume, input, cursor, maximum_files).await
    }
    pub(crate) async fn observe_source(
        volume: &(impl crate::task_volume::FileQuerySource + ?Sized),
        input: &mut PromptInput,
        cursor: Option<Cursor>,
        maximum_files: usize,
    ) -> crate::Result<Self> {
        // Use the shared file API's portable byte ceiling; the caller's
        // existing file-count bound remains the completion selection bound.
        let (follow, snapshot) = FileFollow::open(
            volume,
            Query::Inputs,
            cursor.clone(),
            maximum_files,
            usize::try_from(crate::conversation::MAX_EXACT_JS_INTEGER).unwrap_or(usize::MAX),
        )
        .await?;
        let cursor = if let Some(snapshot) = snapshot {
            // Build off to the side: malformed files never leave a partial snapshot.
            let mut menu = input.completion.clone();
            menu.clear_files();
            for file in snapshot.files {
                menu.set_file(&file.key.path(), Some(&file.bytes))
                    .map_err(|e| crate::Error::Invalid(e.to_string()))?;
            }
            input.completion = menu;
            snapshot.cursor
        } else {
            cursor
                .ok_or_else(|| crate::Error::Invalid("query returned no initial snapshot".into()))?
        };
        input.refresh_completions();
        Ok(Self { cursor, follow })
    }

    /// Observe the owner-authorized binding supplied by LocalHarness.
    pub async fn observe_authorized<P: StreamProvider + ?Sized>(
        volume: &crate::task_volume::TaskFiles<P>,
        input: &mut PromptInput,
        cursor: Option<Cursor>,
        maximum_files: usize,
    ) -> crate::Result<Self> {
        Self::observe_source(volume, input, cursor, maximum_files).await
    }

    /// Read without changing the prompt or committed query cursor. A selected
    /// event loop applies this value synchronously after its wait has completed.
    pub async fn read_change(&mut self) -> crate::Result<FileUpdate> {
        self.follow.read().await
    }

    /// Publish a selected change or replacement before advancing its cursor.
    pub fn apply_change(
        &mut self,
        update: FileUpdate,
        input: &mut PromptInput,
    ) -> crate::Result<()> {
        let cursor = update.cursor().clone();
        if let FileUpdate::Snapshot(snapshot) = update {
            self.follow.validate_snapshot(&snapshot)?;
            let mut menu = input.completion.clone();
            menu.clear_files();
            for file in snapshot.files {
                menu.set_file(&file.key.path(), Some(&file.bytes))
                    .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            }
            input.completion = menu;
            input.refresh_completions();
            self.follow.accept(cursor.clone());
            self.cursor = cursor;
            return Ok(());
        }
        let FileUpdate::Change(change) = update else {
            unreachable!()
        };
        let files: Vec<_> = change
            .removed
            .iter()
            .map(|key| (key.path(), None))
            .chain(
                change
                    .file
                    .iter()
                    .chain(&change.files)
                    .map(|file| (file.key.path(), Some(file.bytes.as_ref()))),
            )
            .collect();
        if !files.is_empty() {
            input
                .completion
                .set_files(files.iter().map(|(path, bytes)| (path.as_str(), *bytes)))
                .map_err(|e| crate::Error::Invalid(e.to_string()))?;
            input.refresh_completions();
        }
        self.follow.accept(cursor.clone());
        self.cursor = cursor;
        Ok(())
    }

    /// Apply one committed file change outside a competing event-loop wait.
    pub async fn next(&mut self, input: &mut PromptInput) -> crate::Result<()> {
        let change = self.read_change().await?;
        self.apply_change(change, input)
    }
}
