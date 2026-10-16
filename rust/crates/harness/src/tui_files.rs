//! Completion is a disposable fold of the ordinary task-volume live query.
use crate::{
    task_volume::{ChangeStream, Cursor, Query, TaskVolume},
    tui::PromptInput,
};
use acyclic_stream::StreamProvider;
use futures::StreamExt;

/// Cursor-follow state; no background task, callback or additional authoritative store.
pub struct CompletionFiles {
    /// Last fully applied publication cut; persist alongside the host's view cursor.
    pub cursor: Cursor,
    changes: ChangeStream,
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
        let observation = volume
            .observe(Query::Inputs, cursor.clone(), maximum_files)
            .await
            .map_err(|e| crate::Error::Storage(e.to_string()))?;
        let cursor = if let Some(snapshot) = observation.snapshot {
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
        Ok(Self {
            cursor,
            changes: observation.changes,
        })
    }

    /// Apply the next committed file change and refresh the popup. Compose this
    /// future with terminal input in the host's event loop.
    pub async fn next(&mut self, input: &mut PromptInput) -> crate::Result<()> {
        let change = self
            .changes
            .next()
            .await
            .ok_or_else(|| crate::Error::Storage("completion query ended; reobserve".into()))?
            .map_err(|e| crate::Error::Storage(e.to_string()))?;
        if let Some(file) = change.file {
            input
                .completion
                .set_file(&file.key.path(), Some(&file.bytes))
                .map_err(|e| crate::Error::Invalid(e.to_string()))?;
        }
        self.cursor = change.cursor;
        input.refresh_completions();
        Ok(())
    }
}
