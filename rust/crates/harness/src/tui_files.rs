//! Completion is a disposable fold of the ordinary task-volume live query.
use crate::{
    task_volume::{Change, ChangeStream, Cursor, Query, TaskVolume},
    tui::PromptInput,
};
use acyclic_stream::StreamProvider;
use futures::StreamExt;

/// Cursor-follow state for completion files and its cancellable request worker.
pub struct CompletionFiles {
    /// Last fully applied publication cut; persist alongside the host's view cursor.
    pub cursor: Cursor,
    changes: ChangeStream,
    #[cfg(all(feature = "native-execution", feature = "filesystem-local"))]
    search: Option<SearchBinding>,
}

#[cfg(all(feature = "native-execution", feature = "filesystem-local"))]
struct SearchBinding {
    files: std::sync::Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
    worker: tokio::task::JoinHandle<crate::Result<()>>,
    request: Option<NameSearchRequest>,
}

#[cfg(all(feature = "native-execution", feature = "filesystem-local"))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NameSearchRequest {
    pub request: crate::OperationId,
    pub task: Option<crate::TaskId>,
    pub query: String,
}

#[cfg(all(feature = "native-execution", feature = "filesystem-local"))]
impl Drop for SearchBinding {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl CompletionFiles {
    /// Load the four completion publications, or resume a previously folded cut.
    /// Pass a cursor only with the matching retained input fold. Lost history
    /// replaces the fold using the query primitive's explicit reset snapshot.
    pub async fn observe<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        input: &mut PromptInput,
        cursor: Option<Cursor>,
    ) -> crate::Result<Self> {
        let observation = volume
            .observe(Query::Completions, cursor.clone(), 4)
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
            #[cfg(all(feature = "native-execution", feature = "filesystem-local"))]
            search: None,
        })
    }

    /// Observe the owner-authorized binding supplied by LocalHarness.
    pub async fn observe_authorized<P: StreamProvider + ?Sized>(
        volume: &crate::task_volume::TaskFiles<P>,
        input: &mut PromptInput,
        cursor: Option<Cursor>,
    ) -> crate::Result<Self> {
        Self::observe(&volume.inner, input, cursor).await
    }

    /// Binds a filesystem owner that consumes ordinary search request files.
    #[cfg(all(feature = "native-execution", feature = "filesystem-local"))]
    pub(crate) fn bind_search(
        &mut self,
        input: &mut PromptInput,
        files: std::sync::Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
        worker: tokio::task::JoinHandle<crate::Result<()>>,
    ) -> crate::Result<()> {
        // The initial snapshot may belong to a previous client/task. Before
        // the first frame, discard results that have no current request owner.
        input
            .completion
            .set_files([
                ("search/results.json", None),
                ("search/progress.json", None),
            ])
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        input.refresh_completions();
        self.search = Some(SearchBinding {
            files,
            worker,
            request: None,
        });
        Ok(())
    }

    /// Publish a changed token before entering the file/input wait. The owner
    /// cancels superseded queries; the UI never walks or hydrates the workspace.
    pub async fn refresh_query(
        &mut self,
        input: &mut PromptInput,
        task: Option<crate::TaskId>,
    ) -> crate::Result<()> {
        #[cfg(all(feature = "native-execution", feature = "filesystem-local"))]
        if let Some(search) = &mut self.search {
            let request = crate::tui::completion_request(&input.editor)
                .filter(|request| request.kind == crate::tui::CompletionKind::Mention);
            let query = request.map(|request| request.query);
            if search
                .request
                .as_ref()
                .map(|request| (request.task, request.query.as_str()))
                == query.as_ref().map(|query| (task, query.as_str()))
            {
                return Ok(());
            }
            if search.worker.is_finished() {
                return Err(crate::Error::Storage(
                    "file-name search owner stopped".into(),
                ));
            }
            let key = crate::task_volume::FileKey::parse("search/query.json")
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            let request = query.map(|query| NameSearchRequest {
                request: crate::OperationId::new(),
                task,
                query,
            });
            // Clear before publishing the new binding. Old publications remain
            // followed, but their request identity cannot refill this popup.
            input
                .completion
                .set_files([
                    ("search/results.json", None),
                    ("search/progress.json", None),
                ])
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            input.refresh_completions();
            let bytes = serde_json::to_vec(&request)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            loop {
                let snapshot = search
                    .files
                    .inner
                    .snapshot(&Query::File(key.clone()), 1)
                    .await
                    .map_err(|error| crate::Error::Storage(error.to_string()))?;
                match search
                    .files
                    .write(
                        snapshot.generation,
                        &crate::task_volume::FileValue::new(key.clone(), bytes.clone()),
                    )
                    .await
                {
                    Ok(_) => break,
                    Err(crate::task_volume::Error::Conflict(_)) => continue,
                    Err(error) => return Err(crate::Error::Storage(error.to_string())),
                }
            }
            search.request = request;
        }
        let _ = (input, task);
        Ok(())
    }

    /// Read without changing the prompt or committed query cursor. A selected
    /// event loop applies this value synchronously after its wait has completed.
    pub async fn read_change(&mut self) -> crate::Result<Change> {
        self.changes
            .next()
            .await
            .ok_or_else(|| crate::Error::Storage("completion query ended; reobserve".into()))?
            .map_err(|e| crate::Error::Storage(e.to_string()))
    }

    /// Publish a selected change atomically before advancing the query cursor.
    pub fn apply_change(&mut self, change: Change, input: &mut PromptInput) -> crate::Result<()> {
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
            .filter(|(path, bytes)| self.accepts_publication(path, *bytes))
            .collect();
        if !files.is_empty() {
            input
                .completion
                .set_files(files.iter().map(|(path, bytes)| (path.as_str(), *bytes)))
                .map_err(|e| crate::Error::Invalid(e.to_string()))?;
            input.refresh_completions();
        }
        self.cursor = change.cursor;
        Ok(())
    }

    fn accepts_publication(&self, path: &str, bytes: Option<&[u8]>) -> bool {
        #[cfg(all(feature = "native-execution", feature = "filesystem-local"))]
        if let Some(search) = &self.search
            && matches!(path, "search/results.json" | "search/progress.json")
        {
            #[derive(serde::Deserialize)]
            struct Binding {
                request: crate::OperationId,
            }
            let expected = search.request.as_ref().map(|request| request.request);
            return bytes.is_none_or(|bytes| {
                serde_json::from_slice::<Binding>(bytes)
                    .is_ok_and(|binding| Some(binding.request) == expected)
            });
        }
        let _ = (path, bytes);
        true
    }

    /// Apply one committed file change outside a competing event-loop wait.
    pub async fn next(&mut self, input: &mut PromptInput) -> crate::Result<()> {
        let change = self.read_change().await?;
        self.apply_change(change, input)
    }
}

#[cfg(all(
    test,
    feature = "native-execution",
    feature = "filesystem-local",
    not(acyclic_harness_loom)
))]
mod name_search_tests;
