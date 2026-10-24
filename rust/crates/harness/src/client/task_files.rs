//! Native task and UI files use the same disposable ClientViews as WASM.
use super::{
    ClientViewLimits, ClientViews, QueryViewLimits, command_output::follow::DisplayWindowBudget,
};
use crate::{
    Error, Result,
    task_volume::{Change, ChangeStream, FileQuerySource, Query},
    wire,
};
use acyclic_stream::StreamPath;
use futures::{FutureExt as _, StreamExt as _};

/// Authorized snapshot/follow inputs feeding the single renderer-neutral fold.
pub struct TaskFileClient {
    views: ClientViews,
    source: std::sync::Arc<dyn FileQuerySource>,
    changes: ChangeStream,
    commands: Option<ChangeStream>,
    command_source: Option<std::sync::Arc<dyn FileQuerySource>>,
    command_selection: Option<([u8; 32], [u8; 32])>,
    task_selection: ([u8; 32], [u8; 32]),
    selected: Option<crate::TaskId>,
    bounds: DisplayWindowBudget,
    presentation_revision: u64,
}
impl TaskFileClient {
    /// Observe the authorized task folder at one atomic cut.
    pub async fn observe(
        files: &impl FileQuerySource,
        owner: &wire::Authority,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<Self> {
        Self::observe_query(files, owner, Query::Tasks, maximum_files, maximum_bytes).await
    }
    async fn observe_query(
        files: &impl FileQuerySource,
        owner: &wire::Authority,
        query: Query,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<Self> {
        let mut observed = files
            .observe_files(query.clone(), None, maximum_files, maximum_bytes)
            .await?;
        let snapshot = observed
            .snapshot
            .take()
            .ok_or_else(|| Error::Conflict("task snapshot missing".into()))?;
        let task_selection = (snapshot.cursor.volume, snapshot.cursor.query);
        let mut views = ClientViews::new(
            StreamPath::new(format!("tasks/{}", owner.id))
                .map_err(|error| Error::Invalid(error.to_string()))?,
            ClientViewLimits {
                agents: 1,
                selected: 1,
                overview_values: maximum_files,
                overview_bytes: maximum_bytes as u64,
                query: QueryViewLimits {
                    files: maximum_files,
                    segments: maximum_files,
                    bytes: maximum_bytes as u64,
                },
            },
        )?;
        views.query_snapshot(query, snapshot, maximum_files, maximum_bytes)?;
        Ok(Self {
            views,
            source: files.retain(),
            changes: observed.changes,
            commands: None,
            command_source: None,
            command_selection: None,
            task_selection,
            selected: None,
            bounds: DisplayWindowBudget {
                files: maximum_files,
                bytes: maximum_bytes,
            },
            presentation_revision: 0,
        })
    }
    /// Retain only the selected task's bounded display tail.
    pub async fn observe_task(
        files: &impl FileQuerySource,
        owner: &wire::Authority,
        task: crate::TaskId,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<Self> {
        let mut client = Self::observe_query(
            files,
            owner,
            Self::task_query(task)?,
            maximum_files,
            maximum_bytes,
        )
        .await?;
        client.selected = Some(task);
        Ok(client)
    }
    fn task_query(task: crate::TaskId) -> Result<Query> {
        Ok(Query::TaskView {
            task: crate::task_volume::Component::new(task.to_string())?,
            recent: 128,
            reasoning: true,
            details: true,
            terminal: true,
        })
    }
    /// Replace the selected query and release its old files together.
    pub async fn follow_task(
        &mut self,
        files: &impl FileQuerySource,
        task: crate::TaskId,
    ) -> Result<()> {
        if self.selected == Some(task) {
            return Ok(());
        }
        let query = Self::task_query(task)?;
        let observed = files
            .observe_files(query.clone(), None, self.bounds.files, self.bounds.bytes)
            .await?;
        let snapshot = observed
            .snapshot
            .ok_or_else(|| Error::Conflict("task snapshot missing".into()))?;
        let identity = (snapshot.cursor.volume, snapshot.cursor.query);
        self.views
            .query_snapshot(query, snapshot, self.bounds.files, self.bounds.bytes)?;
        if self.task_selection != identity {
            self.views.remove_selection(&self.task_selection);
        }
        self.task_selection = identity;
        self.source = files.retain();
        self.changes = observed.changes;
        self.selected = Some(task);
        self.advance_presentation();
        Ok(())
    }
    /// Follow only the UI session's bounded command outcomes; UI files stay separate.
    pub async fn follow_commands<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &super::CommandSession<P>,
    ) -> Result<()> {
        let (query, source) = files.view_source()?;
        let observed = source
            .observe_files(query.clone(), None, self.bounds.files, self.bounds.bytes)
            .await?;
        let snapshot = observed
            .snapshot
            .ok_or_else(|| Error::Conflict("command snapshot missing".into()))?;
        let identity = (snapshot.cursor.volume, snapshot.cursor.query);
        self.views
            .query_snapshot(query, snapshot, self.bounds.files, self.bounds.bytes)?;
        if let Some(previous) = self.command_selection {
            if previous != identity {
                self.views.remove_selection(&previous);
            }
        }
        self.command_selection = Some(identity);
        self.command_source = Some(source);
        self.commands = Some(observed.changes);
        self.advance_presentation();
        Ok(())
    }
    /// The exact fold exposed through the followed WASM handle too.
    pub fn views(&self) -> &ClientViews {
        &self.views
    }
    /// Ephemeral revision of accepted presentation files, excluding checkpoints.
    pub fn presentation_revision(&self) -> u64 {
        self.presentation_revision
    }
    fn advance_presentation(&mut self) {
        self.presentation_revision = self
            .presentation_revision
            .checked_add(1)
            .expect("presentation revision overflow");
    }
    /// Bind another authorized ordinary-file snapshot/follow to the same fold.
    pub fn views_mut(&mut self) -> &mut ClientViews {
        self.advance_presentation();
        &mut self.views
    }
    /// Apply a pushed ordinary-file publication with its original cursor.
    pub fn apply_change(&mut self, change: Change) -> Result<()> {
        let visible = !change.is_checkpoint();
        if self.views.query_change(change)? && visible {
            self.advance_presentation();
        }
        Ok(())
    }
    /// Apply one selected document delivery before committing its resume cut.
    pub fn apply_document_change(
        &mut self,
        documents: &mut super::DocumentFiles,
        update: super::DocumentUpdate,
    ) -> Result<()> {
        if documents.apply_change(update, &mut self.views)? {
            self.advance_presentation();
        }
        Ok(())
    }
    /// Select only push sources; consuming a source and folding it cannot be cancelled apart.
    pub async fn next_change(&mut self) -> Result<()> {
        let commands = &mut self.commands;
        let command = async {
            match commands {
                Some(changes) => changes.next().await,
                None => futures::future::pending().await,
            }
        }
        .fuse();
        let task = self.changes.next().fuse();
        futures::pin_mut!(command, task);
        let (command, change) = futures::select! { change = task => (false, change), change = command => (true, change) };
        match change {
            Some(change) => self.apply_change(change?),
            None => self.resume(command).await,
        }
    }

    async fn resume(&mut self, command: bool) -> Result<()> {
        let identity = if command {
            self.command_selection
                .ok_or_else(|| Error::Conflict("command selection missing".into()))?
        } else {
            self.task_selection
        };
        let selection = self
            .views
            .selection(&identity)
            .ok_or_else(|| Error::Conflict("follow selection missing".into()))?;
        let source = if command {
            self.command_source
                .as_ref()
                .ok_or_else(|| Error::Conflict("command source missing".into()))?
        } else {
            &self.source
        };
        let query = selection.query().clone();
        let cursor = selection.cursor().clone();
        let observed = source
            .observe_files(
                query.clone(),
                Some(cursor.clone()),
                self.bounds.files,
                self.bounds.bytes,
            )
            .await?;
        if observed.reset && observed.snapshot.is_none() {
            return Err(Error::Conflict("follow reset snapshot missing".into()));
        }
        // Install only a fully validated replacement. Cancellation before this
        // point leaves the old cut and reader intact for the next attempt.
        let mut changes = observed.changes;
        if let Some(snapshot) = observed.snapshot {
            let progress = snapshot.cursor != cursor;
            let next = (snapshot.cursor.volume, snapshot.cursor.query);
            self.views
                .query_snapshot(query, snapshot, self.bounds.files, self.bounds.bytes)?;
            if next != identity {
                self.views.remove_selection(&identity);
            }
            if command {
                self.command_selection = Some(next);
            } else {
                self.task_selection = next;
            }
            if progress {
                self.advance_presentation();
            } else {
                let change = changes.next().await.ok_or_else(|| {
                    Error::Conflict("resumed file follow ended without progress".into())
                })??;
                self.apply_change(change)?;
            }
        } else {
            let change = changes.next().await.ok_or_else(|| {
                Error::Conflict("resumed file follow ended without progress".into())
            })??;
            self.apply_change(change)?;
        }
        if command {
            self.commands = Some(changes);
        } else {
            self.changes = changes;
        }
        Ok(())
    }
}
#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;
