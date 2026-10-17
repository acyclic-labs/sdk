//! Native task and UI files use the same disposable ClientViews as WASM.
use super::{ClientViewLimits, ClientViews, QueryViewLimits};
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
    changes: ChangeStream,
    commands: Option<ChangeStream>,
    task_selection: ([u8; 32], [u8; 32]),
    selected: Option<crate::TaskId>,
    bounds: (usize, usize),
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
            changes: observed.changes,
            commands: None,
            task_selection,
            selected: None,
            bounds: (maximum_files, maximum_bytes),
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
        _owner: &wire::Authority,
        task: crate::TaskId,
    ) -> Result<()> {
        if self.selected == Some(task) {
            return Ok(());
        }
        let query = Self::task_query(task)?;
        let observed = files
            .observe_files(query.clone(), None, self.bounds.0, self.bounds.1)
            .await?;
        let snapshot = observed
            .snapshot
            .ok_or_else(|| Error::Conflict("task snapshot missing".into()))?;
        let identity = (snapshot.cursor.volume, snapshot.cursor.query);
        self.views
            .query_snapshot(query, snapshot, self.bounds.0, self.bounds.1)?;
        if self.task_selection != identity {
            self.views.remove_selection(&self.task_selection);
        }
        self.task_selection = identity;
        self.changes = observed.changes;
        self.selected = Some(task);
        Ok(())
    }
    /// Follow the existing UI session's files, including its retained outcomes.
    pub async fn follow_commands<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &super::CommandSession<P>,
    ) -> Result<()> {
        let (query, observed) = files.observe_view().await?;
        let snapshot = observed
            .snapshot
            .ok_or_else(|| Error::Conflict("command snapshot missing".into()))?;
        let (count, bytes) = files.view_bounds();
        self.views.query_snapshot(query, snapshot, count, bytes)?;
        self.commands = Some(observed.changes);
        Ok(())
    }
    /// The exact fold exposed through the followed WASM handle too.
    pub fn views(&self) -> &ClientViews {
        &self.views
    }
    /// Bind another authorized ordinary-file snapshot/follow to the same fold.
    pub fn views_mut(&mut self) -> &mut ClientViews {
        &mut self.views
    }
    /// Apply a pushed ordinary-file publication with its original cursor.
    pub fn apply_change(&mut self, change: Change) -> Result<()> {
        self.views.query_change(change).map(|_| ())
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
        let change = futures::select! { change = task => change, change = command => change };
        let change = change
            .ok_or_else(|| Error::Conflict("file follow ended; reopen snapshot".into()))??;
        self.views.query_change(change)?;
        Ok(())
    }
}
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
