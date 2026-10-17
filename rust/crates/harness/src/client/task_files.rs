//! Disposable client views over the authorized task-volume snapshot and follow.
use super::{ClientSession, MemoryClientState, TaskFileViews};
use crate::{
    Error, Result,
    task_volume::{ChangeStream, FileQuerySource, Query},
    wire,
};
use futures::StreamExt as _;

/// File-backed client connection; cursors and payloads come from the volume query.
pub struct TaskFileClient {
    session: ClientSession<MemoryClientState>,
    changes: ChangeStream,
    command_projection: super::command_output::Projection,
    commands_dirty: bool,
    selected: Option<crate::TaskId>,
    bounds: (usize, usize),
    presentation_revision: u64,
}
impl TaskFileClient {
    /// Observe an authorized task selection at one atomic cut.
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
        let mut observation = files
            .observe_files(query.clone(), None, maximum_files, maximum_bytes)
            .await?;
        let snapshot = observation
            .snapshot
            .take()
            .ok_or_else(|| Error::Conflict("task snapshot missing".into()))?;
        let mut session = ClientSession::open(MemoryClientState::default())?;
        session.track(wire::ReplayCursor {
            authority: Some(owner.clone()),
            generation: "task-files".into(),
            revision: 0,
        })?;
        session.query_snapshot(query, snapshot, maximum_files, maximum_bytes)?;
        Ok(Self {
            session,
            changes: observation.changes,
            command_projection: super::command_output::Projection::default(),
            commands_dirty: false,
            selected: None,
            bounds: (maximum_files, maximum_bytes),
            presentation_revision: 0,
        })
    }
    /// Follow only the current task's bounded display tail, excluding inbox archives.
    pub async fn observe_task(
        files: &impl FileQuerySource,
        owner: &wire::Authority,
        task: crate::TaskId,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<Self> {
        let query = Query::TaskView {
            task: crate::task_volume::Component::new(task.to_string())?,
            recent: 128,
            reasoning: true,
            details: true,
            terminal: true,
        };
        let mut client =
            Self::observe_query(files, owner, query, maximum_files, maximum_bytes).await?;
        client.selected = Some(task);
        Ok(client)
    }

    /// Replace the disposable tail subscription when the followed session selects a task.
    pub async fn follow_task(
        &mut self,
        files: &impl FileQuerySource,
        owner: &wire::Authority,
        task: crate::TaskId,
    ) -> Result<()> {
        if self.selected != Some(task) {
            let replacement =
                Self::observe_task(files, owner, task, self.bounds.0, self.bounds.1).await?;
            self.session.replace_task_views(replacement.session, owner);
            self.changes = replacement.changes;
            self.selected = Some(task);
            self.commands_dirty = true;
            self.advance_presentation();
        }
        Ok(())
    }

    /// Presentation state reconstructed from the followed files.
    pub fn views(&self) -> &TaskFileViews {
        self.session.views()
    }
    /// Ephemeral revision of the accepted file/view fold, for presentation reuse.
    pub fn presentation_revision(&self) -> u64 {
        self.presentation_revision
    }

    fn advance_presentation(&mut self) {
        self.presentation_revision = self
            .presentation_revision
            .checked_add(1)
            .expect("presentation revision overflow");
    }

    /// Project this UI session's completed commands into its shared transcript.
    pub fn project_commands<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &super::CommandSession<P>,
        owner: &wire::Authority,
    ) -> Result<()> {
        let changed = self.command_projection.refresh(files, owner)?;
        if changed || self.commands_dirty {
            self.session
                .project_commands(&mut self.command_projection, files.active_task());
            self.commands_dirty = false;
            self.advance_presentation();
        }
        Ok(())
    }

    /// Apply a separate authorized document query.
    pub fn apply_update(&mut self, update: super::ViewUpdate) -> Result<()> {
        self.session.apply_update(update)?;
        self.advance_presentation();
        Ok(())
    }
    /// Read the next push publication; no local polling or execution host.
    pub async fn next_change(&mut self) -> Result<()> {
        let change = self
            .changes
            .next()
            .await
            .ok_or_else(|| Error::Conflict("task follow ended; reopen snapshot".into()))??;
        if self.session.query_change(change)? {
            self.commands_dirty = true;
            self.advance_presentation();
        }
        Ok(())
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
