//! File-backed agent tree: followed status files in, `layout.json` selection out.
use super::{
    Action, AgentState, AgentStatus, AgentTree, AppAction, InputEffect,
    config::ThemeStyles,
    ratatui::{Frame, layout::Rect},
};
use crate::{
    Error, Result, TaskId,
    client::{ClientViews, CommandFileRoot, CommandSession},
    task_volume::{FileKey, Query, TaskFile, TaskState, TaskStatus},
};
use acyclic_stream::StreamProvider;

/// Disposable panel state. The status files and the session layout stay
/// authoritative; the tree is rebuilt only when the status query advances.
#[derive(Default)]
pub struct AgentPanel {
    tree: AgentTree,
    cut: Option<([u8; 32], u64)>,
    selected: Option<String>,
    open: bool,
    #[cfg(test)]
    builds: usize,
}

impl AgentPanel {
    /// Follow the shared task-overview fold and the session layout. Frames at
    /// an unchanged status cut do no tree work.
    pub fn update<P: StreamProvider + ?Sized>(
        &mut self,
        views: &ClientViews,
        files: &CommandSession<P>,
    ) -> Result<()> {
        // Layout rule: an open view (help, feedback, history, plan or any
        // later overlay) owns the whole transcript area. The tree is a side
        // panel, drawn only beside the transcript, never over an overlay.
        self.open = files
            .state(CommandFileRoot::Session, "layout.json")?
            .is_some_and(|layout| layout["tree"] == true && layout["open_view"].is_null());
        self.selected = files.selected_task()?.map(|task| task.to_string());
        let view = views
            .selections()
            .map(|(_, view)| view)
            .find(|view| matches!(view.query(), Query::TaskOverviews));
        let cut = view.map(|view| (view.cursor().volume, view.revision()));
        if cut == self.cut {
            return Ok(());
        }
        let mut agents = Vec::new();
        for file in view.into_iter().flat_map(|view| view.files().values()) {
            let FileKey::Task {
                task,
                file: TaskFile::Status,
            } = &file.key
            else {
                continue;
            };
            let id = TaskId::parse(task.as_str())?;
            let bytes = match file.segments.as_slice() {
                [bytes] => std::borrow::Cow::Borrowed(&bytes[..]),
                segments => std::borrow::Cow::Owned(
                    segments
                        .iter()
                        .flat_map(|segment| segment.iter().copied())
                        .collect(),
                ),
            };
            let status = TaskStatus::parse(&bytes)
                .map_err(|error| Error::Invalid(format!("task status: {error}")))?;
            if status.task != id {
                return Err(Error::Invalid(format!(
                    "task status for {} under {id}",
                    status.task
                )));
            }
            let state = match status.state {
                TaskState::Pending => AgentState::Busy,
                TaskState::Completed => AgentState::Idle,
                TaskState::Cancelled => AgentState::Stopped,
                TaskState::Failed | TaskState::Indeterminate => AgentState::Failed,
                TaskState::Deleted => continue,
            };
            agents.push((
                id.to_string(),
                AgentStatus {
                    parent: status.parent.map(|parent| parent.to_string()),
                    state,
                    attention: status.attention,
                },
            ));
        }
        self.tree.replace(agents);
        self.cut = cut;
        #[cfg(test)]
        {
            self.builds += 1;
        }
        Ok(())
    }

    /// Whether the followed layout shows the tree.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Paint the open tree over the right of the transcript; only visible rows
    /// are visited.
    pub fn render(&self, frame: &mut Frame<'_>, transcript: Rect, theme: &ThemeStyles) {
        if !self.open || transcript.width < 8 || transcript.height < 3 {
            return;
        }
        let width = (transcript.width / 3).clamp(24, 48).min(transcript.width);
        let area = Rect::new(
            transcript.right() - width,
            transcript.y,
            width,
            transcript.height,
        );
        frame.render_widget(self.tree.view(self.selected.as_deref(), theme), area);
    }

    /// Route one input effect as `view::run` does: structural navigation is
    /// handled here and everything else takes the ordinary file input path.
    /// Both report through the one input error policy, so a navigation write
    /// that loses the layout race to a peer becomes feedback, not an exit.
    #[cfg(feature = "tui")]
    pub async fn dispatch<P: StreamProvider + ?Sized>(
        &self,
        files: &mut CommandSession<P>,
        reader: &dyn crate::conversation::ContentResidencyVerifier,
        effect: InputEffect,
        page_rows: usize,
        maximum_scroll: usize,
    ) -> Result<()> {
        match self.input(files, effect).await {
            Ok(Some(effect)) => {
                crate::client::file_tui_input(files, reader, effect, page_rows, maximum_scroll)
                    .await
            }
            outcome => crate::client::report_tui_input(files, outcome.map(drop)).await,
        }
    }

    /// Consume structural navigation by selecting the target task in the
    /// session layout. Navigation without a target leaves the selection; every
    /// other effect is returned for ordinary input handling. Callers route
    /// input through [`Self::dispatch`], which applies the input error policy.
    pub async fn input<P: StreamProvider + ?Sized>(
        &self,
        files: &mut CommandSession<P>,
        effect: InputEffect,
    ) -> Result<Option<InputEffect>> {
        let InputEffect::Host(Action::App(action)) = &effect else {
            return Ok(Some(effect));
        };
        if !matches!(
            action,
            AppAction::Parent
                | AppAction::Child
                | AppAction::SiblingNext
                | AppAction::SiblingPrevious
        ) {
            return Ok(Some(effect));
        }
        let Some(current) = files.selected_task()?.map(|task| task.to_string()) else {
            return Ok(None);
        };
        let target = match action {
            AppAction::Parent => self.tree.parent(&current),
            AppAction::Child => self.tree.child(&current),
            AppAction::SiblingNext => self.tree.sibling(&current, true),
            _ => self.tree.sibling(&current, false),
        };
        if let Some(target) = target {
            let mut layout = files
                .state(CommandFileRoot::Session, "layout.json")?
                .unwrap_or_else(|| serde_json::json!({}));
            layout["selected_task"] = serde_json::json!(target);
            files
                .write_state(CommandFileRoot::Session, "layout.json", &layout)
                .await?;
        }
        Ok(None)
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
