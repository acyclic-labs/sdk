use super::{ClientViews, GoalFileView};
use crate::{
    Error, OperationId, Result,
    task_volume::{Cursor, FileKey, FileValue, Query},
};
use serde::Serialize;

/// Read-only progress derived from the existing retained request head.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct GoalProgress {
    /// Admitted or staged task occupying the ordinary request head.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation: OperationId,
    /// Original user turn for this goal loop.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub root: OperationId,
    /// Automatic continuations retained by that loop; edits never reset it.
    pub continuations: u32,
    /// GOAL.md write publication acknowledged by this retained request head.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub goal_revision: Option<OperationId>,
}

impl GoalProgress {
    pub(super) fn from_file(file: Option<&FileValue>) -> Option<Self> {
        let turn = crate::goal::request_head::decode(&file?.bytes).ok()?;
        Some(Self {
            operation: turn.operation,
            root: turn.root,
            continuations: turn.automatic_turns,
            goal_revision: turn.goal_revision,
        })
    }
}

/// One goal rendering over two independent, accepted filesystem cuts.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct GoalQueryView<'a> {
    /// Current GOAL.md publication cursor.
    pub cursor: &'a Cursor,
    /// Independent request-head cursor, absent until that source is observed.
    pub request_cursor: Option<&'a Cursor>,
    /// File-driven status, reason and body.
    pub goal: &'a GoalFileView,
    /// Retained counter; pending for blocked goals until the head acknowledges
    /// their exact write publication, or absent for a missing/invalid head.
    pub progress: Option<&'a GoalProgress>,
    #[serde(skip)]
    #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
    pub(super) line_starts: &'a [usize],
}

impl GoalQueryView<'_> {
    #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
    pub(super) fn lines(&self) -> impl Iterator<Item = &str> {
        let markdown = match self.goal {
            GoalFileView::Present { goal } => goal.markdown.as_str(),
            _ => "",
        };
        self.line_starts
            .iter()
            .enumerate()
            .map(move |(index, start)| {
                let end = self
                    .line_starts
                    .get(index + 1)
                    .copied()
                    .unwrap_or(markdown.len());
                let line = markdown.get(*start..end).unwrap_or_default();
                match line.strip_suffix('\n') {
                    Some(line) => line.strip_suffix('\r').unwrap_or(line),
                    None => line,
                }
            })
    }
}

impl ClientViews {
    /// Associates existing volume-scoped follows. It creates no publication or store.
    pub fn bind_goal_sources(
        &mut self,
        goal: ([u8; 32], [u8; 32]),
        request: ([u8; 32], [u8; 32]),
    ) -> Result<()> {
        if !self
            .selection(&goal)
            .is_some_and(|view| view.query().matches(&FileKey::Goal))
            || !self
                .selection(&request)
                .is_some_and(|view| view.query() == &Query::File(FileKey::RequestHead))
        {
            return Err(Error::Invalid(
                "goal sources require observed goal and request-head queries".into(),
            ));
        }
        self.goal_sources.insert(goal, request);
        Ok(())
    }

    /// Re-derives the rendering from current source cuts; arrival order adds no state.
    pub fn goal_view(&self, volume: [u8; 32], query: [u8; 32]) -> Option<GoalQueryView<'_>> {
        let identity = (volume, query);
        self.goal_selection(&identity, self.selection(&identity)?)
    }

    pub(super) fn goal_selection<'a>(
        &'a self,
        identity: &([u8; 32], [u8; 32]),
        file: &'a super::FileQueryView,
    ) -> Option<GoalQueryView<'a>> {
        if !file.query().matches(&FileKey::Goal) {
            return None;
        }
        let request = self
            .goal_sources
            .get(identity)
            .and_then(|key| self.selection(key))
            .or_else(|| file.query().matches(&FileKey::RequestHead).then_some(file));
        Some(GoalQueryView {
            cursor: file.cursor(),
            request_cursor: request.map(|view| view.cursor()),
            goal: file.goal(),
            progress: request.and_then(|view| view.progress()).filter(|progress| {
                !matches!(file.goal(), GoalFileView::Present { goal }
                    if goal.status == crate::goal::GoalStatus::Blocked)
                    || file
                        .goal_revision()
                        .is_some_and(|revision| progress.goal_revision == Some(revision))
            }),
            #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
            line_starts: file.goal_line_starts(),
        })
    }
}

pub(super) struct Goals<'a>(pub &'a ClientViews);

impl Serialize for Goals<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.goals())
    }
}
