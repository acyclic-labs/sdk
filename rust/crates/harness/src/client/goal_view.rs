use super::{ClientViews, GoalFileView};
use crate::{
    Error, OperationId, Result,
    task_volume::{Cursor, FileKey, FileValue, Query},
};
use serde::Serialize;

/// Read-only progress derived from the existing retained request head.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GoalProgress {
    /// Admitted or staged task occupying the ordinary request head.
    pub operation: OperationId,
    /// Original user turn for this goal loop.
    pub root: OperationId,
    /// Automatic continuations retained by that loop; edits never reset it.
    pub continuations: u32,
}

impl GoalProgress {
    pub(super) fn from_file(file: Option<&FileValue>) -> Option<Self> {
        let turn = crate::goal::request_head::decode(&file?.bytes).ok()?;
        Some(Self {
            operation: turn.operation,
            root: turn.root,
            continuations: turn.automatic_turns,
        })
    }
}

/// One goal rendering over two independent, accepted filesystem cuts.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GoalQueryView<'a> {
    /// Current GOAL.md publication cursor.
    pub cursor: &'a Cursor,
    /// Independent request-head cursor, absent until that source is observed.
    pub request_cursor: Option<&'a Cursor>,
    /// File-driven status, reason and body.
    pub goal: &'a GoalFileView,
    /// Retained counter; absent for a missing or invalid request head.
    pub progress: Option<&'a GoalProgress>,
    #[serde(skip)]
    pub(super) line_starts: &'a [usize],
}

impl GoalQueryView<'_> {
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
            .queries
            .get(&goal)
            .is_some_and(|view| view.query().matches(&FileKey::Goal))
            || !self
                .queries
                .get(&request)
                .is_some_and(|view| view.query() == &Query::File(FileKey::RequestHead))
        {
            return Err(Error::Invalid(
                "goal sources require observed goal and request-head queries".into(),
            ));
        }
        self.goal_sources.retain(|goal, request| {
            self.queries.contains_key(goal) && self.queries.contains_key(request)
        });
        self.goal_sources.insert(goal, request);
        Ok(())
    }

    /// Re-derives the rendering from current source cuts; arrival order adds no state.
    pub fn goal_view(&self, volume: [u8; 32], query: [u8; 32]) -> Option<GoalQueryView<'_>> {
        let identity = (volume, query);
        let file = self.queries.get(&identity)?;
        if !file.query().matches(&FileKey::Goal) {
            return None;
        }
        let request = self
            .goal_sources
            .get(&identity)
            .and_then(|key| self.queries.get(key));
        Some(GoalQueryView {
            cursor: file.cursor(),
            request_cursor: request.map(|view| view.cursor()),
            goal: file.goal(),
            progress: request.and_then(|view| view.progress()),
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
        serializer.collect_seq(
            self.0
                .queries
                .keys()
                .filter_map(|(volume, query)| self.0.goal_view(*volume, *query)),
        )
    }
}
