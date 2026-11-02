//! One visible page on the selected conversation's authenticated connection.
use super::{Conversation, Failure, Line, diff};
use acyclic_harness::{client::AgentPageRequest, tui::QueryPage};
use std::sync::Arc;

#[derive(Clone)]
pub(super) enum Source {
    Diff(Option<String>),
    History(Option<String>),
}

impl Source {
    pub(super) fn title(&self) -> &'static str {
        match self {
            Self::Diff(_) => "Diff",
            Self::History(_) => "History",
        }
    }

    pub(super) fn newest(&self) -> Self {
        match self {
            Self::Diff(_) => Self::Diff(None),
            Self::History(_) => Self::History(None),
        }
    }

    pub(super) fn follows_tail(&self) -> bool {
        matches!(self, Self::Diff(None) | Self::History(None))
    }
}

pub(super) type Panel = QueryPage<Source>;

/// The existing session owns the wait. Closing or replacing the selection
/// drops that consumer; input and redraw keep the same pending future.
pub(super) async fn load(
    conversation: Arc<Conversation>,
    rows: u32,
    source: Source,
) -> Result<Panel, Failure> {
    match source {
        Source::Diff(cursor) => diff::query(conversation, rows, cursor).await,
        Source::History(cursor) => {
            let continued = cursor.is_some();
            let history = conversation
                .history_page(AgentPageRequest::new(rows, 64 * 1024, cursor)?)
                .await?;
            let mut lines = Vec::with_capacity(history.items.len() + 2);
            if history.items.is_empty() {
                lines.push(Line::raw("No settled history"));
            }
            for item in history.items {
                lines.push(Line::raw(format!(
                    "Cut {} · {} project volumes · context through {}",
                    item.conversation, item.volume_count, item.context_tail,
                )));
            }
            lines.push(Line::raw("PgDn: older · PgUp: newest · Esc: close"));
            Ok(Panel::new(
                lines,
                history.next.map(|cursor| Source::History(Some(cursor))),
                continued,
            ))
        }
    }
}
