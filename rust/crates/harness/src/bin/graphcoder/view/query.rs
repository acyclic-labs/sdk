//! Visible query selection over the connection's original authenticated pool.
use super::{Conversation, Failure, Line, diff};
use acyclic_harness::{client::AgentPageRequest, tui::QueryPage};
use std::{ops::RangeInclusive, sync::Arc};

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

    pub(super) fn restart(&self) -> Self {
        match self {
            Self::Diff(_) => Self::Diff(None),
            Self::History(_) => Self::History(None),
        }
    }

    pub(super) fn follows_tail(&self) -> bool {
        matches!(self, Self::Diff(None) | Self::History(None))
    }
}

pub(super) struct Panel {
    pub page: QueryPage<Source>,
    history: Option<RangeInclusive<u64>>,
}

impl Panel {
    /// Select by the immutable cut printed on screen, never by a moving row.
    pub(super) fn history_turn(&self, cut: u64) -> Option<u64> {
        if !self.history.as_ref()?.contains(&cut) {
            return None;
        }
        cut.checked_sub(1)
    }
}

pub(super) async fn load(
    conversation: Arc<Conversation>,
    rows: u32,
    source: Source,
) -> Result<Panel, Failure> {
    match source {
        Source::Diff(cursor) => Ok(Panel {
            page: diff::query(conversation, rows, cursor).await?,
            history: None,
        }),
        Source::History(cursor) => {
            let continued = cursor.is_some();
            let history = conversation
                .history_page(AgentPageRequest::new(rows, 64 * 1024, cursor)?)
                .await?;
            let mut lines = Vec::with_capacity(history.items.len() + 2);
            if history.items.is_empty() {
                lines.push(Line::raw("No settled history"));
            }
            for item in &history.items {
                lines.push(Line::raw(format!(
                    "{} · {} project volumes · context through {}",
                    item.conversation,
                    item.volumes.len(),
                    item.context.tail,
                )));
            }
            let selected = history
                .items
                .first()
                .zip(history.items.last())
                .map(|(newest, oldest)| oldest.conversation..=newest.conversation);
            lines.push(Line::raw(
                "Enter a displayed cut to select · PgDn: older · PgUp: newest · Esc: close",
            ));
            Ok(Panel {
                page: QueryPage::new(
                    lines,
                    history.next.map(|cursor| Source::History(Some(cursor))),
                    continued,
                ),
                history: selected,
            })
        }
    }
}
