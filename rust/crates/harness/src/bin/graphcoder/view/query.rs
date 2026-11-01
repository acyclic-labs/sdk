//! Visible query selection over the connection's original authenticated pool.
use super::{Conversation, Failure, Line, diff};
use acyclic_harness::{agent::IntentId, interaction::ApprovalBinding};
use acyclic_harness::{
    client::AgentPageRequest,
    tui::{QueryPage, display_text, display_text_prefix},
};
use std::{ops::RangeInclusive, sync::Arc};

/// Keep the original query owned by its pending future across input and draws.
pub(super) async fn usage(
    mut query: acyclic_harness::local::agent_conversation::UsageQuery,
) -> (
    acyclic_harness::local::agent_conversation::UsageQuery,
    acyclic_harness::Result<bool>,
) {
    let result = query.advance().await;
    (query, result)
}

#[derive(Clone)]
pub(super) enum Source {
    Diff(Option<String>),
    History(Option<String>),
    Approval {
        request: IntentId,
        binding: ApprovalBinding,
        status: Option<u64>,
        offset: u64,
    },
}

impl Source {
    pub(super) fn title(&self) -> &'static str {
        match self {
            Self::Diff(_) => "Diff",
            Self::History(_) => "History",
            Self::Approval { .. } => "Approval request",
        }
    }

    pub(super) fn restart(&self) -> Self {
        match self {
            Self::Diff(_) => Self::Diff(None),
            Self::History(_) => Self::History(None),
            Self::Approval {
                request,
                binding,
                status,
                ..
            } => Self::Approval {
                request: request.clone(),
                binding: binding.clone(),
                status: *status,
                offset: 0,
            },
        }
    }

    pub(super) fn follows_tail(&self) -> bool {
        matches!(self, Self::Diff(None) | Self::History(None))
    }
}

pub(super) struct Panel {
    pub page: QueryPage<Source>,
    pub approval_status: Option<u64>,
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
            approval_status: None,
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
                    item.conversation, item.volume_count, item.context_tail,
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
                approval_status: None,
                page: QueryPage::new(
                    lines,
                    history.next.map(|cursor| Source::History(Some(cursor))),
                    continued,
                ),
                history: selected,
            })
        }
        Source::Approval {
            request,
            binding,
            status,
            offset,
        } => {
            let status = match status {
                Some(status) => status,
                None => {
                    let pending = conversation
                        .pending_approval(&request)
                        .await?
                        .filter(|pending| pending.binding == binding)
                        .ok_or("The selected approval is no longer pending.")?;
                    pending.status
                }
            };
            let (bytes, length) = conversation.approval_request(status, offset).await?;
            let range_end = offset
                .checked_add(bytes.len() as u64)
                .ok_or("Approval range overflows.")?;
            // A byte page can end inside a character. Leave that suffix at the
            // original offset for the next range; never replace or lose bytes.
            let text = display_text_prefix(&bytes, range_end == length)?;
            let end = offset
                .checked_add(text.len() as u64)
                .ok_or("Approval range overflows.")?;
            if end == offset && end < length {
                return Err("Approval range made no progress.".into());
            }
            let mut lines = Vec::new();
            lines.push(Line::raw(format!(
                "Request {} · bytes {offset}–{end} of {length}",
                display_text(request.as_str())
            )));
            lines.extend(
                text.lines()
                    .map(|line| Line::raw(display_text(line).into_owned())),
            );
            lines.push(Line::raw(
                "PgDn: more · PgUp: start · y: approve · n: decline · Esc: close",
            ));
            Ok(Panel {
                approval_status: Some(status),
                page: QueryPage::new(
                    lines,
                    (end < length).then_some(Source::Approval {
                        request,
                        binding,
                        status: Some(status),
                        offset: end,
                    }),
                    offset != 0,
                ),
                history: None,
            })
        }
    }
}
