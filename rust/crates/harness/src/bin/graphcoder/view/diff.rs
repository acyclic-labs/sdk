//! The terminal renders one authorized query page and retains its continuation.
use super::{Conversation, Failure, Line};
use acyclic_harness::{
    client::{DiffFilePage, DiffKind},
    tui::display_text,
};
use acyclic_harness_client::AgentPageRequest;
use futures::future::LocalBoxFuture;
use std::sync::Arc;

pub(super) struct DiffPanel {
    pub(super) lines: Vec<Line<'static>>,
    pub(super) next: Option<String>,
    pub(super) continued: bool,
}

/// Keep only the visible page. The existing session owns the request's wait;
/// dropping this future on close or replacement cancels that consumer.
pub(super) fn query(
    conversation: Arc<Conversation>,
    rows: u32,
    cursor: Option<String>,
) -> LocalBoxFuture<'static, Result<DiffPanel, Failure>> {
    Box::pin(async move {
        let page = AgentPageRequest::new(rows, 64 * 1024, cursor.clone())?;
        let fragments = conversation.diff(page).await?;
        let next = fragments
            .last()
            .and_then(|fragment| fragment.next.as_ref())
            .map(serde_json::to_string)
            .transpose()?;
        let mut lines = Vec::new();
        if fragments.is_empty() {
            lines.push(Line::raw("No changes"));
        }
        for fragment in fragments {
            render(fragment, &mut lines);
        }
        if next.is_some() {
            lines.push(Line::raw("PgDn: next page · PgUp: first page · Esc: close"));
        } else {
            lines.push(Line::raw("PgUp: first page · Esc: close"));
        }
        Ok(DiffPanel {
            lines,
            next,
            continued: cursor.is_some(),
        })
    })
}

fn render(fragment: DiffFilePage, lines: &mut Vec<Line<'static>>) {
    if !fragment.continued {
        let kind = match fragment.kind {
            DiffKind::Added => "added",
            DiffKind::Removed => "removed",
            DiffKind::Modified => "modified",
        };
        let detail = if let Some(bytes) = fragment.omitted_bytes {
            format!("file too large to diff ({bytes} bytes)")
        } else if fragment.binary {
            "binary file".to_owned()
        } else {
            format!("+{} -{}", fragment.added, fragment.removed)
        };
        lines.push(Line::raw(format!(
            "{} ({kind}) {detail}",
            display_text(&fragment.path),
        )));
    }
    lines.extend(
        fragment
            .patch
            .lines()
            .map(|line| Line::raw(display_text(line).into_owned())),
    );
}
