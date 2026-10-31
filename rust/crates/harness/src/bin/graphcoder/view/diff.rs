//! The terminal renders one authorized query page and retains its continuation.
use super::{Conversation, Failure, Line};
use acyclic_harness::{
    client::{AgentPageRequest, DiffFilePage, DiffKind},
    tui::display_text,
};
use std::sync::Arc;

pub(super) type DiffPanel = acyclic_harness::tui::QueryPage<super::query::Source>;

/// Keep only the visible page. The existing session owns the request's wait;
/// dropping this future on close or replacement cancels that consumer.
pub(super) async fn query(
    conversation: Arc<Conversation>,
    rows: u32,
    cursor: Option<String>,
) -> Result<DiffPanel, Failure> {
    let continued = cursor.is_some();
    let page = AgentPageRequest::new(rows, 64 * 1024, cursor)?;
    let fragments = conversation.diff(page).await?;
    let next = fragments
        .last()
        .and_then(|fragment| fragment.next.as_ref())
        .map(serde_json::to_string)
        .transpose()?
        .map(|cursor| super::query::Source::Diff(Some(cursor)));
    let mut lines = Vec::new();
    if fragments.is_empty() {
        lines.push(Line::raw("No changes"));
    }
    for fragment in fragments {
        render(fragment, &mut lines);
    }
    if next.is_some() {
        lines.push(Line::raw("PgDn: more · PgUp: back · Esc: close"));
    } else {
        lines.push(Line::raw("PgDn/PgUp: scroll · Esc: close"));
    }
    Ok(DiffPanel::new(lines, next, continued))
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
