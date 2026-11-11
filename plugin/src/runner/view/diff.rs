//! The terminal renders one authorized query page and retains its continuation.
use super::{Conversation, Failure, Line, query::Source};
use acyclic_harness::{
    client::{AgentPageRequest, DiffFilePage, DiffKind, DiffProjection, DiffQueryEvent},
    tui::display_text,
};
use std::sync::Arc;

pub(super) type Continuation =
    Arc<tokio::sync::Mutex<acyclic_harness::local::agent_conversation::WorkspaceDiff>>;

type DiffPanel = acyclic_harness::tui::QueryPage<Source>;

/// Keep only the visible page. The existing session owns the request's wait;
/// dropping this future on close or replacement cancels that consumer.
pub(super) async fn query(
    conversation: Arc<Conversation>,
    rows: u32,
    cursor: Option<Continuation>,
    agent: Option<acyclic_harness::agent::AgentPath>,
) -> Result<DiffPanel, Failure> {
    let continued = cursor.is_some();
    let cursor = match cursor {
        Some(cursor) => cursor,
        None => Arc::new(tokio::sync::Mutex::new(
            conversation
                .diff_query(
                    AgentPageRequest::new(rows, 64 * 1024, None)?,
                    DiffProjection::Patch,
                )
                .await?,
        )),
    };
    let mut reader = cursor.lock().await;
    let mut lines = Vec::new();
    let more = loop {
        match reader.next().await? {
            Some(DiffQueryEvent::Snapshot(_)) => {}
            Some(DiffQueryEvent::Progress) => {
                // This consumes real source progress. Yield to the key handler
                // even when the session already credited a run of filtered rows.
                tokio::task::yield_now().await;
            }
            Some(DiffQueryEvent::Page(fragments)) => {
                for fragment in &fragments {
                    render(fragment, &mut lines);
                }
                break true;
            }
            Some(DiffQueryEvent::Summary(_)) => {
                return Err("patch query returned summary data".into());
            }
            None => {
                lines.push(Line::raw(if continued {
                    "End of changes"
                } else {
                    "No changes"
                }));
                break false;
            }
        }
    };
    drop(reader);
    if more {
        lines.push(Line::raw("PgDn: more · PgUp: newest · Esc: close"));
    } else {
        lines.push(Line::raw("PgDn/PgUp: scroll · Esc: close"));
    }

    Ok(DiffPanel::new(
        lines,
        more.then(|| match agent {
            Some(agent) => Source::AgentDiff(agent, Some(cursor)),
            None => Source::Diff(Some(cursor)),
        }),
        continued,
    ))
}

fn render(fragment: &DiffFilePage, lines: &mut Vec<Line<'static>>) {
    if !fragment.continued {
        let kind = match fragment.kind {
            DiffKind::Added => "added",
            DiffKind::Removed => "removed",
            DiffKind::Modified => "modified",
        };
        let detail = if let Some(bytes) = fragment.omitted_bytes {
            format!("content omitted ({bytes} bytes)")
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
