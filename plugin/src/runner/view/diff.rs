//! The terminal renders one authorized query page and retains its continuation.
use super::{
    Conversation, Failure,
    query::{Context, Source},
};
use acyclic_harness::{
    client::{AgentPageRequest, DiffFilePage, DiffKind, DiffProjection, DiffQueryEvent},
    tui::{DiffPage, QueryPage, display_text},
};
use std::sync::Arc;

pub(super) type Continuation =
    Arc<tokio::sync::Mutex<acyclic_harness::local::agent_conversation::WorkspaceDiff>>;

type DiffPanel = super::query::Panel;

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
    let mut presentation = DiffPage::default();
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
                    presentation.file(header(fragment), &fragment.patch);
                }
                break true;
            }
            Some(DiffQueryEvent::Summary(_)) => {
                return Err("patch query returned summary data".into());
            }
            None => {
                presentation.hint(if continued {
                    "End of changes"
                } else {
                    "No changes"
                });
                break false;
            }
        }
    };
    drop(reader);
    if more {
        presentation.hint("PgDn: more · PgUp: newest · Tab: hunk · Space: fold · Esc: close");
    } else {
        presentation.hint("PgDn/PgUp: scroll · Esc: close");
    }

    let mut context = Context::default();
    context.diff = Some(presentation);
    Ok(QueryPage::new(
        Vec::new(),
        more.then(|| match agent {
            Some(agent) => Source::AgentDiff(agent, Some(cursor)),
            None => Source::Diff(Some(cursor)),
        }),
        continued,
    )
    .with_context(context))
}

fn header(fragment: &DiffFilePage) -> Option<String> {
    (!fragment.continued).then(|| {
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
        format!("{} ({kind}) {detail}", display_text(&fragment.path),)
    })
}
