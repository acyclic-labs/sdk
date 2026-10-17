//! Transcript navigation is an ordinary followed layout-file change.
use super::{CommandFileRoot, CommandSession};
use crate::{Error, Result};
use acyclic_stream::StreamProvider;

fn modal(layout: &serde_json::Value) -> bool {
    layout.get("open_view").and_then(serde_json::Value::as_str) == Some("shortcuts")
}

/// Rows retained above the followed view tail. Transcript starts at its tail;
/// modal help starts at its first row and retains a separate navigation offset.
pub fn tui_scroll<P: StreamProvider + ?Sized>(files: &CommandSession<P>) -> Result<usize> {
    let layout = files.state(CommandFileRoot::Session, "layout.json")?;
    let is_modal = layout.as_ref().is_some_and(modal);
    let key = if is_modal {
        "view_back"
    } else {
        "transcript_back"
    };
    let Some(value) = layout.as_ref().and_then(|layout| layout.get(key)) else {
        return Ok(if is_modal { usize::MAX } else { 0 });
    };
    let rows = value
        .as_u64()
        .ok_or_else(|| Error::Invalid("scroll offset must be a row count".into()))?;
    usize::try_from(rows).map_err(|error| Error::Invalid(error.to_string()))
}

/// Publish a page movement bounded by the actual rendered transcript viewport.
pub async fn scroll_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    up: bool,
    page_rows: usize,
    maximum: usize,
) -> Result<()> {
    let back = tui_scroll(files)?;
    let current = back.min(maximum);
    let next = if up {
        current.saturating_add(page_rows).min(maximum)
    } else {
        current.saturating_sub(page_rows)
    };
    if next != back {
        let mut layout = files
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or_else(|| serde_json::json!({}));
        let key = if modal(&layout) {
            "view_back"
        } else {
            "transcript_back"
        };
        layout[key] = serde_json::json!(next);
        files
            .write_state(CommandFileRoot::Session, "layout.json", &layout)
            .await?;
    }
    Ok(())
}
