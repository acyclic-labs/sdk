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

/// Rows an input effect moves the focused view toward older output: a page is
/// the rendered transcript viewport, a wheel notch its fixed row count.
pub fn transcript_motion(effect: &crate::tui::InputEffect, page_rows: usize) -> Option<isize> {
    use crate::tui::{Action, InputEffect};
    let page = isize::try_from(page_rows).unwrap_or(isize::MAX);
    match effect {
        InputEffect::Host(Action::TranscriptUp) => Some(page),
        InputEffect::Host(Action::TranscriptDown) => Some(-page),
        InputEffect::Scroll(rows) => Some(*rows),
        _ => None,
    }
}

/// One movement applied against the bounds of the rendered viewport.
fn scroll_step(current: usize, rows: isize, maximum: usize) -> usize {
    if rows >= 0 {
        current.saturating_add(rows.unsigned_abs()).min(maximum)
    } else {
        current.saturating_sub(rows.unsigned_abs())
    }
}

/// Publish movements bounded by the actual rendered transcript viewport,
/// each folded against the bounds in order, as one layout change. A burst
/// that changes nothing appends nothing.
pub async fn scroll_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    motions: impl IntoIterator<Item = isize>,
    maximum: usize,
) -> Result<()> {
    let back = tui_scroll(files)?;
    let next = motions
        .into_iter()
        .fold(back.min(maximum), |current, rows| {
            scroll_step(current, rows, maximum)
        });
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

/// Effects for the next decoded input event plus the view motions decoded
/// in the same read behind it. A wheel flick or held `PageUp` arrives as one
/// input batch; its motions publish as one layout change and draw one frame.
/// Draining stops after the first event that is neither a motion nor an ignored
/// pointer. Events reading a presented viewport stay queued at the frame boundary.
/// Later events stay queued, and the next read is
/// never touched, so read boundaries stay frame boundaries.
pub fn input_burst<I: IntoIterator<Item = crate::tui::InputEffect>>(
    batch: &mut std::collections::VecDeque<crate::tui::key_event::InputEvent>,
    page_rows: usize,
    effects: &mut Vec<crate::tui::InputEffect>,
    mut route: impl FnMut(crate::tui::key_event::InputEvent) -> I,
) {
    effects.clear();
    while let Some(event) = batch.pop_front() {
        let ignored_pointer = matches!(
            &event,
            crate::tui::key_event::InputEvent::Mouse(mouse)
                if !crate::tui::PromptInput::reads_presented_viewport(mouse)
        );
        let first = effects.len();
        effects.extend(route(event));
        let foldable = (effects.len() > first || ignored_pointer)
            && effects[first..]
                .iter()
                .all(|effect| transcript_motion(effect, page_rows).is_some());
        if !foldable || frame_due(batch) {
            break;
        }
    }
}

/// Whether the frame must reach the terminal before the batch's next event is
/// routed. Only left-button down/drag can hit-test the presented prompt viewport;
/// all other events route against recomputed layout without presenting. A read
/// presents once at its end, plus a boundary before each queued left down/drag.
pub fn frame_due(batch: &std::collections::VecDeque<crate::tui::key_event::InputEvent>) -> bool {
    use crate::tui::{PromptInput, key_event::InputEvent};
    match batch.front() {
        None => true,
        Some(InputEvent::Mouse(mouse)) => PromptInput::reads_presented_viewport(mouse),
        Some(_) => false,
    }
}
