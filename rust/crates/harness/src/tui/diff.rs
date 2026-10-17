//! Presentation of the captured-volume query's bounded unified patches.
use super::ratatui::text::Line;
use crate::client::{DiffKind, FileDiff};
use std::collections::BTreeMap;

/// Render the existing diff model, preserving binary and empty-file changes.
/// Input bounds and exact-path validation belong to DiffView's query fold.
pub fn diff_lines(files: &BTreeMap<String, FileDiff>) -> Vec<Line<'static>> {
    let mut rows = Vec::new();
    for (path, file) in files {
        let kind = match file.kind {
            DiffKind::Added => "added",
            DiffKind::Removed => "removed",
            DiffKind::Modified => "modified",
        };
        rows.push(Line::raw(format!(
            "{} ({kind}) +{} -{}",
            path.escape_debug(),
            file.added,
            file.removed
        )));
        if file.binary {
            rows.push(Line::raw("Binary file changed"));
        } else {
            rows.extend(
                file.patch
                    .lines()
                    .map(|line| Line::raw(super::display_text(line).into_owned())),
            );
        }
    }
    rows
}
