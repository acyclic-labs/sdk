//! The terminal renders one authorized query page and retains its continuation.
use super::{Conversation, Failure, Line};
use acyclic_harness::{
    client::{AgentPageRequest, DiffFilePage, DiffKind},
    tui::{Editor, Layout, display_text},
};
use std::sync::Arc;

pub(super) struct DiffPanel {
    pub(super) lines: Vec<Line<'static>>,
    pub(super) next: Option<String>,
    pub(super) continued: bool,
    source: Vec<Line<'static>>,
    columns: u16,
}

impl DiffPanel {
    /// Reflow only this admitted query page, using the shared grapheme layout.
    /// The retained source and continuation survive a resize without another IO.
    pub(super) fn wrap(&mut self, columns: u16) {
        if self.columns == columns {
            return;
        }
        self.columns = columns;
        self.lines.clear();
        if columns == 0 {
            return;
        }
        for line in &self.source {
            if line.width() <= usize::from(columns)
                && !line.spans.iter().any(|span| span.content.contains('\t'))
            {
                self.lines.push(line.clone());
                continue;
            }
            let text = line.to_string();
            let layout = Layout::new(&Editor::new(&text), usize::from(columns));
            for row in layout.rows {
                // The editor reserves an insertion row after an exact-width
                // final row. A read-only page has no insertion cursor.
                if !text.is_empty() && row.cells.is_empty() {
                    continue;
                }
                self.lines.push(Line::raw(
                    row.cells
                        .into_iter()
                        .map(|cell| cell.symbol)
                        .collect::<String>(),
                ));
            }
        }
    }

    /// Reveal retained wrapped rows before requesting another source page.
    /// Returns true only when already at this page's existing boundary.
    pub(super) fn page_boundary(&self, top: &mut usize, rows: isize, height: u16) -> bool {
        let tail = self.lines.len().saturating_sub(usize::from(height));
        // Page-key effects include the outer border; never jump over content
        // that did not fit inside it.
        let step = rows.unsigned_abs().min(usize::from(height));
        let next = if rows < 0 {
            top.saturating_add(step).min(tail)
        } else {
            top.saturating_sub(step)
        };
        if next == *top {
            true
        } else {
            *top = next;
            false
        }
    }
}

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
        .transpose()?;
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
    Ok(DiffPanel {
        source: lines,
        lines: Vec::new(),
        columns: 0,
        next,
        continued,
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

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::tui::{
        PromptInput, Screen, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    };
    use std::collections::BTreeMap;

    #[test]
    fn wrapped_diff_pages_reveal_every_row_before_the_source_continuation() -> Result<(), Failure> {
        let mut source = Vec::new();
        render(
            DiffFilePage {
                publication: [3; 32],
                path: format!("{}/header-end", "long-directory".repeat(8)),
                kind: DiffKind::Modified,
                added: 1,
                removed: 0,
                binary: false,
                omitted_bytes: None,
                continued: false,
                patch: format!(
                    "+{}界e\u{301} unreadable-tail\n+\t\tshort-tab-tail\n\n",
                    "abcdef".repeat(60)
                ),
                next: None,
            },
            &mut source,
        );
        let expected = source
            .iter()
            .map(ToString::to_string)
            .collect::<String>()
            .replace([' ', '\t'], "");
        let mut panel = DiffPanel {
            source,
            lines: Vec::new(),
            columns: 0,
            next: Some("original-continuation".into()),
            continued: true,
        };
        let original_source = panel.source.as_ptr();
        let input = PromptInput::default();
        let mut viewport = Viewport::default();
        let mut detects_clipping = false;
        for width in [12, 20, 44] {
            let mut terminal = Terminal::new(TestBackend::new(width, 12))?;
            let mut areas = Screen::default().areas(terminal.get_frame().area(), &input);
            panel.wrap(areas.overlay_columns());
            let mut top = 0;
            let mut seen = BTreeMap::new();
            loop {
                // Inspect the completed render, not the backend's sparse
                // diff-write history beneath earlier wide glyphs.
                let painted = terminal.draw(|frame| {
                    areas = Screen {
                        overlay: &panel.lines,
                        overlay_title: "Diff",
                        ..Screen::default()
                    }
                    .render(frame, &input, &mut viewport, top, 0);
                })?;
                for row in 0..usize::from(areas.overlay_rows(0)) {
                    if top + row >= panel.lines.len() {
                        break;
                    }
                    let mut text = String::new();
                    let mut column = 0;
                    while column < areas.overlay_columns() {
                        let symbol = painted.buffer[(
                            areas.transcript.x + 1 + column,
                            areas.transcript.y + 1 + u16::try_from(row)?,
                        )]
                            .symbol();
                        text.push_str(symbol);
                        // A wide glyph covers the next buffer cell; that cell's
                        // previous symbol is not visible in the terminal.
                        column += u16::try_from(Line::raw(symbol).width().max(1))?;
                    }
                    seen.insert(top + row, text.replace(' ', ""));
                }
                if panel.page_boundary(
                    &mut top,
                    -isize::try_from(areas.transcript.height)?,
                    areas.overlay_rows(0),
                ) {
                    break;
                }
            }
            assert_eq!(seen.into_values().collect::<String>(), expected);
            assert_eq!(panel.next.as_deref(), Some("original-continuation"));
            assert_eq!(
                panel.source.as_ptr(),
                original_source,
                "resize retains the admitted source"
            );
            while top != 0 {
                assert!(!panel.page_boundary(
                    &mut top,
                    isize::try_from(areas.transcript.height)?,
                    areas.overlay_rows(0)
                ));
            }
            assert!(panel.page_boundary(&mut top, 1, areas.overlay_rows(0)));

            // The former single-row renderer loses the actual patch suffix.
            let unwrapped = terminal.draw(|frame| {
                Screen {
                    overlay: &panel.source,
                    ..Screen::default()
                }
                .render(frame, &input, &mut viewport, 0, 0);
            })?;
            detects_clipping |= !unwrapped
                .buffer
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>()
                .contains("unreadable-tail");
        }
        assert!(
            detects_clipping,
            "the unwrapped renderer must fail the same visibility requirement"
        );
        panel.wrap(0);
        assert!(panel.lines.is_empty());
        assert_eq!(panel.source.as_ptr(), original_source);
        Ok(())
    }
}
