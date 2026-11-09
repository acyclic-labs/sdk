//! Layout of one admitted live-query page. No I/O, source discovery or runtime
//! logic lives here: callers supply rows and retain their source-owned cursor.
use crate::Layout;
use ratatui::text::Line;

/// Presentation shared by paged file views. Retained work is proportional to
/// one admitted page; resize only reflows that page and never reads its source.
pub struct QueryPage<C> {
    /// Wrapped visible-page rows; no other source pages are retained.
    pub lines: Vec<Line<'static>>,
    /// Opaque continuation supplied by the source query owner.
    pub next: Option<C>,
    /// Whether this page was opened from a source continuation.
    pub continued: bool,
    source: Vec<Line<'static>>,
    columns: u16,
}

impl<C> QueryPage<C> {
    /// Admit one source page. The caller owns its query, follow and file actions;
    /// this value only lays out the returned rows and retains the opaque cursor.
    pub fn new(source: Vec<Line<'static>>, next: Option<C>, continued: bool) -> Self {
        Self {
            source,
            lines: Vec::new(),
            columns: 0,
            next,
            continued,
        }
    }

    /// Reflow only this admitted query page, using the shared grapheme layout.
    /// The retained source and continuation survive a resize without another IO.
    pub fn wrap(&mut self, columns: u16) {
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
            let layout = Layout::from_text(&text, usize::from(columns));
            let mut end = 0;
            let mut style_at = crate::styled_cells::ordered_styles(line.spans.iter().map(|span| {
                let start = end;
                end += span.content.len();
                (start..end, span.style)
            }));
            for row in layout.rows {
                // The editor reserves an insertion row after an exact-width
                // final row. A read-only page has no insertion cursor.
                if !text.is_empty() && row.cells.is_empty() {
                    continue;
                }
                let mut spans = Vec::new();
                crate::styled_cells::append(
                    &mut spans,
                    row.cells
                        .into_iter()
                        .map(|cell| (cell.source.start, cell.symbol)),
                    &mut style_at,
                );
                let mut wrapped = Line::from(spans);
                wrapped.style = line.style;
                wrapped.alignment = line.alignment;
                self.lines.push(wrapped);
            }
        }
    }

    /// Reveal retained wrapped rows before requesting another source page.
    /// Returns true only when already at this page's existing boundary.
    pub fn page_boundary(&self, top: &mut usize, rows: isize, height: u16) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        PromptInput, Screen, Viewport,
        ratatui::{
            Terminal,
            backend::TestBackend,
            style::{Color, Modifier, Style},
            text::Span,
        },
    };
    use std::collections::BTreeMap;
    type Failure = Box<dyn std::error::Error>;

    #[test]
    fn wrapped_query_pages_reveal_every_row_before_the_source_continuation() -> Result<(), Failure>
    {
        let source = vec![
            Line::raw(format!("{}/header-end", "long-directory".repeat(8))),
            Line::raw(format!(
                "+{}界e\u{301} unreadable-tail",
                "abcdef".repeat(60)
            )),
            Line::raw("+\t\tshort-tab-tail"),
            Line::from(vec![
                Span::styled("Ω".repeat(50), Style::default().fg(Color::Yellow)),
                Span::raw(""),
                Span::styled(
                    "Ж\t".repeat(10),
                    Style::default()
                        .fg(Color::Cyan)
                        .bg(Color::Blue)
                        .add_modifier(Modifier::BOLD),
                ),
            ])
            .style(Style::default().bg(Color::DarkGray)),
            Line::raw(""),
        ];
        let expected = source
            .iter()
            .map(ToString::to_string)
            .collect::<String>()
            .replace([' ', '\t'], "");
        let mut panel = QueryPage::<String> {
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
            let mut styled = false;
            loop {
                // Inspect the completed render, not the backend's sparse
                // diff-write history beneath earlier wide glyphs.
                let painted = terminal.draw(|frame| {
                    areas = Screen {
                        overlay: &panel.lines,
                        overlay_title: "Query",
                        overlay_scroll: top,
                        ..Screen::default()
                    }
                    .render(frame, &input, &mut viewport, 0, 0);
                })?;
                for row in 0..usize::from(areas.overlay_rows(0)) {
                    if top + row >= panel.lines.len() {
                        break;
                    }
                    let mut text = String::new();
                    let mut column = 0;
                    while column < areas.overlay_columns() {
                        let cell = &painted.buffer[(
                            areas.overlay.x + 1 + column,
                            areas.overlay.y + 1 + u16::try_from(row)?,
                        )];
                        let symbol = cell.symbol();
                        match symbol {
                            "Ω" => {
                                assert_eq!(cell.fg, Color::Yellow);
                                assert_eq!(cell.bg, Color::DarkGray);
                                styled = true;
                            }
                            "Ж" => {
                                assert_eq!(cell.fg, Color::Cyan);
                                assert_eq!(cell.bg, Color::Blue);
                                assert!(cell.modifier.contains(Modifier::BOLD));
                            }
                            _ => {}
                        }
                        text.push_str(symbol);
                        // A wide glyph covers the next buffer cell; that cell's
                        // previous symbol is not visible in the terminal.
                        column += u16::try_from(Line::raw(symbol).width().max(1))?;
                    }
                    seen.insert(top + row, text.replace(' ', ""));
                }
                if panel.page_boundary(
                    &mut top,
                    -isize::try_from(areas.overlay.height)?,
                    areas.overlay_rows(0),
                ) {
                    break;
                }
            }
            assert_eq!(seen.into_values().collect::<String>(), expected);
            assert!(styled, "the paged paint must reach the styled source");
            assert_eq!(panel.next.as_deref(), Some("original-continuation"));
            assert_eq!(
                panel.source.as_ptr(),
                original_source,
                "resize retains the admitted source"
            );
            while top != 0 {
                assert!(!panel.page_boundary(
                    &mut top,
                    isize::try_from(areas.overlay.height)?,
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

        // Both projections use the same source-style walk. Unstyled gaps
        // must reset emphasis, including after a wrapped row or a resize.
        let bold = Style::default().add_modifier(Modifier::BOLD);
        let mut query = QueryPage::<()>::new(
            vec![Line::from(vec![
                Span::styled("bold", bold),
                Span::raw(" gap "),
                Span::styled("again", bold),
                Span::raw(" end"),
            ])],
            None,
            false,
        );
        let mut transcript = crate::TranscriptRows::default();
        transcript.push(false, "**bold** gap **again** end");
        let expected = [
            ("bold", true),
            ("gap", false),
            ("again", true),
            ("end", false),
        ]
        .into_iter()
        .flat_map(|(text, bold)| text.chars().map(move |character| (character, bold)))
        .collect::<Vec<_>>();
        for width in [1, 2, 3, 7, 13, 20] {
            query.wrap(width);
            for rows in [query.lines.as_slice(), transcript.rows(width)] {
                let actual = rows
                    .iter()
                    .flat_map(|line| &line.spans)
                    .flat_map(|span| {
                        span.content.chars().filter_map(move |character| {
                            (!character.is_whitespace()).then_some((
                                character,
                                span.style.add_modifier.contains(Modifier::BOLD),
                            ))
                        })
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
        }
        Ok(())
    }
}
