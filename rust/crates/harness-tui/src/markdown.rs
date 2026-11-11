//! Answer text as displayed: [`acyclic_markdown`]'s styled lines, flattened
//! to one text and the style of each of its byte ranges for wrapping, with
//! each tone in its theme token's colour.

use crate::config::{ThemeStyles, Token};
use acyclic_markdown::Tone;
use ratatui::style::{Modifier, Style};
use std::{ops::Range, sync::Arc};

/// Printable text, disjoint styles, link destinations and code bands, all
/// indexed by UTF-8 source bytes until the shared layout wraps graphemes.
pub(crate) struct Rendered {
    pub text: String,
    pub styles: Vec<(Range<usize>, Style)>,
    pub links: Vec<(Range<usize>, Arc<str>)>,
    pub bands: Vec<Range<usize>>,
    pub code: Vec<(Range<usize>, acyclic_markdown::CodeLine)>,
}

/// Deferred syntax of one wrapped row. Source offsets stay in the immutable
/// code line; display offsets refer to this row's sanitized graphemes.
pub struct CodeRow {
    pub(crate) source: acyclic_markdown::CodeLine,
    pub(crate) cells: Vec<(Range<usize>, Range<usize>)>,
}

impl CodeRow {
    pub(crate) fn paint(
        &self,
        line: &ratatui::text::Line<'_>,
        theme: &ThemeStyles,
    ) -> ratatui::text::Line<'static> {
        use unicode_segmentation::UnicodeSegmentation as _;
        let text = line.to_string();
        let mut end = 0;
        let mut original = crate::styled_cells::ordered_styles(line.spans.iter().map(|span| {
            let start = end;
            end += span.content.len();
            (start..end, span.style)
        }));
        let visible = self.cells.first().map_or(0, |(_, source)| source.start)
            ..self.cells.last().map_or(0, |(_, source)| source.end);
        let ranges = self.source.styles_for(visible);
        let mut syntax = crate::styled_cells::ordered_styles(
            ranges
                .iter()
                .map(|(range, source)| (range.clone(), style(*source, theme))),
        );
        let mut cells = self.cells.iter().peekable();
        let mut spans = Vec::new();
        crate::styled_cells::append(
            &mut spans,
            text.grapheme_indices(true)
                .map(|(at, text)| (at, text.to_owned())),
            |at| {
                let base = original(at);
                while cells.peek().is_some_and(|(range, _)| range.end <= at) {
                    cells.next();
                }
                cells
                    .peek()
                    .filter(|(range, _)| range.contains(&at))
                    .map_or(base, |(_, source)| base.patch(syntax(source.start)))
            },
        );
        let mut painted = ratatui::text::Line::from(spans).style(line.style);
        painted.alignment = line.alignment;
        painted
    }
}

/// `text` presented as markdown with tables sized to `width` columns: its
/// visible text, lines joined by `\n`, with disjoint styled byte ranges,
/// code bands and destinations retained separately from printable labels.
#[must_use]
pub fn render(text: &str, width: usize, theme: &ThemeStyles) -> Rendered {
    let mut parser =
        acyclic_markdown::Markdown::with_width(u16::try_from(width).unwrap_or(u16::MAX));
    parser.push(text);
    render_lines(parser.lines(), theme)
}

pub(crate) fn render_lines(lines: &[acyclic_markdown::Line], theme: &ThemeStyles) -> Rendered {
    let mut shown = String::new();
    let mut styles = Vec::new();
    let mut links = Vec::new();
    let mut bands = Vec::new();
    let mut code = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            shown.push('\n');
        }
        let line_start = shown.len();
        let mut band = false;
        for span in &line.spans {
            let start = shown.len();
            shown.push_str(&span.text);
            let style = style(span.style, theme);
            if style != Style::default() {
                styles.push((start..shown.len(), style));
            }
            if let Some(link) = &span.link {
                links.push((start..shown.len(), Arc::clone(link)));
            }
            band |= span.code;
            if let Some(source) = &span.syntax {
                code.push((start..shown.len(), source.clone()));
            }
        }
        if band {
            bands.push(line_start..shown.len());
        }
    }
    Rendered {
        text: shown,
        styles,
        links,
        bands,
        code,
    }
}

/// A tone takes its token's colour; muted text without one takes `muted`.
fn style(source: acyclic_markdown::Style, theme: &ThemeStyles) -> Style {
    let token = match source.foreground {
        Some(Tone::Accent) => Some(Token::Accent),
        Some(Tone::Attention) => Some(Token::Attention),
        Some(Tone::Success) => Some(Token::Success),
        Some(Tone::Error) => Some(Token::Error),
        None => source.dim.then_some(Token::Muted),
    };
    let mut style = token.map_or_else(Style::default, |token| theme.token(token));
    for (set, modifier) in [
        (source.bold, Modifier::BOLD),
        (source.italic, Modifier::ITALIC),
        (source.underline, Modifier::UNDERLINED),
        (source.strikethrough, Modifier::CROSSED_OUT),
    ] {
        if set {
            style = style.add_modifier(modifier);
        }
    }
    style
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markers_become_styles() {
        let Rendered {
            text: shown,
            styles,
            ..
        } = render("streaming **test answer**", 80, &ThemeStyles::default());
        assert_eq!(shown, "streaming test answer");
        let (range, style) = &styles[0];
        assert_eq!(&shown[range.clone()], "test answer");
        assert!(style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn a_link_keeps_its_destination() {
        let theme = ThemeStyles::default();
        let Rendered {
            text: shown, links, ..
        } = render("see [the **docs**](https://example.com/a) now", 80, &theme);
        assert_eq!(shown, "see the docs now");
        assert!(
            links
                .iter()
                .all(|(_, target)| target.as_ref() == "https://example.com/a")
        );
        let Rendered { text: shown, .. } = render("<https://example.com/b>", 80, &theme);
        assert_eq!(shown, "https://example.com/b");
        let Rendered {
            text: shown,
            styles,
            links,
            ..
        } = render(
            "see [ https://**example**.com/a ](https://example.com/a) [**next**](https://example.com/b)",
            80,
            &theme,
        );
        assert_eq!(shown, "see  https://example.com/a  next");
        assert!(styles.iter().any(|(range, style)| {
            &shown[range.clone()] == "example" && style.add_modifier.contains(Modifier::BOLD)
        }));
        assert!(links.iter().any(|(range, target)| {
            &shown[range.clone()] == "next" && target.as_ref() == "https://example.com/b"
        }));
    }
}
