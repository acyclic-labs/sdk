//! Answer text as displayed: [`acyclic_markdown`]'s styled lines, flattened
//! to one text and the style of each of its byte ranges for wrapping.

use ratatui::style::{Color, Modifier, Style};
use std::ops::Range;

/// `text` presented as markdown with tables sized to `width` columns: its
/// visible text, lines joined by `\n`, and the style of each styled byte
/// range of it, in order and disjoint. A link shows its destination after
/// its label, `label (url)`, unless the label is the destination itself, so
/// it stays readable and a terminal can open it.
#[must_use]
pub fn render(text: &str, width: usize) -> (String, Vec<(Range<usize>, Style)>) {
    let mut parser =
        acyclic_markdown::Markdown::with_width(u16::try_from(width).unwrap_or(u16::MAX));
    parser.push(text);
    let mut shown = String::with_capacity(text.len());
    let mut styles = Vec::new();
    for (index, line) in parser.lines().iter().enumerate() {
        if index > 0 {
            shown.push('\n');
        }
        let mut label = String::new();
        for (at, span) in line.spans.iter().enumerate() {
            let start = shown.len();
            shown.push_str(&span.text);
            let style = style(span.style);
            if style != Style::default() {
                styles.push((start..shown.len(), style));
            }
            let Some(link) = &span.link else {
                continue;
            };
            label.push_str(&span.text);
            // A link's label may span several styled spans: its destination
            // follows the last of them.
            let continues = line
                .spans
                .get(at + 1)
                .is_some_and(|next| next.link.as_ref() == Some(link));
            if continues {
                continue;
            }
            if label.trim() != link.as_ref() {
                shown.push_str(" (");
                let start = shown.len();
                shown.push_str(link);
                styles.push((
                    start..shown.len(),
                    Style::default().add_modifier(Modifier::UNDERLINED),
                ));
                shown.push(')');
            }
            label.clear();
        }
    }
    (shown, styles)
}

fn style(source: acyclic_markdown::Style) -> Style {
    let mut style = Style::default();
    if let Some(acyclic_markdown::Color(r, g, b)) = source.foreground {
        style = style.fg(Color::Rgb(r, g, b));
    }
    for (set, modifier) in [
        (source.bold, Modifier::BOLD),
        (source.italic, Modifier::ITALIC),
        (source.dim, Modifier::DIM),
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
        let (shown, styles) = render("streaming **test answer**", 80);
        assert_eq!(shown, "streaming test answer");
        let (range, style) = &styles[0];
        assert_eq!(&shown[range.clone()], "test answer");
        assert!(style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn a_link_keeps_its_destination() {
        let (shown, _) = render("see [the **docs**](https://example.com/a) now", 80);
        assert_eq!(shown, "see the docs (https://example.com/a) now");
        let (shown, _) = render("<https://example.com/b>", 80);
        assert_eq!(shown, "https://example.com/b");
    }
}
