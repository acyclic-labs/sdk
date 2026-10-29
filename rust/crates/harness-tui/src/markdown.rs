//! Answer text as displayed: [`acyclic_markdown`]'s styled lines, flattened
//! to one text and the style of each of its byte ranges for wrapping.

use ratatui::style::{Color, Modifier, Style};
use std::ops::Range;

/// `text` presented as markdown with tables sized to `width` columns: its
/// visible text, lines joined by `\n`, and the style of each styled byte
/// range of it, in order and disjoint.
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
        for span in &line.spans {
            let start = shown.len();
            shown.push_str(&span.text);
            let style = style(span.style);
            if style != Style::default() {
                styles.push((start..shown.len(), style));
            }
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
}
