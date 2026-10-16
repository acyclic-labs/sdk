//! Ratatui adapter for the portable Markdown presentation.

use acyclic_markdown::{Line, Style};
use ratatui::{
    style::{Color, Modifier},
    text::{Line as TuiLine, Span},
};

/// Borrow display text; the Markdown stream retains its cached lines.
pub fn lines(source: &[Line]) -> impl Iterator<Item = TuiLine<'_>> {
    source.iter().map(|line| {
        TuiLine::from(
            line.spans
                .iter()
                .map(|span| Span::styled(terminal_text(&span.text), style(span.style)))
                .collect::<Vec<_>>(),
        )
    })
}

fn terminal_text(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.chars().any(char::is_control) {
        return text.into();
    }
    let mut escaped = String::new();
    for character in text.chars() {
        if character.is_control() {
            escaped.extend(character.escape_debug());
        } else {
            escaped.push(character);
        }
    }
    escaped.into()
}

fn style(source: Style) -> ratatui::style::Style {
    let mut target = ratatui::style::Style::default();
    if let Some(acyclic_markdown::Color(r, g, b)) = source.foreground {
        target = target.fg(Color::Rgb(r, g, b));
    }
    for (set, modifier) in [
        (source.bold, Modifier::BOLD),
        (source.italic, Modifier::ITALIC),
        (source.dim, Modifier::DIM),
        (source.underline, Modifier::UNDERLINED),
        (source.strikethrough, Modifier::CROSSED_OUT),
    ] {
        if set {
            target = target.add_modifier(modifier);
        }
    }
    target
}

#[cfg(test)]
#[path = "markdown_tests.rs"]
mod tests;
