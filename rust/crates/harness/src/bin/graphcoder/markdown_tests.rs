use super::*;
use acyclic_markdown::{Markdown, Span as MarkdownSpan};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::{Paragraph, Widget, Wrap},
};

#[test]
fn adapter_maps_all_attributes_and_borrows_text() {
    let source = [Line {
        spans: vec![MarkdownSpan {
            text: "hello".into(),
            style: Style {
                foreground: Some(acyclic_markdown::Color(1, 2, 3)),
                bold: true,
                italic: true,
                dim: true,
                underline: true,
                strikethrough: true,
            },
            link: Some("https://example.com".into()),
        }],
    }];
    let converted: Vec<_> = lines(&source).collect();
    let span = converted.first().and_then(|line| line.spans.first());
    assert!(span.is_some_and(|span| {
        span.content == "hello"
            && matches!(span.content, std::borrow::Cow::Borrowed(_))
            && span.style.fg == Some(Color::Rgb(1, 2, 3))
            && span.style.add_modifier.contains(
                Modifier::BOLD
                    | Modifier::ITALIC
                    | Modifier::DIM
                    | Modifier::UNDERLINED
                    | Modifier::CROSSED_OUT,
            )
    }));
}

#[test]
fn streamed_markdown_renders_to_a_ratatui_buffer_and_wraps() {
    let mut markdown = Markdown::default();
    for delta in [
        "# Tit",
        "le\n\n**bold",
        "** and [link](https://example.com)\n\n",
        "```rust\nlet x = 1;\n",
        "```",
    ] {
        markdown.push(delta);
    }
    let area = Rect::new(0, 0, 30, 9);
    let mut buffer = Buffer::empty(area);
    Paragraph::new(lines(markdown.lines()).collect::<Vec<_>>()).render(area, &mut buffer);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .into()
        })
        .collect();
    assert_eq!(
        rows.join("\n").trim_end(),
        "# Title\n\nbold and link\n\n┌─ rust\n│ let x = 1;\n└─"
    );
    assert!(buffer[(0, 2)].modifier.contains(Modifier::BOLD));
    assert!(buffer[(9, 2)].modifier.contains(Modifier::UNDERLINED));
    markdown.clear();
    markdown.push("one two three four");
    let area = Rect::new(0, 0, 8, 3);
    let mut buffer = Buffer::empty(area);
    Paragraph::new(lines(markdown.lines()).collect::<Vec<_>>())
        .wrap(Wrap { trim: false })
        .render(area, &mut buffer);
    assert_eq!(buffer[(0, 1)].symbol(), "t");
}

#[test]
fn provider_control_characters_are_escaped_before_rendering() {
    let mut markdown = Markdown::default();
    markdown.push("escape \u{1b}[2J and tab\there");
    let text = lines(markdown.lines())
        .flat_map(|line| line.spans.into_iter().map(|span| span.content.into_owned()))
        .collect::<String>();
    assert_eq!(text, "escape \\u{1b}[2J and tab\\there");
}

#[test]
fn multiline_html_renders_as_separate_terminal_rows() {
    let mut markdown = Markdown::default();
    for delta in ["<div>\n", "hello\n", "</div>\n"] {
        markdown.push(delta);
    }
    let area = Rect::new(0, 0, 12, 3);
    let mut buffer = Buffer::empty(area);
    Paragraph::new(lines(markdown.lines()).collect::<Vec<_>>()).render(area, &mut buffer);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .collect();
    assert_eq!(rows, ["<div>", "hello", "</div>"]);
}
