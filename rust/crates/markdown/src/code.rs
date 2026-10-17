use crate::{Color, Line, Style, style::MUTED};
use pulldown_cmark::{Event, Parser};
use std::sync::OnceLock;
use syntect::{
    easy::HighlightLines,
    highlighting::{FontStyle, HighlightState, Theme, ThemeSet},
    parsing::{ParseState, SyntaxSet},
};

static SYNTAXES: OnceLock<SyntaxSet> = OnceLock::new();
static THEME: OnceLock<Theme> = OnceLock::new();

#[cfg(test)]
std::thread_local! {
    pub(crate) static HIGHLIGHTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone)]
pub(crate) struct Code {
    state: Option<(HighlightState, ParseState)>,
}

fn theme() -> &'static Theme {
    THEME.get_or_init(|| {
        ThemeSet::load_defaults()
            .themes
            .remove("base16-ocean.dark")
            .unwrap_or_default()
    })
}

pub(crate) fn highlighter(language: &str) -> Code {
    let syntaxes = SYNTAXES.get_or_init(SyntaxSet::load_defaults_newlines);
    let language = match language {
        "ts" | "typescript" => "JavaScript",
        "sh" | "bash" => "sh",
        other => other,
    };
    let syntax = syntaxes
        .find_syntax_by_token(language)
        .unwrap_or_else(|| syntaxes.find_syntax_plain_text());
    let (highlight, parse) = HighlightLines::new(syntax, theme()).state();
    Code {
        state: Some((highlight, parse)),
    }
}

pub(crate) fn highlight(code: &mut Code, text: &str) -> Line {
    #[cfg(test)]
    HIGHLIGHTED.with(|count| count.set(count.get() + 1));
    let syntaxes = SYNTAXES.get_or_init(SyntaxSet::load_defaults_newlines);
    let (highlight, parse) = code
        .state
        .take()
        .unwrap_or_else(|| HighlightLines::new(syntaxes.find_syntax_plain_text(), theme()).state());
    let mut highlighter = HighlightLines::from_state(theme(), highlight, parse);
    let mut line = Line::plain("│ ", MUTED);
    match highlighter.highlight_line(text, syntaxes) {
        Ok(spans) => {
            for (style, text) in spans {
                let fg = style.foreground;
                line.push(
                    text.trim_end_matches(['\r', '\n']),
                    Style {
                        foreground: Some(Color(fg.r, fg.g, fg.b)),
                        bold: style.font_style.contains(FontStyle::BOLD),
                        italic: style.font_style.contains(FontStyle::ITALIC),
                        underline: style.font_style.contains(FontStyle::UNDERLINE),
                        ..Style::default()
                    },
                    None,
                );
            }
        }
        Err(_) => line.push(text.trim_end_matches(['\r', '\n']), Style::default(), None),
    }
    code.state = Some(highlighter.state());
    line
}

pub(crate) fn header(language: &str) -> Line {
    Line::plain(
        &format!(
            "┌─ {}",
            if language.is_empty() {
                "code"
            } else {
                language
            }
        ),
        MUTED,
    )
}

pub(crate) fn footer() -> Line {
    Line::plain("└─", MUTED)
}

pub(crate) fn opening(text: &str) -> Option<(u8, usize, usize, &str)> {
    let first = text.split_once('\n')?.0.trim_end_matches('\r');
    let indent = first.len() - first.trim_start_matches(' ').len();
    if indent > 3 {
        return None;
    }
    let first = first.trim_start_matches(' ');
    let marker = *first.as_bytes().first()?;
    if marker != b'`' && marker != b'~' {
        return None;
    }
    let count = first.bytes().take_while(|byte| *byte == marker).count();
    (count >= 3).then(|| {
        (
            marker,
            count,
            indent,
            first.get(count..).unwrap_or_default().trim(),
        )
    })
}

pub(crate) fn closing(text: &str, marker: u8, count: usize) -> bool {
    let input = text;
    let text = text.trim_end_matches(['\r', '\n']);
    let indent = text.len() - text.trim_start_matches(' ').len();
    if indent > 3 {
        return false;
    }
    let text = text.trim_start_matches(' ');
    let found = text.bytes().take_while(|byte| *byte == marker).count();
    if found < count {
        return false;
    }
    // Share the parser's closing-fence grammar, including incomplete EOF lines.
    // Ordinary code lines never allocate or invoke this candidate parse.
    let opening = if marker == b'`' { "```" } else { "~~~" };
    let candidate = format!("{opening}\n{input}");
    #[cfg(test)]
    crate::references::PARSED_BYTES.with(|work| work.set(work.get() + candidate.len()));
    !Parser::new(&candidate).any(|event| matches!(event, Event::Text(_)))
}

pub(crate) fn dedent(text: &str, indent: usize) -> std::borrow::Cow<'_, str> {
    let mut column = 0;
    let mut bytes = 0;
    for byte in text.bytes() {
        if column >= indent {
            break;
        }
        column += match byte {
            b' ' => 1,
            b'\t' => 4 - column % 4,
            _ => break,
        };
        bytes += 1;
    }
    let rest = text.get(bytes..).unwrap_or_default();
    if column > indent {
        format!("{}{rest}", " ".repeat(column - indent)).into()
    } else {
        rest.into()
    }
}
