use crate::{
    Line, Style, Tone,
    style::{MUTED, toned},
};
use pulldown_cmark::{Event, Parser};
use std::sync::OnceLock;
use syntect::parsing::{ParseState, Scope, ScopeStack, SyntaxSet};

static SYNTAXES: OnceLock<SyntaxSet> = OnceLock::new();
static RULES: OnceLock<Vec<(Scope, Style)>> = OnceLock::new();

/// The style of each grammar scope family, by tone: code shares the theme's
/// tokens instead of carrying a palette of its own. The innermost scope with
/// a rule styles a run, so an operator keyword stays plain and a string's
/// quotes take the string's tone.
const TONES: [(&str, Style); 9] = [
    ("comment", MUTED),
    ("string", toned(Tone::Success)),
    ("constant.numeric", toned(Tone::Attention)),
    ("constant.language", toned(Tone::Attention)),
    ("constant.character", toned(Tone::Attention)),
    ("keyword.operator", Style::DEFAULT),
    ("keyword", toned(Tone::Accent)),
    ("storage", toned(Tone::Accent)),
    ("invalid", toned(Tone::Error)),
];

#[cfg(test)]
std::thread_local! {
    pub(crate) static HIGHLIGHTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A fence's grammar state between its lines.
#[derive(Clone)]
pub(crate) struct Code {
    state: Option<(ParseState, ScopeStack)>,
}

fn syntaxes() -> &'static SyntaxSet {
    SYNTAXES.get_or_init(SyntaxSet::load_defaults_newlines)
}

/// The style of text under `stack`.
fn tone(stack: &ScopeStack) -> Style {
    let rules = RULES.get_or_init(|| {
        TONES
            .iter()
            .filter_map(|(name, style)| Some((Scope::new(name).ok()?, *style)))
            .collect()
    });
    stack
        .as_slice()
        .iter()
        .rev()
        .find_map(|scope| {
            rules
                .iter()
                .find(|(rule, _)| rule.is_prefix_of(*scope))
                .map(|(_, style)| *style)
        })
        .unwrap_or_default()
}

pub(crate) fn highlighter(language: &str) -> Code {
    let syntaxes = syntaxes();
    let language = match language {
        "ts" | "typescript" => "JavaScript",
        "sh" | "bash" => "sh",
        other => other,
    };
    let syntax = syntaxes
        .find_syntax_by_token(language)
        .unwrap_or_else(|| syntaxes.find_syntax_plain_text());
    Code {
        state: Some((ParseState::new(syntax), ScopeStack::new())),
    }
}

pub(crate) fn highlight(code: &mut Code, text: &str) -> Line {
    #[cfg(test)]
    HIGHLIGHTED.with(|count| count.set(count.get() + 1));
    let syntaxes = syntaxes();
    let (mut parse, mut stack) = code.state.take().unwrap_or_else(|| {
        (
            ParseState::new(syntaxes.find_syntax_plain_text()),
            ScopeStack::new(),
        )
    });
    let shown = text.trim_end_matches(['\r', '\n']);
    let mut line = Line::plain("│ ", MUTED);
    let mut at = 0;
    // A grammar error leaves the rest of the line in the current style.
    if let Ok(operations) = parse.parse_line(text, syntaxes) {
        for (offset, operation) in operations {
            let end = offset.min(shown.len());
            if end > at {
                line.push(shown.get(at..end).unwrap_or_default(), tone(&stack), None);
                at = end;
            }
            if stack.apply(&operation).is_err() {
                break;
            }
        }
    }
    line.push(shown.get(at..).unwrap_or_default(), tone(&stack), None);
    code.state = Some((parse, stack));
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
    acyclic_markdown_work::record!(scan(candidate.len()));
    #[cfg(test)]
    crate::references::PARSED_BYTES.with(|work| work.set(work.get() + candidate.len()));
    !Parser::new(&candidate).any(|event| {
        acyclic_markdown_work::record!(node());
        matches!(event, Event::Text(_))
    })
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
