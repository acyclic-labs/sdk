use crate::{
    Line, Style, Tone,
    style::{MUTED, toned},
};
use pulldown_cmark::{Event, Parser};
use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
    ops::Range,
    sync::{Arc, Mutex, OnceLock, Weak},
};
use syntect::parsing::{ParseState, Scope, ScopeStack, ScopeStackOp, SyntaxSet};

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
    pub(crate) static PARSED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A fence's grammar state between its lines.
#[derive(Clone)]
pub(crate) struct Code {
    language: Arc<str>,
    previous: Option<Arc<Node>>,
    deferred: bool,
    cache: Option<CodeCache>,
}

/// A disposable presentation's source-keyed cache. Nodes remove their own weak
/// entries on drop; exact context checks make fingerprint collisions harmless.
#[derive(Clone, Default)]
pub struct CodeCache(Arc<Mutex<HashMap<u64, Weak<Node>>>>);

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

/// An immutable code line and its grammar context. Calling styles resolves
/// only this line's colours. Stateful grammars scan an uncached preceding
/// prefix once to establish lexical context; unseen rows allocate no styles.
/// Clones share the same cache, including across speculative stream tails.
#[derive(Clone)]
pub struct CodeLine(Arc<Node>);

struct Node {
    fingerprint: u64,
    cache: Weak<Mutex<HashMap<u64, Weak<Node>>>>,
    language: Arc<str>,
    previous: Option<Arc<Node>>,
    text: Arc<str>,
    parsed: OnceLock<Parsed>,
    styles: Mutex<BTreeMap<(usize, usize), Arc<[(Range<usize>, Style)]>>>,
}
struct Parsed {
    state: ParseState,
    after: ScopeStack,
    before: ScopeStack,
    operations: Vec<(usize, ScopeStackOp)>,
}

// A whole fence can be dropped in any order, without recursive Arc destruction.
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.upgrade() {
            let mut cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if cache
                .get(&self.fingerprint)
                .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
            {
                cache.remove(&self.fingerprint);
            }
        }
        let mut previous = self.previous.take();
        while let Some(node) = previous {
            match Arc::try_unwrap(node) {
                Ok(mut node) => previous = node.previous.take(),
                Err(_) => break,
            }
        }
    }
}
impl std::fmt::Debug for CodeLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodeLine")
            .field("language", &self.0.language)
            .field("text", &self.0.text)
            .finish()
    }
}
impl PartialEq for CodeLine {
    fn eq(&self, other: &Self) -> bool {
        let (mut left, mut right) = (Some(self.0.as_ref()), Some(other.0.as_ref()));
        loop {
            match (left, right) {
                (Some(a), Some(b)) => {
                    if std::ptr::eq(a, b) {
                        return true;
                    }
                    if a.language != b.language || a.text != b.text {
                        return false;
                    }
                    left = a.previous.as_deref();
                    right = b.previous.as_deref();
                }
                (None, None) => return true,
                _ => return false,
            }
        }
    }
}
impl Eq for CodeLine {}

impl Node {
    fn parse(&self) -> Parsed {
        #[cfg(test)]
        PARSED.with(|count| count.set(count.get() + 1));
        let (mut state, mut stack) = self
            .previous
            .as_ref()
            .and_then(|previous| previous.parsed.get())
            .map_or_else(
                || (parser(&self.language), ScopeStack::new()),
                |cached| (cached.state.clone(), cached.after.clone()),
            );
        let before = stack.clone();
        let operations = state.parse_line(&self.text, syntaxes()).unwrap_or_default();
        for (_, operation) in &operations {
            if stack.apply(operation).is_err() {
                break;
            }
        }
        Parsed {
            state,
            after: stack,
            before,
            operations,
        }
    }

    fn parsed(&self) -> &Parsed {
        let mut missing = Vec::new();
        let mut cursor = self.previous.as_deref();
        while let Some(node) = cursor {
            if node.parsed.get().is_some() {
                break;
            }
            missing.push(node);
            cursor = node.previous.as_deref();
        }
        // Each hidden prefix line retains its grammar checkpoint, never styles.
        // Revealing earlier rows therefore performs no repeated prefix parsing.
        for node in missing.into_iter().rev() {
            node.parsed.get_or_init(|| node.parse());
        }
        self.parsed.get_or_init(|| self.parse())
    }
}

fn parser(language: &str) -> ParseState {
    let syntaxes = syntaxes();
    let language = match language {
        "ts" | "typescript" => "JavaScript",
        "sh" | "bash" => "sh",
        other => other,
    };
    ParseState::new(
        syntaxes
            .find_syntax_by_token(language)
            .unwrap_or_else(|| syntaxes.find_syntax_plain_text()),
    )
}
impl CodeLine {
    /// Original code bytes, without its line terminator or display gutter.
    pub fn text(&self) -> &str {
        self.0.text.trim_end_matches(['\r', '\n'])
    }

    /// Disjoint code-byte styles. The result is cached in the immutable line,
    /// independent of theme and viewport, with no global cache or eviction scan.
    pub fn styles(&self) -> Arc<[(Range<usize>, Style)]> {
        self.styles_for(0..self.text().len())
    }

    /// Resolve a visible source-byte window, including a wrapped portion of a
    /// very long line. Only that window gets styles; grammar state is shared.
    pub fn styles_for(&self, visible: Range<usize>) -> Arc<[(Range<usize>, Style)]> {
        let start = visible.start.min(self.text().len());
        let end = visible.end.min(self.text().len()).max(start);
        let mut cache = self
            .0
            .styles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(cache.entry((start, end)).or_insert_with(|| {
            #[cfg(test)]
            HIGHLIGHTED.with(|count| count.set(count.get() + 1));
            let parsed = self.0.parsed();
            let mut stack = parsed.before.clone();
            let mut styles = Vec::new();
            let mut at = 0;
            for (offset, operation) in &parsed.operations {
                let end = (*offset).min(self.text().len());
                if end > at {
                    let from = at.max(start);
                    let to = end.min(visible.end);
                    if from < to {
                        styles.push((from..to, tone(&stack)));
                    }
                    at = end;
                }
                if at >= visible.end {
                    break;
                }
                if stack.apply(operation).is_err() {
                    break;
                }
            }
            if at < self.text().len() {
                let from = at.max(start);
                let to = self.text().len().min(visible.end);
                if from < to {
                    styles.push((from..to, tone(&stack)));
                }
            }
            styles.into()
        }))
    }
}

pub(crate) fn highlighter(language: &str, deferred: bool, cache: Option<CodeCache>) -> Code {
    Code {
        language: language.into(),
        previous: None,
        deferred,
        cache,
    }
}

pub(crate) fn highlight(code: &mut Code, text: &str) -> Line {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    code.language.hash(&mut hash);
    code.previous
        .as_ref()
        .map(|node| node.fingerprint)
        .hash(&mut hash);
    text.hash(&mut hash);
    let fingerprint = hash.finish();
    // Release the index lock before upgrading or dropping a node: node drop
    // removes its entry through the same lock.
    let cached = code
        .cache
        .as_ref()
        .and_then(|cache| {
            cache
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&fingerprint)
                .cloned()
        })
        .and_then(|entry| entry.upgrade())
        .filter(|node| {
            node.language == code.language
                && node.text.as_ref() == text
                && match (&node.previous, &code.previous) {
                    (None, None) => true,
                    (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                    _ => false,
                }
        });
    let source = CodeLine(cached.unwrap_or_else(|| {
        Arc::new(Node {
            fingerprint,
            cache: code
                .cache
                .as_ref()
                .map_or_else(Weak::new, |cache| Arc::downgrade(&cache.0)),
            language: Arc::clone(&code.language),
            previous: code.previous.clone(),
            text: text.into(),
            parsed: OnceLock::new(),
            styles: Mutex::new(BTreeMap::new()),
        })
    }));
    if let Some(cache) = &code.cache {
        cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(fingerprint, Arc::downgrade(&source.0));
    }
    code.previous = Some(Arc::clone(&source.0));
    let mut line = Line::plain("│ ", MUTED);
    line.code = true;
    if code.deferred {
        line.push(source.text(), Style::DEFAULT, None);
        line.syntax = Some(source);
    } else {
        for (range, style) in source.styles().iter() {
            line.push(
                source.text().get(range.clone()).unwrap_or_default(),
                *style,
                None,
            );
        }
    }
    line
}
pub(crate) fn header(language: &str) -> Line {
    let mut line = Line::plain(
        &format!(
            "┌─ {}",
            if language.is_empty() {
                "code"
            } else {
                language
            }
        ),
        MUTED,
    );
    line.code = true;
    line
}

pub(crate) fn footer() -> Line {
    let mut line = Line::plain("└─", MUTED);
    line.code = true;
    line
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_cache_releases_discarded_previews_and_preserves_live_lines() {
        let cache = CodeCache::default();
        let mut code = highlighter("rust", true, Some(cache.clone()));
        let retained = highlight(&mut code, "let retained = 42;\n");
        let size = || {
            cache
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        };
        for count in 0..1000 {
            let mut preview = highlighter("rust", true, Some(cache.clone()));
            let line = highlight(&mut preview, &format!("let preview = {count};\n"));
            assert_eq!(size(), 2, "live previews must remain reusable");
            drop(preview);
            assert_eq!(size(), 2, "the visible line still owns its entry");
            drop(line);
            assert_eq!(size(), 1, "a discarded preview left an index allocation");
        }
        drop(code);
        assert_eq!(size(), 1);
        drop(retained);
        assert_eq!(size(), 0);
    }
}
