use super::{State, delimiters};
use crate::inline_scan::tokens::{Kind, Token};
use std::ops::Range;

pub(super) fn literal(state: &mut State, token: Option<&Token>) -> Option<usize> {
    let token = token?;
    if matches!(token.kind, Kind::Run { .. })
        || matches!(token.kind, Kind::Escape('*' | '_' | '~' | '`'))
    {
        return None;
    }
    #[cfg(test)]
    crate::work::scan(1);
    let previous = state.forest.snapshot();
    let root = state.forest.text(token.range.clone());
    state.invalid |= !state.forest.restore(previous);
    Some(root)
}

#[derive(Clone, Copy)]
struct Heads {
    delimiter: delimiters::Snapshot,
    forest: usize,
}

impl Heads {
    fn read(state: &State) -> Self {
        Self {
            delimiter: state.delimiter.snapshot(),
            forest: state.forest.snapshot(),
        }
    }

    fn restore(self, state: &mut State) {
        state.delimiter.restore(self.delimiter);
        state.invalid |= !state.forest.restore(self.forest);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Key {
    start: usize,
    byte: u8,
    before: delimiters::Flank,
    escaped: bool,
}

struct Partial {
    before: Heads,
    start: usize,
}

struct Projection {
    current: Heads,
    partial: Option<Partial>,
    tail: Range<usize>,
    end: usize,
}

#[derive(Default)]
pub(super) struct Projections {
    key: Option<Key>,
    // Original run length modulo three determines emphasis eligibility. Each
    // residue advances independently; none replays already completed groups.
    classes: [Option<Projection>; 3],
}

impl Projections {
    pub(super) fn invalidate(&mut self) {
        self.key = None;
    }

    pub(super) fn project(&mut self, state: &mut State, token: Option<&Token>) -> usize {
        let Some(token) = token else {
            return state.forest.snapshot();
        };
        #[cfg(test)]
        crate::work::scan(1);
        let (byte, before, escaped) = match token.kind {
            Kind::Run { byte, before, .. } => (byte, before, false),
            Kind::Escape(character) if matches!(character, '*' | '_' | '~' | '`') => {
                (character as u8, character.into(), true)
            }
            _ => {
                let previous = state.forest.snapshot();
                let root = state.forest.text(token.range.clone());
                state.invalid |= !state.forest.restore(previous);
                return root;
            }
        };
        let original = Heads::read(state);
        if byte == b'`' {
            let physical = token.range.start + usize::from(escaped)..token.range.end;
            #[cfg(test)]
            crate::work::scan(1);
            if let Some(code) = state.preview_code(token) {
                let (root, opening) = (code.before.forest(), code.opening.clone());
                state.invalid |= !state.forest.restore(root);
                state.forest.code(opening, physical);
            } else if escaped {
                state
                    .forest
                    .escape(token.range.start..token.range.start + 2);
                state.forest.text(token.range.start + 2..token.range.end);
            } else {
                state.forest.text(token.range.clone());
            }
            let root = state.forest.snapshot();
            original.restore(state);
            return root;
        }
        let key = Key {
            start: token.range.start,
            byte,
            before,
            escaped,
        };
        if self.key != Some(key) {
            self.classes = [None, None, None];
            self.key = Some(key);
        }
        let start = key.start + if escaped { 2 } else { 0 };
        let length = token.range.end - start;
        let class = length % 3;
        // Strikethrough runs have only two valid lengths, so they cannot
        // accumulate a growing sequence of partial matches.
        let cached = if byte == b'~' {
            None
        } else {
            self.classes.get_mut(class).and_then(Option::take)
        };
        let mut projection = cached.unwrap_or_else(|| {
            if escaped {
                state.forest.escape(key.start..start);
            }
            Projection {
                current: Heads::read(state),
                partial: None,
                tail: start..start,
                end: start,
            }
        });
        projection.extend(state, key, token.range.end, length);
        let root = projection.current.forest;
        if let Some(slot) = self.classes.get_mut(class) {
            *slot = Some(projection);
        }
        original.restore(state);
        root
    }
}

impl Projection {
    fn extend(&mut self, state: &mut State, key: Key, end: usize, original: usize) {
        #[cfg(test)]
        crate::work::scan(1);
        if self.end == end {
            return;
        }
        let (heads, start) = if let Some(partial) = self.partial.take() {
            // A partial match can change from emphasis to strong as it grows.
            // Restore its original body, replacing the group instead of nesting
            // another style around the obsolete projection.
            (partial.before, partial.start)
        } else if !self.tail.is_empty() {
            self.current.restore(state);
            let prefix = state.forest.text_prefix(self.current.forest);
            debug_assert!(prefix.is_some(), "unmatched EOF tail is a text leaf");
            if let Some((prefix, _)) = prefix {
                state.invalid |= !state.forest.restore(prefix);
                state.forest.text(self.tail.start..end);
            } else {
                state.invalid = true;
            }
            self.tail.end = end;
            self.end = end;
            self.current = Heads::read(state);
            return;
        } else {
            (self.current, self.end)
        };
        heads.restore(state);
        let range = start..end;
        let cursor = state.forest.text(range.clone());
        let mut run = delimiters::Run::new(range, key.byte, key.before, ' ');
        run.cursor = cursor;
        let mut partial = None;
        let forest = &mut state.forest;
        let invalid = &mut state.invalid;
        self.tail = state
            .delimiter
            .feed_original(run, original, |matched, before| {
                let opening = forest.text_prefix(matched.opener_cursor);
                let closing = forest.text_prefix(matched.closer_cursor);
                if let (Some((_, opening)), Some((prefix, closing))) = (opening, closing) {
                    if opening.start < matched.opener.start {
                        partial = Some(Partial {
                            before: Heads {
                                delimiter: before,
                                forest: prefix,
                            },
                            start: closing.start,
                        });
                    }
                } else {
                    *invalid = true;
                }
                let wrapped = forest.wrap(&matched);
                debug_assert!(wrapped.is_some(), "active EOF delimiter cursors");
                *invalid |= wrapped.is_none();
                wrapped.unwrap_or_default()
            });
        self.partial = partial;
        self.end = end;
        self.current = Heads::read(state);
    }
}
