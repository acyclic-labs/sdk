use std::{collections::VecDeque, ops::Range};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Text,
    Escape(char),
    Run {
        byte: u8,
        before: super::delimiters::Flank,
        after: super::delimiters::Flank,
    },
    Open {
        image: bool,
    },
    Close,
    Entity,
    AngleOpen,
    AngleClose,
    ParenOpen,
    ParenClose,
    Quote(char),
    Break,
}

impl Kind {
    // Preserve the parser's first-pass expansion state across source checkpoints.
    // A preceding expansion also processes a terminal escaped hard break.
    pub(super) fn expands(self, range: Range<usize>) -> bool {
        match self {
            Self::Open { .. }
            | Self::Close
            | Self::AngleOpen
            | Self::Escape('`')
            | Self::Run { byte: b'`', .. } => true,
            Self::Run {
                byte,
                before,
                after,
            } => {
                let run = super::delimiters::Run::new(range, byte, before, after);
                (byte != b'~' || run.range.len() <= 2) && (run.open || run.close)
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Token {
    pub(super) range: Range<usize>,
    pub(super) kind: Kind,
}

#[derive(Default)]
enum Pending {
    #[default]
    None,
    Escape(usize),
    Bang(usize),
    Entity(usize),
    Run {
        start: usize,
        byte: u8,
        before: char,
        escaped: bool,
    },
}

#[derive(Default, Debug)]
pub(super) struct Queue {
    pages: VecDeque<Vec<Token>>,
    start: usize,
    length: usize,
}
impl Queue {
    pub(super) fn len(&self) -> usize {
        self.length
    }
    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.pages.iter().map(Vec::capacity).sum()
    }
    pub(super) fn get(&self, index: usize) -> Option<&Token> {
        if index >= self.length {
            return None;
        }
        let (page, offset) = super::forest::slot(self.start + index);
        let first = super::forest::slot(self.start).0;
        self.pages.get(page.checked_sub(first)?)?.get(offset)
    }
    fn front(&self) -> Option<&Token> {
        self.get(0)
    }
    fn front_mut(&mut self) -> Option<&mut Token> {
        self.pages
            .front_mut()?
            .get_mut(super::forest::slot(self.start).1)
    }
    pub(super) fn back(&self) -> Option<&Token> {
        if self.length == 0 {
            return None;
        }
        self.pages.back()?.last()
    }
    fn back_mut(&mut self) -> Option<&mut Token> {
        if self.length == 0 {
            return None;
        }
        self.pages.back_mut()?.last_mut()
    }
    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = &Token> {
        self.pages
            .iter()
            .enumerate()
            .flat_map(move |(index, page)| {
                page.iter().skip(if index == 0 {
                    super::forest::slot(self.start).1
                } else {
                    0
                })
            })
    }
    pub(super) fn pop_front(&mut self) -> Option<Token> {
        let token = self.front()?.clone();
        self.start += 1;
        self.length -= 1;
        if super::forest::slot(self.start).1 == 0 {
            self.pages.pop_front();
        }
        Some(token)
    }
    fn push_back(&mut self, token: Token) {
        #[cfg(test)]
        if MOVE_QUEUE.get() {
            let moved = self.pages.iter().cloned().collect();
            self.pages = moved;
        }
        // Use the same immutable page addressing as the forest. Advancing the
        // queue never copies its live tokens or resets the page geometry.
        let (page, offset) = super::forest::slot(self.start + self.length);
        if offset == 0 {
            #[cfg(test)]
            if self.pages.len() == self.pages.capacity() {
                crate::work::copy(self.pages.len() * std::mem::size_of::<Vec<Token>>());
            }
            if self.pages.len() == self.pages.capacity() {
                self.pages.reserve_exact(self.pages.len().max(1));
            }
            let tokens = super::forest::new_page(page);
            #[cfg(test)]
            let tokens = if crate::work::QUADRUPLE_TOKEN_PAGE.get() {
                Vec::with_capacity(super::forest::PAGE)
            } else {
                tokens
            };
            self.pages.push_back(tokens);
        }
        let Some(page) = self.pages.back_mut() else {
            return;
        };
        debug_assert_eq!(offset, page.len());
        page.push(token);
        self.length += 1;
    }
}

#[cfg(test)]
thread_local! { static MOVE_QUEUE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
impl PartialEq for Queue {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
#[cfg(test)]
impl Iterator for Queue {
    type Item = Token;
    fn next(&mut self) -> Option<Token> {
        self.pop_front()
    }
}
#[cfg(test)]
impl<'a> IntoIterator for &'a Queue {
    type Item = &'a Token;
    type IntoIter = Box<dyn Iterator<Item = &'a Token> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

#[derive(Default)]
pub(super) struct Tokens {
    pub(super) ready: Queue,
    starts: super::forest::glyphs::rank::Rank,
    openers: super::forest::glyphs::rank::Rank,
    ranked: usize,
    origin: usize,
    ordinal_origin: usize,
    indexed: bool,
    emitted: usize,
    expansion: Option<usize>,
    offset: usize,
    previous: Option<char>,
    pending: Pending,
}

impl Tokens {
    pub(super) fn at(offset: usize, previous: Option<char>) -> Self {
        Self {
            offset,
            ranked: offset,
            origin: offset,
            previous,
            ..Self::default()
        }
    }

    pub(super) fn discard_before(&mut self, offset: usize) {
        while self
            .ready
            .front()
            .is_some_and(|token| token.range.end <= offset)
        {
            self.ready.pop_front();
        }
        if let Some(token) = self.ready.front_mut() {
            token.range.start = token.range.start.max(offset);
        }
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.ready.capacity() * std::mem::size_of::<Token>()
            + self.ready.pages.capacity() * std::mem::size_of::<Vec<Token>>()
            + self.starts.retained()
            + self.openers.retained()
    }

    pub(super) fn push(&mut self, source: &str) {
        let base = self.offset;
        let Some(appended) = source.get(base..) else {
            return;
        };
        for (relative, character) in appended.char_indices() {
            let position = base + relative;
            #[cfg(test)]
            crate::work::scan(character.len_utf8());
            self.character(position, character);
            self.offset = position + character.len_utf8();
            self.previous = Some(character);
        }
    }

    // EOF is a preview, never a state transition: a following chunk can still
    // extend this run or supply the escaped character/entity terminator.
    pub(super) fn preview(&self) -> Option<Token> {
        let (start, kind) = match self.pending {
            Pending::None => return None,
            Pending::Run {
                start,
                byte,
                before,
                escaped,
            } => (
                start,
                if escaped {
                    Kind::Escape(char::from(byte))
                } else {
                    Kind::Run {
                        byte,
                        before: before.into(),
                        after: ' '.into(),
                    }
                },
            ),
            Pending::Escape(start) | Pending::Bang(start) | Pending::Entity(start) => {
                (start, Kind::Text)
            }
        };
        Some(Token {
            range: start..self.offset,
            kind,
        })
    }

    pub(super) fn expands_after(&self, start: usize) -> bool {
        self.expansion.is_some_and(|at| at >= start)
            || self
                .preview()
                .is_some_and(|token| token.range.start >= start && token.kind.expands(token.range))
    }

    pub(super) fn covering(&self, position: usize) -> Option<Range<usize>> {
        let indexed = position
            .checked_sub(self.origin)
            .and_then(|position| self.starts.before(position))
            .and_then(|ordinal| {
                (ordinal + self.ordinal_origin).checked_sub(self.emitted - self.ready.len())
            })
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| self.ready.get(index));
        let settled = if let Some(token) = indexed {
            Some(token)
        } else {
            (0..self.ready.len()).rev().find_map(|index| {
                #[cfg(test)]
                crate::work::scan(1);
                let token = self.ready.get(index)?;
                (token.range.start < position).then_some(token)
            })
        };
        settled
            .filter(|token| token.kind != Kind::Text)
            .map(|token| token.range.clone())
            .into_iter()
            .chain(self.preview().map(|token| token.range))
            .find(|range| range.start < position && position < range.end)
    }

    pub(super) fn anchors_before(&self, position: usize) -> Option<usize> {
        if !self.indexed {
            return Some(0);
        }
        self.openers
            .before(self.starts.before(position.checked_sub(self.origin)?)?)
    }

    pub(super) fn anchor_at(&self, position: usize) -> Option<usize> {
        let ordinal = self.ordinal_at(position)?;
        let index = ordinal.checked_sub(self.emitted - self.ready.len())?;
        if !matches!(
            self.ready.get(index)?.kind,
            Kind::Open { .. } | Kind::AngleOpen
        ) {
            return None;
        }
        self.openers
            .before(ordinal.checked_sub(self.ordinal_origin)? + 1)?
            .checked_sub(1)
    }

    pub(super) fn ordinal_at(&self, position: usize) -> Option<usize> {
        let ordinal = self.starts.before(position.checked_sub(self.origin)?)? + self.ordinal_origin;
        let index = ordinal.checked_sub(self.emitted - self.ready.len())?;
        (self.ready.get(index)?.range.start == position).then_some(ordinal)
    }

    pub(super) fn after_absolute(&self, end: usize) -> Option<usize> {
        let ordinal = self.starts.before(end.checked_sub(self.origin)?)? + self.ordinal_origin;
        let index = ordinal
            .checked_sub(self.emitted - self.ready.len())?
            .checked_sub(1)?;
        (self.ready.get(index)?.range.end == end).then_some(ordinal)
    }

    pub(super) fn after(&self, end: usize) -> Option<usize> {
        self.after_absolute(end)?
            .checked_sub(self.emitted - self.ready.len())
    }

    pub(super) fn collapsed_end(&self, end: usize, discarded: usize) -> Option<usize> {
        #[cfg(test)]
        crate::work::scan(2);
        let after = self.after_absolute(end)?.checked_sub(discarded)?;
        let opening = self.ready.get(after)?;
        let closing = self.ready.get(after + 1)?;
        (opening.range.start == end
            && matches!(
                opening.kind,
                Kind::Open { image: false } | Kind::Escape('[')
            )
            && closing.kind == Kind::Close
            && opening.range.end == closing.range.start)
            .then_some(closing.range.end)
    }

    pub(super) fn neutral(&self) -> bool {
        matches!(self.pending, Pending::None)
    }

    fn character(&mut self, position: usize, character: char) {
        let end = position + character.len_utf8();
        match std::mem::take(&mut self.pending) {
            Pending::Escape(start) => {
                // Inside code the backslash is literal, so the physical run
                // can still be a closing delimiter extended by another chunk.
                if matches!(character, '*' | '_' | '~' | '`') {
                    self.pending = Pending::Run {
                        start,
                        byte: character as u8,
                        before: '\\',
                        escaped: true,
                    };
                    return;
                }
                if character.is_ascii_punctuation() || character == '\n' {
                    self.emit(start..end, Kind::Escape(character));
                    return;
                }
                self.emit(start..position, Kind::Text);
            }
            Pending::Bang(start) => {
                if character == '[' {
                    self.emit(start..end, Kind::Open { image: true });
                    return;
                }
                self.emit(start..position, Kind::Text);
            }
            Pending::Run {
                start,
                byte,
                before,
                escaped,
            } => {
                if character == char::from(byte) {
                    self.pending = Pending::Run {
                        start,
                        byte,
                        before,
                        escaped,
                    };
                    return;
                }
                let end = if escaped && byte == b'`' {
                    // Code treats the whole physical run as a possible closer,
                    // including its escaped first tick. Keep that unit intact.
                    self.emit(start..position, Kind::Escape('`'));
                    position
                } else if escaped {
                    self.emit(start..start + 2, Kind::Escape(char::from(byte)));
                    start + 2
                } else {
                    start
                };
                if end < position {
                    self.emit(
                        end..position,
                        Kind::Run {
                            byte,
                            before: if escaped { char::from(byte) } else { before }.into(),
                            after: character.into(),
                        },
                    );
                }
            }
            Pending::Entity(start) => {
                if character == ';' {
                    self.emit(start..end, Kind::Entity);
                    return;
                }
                if character.is_ascii_alphanumeric() || character == '#' {
                    self.pending = Pending::Entity(start);
                    return;
                }
                self.emit(start..position, Kind::Text);
            }
            Pending::None => {}
        }
        self.begin(position, character);
    }

    fn begin(&mut self, position: usize, character: char) {
        let kind = match character {
            '\\' => {
                self.pending = Pending::Escape(position);
                return;
            }
            '!' => {
                self.pending = Pending::Bang(position);
                return;
            }
            '&' => {
                self.pending = Pending::Entity(position);
                return;
            }
            '*' | '_' | '~' | '`' => {
                self.pending = Pending::Run {
                    start: position,
                    byte: character as u8,
                    before: self.previous.unwrap_or(' '),
                    escaped: false,
                };
                return;
            }
            '[' => Kind::Open { image: false },
            ']' => Kind::Close,
            '<' => Kind::AngleOpen,
            '>' => Kind::AngleClose,
            '(' => Kind::ParenOpen,
            ')' => Kind::ParenClose,
            '\'' | '"' => Kind::Quote(character),
            '\n' => Kind::Break,
            _ => Kind::Text,
        };
        self.emit(position..position + character.len_utf8(), kind);
    }

    fn rank(&mut self, end: usize, start: Option<usize>) {
        if !self.indexed {
            return;
        }
        while self.ranked < end {
            self.starts.push(start == Some(self.ranked));
            self.ranked += 1;
        }
    }

    fn emit(&mut self, range: Range<usize>, kind: Kind) {
        if kind == Kind::Text
            && let Some(last) = self.ready.back_mut()
            && last.kind == Kind::Text
            && last.range.end == range.start
        {
            last.range.end = range.end;
            self.rank(range.end, None);
            return;
        }
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Token>());
        if kind.expands(range.clone()) {
            self.expansion = Some(range.start);
        }
        let opener = matches!(kind, Kind::Open { .. } | Kind::AngleOpen);
        if opener && !self.indexed {
            self.indexed = true;
            self.origin = range.start;
            self.ranked = range.start;
            self.ordinal_origin = self.emitted;
        }
        if self.indexed {
            self.openers.push(opener);
        }
        self.emitted += 1;
        self.rank(range.end, Some(range.start));
        self.ready.push_back(Token { range, kind });
    }
}

#[cfg(test)]
#[path = "../tests/inline_tokens.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/token_queue_work.rs"]
mod queue_work;
