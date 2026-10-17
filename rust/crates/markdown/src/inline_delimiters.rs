use std::ops::Range;
#[path = "inline_punctuation.rs"]
mod punctuation;
use punctuation::punctuation;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Heads {
    heads: [usize; 3],
    tilde_floor: usize,
}

impl Heads {
    fn depth(self) -> usize {
        self.heads.into_iter().max().unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Snapshot {
    head: usize,
    tilde_floor: usize,
}

impl Snapshot {
    pub(super) fn parts(self) -> (usize, usize) {
        (self.head, self.tilde_floor)
    }
    pub(super) fn from_parts(head: usize, tilde_floor: usize) -> Self {
        Self { head, tilde_floor }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Flank {
    Space,
    Punctuation,
    Tilde,
    Other,
}
impl From<char> for Flank {
    fn from(c: char) -> Self {
        if c.is_whitespace() {
            Self::Space
        } else if c == '~' {
            Self::Tilde
        } else if punctuation(c) {
            Self::Punctuation
        } else {
            Self::Other
        }
    }
}
impl Flank {
    fn is_whitespace(self) -> bool {
        self == Self::Space
    }
    fn punctuation(self) -> bool {
        matches!(self, Self::Punctuation | Self::Tilde)
    }
}

#[derive(Clone, Debug)]
pub(super) struct Run {
    pub(super) range: Range<usize>,
    pub(super) byte: u8,
    pub(super) open: bool,
    pub(super) close: bool,
    pub(super) cursor: usize,
}

impl Run {
    pub(super) fn new(
        range: Range<usize>,
        byte: u8,
        before: impl Into<Flank>,
        after: impl Into<Flank>,
    ) -> Self {
        let (before, after) = (before.into(), after.into());
        let intraword = byte == b'*' || byte == b'~' && range.len() == 2;
        let open = !after.is_whitespace()
            && (byte == b'~' && range.len() == 2
                || intraword && !after.punctuation()
                || before.is_whitespace()
                || before.punctuation());
        let close = !before.is_whitespace()
            && (intraword && !before.punctuation()
                || byte == b'~' && before == Flank::Tilde
                || after.is_whitespace()
                || after.punctuation());
        Self {
            range,
            byte,
            open,
            close,
            cursor: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Matched {
    pub(super) opener: Range<usize>,
    pub(super) closer: Range<usize>,
    pub(super) byte: u8,
    pub(super) opener_cursor: usize,
    pub(super) closer_cursor: usize,
}

#[derive(Clone)]
struct Entry {
    before: [usize; 3],
    candidates: [usize; 3],
    range: Range<usize>,
    original: usize,
    first: usize,
    both: bool,
    class: u8,
    cursor: usize,
    #[cfg(test)]
    byte: u8,
}

impl Entry {
    fn own(&self) -> usize {
        if self.class == 2 {
            self.original - 1
        } else {
            self.original % 3
        }
    }
    fn candidate(&self, id: usize, slot: usize) -> usize {
        let own = self.own();
        if slot == own || slot == 3 && !self.both {
            id
        } else {
            self.candidates
                .get(slot - usize::from(slot > own))
                .copied()
                .unwrap_or_default()
        }
    }
}

#[derive(Default)]
pub(super) struct Delimiters {
    entries: Vec<Vec<Entry>>,
    length: usize,
    heads: Heads,
    #[cfg(test)]
    scan: bool,
}

fn group(byte: u8) -> u8 {
    match byte {
        b'*' => 0,
        b'_' => 1,
        _ => 2,
    }
}

impl Delimiters {
    pub(super) fn reset(&mut self) {
        self.entries.clear();
        self.length = 0;
        self.heads = Heads::default();
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            head: self.heads.depth(),
            tilde_floor: self.heads.tilde_floor,
        }
    }

    pub(super) fn restore(&mut self, snapshot: Snapshot) {
        let mut heads = if let Some(entry) = self.entry(snapshot.head) {
            let mut heads = Heads {
                heads: entry.before,
                tilde_floor: 0,
            };
            if let Some(head) = heads.heads.get_mut(usize::from(entry.class)) {
                *head = snapshot.head;
            }
            heads
        } else {
            Heads::default()
        };
        heads.tilde_floor = snapshot.tilde_floor;
        self.heads = heads;
    }

    pub(super) fn first(&self) -> Option<usize> {
        self.heads
            .heads
            .into_iter()
            .max()
            .and_then(|id| self.entry(id))
            .map(|entry| entry.first)
    }

    pub(super) fn neutral(&self) -> bool {
        self.heads == Heads::default()
    }

    fn entry(&self, id: usize) -> Option<&Entry> {
        let index = id.checked_sub(1)?;
        let (page, offset) = super::forest::slot(index);
        self.entries.get(page)?.get(offset)
    }

    fn push(&mut self, range: Range<usize>, original: usize, byte: u8, both: bool, cursor: usize) {
        let class = group(byte);
        let head = self
            .heads
            .heads
            .get(usize::from(class))
            .copied()
            .unwrap_or_default();
        let inherited: [usize; 4] = self.entry(head).map_or([0; 4], |entry| {
            std::array::from_fn(|slot| entry.candidate(head, slot))
        });
        let id = self.length + 1;
        let own = if byte == b'~' {
            original - 1
        } else {
            original % 3
        };
        let candidates = std::array::from_fn(|slot| {
            inherited
                .get(slot + usize::from(slot >= own))
                .copied()
                .unwrap_or_default()
        });
        let first = self.first().unwrap_or(range.start);
        #[cfg(test)]
        {
            crate::work::scan(1);
            crate::work::copy(2 * std::mem::size_of::<[usize; 3]>());
        }
        if super::forest::slot(self.length).1 == 0 {
            #[cfg(test)]
            if self.entries.len() == self.entries.capacity() {
                crate::work::copy(self.entries.len() * std::mem::size_of::<Vec<Entry>>());
            }
            self.entries
                .push(super::forest::new_page(self.entries.len()));
        }
        let Some(page) = self.entries.last_mut() else {
            return;
        };
        self.length += 1;
        super::forest::push_page(
            page,
            Entry {
                before: self.heads.heads,
                candidates,
                range,
                original,
                first,
                both,
                class,
                cursor,
                #[cfg(test)]
                byte,
            },
        );
        if let Some(head) = self.heads.heads.get_mut(usize::from(class)) {
            *head = id;
        }
    }

    fn candidate(&self, byte: u8, count: usize, both: bool) -> Option<usize> {
        #[cfg(test)]
        if self.scan {
            let mut cursor = self.heads.heads.into_iter().max().unwrap_or_default();
            while let Some(entry) = self.entry(cursor) {
                crate::work::scan(1);
                if entry.byte == byte
                    && (byte != b'~'
                        || entry.original == count
                            && entry.before.into_iter().max().unwrap_or(0)
                                >= self.heads.tilde_floor)
                    && (!both && !entry.both
                        || !(entry.original + count).is_multiple_of(3)
                        || count.is_multiple_of(3))
                {
                    return Some(cursor);
                }
                cursor = entry.before.into_iter().max().unwrap_or_default();
            }
            return None;
        }
        let head = self.heads.heads.get(usize::from(group(byte))).copied()?;
        let entry = self.entry(head)?;
        if byte == b'~' {
            #[cfg(test)]
            crate::work::scan(1);
            return Some(entry.candidate(head, count.checked_sub(1)?)).filter(|id| {
                self.entry(*id).is_some_and(|entry| {
                    entry.before.into_iter().max().unwrap_or(0) >= self.heads.tilde_floor
                })
            });
        }
        let mut latest = 0;
        for residue in 0..3 {
            let id = entry.candidate(head, residue);
            #[cfg(test)]
            crate::work::scan(1);
            if count.is_multiple_of(3) || !(count + residue).is_multiple_of(3) {
                latest = latest.max(id);
            }
        }
        if !both {
            latest = latest.max(entry.candidate(head, 3));
        }
        (latest != 0).then_some(latest)
    }

    pub(super) fn feed(
        &mut self,
        run: Run,
        mut matched: impl FnMut(Matched) -> (usize, usize),
    ) -> Range<usize> {
        let original = run.range.len();
        self.feed_original(run, original, |value, _| matched(value))
    }

    pub(super) fn feed_original(
        &mut self,
        run: Run,
        original: usize,
        mut matched: impl FnMut(Matched, Snapshot) -> (usize, usize),
    ) -> Range<usize> {
        if original == 0 || run.byte == b'~' && original > 2 {
            return run.range;
        }
        let both = run.open && run.close;
        let mut remaining = run.range;
        let mut cursor = run.cursor;
        while run.close && !remaining.is_empty() {
            let Some(id) = self.candidate(run.byte, original, both) else {
                // The pinned parser shares this floor across both tilde lengths.
                if run.byte == b'~' {
                    self.heads.tilde_floor = self.heads.depth();
                }
                break;
            };
            let Some(entry) = self.entry(id) else {
                break;
            };
            let (before, opener, opener_length, opener_both, opener_cursor) = (
                entry.before,
                entry.range.clone(),
                entry.original,
                entry.both,
                entry.cursor,
            );
            let count = opener.len().min(remaining.len());
            let open = opener.end - count..opener.end;
            let close = remaining.start..remaining.start + count;
            let floor = self
                .heads
                .tilde_floor
                .min(before.into_iter().max().unwrap_or(0));
            let heads = self.snapshot();
            self.heads = Heads {
                heads: before,
                tilde_floor: floor,
            };
            self.heads.tilde_floor = floor;
            remaining.start += count;
            let opener_end = open.start;
            let (left, right) = matched(
                Matched {
                    opener: open,
                    closer: close,
                    byte: run.byte,
                    opener_cursor,
                    closer_cursor: cursor,
                },
                heads,
            );
            if opener.start < opener_end {
                self.push(
                    opener.start..opener_end,
                    opener_length,
                    run.byte,
                    opener_both,
                    left,
                );
            }
            cursor = right;
        }
        if run.open && !remaining.is_empty() {
            self.push(remaining.clone(), original, run.byte, both, cursor);
        }
        remaining
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<Vec<Entry>>()
            + self
                .entries
                .iter()
                .map(|page| page.capacity() * std::mem::size_of::<Entry>())
                .sum::<usize>()
    }
}

#[cfg(test)]
#[path = "../tests/inline_delimiters.rs"]
mod tests;
