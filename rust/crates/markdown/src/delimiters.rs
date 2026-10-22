#[cfg(test)]
use crate::work;
use std::ops::Range;

#[derive(Clone, Copy)]
pub(super) struct Opener {
    byte: u8,
    count: usize,
    both: bool,
}

impl Opener {
    pub(super) const fn byte(self) -> u8 {
        self.byte
    }

    pub(super) const fn count(self) -> usize {
        self.count
    }

    // Openers in one class match the same later closers.
    pub(super) const fn class(self) -> u32 {
        let family = match self.byte {
            b'*' => 0,
            b'_' => 1,
            _ => 2,
        };
        1 << (family * 6 + self.count % 3 * 2 + self.both as usize)
    }
}

// An opener run ending at `end` (bounded by its text event). `start` is its
// run start when a reparse can resume there: no part of the run closes a span.
#[derive(Clone, Copy)]
pub(super) struct Candidate {
    pub(super) end: usize,
    pub(super) start: Option<usize>,
    pub(super) opener: Opener,
}

#[derive(Default)]
pub(super) struct State {
    openers: Vec<Opener>,
    // Index of the opener whose span a checkpoint resumes inside.
    reopen: Option<usize>,
}

impl Clone for State {
    fn clone(&self) -> Self {
        #[cfg(test)]
        work::copy(self.openers.len() * std::mem::size_of::<Opener>());
        Self {
            openers: self.openers.clone(),
            reopen: self.reopen,
        }
    }
}

impl State {
    // `marked` indexes a reopened span's opener among the candidates: the
    // openers before it and the span's own openers each keep one per class.
    // Also returns the index of the candidate behind each kept opener.
    pub(super) fn compile(
        candidates: &[Candidate],
        end: usize,
        marked: Option<usize>,
    ) -> (Self, Vec<usize>) {
        let mut state = Vec::<Opener>::new();
        let mut kept = Vec::new();
        let mut reopen = None;
        // Classes seen before and inside the reopened span.
        let mut seen = [0u32; 2];
        for (index, candidate) in candidates
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, candidate)| candidate.end <= end)
        {
            let opener = &candidate.opener;
            #[cfg(test)]
            work::scan(1);
            // The reopened span closes inside the reparse without a crossing
            // match, which removes its opener and children. Later closers then
            // match the latest earlier opener in their class, so it stays.
            let group = usize::from(marked.is_some_and(|marked| index >= marked));
            let bit = opener.class();
            if let Some(seen) = seen.get_mut(group)
                && *seen & bit == 0
            {
                *seen |= bit;
                #[cfg(test)]
                {
                    work::copy(std::mem::size_of::<Opener>());
                    work::grow(std::mem::size_of::<Opener>());
                }
                if Some(index) == marked {
                    reopen = Some(state.len());
                }
                state.push(*opener);
                kept.push(index);
            }
        }
        state.reverse();
        kept.reverse();
        #[cfg(test)]
        work::copy(3 * (state.len() / 2) * std::mem::size_of::<Opener>());
        (
            Self {
                reopen: reopen.map(|index| state.len() - 1 - index),
                openers: state,
            },
            kept,
        )
    }

    // The opener whose span a checkpoint resumes inside.
    pub(super) const fn reopen(&self) -> Option<usize> {
        self.reopen
    }

    // Synthetic left context, and each opener's run in it.
    pub(super) fn prefix(&self, context: &str) -> (String, Vec<Range<usize>>) {
        if self.openers.is_empty() {
            return (context.to_owned(), Vec::new());
        }
        let leading = context.starts_with('|');
        let mut prefix = String::from(if leading { "|x " } else { "x " });
        let mut runs = Vec::with_capacity(self.openers.len());
        for opener in &self.openers {
            prefix.push_str(if opener.both { "." } else { " " });
            let count = (opener.count - 1) % 3 + 1;
            runs.push(prefix.len()..prefix.len() + count);
            prefix.extend(std::iter::repeat_n(char::from(opener.byte), count));
            prefix.push_str(if opener.both { ". " } else { "a " });
        }
        prefix.push_str(context.get(usize::from(leading)..).unwrap_or_default());
        #[cfg(test)]
        work::grow(runs.len() * std::mem::size_of::<Range<usize>>());
        (prefix, runs)
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.openers.capacity() * std::mem::size_of::<Opener>()
    }
}

// Keep the latest unmatched opener in each compatibility class. A match across
// the synthetic prefix reopens the source block, so older members are needed
// only on that full parse. Run length modulo three preserves CommonMark's rule.
pub(super) fn scan(
    source: &str,
    position: usize,
    quoted_end: usize,
) -> Option<(usize, Option<Opener>)> {
    let byte = *source.as_bytes().get(position)?;
    let mut start = position;
    while start > quoted_end && source.as_bytes().get(start - 1) == Some(&byte) {
        start -= 1;
    }
    let count = source
        .get(start..)?
        .bytes()
        .take_while(|next| *next == byte)
        .count();
    let end = start + count;
    #[cfg(test)]
    work::scan(position - start + count);
    let before = source.get(..start)?.chars().next_back().unwrap_or(' ');
    let after = source.get(end..)?.chars().next().unwrap_or(' ');
    if !before.is_ascii() || !after.is_ascii() {
        return None;
    }
    let valid = byte != b'~' || count <= 2;
    let open = !after.is_whitespace()
        && (start == 0
            || byte == b'*' && !after.is_ascii_punctuation()
            || byte == b'~' && count > 1
            || before.is_whitespace()
            || before.is_ascii_punctuation());
    let close = start > 0
        && !before.is_whitespace()
        && (end == source.len()
            || (byte == b'*' || byte == b'~' && count > 1) && !before.is_ascii_punctuation()
            || byte == b'~' && before == '~'
            || after.is_whitespace()
            || after.is_ascii_punctuation());
    Some((
        end,
        (valid && open && end < source.len()).then_some(Opener {
            byte,
            count,
            both: close,
        }),
    ))
}

pub(super) fn safe_boundary(source: &str, end: usize) -> bool {
    let before = source.as_bytes().get(end.saturating_sub(1));
    !before.is_some_and(u8::is_ascii_punctuation) || before != source.as_bytes().get(end)
}

pub(super) fn text_frontier(text: &str, start: usize) -> usize {
    let Some((last, character)) = text.char_indices().next_back() else {
        return start;
    };
    let mut end = last;
    if character.is_ascii_punctuation() {
        while end > 0 && text.as_bytes().get(end - 1) == text.as_bytes().get(last) {
            end -= 1;
        }
    }
    start + end
}
