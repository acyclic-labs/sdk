#[cfg(test)]
use crate::work;

#[derive(Clone, Copy)]
pub(super) struct Opener {
    byte: u8,
    count: usize,
    both: bool,
}

#[derive(Default)]
pub(super) struct State(Vec<Opener>);

impl Clone for State {
    fn clone(&self) -> Self {
        #[cfg(test)]
        work::copy(self.0.len() * std::mem::size_of::<Opener>());
        Self(self.0.clone())
    }
}

impl State {
    pub(super) fn compile(candidates: &[(usize, Opener)], end: usize) -> Self {
        let mut state = Vec::<Opener>::new();
        let mut seen = 0u32;
        for (_, opener) in candidates
            .iter()
            .rev()
            .filter(|(position, _)| *position <= end)
        {
            #[cfg(test)]
            work::scan(1);
            let family = match opener.byte {
                b'*' => 0,
                b'_' => 1,
                _ => 2,
            };
            let bit = 1 << (family * 6 + opener.count % 3 * 2 + usize::from(opener.both));
            if seen & bit == 0 {
                seen |= bit;
                #[cfg(test)]
                work::copy(
                    (1 + if state.len() == state.capacity() {
                        state.len()
                    } else {
                        0
                    }) * std::mem::size_of::<Opener>(),
                );
                state.push(*opener);
            }
        }
        state.reverse();
        #[cfg(test)]
        work::copy(3 * (state.len() / 2) * std::mem::size_of::<Opener>());
        Self(state)
    }

    pub(super) fn prefix(&self, context: &str) -> String {
        if self.0.is_empty() {
            return context.to_owned();
        }
        let leading = context.starts_with('|');
        let mut prefix = String::from(if leading { "|x " } else { "x " });
        for opener in &self.0 {
            prefix.push_str(if opener.both { "." } else { " " });
            let count = (opener.count - 1) % 3 + 1;
            prefix.extend(std::iter::repeat_n(char::from(opener.byte), count));
            prefix.push_str(if opener.both { ". " } else { "a " });
        }
        prefix.push_str(context.get(usize::from(leading)..).unwrap_or_default());
        prefix
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.0.capacity() * std::mem::size_of::<Opener>()
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
