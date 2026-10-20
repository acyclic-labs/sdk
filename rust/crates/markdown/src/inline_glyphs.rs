use crate::Text;
use std::ops::Range;

#[path = "inline_rank.rs"]
pub(in crate::inline_scan) mod rank;

#[derive(Default)]
pub(super) struct Input {
    origin: Option<usize>,
    raw: Text,
    code: Text,
    nonspace: rank::Rank,
    nonhorizontal: rank::Rank,
    breaks: Vec<usize>,
    break_counts: rank::Rank,
    indent: rank::Rank,
    leading: bool,
}

impl Input {
    fn feed(&mut self, source: &str, start: usize) {
        if self.origin.is_none() {
            self.leading = true;
        }
        let origin = *self.origin.get_or_insert(start);
        let Some(delta) = source
            .get(origin..)
            .and_then(|source| source.get(self.raw.len()..))
        else {
            return;
        };
        #[cfg(test)]
        {
            crate::work::render_feed(delta.len());
        }
        let raw_start = self.raw.len();
        self.raw.push_str(delta);
        let mut normalized = String::with_capacity(delta.len());
        for (offset, character) in delta.char_indices() {
            let indent = self.leading && matches!(character, ' ' | '\t');
            self.leading = indent || matches!(character, '\r' | '\n');
            if matches!(character, '\r' | '\n') {
                #[cfg(test)]
                if self.breaks.len() == self.breaks.capacity() {
                    crate::work::copy(self.breaks.len() * std::mem::size_of::<usize>());
                }
                self.breaks.push(origin + raw_start + offset);
            }
            let value = match character {
                '\r' | '\n' => ' ',
                c => c,
            };
            normalized.push(value);
            for byte in 0..character.len_utf8() {
                self.nonspace.push(value != ' ');
                self.nonhorizontal
                    .push(!matches!(character, ' ' | '\t' | '\x0b' | '\x0c'));
                self.break_counts
                    .push(byte == 0 && matches!(character, '\r' | '\n'));
                self.indent.push(indent);
            }
        }
        #[cfg(test)]
        crate::work::copy(normalized.len());
        debug_assert_eq!(
            normalized.len(),
            delta.len(),
            "normalization preserves byte offsets"
        );
        self.code.push_str(&normalized);
    }

    fn text(&self, source: &str, range: Range<usize>) -> Option<Text> {
        let origin = self.origin?;
        self.raw.source_range(
            range.start.checked_sub(origin)?..range.end.checked_sub(origin)?,
            source.get(origin..)?,
        )
    }

    fn literal_range(&self, range: Range<usize>) -> Option<Range<usize>> {
        let origin = self.origin?;
        let (left, right) = (
            range.start.checked_sub(origin)?,
            range.end.checked_sub(origin)?,
        );
        if self.break_counts.before(right)? != self.break_counts.before(left)? {
            return None;
        }
        let skipped = self.indent.before(right)? - self.indent.before(left)?;
        Some(range.start + skipped..range.end)
    }

    fn trim_end(&self, range: Range<usize>) -> Option<usize> {
        let origin = self.origin?;
        let end = range.end.checked_sub(origin)?;
        Some(
            self.nonhorizontal
                .last_before(end)
                .map_or(origin, |last| origin + last + 1)
                .max(range.start),
        )
    }

    fn code(&self, range: Range<usize>, source: &str) -> Option<Text> {
        let origin = self.origin?;
        let (mut start, mut end) = (
            range.start.checked_sub(origin)?,
            range.end.checked_sub(origin)?,
        );
        let source = source.get(origin..)?;
        source.get(start..end)?;
        if start == end {
            return Some(Text::default());
        }
        let space = |byte: &u8| matches!(byte, b' ' | b'\r' | b'\n');
        if source.as_bytes().get(start).is_some_and(space)
            && source
                .as_bytes()
                .get(end.checked_sub(1)?)
                .is_some_and(space)
            && self.nonspace.before(end)? > self.nonspace.before(start)?
        {
            start += 1;
            end -= 1;
        }
        self.code.source_range(start..end, source)
    }

    fn newlines(&self, range: Range<usize>) -> Option<impl Iterator<Item = usize> + '_> {
        let origin = self.origin?;
        let first = self.break_counts.before(range.start.checked_sub(origin)?)?;
        let end = self.break_counts.before(range.end.checked_sub(origin)?)?;
        Some(self.breaks.get(first..end)?.iter().copied())
    }

    #[cfg(test)]
    fn retained(&self, texts: &mut std::collections::HashSet<usize>) -> (usize, usize) {
        let (raw, raw_meta) = self.raw.retained(texts);
        let (code, code_meta) = self.code.retained(texts);
        (
            raw + code,
            raw_meta
                + code_meta
                + self.nonspace.retained()
                + self.nonhorizontal.retained()
                + self.break_counts.retained()
                + self.indent.retained()
                + self.breaks.capacity() * std::mem::size_of::<usize>(),
        )
    }
}

#[path = "inline_events.rs"]
pub(in crate::inline_scan) mod events;
pub(in crate::inline_scan) use events::Cache;

#[cfg(test)]
thread_local! { static REPLAY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_glyphs.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/inline_render_work.rs"]
mod work_tests;
