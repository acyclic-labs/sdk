//! Diagnostic matching after an exact edit has been refused. Normalized literal
//! matches take precedence; otherwise rolling line windows rank shared Unicode
//! characters and adjacent pairs by Dice similarity. Source work is O(N log M),
//! retained storage O(M), independent of file size. No candidate authorizes an edit.
use super::*;
use std::{fmt, ops::Range};

fn quoted(text: &str) -> impl Iterator<Item = (usize, char, usize)> + '_ {
    let mut characters = text.char_indices().peekable();
    std::iter::from_fn(move || {
        let (start, mut character) = characters.next()?;
        let mut end = start + character.len_utf8();
        if character == '\\'
            && let Some((offset, quoted)) = characters.next_if(|(_, c)| matches!(c, '\'' | '"'))
        {
            character = quoted;
            end = offset + quoted.len_utf8();
        }
        Some((start, character, end))
    })
}

fn normalized(text: &str) -> impl Iterator<Item = (usize, char, usize)> + '_ {
    quoted(text).filter(|(_, character, _)| !character.is_whitespace())
}

fn normalized_match(
    source: &str,
    pattern: &[char],
    work: &mut acyclic_fs::NativeCustody,
) -> Result<Option<Range<usize>>> {
    if pattern.is_empty() {
        return Ok(None);
    }
    let mut prefix = Vec::new();
    let mut starts = Vec::new();
    reserve_vec(work, &mut prefix, pattern.len())?;
    reserve_vec(work, &mut starts, pattern.len())?;
    // `prefix[i]` is the longest proper border of `pattern[..=i]`. Every
    // border is shorter than its prefix, so each lookup is in range; a
    // missing entry would only restart the automaton, never admit a match.
    let border = |prefix: &[usize], matched: usize| {
        matched
            .checked_sub(1)
            .and_then(|last| prefix.get(last))
            .copied()
            .unwrap_or(0)
    };
    prefix.push(0usize);
    for character in pattern.iter().skip(1) {
        charge(work, 0, 1)?;
        let mut k = border(&prefix, prefix.len());
        while k > 0 && pattern.get(k) != Some(character) {
            charge(work, 0, 1)?;
            k = border(&prefix, k);
        }
        prefix.push(k + usize::from(pattern.get(k) == Some(character)));
    }
    starts.resize(pattern.len(), 0usize);
    let mut matched = 0;
    for (position, (start, character, end)) in normalized(source).enumerate() {
        charge(work, 0, 1)?;
        if let Some(slot) = starts.get_mut(position % pattern.len()) {
            *slot = start;
        }
        while matched > 0 && pattern.get(matched) != Some(&character) {
            charge(work, 0, 1)?;
            matched = border(&prefix, matched);
        }
        matched += usize::from(pattern.get(matched) == Some(&character));
        if matched == pattern.len() {
            return Ok(starts
                .get((position + 1 - matched) % pattern.len())
                .map(|start| *start..end));
        }
    }
    Ok(None)
}

#[derive(Clone, Copy)]
struct Gram {
    key: u64,
    expected: usize,
    actual: usize,
}

fn grams(text: &str, mut visit: impl FnMut(u64)) {
    // Reset adjacency at line boundaries, so a rolling window can add/remove
    // each line independently without retaining an index of the source.
    for line in text.split_inclusive('\n') {
        let mut previous = None;
        for (_, character, _) in normalized(line) {
            let character = u64::from(character as u32);
            visit(character);
            if let Some(previous) = previous {
                visit(((previous + 1) << 21) | character);
            }
            previous = Some(character);
        }
    }
}

struct Window {
    grams: Vec<Gram>,
    common: usize,
    total: usize,
}

impl Window {
    fn update(&mut self, line: &str, add: bool, work: &acyclic_fs::NativeCustody) -> Result<()> {
        let search = u64::from(self.grams.len().max(1).ilog2()) + 2;
        charge(work, 0, (line.len() as u64).saturating_mul(2 * search))?;
        grams(line, |key| {
            if add {
                self.total += 1;
            } else {
                self.total -= 1;
            }
            if let Ok(index) = self.grams.binary_search_by_key(&key, |gram| gram.key)
                && let Some(gram) = self.grams.get_mut(index)
            {
                let before = gram.expected.min(gram.actual);
                if add {
                    gram.actual += 1;
                } else {
                    gram.actual -= 1;
                }
                self.common = self.common - before + gram.expected.min(gram.actual);
            }
        });
        Ok(())
    }
}

fn closest(
    source: &str,
    needle: &str,
    work: &mut acyclic_fs::NativeCustody,
) -> Result<Range<usize>> {
    charge(work, 0, source.len() as u64 + needle.len() as u64)?;
    let mut pattern = Vec::new();
    reserve_vec(work, &mut pattern, needle.chars().count())?;
    pattern.extend(normalized(needle).map(|(_, c, _)| c));
    if let Some(found) = normalized_match(source, &pattern, work)? {
        return Ok(found);
    }
    if pattern.is_empty()
        && let Some(start) = source.find(char::is_whitespace)
    {
        let end = start
            + text_range(source, start.., work)?
                .chars()
                .take_while(|c| c.is_whitespace())
                .take(needle.chars().count())
                .map(char::len_utf8)
                .sum::<usize>();
        return Ok(start..end);
    }
    ranked_window(source, needle, pattern.len(), work)
}

fn ranked_window(
    source: &str,
    needle: &str,
    scalars: usize,
    work: &mut acyclic_fs::NativeCustody,
) -> Result<Range<usize>> {
    let mut inventory = Vec::new();
    let capacity = scalars
        .checked_mul(2)
        .ok_or_else(|| capacity_overflow(work))?;
    reserve_vec(work, &mut inventory, capacity)?;
    grams(needle, |key| {
        inventory.push(Gram {
            key,
            expected: 1,
            actual: 0,
        })
    });
    charge(
        work,
        0,
        (inventory.len() as u64).saturating_mul(u64::from(inventory.len().max(1).ilog2()) + 1),
    )?;
    inventory.sort_unstable_by_key(|gram| gram.key);
    let expected = inventory.len();
    inventory.dedup_by(|next, previous| {
        if next.key != previous.key {
            return false;
        }
        previous.expected += next.expected;
        true
    });
    let mut window = Window {
        grams: inventory,
        common: 0,
        total: 0,
    };
    let lines = needle
        .strip_suffix('\n')
        .unwrap_or(needle)
        .bytes()
        .filter(|b| *b == b'\n')
        .count()
        + 1;
    let mut incoming = source.split_inclusive('\n');
    let outgoing = source.split_inclusive('\n');
    let (mut start, mut end) = (0, 0);
    for line in incoming.by_ref().take(lines) {
        window.update(line, true, work)?;
        end += line.len();
    }
    let mut best = (
        window.common,
        expected
            .checked_add(window.total)
            .ok_or_else(|| capacity_overflow(work))?,
        start..end,
    );
    for (line, removed) in incoming.zip(outgoing) {
        window.update(removed, false, work)?;
        start += removed.len();
        window.update(line, true, work)?;
        end += line.len();
        let total = expected
            .checked_add(window.total)
            .ok_or_else(|| capacity_overflow(work))?;
        if (window.common as u128) * (best.1 as u128) > (best.0 as u128) * (total as u128) {
            best = (window.common, total, start..end);
        }
    }
    let mut selected = best.2;
    let text = text_range(source, selected.clone(), work)?;
    if !needle.ends_with('\n') {
        if text.ends_with("\r\n") {
            if selected.len() > 2 {
                selected.end -= 2;
            }
        } else if selected.len() > 1 && text.ends_with('\n') {
            selected.end -= 1;
        }
    }
    Ok(selected)
}

/// Removing a backslash only when it escapes a literal quote diagnoses accidental
/// double escaping without interpreting JSON, code, paths or other escape sequences.
fn quotes(text: &str) -> impl Iterator<Item = char> + '_ {
    quoted(text).map(|(_, character, _)| character)
}

struct Diagnostic<'a> {
    attempted: &'a str,
    actual: &'a str,
    bytes: Range<usize>,
    lines: Range<usize>,
}

impl fmt::Display for Diagnostic<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "closest span: lines {}-{}, UTF-8 bytes {}..{}; diff (- attempted, + actual): - {:?}; + ",
            self.lines.start, self.lines.end, self.bytes.start, self.bytes.end, self.attempted
        )?;
        // Each end is sized by the attempted text, rather than by the selected
        // line's potentially unbounded length. Borrow UTF-8 slices throughout;
        // the original invocation's formatter owns the only retained copy.
        let width = self.attempted.chars().count().max(1);
        let head_end = self
            .actual
            .char_indices()
            .nth(width)
            .map_or(self.actual.len(), |(offset, _)| offset);
        let tail_start = self
            .actual
            .char_indices()
            .rev()
            .nth(width - 1)
            .map_or(0, |(offset, _)| offset);
        match (self.actual.get(..head_end), self.actual.get(tail_start..)) {
            (Some(head), Some(tail)) if head_end < tail_start => write!(
                f,
                "{:?}; [actual UTF-8 bytes {}..{} omitted; read the same file with range {{\"start\":{},\"end\":{}}}]; + {:?}",
                head,
                self.bytes.start + head_end,
                self.bytes.start + tail_start,
                self.bytes.start + head_end,
                self.bytes.start + tail_start,
                tail
            )?,
            _ => write!(f, "{:?}", self.actual)?,
        }
        if self.attempted != self.actual && quotes(self.attempted).eq(quotes(self.actual)) {
            f.write_str("; escaped-quote mismatch")?;
        } else if self.attempted != self.actual
            && self
                .attempted
                .chars()
                .filter(|c| !c.is_whitespace())
                .eq(self.actual.chars().filter(|c| !c.is_whitespace()))
        {
            f.write_str("; whitespace-only mismatch")?;
        } else if normalized(self.attempted)
            .map(|(_, c, _)| c)
            .eq(normalized(self.actual).map(|(_, c, _)| c))
        {
            f.write_str("; escaped-quote and whitespace mismatch")?;
        }
        Ok(())
    }
}

pub(super) fn rejection(
    source: &str,
    needle: &str,
    work: &acyclic_fs::NativeCustody,
) -> Result<Error> {
    // The returned refusal owns its diagnostic on the same original ledger,
    // independently of the planner's temporary buffers retired on failure.
    #[cfg(not(target_arch = "wasm32"))]
    let original = work.as_ref().map(|custody| custody.work());
    #[cfg(target_arch = "wasm32")]
    let original = None;
    let mut scratch = allocation(original, 0)?;
    let bytes = closest(source, needle, &mut scratch)?;
    // All search buffers are gone before the refusal retains its presentation.
    drop(scratch);
    charge(work, 0, source.len() as u64 + needle.len() as u64)?;
    let first = text_range(source, ..bytes.start, work)?
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1;
    let actual = text_range(source, bytes.clone(), work)?;
    let last = first
        + actual
            .strip_suffix('\n')
            .unwrap_or(actual)
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count();
    let diagnostic = Diagnostic {
        attempted: needle,
        actual,
        bytes,
        lines: first..last,
    };
    #[cfg(not(target_arch = "wasm32"))]
    let diagnostic = if let Some(original) = original {
        let (text, custody) = original.display(&diagnostic)?;
        (text, Some(custody))
    } else {
        (diagnostic.to_string(), None)
    };
    #[cfg(target_arch = "wasm32")]
    let diagnostic = (diagnostic.to_string(), None);
    Ok(Error::ExactEdit(EditRejection {
        occurrences: 0,
        diagnostic: Some(diagnostic),
    }))
}

#[cfg(test)]
mod tests;
