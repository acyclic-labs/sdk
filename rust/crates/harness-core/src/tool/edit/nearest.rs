//! Exact substring edit distance, used only after an exact edit has been refused.
//! One row is retained, independent of file size. Every examined cell and retained
//! byte uses the original edit's custody; this never authorizes an approximate edit.
use super::*;
use std::{fmt, ops::Range};

#[derive(Clone, Copy)]
struct Cell {
    distance: usize,
    start: usize,
}

fn closest(
    source: &str,
    needle: &str,
    work: &mut acyclic_fs::NativeCustody,
) -> Result<Range<usize>> {
    charge(work, 0, needle.len() as u64)?;
    let mut characters = Vec::new();
    reserve_vec(work, &mut characters, needle.chars().count())?;
    characters.extend(needle.chars());
    let mut row = Vec::new();
    reserve_vec(work, &mut row, characters.len() + 1)?;
    row.extend((0..=characters.len()).map(|distance| Cell { distance, start: 0 }));
    let mut best = None;
    for (offset, character) in source.char_indices() {
        charge(work, 0, characters.len() as u64 + 1)?;
        let end = offset + character.len_utf8();
        let mut diagonal = row[0];
        row[0] = Cell {
            distance: 0,
            start: end,
        };
        for (index, expected) in characters.iter().enumerate() {
            let previous = row[index + 1];
            row[index + 1] = [
                Cell {
                    distance: diagonal.distance + usize::from(character != *expected),
                    start: diagonal.start,
                },
                Cell {
                    distance: previous.distance + 1,
                    start: previous.start,
                },
                Cell {
                    distance: row[index].distance + 1,
                    start: row[index].start,
                },
            ]
            .into_iter()
            .min_by_key(|cell| (cell.distance, cell.start))
            .expect("three candidates");
            diagonal = previous;
        }
        let cell = row[characters.len()];
        if cell.start < end {
            let candidate = (cell.distance, cell.start, end);
            if best.is_none_or(|best| candidate < best) {
                best = Some(candidate);
            }
        }
    }
    Ok(best.map_or(0..0, |(_, start, end)| start..end))
}

/// Removing a backslash only when it escapes a literal quote diagnoses accidental
/// double escaping without interpreting JSON, code, paths or other escape sequences.
fn quotes(text: &str) -> impl Iterator<Item = char> + '_ {
    let mut characters = text.chars().peekable();
    std::iter::from_fn(move || {
        let character = characters.next()?;
        if character == '\\'
            && characters
                .peek()
                .is_some_and(|next| matches!(next, '\'' | '"'))
        {
            characters.next()
        } else {
            Some(character)
        }
    })
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
            "closest span: lines {}-{}, UTF-8 bytes {}..{}; diff (- attempted, + actual): - {:?}; + {:?}",
            self.lines.start,
            self.lines.end,
            self.bytes.start,
            self.bytes.end,
            self.attempted,
            self.actual
        )?;
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
    charge(&scratch, 0, source.len() as u64 + needle.len() as u64)?;
    let first = source[..bytes.start]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1;
    let actual = &source[bytes.clone()];
    let last = first
        + actual
            .trim_end_matches('\n')
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count();
    let diagnostic = Diagnostic {
        attempted: needle,
        actual: &source[bytes.clone()],
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
