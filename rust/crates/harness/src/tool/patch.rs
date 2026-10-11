//! Exact, bounded V4A update-file transformation over immutable UTF-8.
//!
//! This accepts the `diff` fragment of an update operation, not paths or a
//! multi-file envelope. Publication stays with the caller's original task
//! context and its existing generation-checked content receipt.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use super::edit::AlgorithmWork;

/// Consumer-selected finite ceilings, pinned with the editing tool definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchLimits {
    /// Separate ceiling for source, diff and resulting UTF-8 bytes.
    pub maximum_bytes: u64,
    /// Maximum bytes examined while comparing context and anchor candidates.
    pub maximum_work: u64,
    /// Maximum number of hunks in one atomic file update.
    pub maximum_hunks: u32,
}

impl PatchLimits {
    /// Rejects absent and unbounded allowances before parsing or source I/O.
    pub fn validate(self) -> Result<()> {
        super::edit::validate_edit_bound(self.maximum_bytes)?;
        super::edit::validate_edit_bound(self.maximum_work)?;
        if self.maximum_hunks == 0 || self.maximum_hunks == u32::MAX {
            return Err(Error::Invalid(
                "patch hunk bound must be positive and finite".into(),
            ));
        }
        Ok(())
    }
}

struct Hunk<'a> {
    anchor: Option<&'a str>,
    lines: Vec<(u8, &'a str)>,
    at_end: bool,
}

fn line_text(line: &str) -> &str {
    // CRLF is a line terminator; a lone CR inside an unterminated line is text.
    line.strip_suffix('\n')
        .map_or(line, |text| text.strip_suffix('\r').unwrap_or(text))
}

fn parse<'a>(diff: &'a str, maximum_hunks: u32, meter: &mut AlgorithmWork) -> Result<Vec<Hunk<'a>>> {
    let mut hunks: Vec<Hunk<'_>> = Vec::new();
    let mut ended = false;
    for raw in diff.split_inclusive('\n') {
        let line = line_text(raw);
        if ended {
            return Err(meter.invalid("patch has data after its end-of-file marker"));
        }
        if line == "@@" || line.starts_with("@@ ") {
            if hunks.len() as u64 >= u64::from(maximum_hunks) {
                return Err(meter.invalid("patch exceeds hunk bound"));
            }
            let anchor = line.strip_prefix("@@ ");
            if anchor == Some("") {
                return Err(meter.invalid("patch anchor is empty"));
            }
            let length = hunks.len() + 1;
            meter.reserve_vec(&mut hunks, length)?;
            hunks.push(Hunk {
                anchor,
                lines: Vec::new(),
                at_end: false,
            });
        } else {
            let hunk = hunks
                .last_mut()
                .ok_or_else(|| meter.invalid("patch requires a hunk header"))?;
            if line == "*** End of File" {
                hunk.at_end = true;
                ended = true;
            } else {
                let prefix = line
                    .as_bytes()
                    .first()
                    .copied()
                    .ok_or_else(|| meter.invalid("patch line lacks a prefix"))?;
                if !matches!(prefix, b' ' | b'+' | b'-') {
                    return Err(meter.invalid("patch line has an unsupported prefix"));
                }
                let text = line
                    .get(1..)
                    .ok_or_else(|| meter.invalid("patch line prefix is invalid"))?;
                let length = hunk.lines.len() + 1;
                meter.reserve_vec(&mut hunk.lines, length)?;
                hunk.lines.push((prefix, text));
            }
        }
    }
    if hunks.is_empty()
        || hunks
            .iter()
            .any(|h| !h.lines.iter().any(|(kind, _)| matches!(kind, b'+' | b'-')))
    {
        return Err(meter.invalid("patch requires a change in every hunk"));
    }
    Ok(hunks)
}

fn same(left: &str, right: &str, remaining: &mut u64, meter: &AlgorithmWork) -> Result<bool> {
    let cost = left.len().max(right.len()).max(1) as u64;
    *remaining = remaining
        .checked_sub(cost)
        .ok_or_else(|| meter.invalid("patch exceeds context comparison work bound"))?;
    meter.examine(cost)?;
    Ok(left == right)
}

fn unique_anchor(source: &[&str], cursor: usize, anchor: &str, work: &mut u64, meter: &AlgorithmWork) -> Result<usize> {
    let mut found = None;
    for (index, line) in source.iter().enumerate().skip(cursor) {
        if same(line_text(line), anchor, work, meter)? {
            if found.is_some() {
                return Err(meter.conflict("patch anchor is ambiguous"));
            }
            found = Some(index);
        }
    }
    found
        .map(|index| index + 1)
        .ok_or_else(|| meter.conflict("patch anchor is missing"))
}

fn locate(source: &[&str], cursor: usize, hunk: &Hunk<'_>, work: &mut u64, meter: &AlgorithmWork) -> Result<usize> {
    let start = match hunk.anchor {
        Some(anchor) => unique_anchor(source, cursor, anchor, work, meter)?,
        None => cursor,
    };
    let old_length = hunk.lines.iter().filter(|(kind, _)| *kind != b'+').count();
    if old_length == 0 {
        // An insertion without context has no unique position except an
        // explicit anchor, EOF, or the only boundary in an empty file.
        return if hunk.at_end {
            Ok(source.len())
        } else if hunk.anchor.is_some() || source.is_empty() {
            Ok(start)
        } else {
            Err(meter.conflict("patch insertion needs context, an anchor or EOF"))
        };
    }
    let mut found = None;
    for offset in start..=source.len() {
        let Some(end) = offset
            .checked_add(old_length)
            .filter(|end| *end <= source.len())
        else {
            break;
        };
        if hunk.at_end && end != source.len() {
            continue;
        }
        let candidate = source
            .get(offset..end)
            .ok_or_else(|| meter.invalid("patch source range is invalid"))?;
        let mut matches = true;
        for (line, (_, expected)) in candidate.iter().zip(hunk.lines.iter().filter(|(kind, _)| *kind != b'+')) {
            if !same(line_text(line), expected, work, meter)? {
                matches = false;
                break;
            }
        }
        if matches {
            if found.is_some() {
                return Err(meter.conflict("patch context is ambiguous"));
            }
            found = Some(offset);
        }
    }
    found.ok_or_else(|| meter.conflict("patch context is missing"))
}

fn append(output: &mut String, text: &str, maximum: u64, meter: &mut AlgorithmWork) -> Result<()> {
    let length = output
        .len()
        .checked_add(text.len())
        .filter(|length| *length as u64 <= maximum)
        .ok_or_else(|| meter.invalid("patch output exceeds byte bound"))?;
    meter.reserve_string(output, length)?;
    meter.copy(text.len())?;
    output.push_str(text);
    Ok(())
}

/// Applies exact V4A context/add/delete hunks and optional `@@ anchor`/EOF.
///
/// Ambiguous/missing context and whitespace mismatches fail explicitly. Hunks
/// consume the original source in order; no fuzzy matching, rebasing or partial
/// publication occurs. Unchanged bytes retain their line endings. Added lines
/// use the source's first line-ending style, or LF when it has none.
pub fn apply_update(source: &str, diff: &str, limits: PatchLimits) -> Result<String> {
    limits.validate()?;
    apply_update_engine(source, diff, limits, &mut AlgorithmWork::ordinary())
}

pub(super) fn apply_update_engine(source: &str, diff: &str, limits: PatchLimits, meter: &mut AlgorithmWork) -> Result<String> {
    if limits.maximum_bytes == 0 || limits.maximum_bytes == u64::MAX
        || limits.maximum_work == 0 || limits.maximum_work == u64::MAX
        || limits.maximum_hunks == 0 || limits.maximum_hunks == u32::MAX
    {
        return Err(meter.invalid("patch limits must be positive and finite"));
    }
    if source.len() as u64 > limits.maximum_bytes || diff.len() as u64 > limits.maximum_bytes {
        return Err(meter.invalid("patch input exceeds byte bound"));
    }
    meter.retain(std::mem::size_of::<Vec<Hunk<'_>>>() + std::mem::size_of::<Vec<&str>>())?;
    let hunks = parse(diff, limits.maximum_hunks, meter)?;
    let mut source_lines = Vec::new();
    meter.reserve_vec(&mut source_lines, source.split_inclusive('\n').count())?;
    source_lines.extend(source.split_inclusive('\n'));
    let ending = source_lines
        .iter()
        .find(|line| line.ends_with('\n'))
        .map_or(
            "\n",
            |line| if line.ends_with("\r\n") { "\r\n" } else { "\n" },
        );
    let mut output = String::new();
    let mut cursor = 0;
    let mut work = limits.maximum_work;
    for hunk in &hunks {
        let start = locate(&source_lines, cursor, hunk, &mut work, meter)?;
        for line in source_lines
            .get(cursor..start)
            .ok_or_else(|| meter.invalid("patch hunks overlap"))?
        {
            append(&mut output, line, limits.maximum_bytes, meter)?;
        }
        cursor = start;
        for &(kind, text) in &hunk.lines {
            match kind {
                b' ' => {
                    let original = source_lines.get(cursor).ok_or_else(|| {
                        meter.conflict("patch context extends beyond source")
                    })?;
                    append(&mut output, original, limits.maximum_bytes, meter)?;
                    cursor += 1;
                }
                b'-' => {
                    cursor += 1;
                }
                b'+' => {
                    // Inserting after an unterminated context/anchor line
                    // creates a line boundary, never concatenates two lines.
                    if !output.is_empty() && !output.ends_with('\n') {
                        append(&mut output, ending, limits.maximum_bytes, meter)?;
                    }
                    append(&mut output, text, limits.maximum_bytes, meter)?;
                    append(&mut output, ending, limits.maximum_bytes, meter)?;
                }
                _ => return Err(meter.invalid("invalid parsed patch prefix")),
            }
        }
    }
    for line in source_lines
        .get(cursor..)
        .ok_or_else(|| meter.conflict("patch extends beyond source"))?
    {
        append(&mut output, line, limits.maximum_bytes, meter)?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: PatchLimits = PatchLimits {
        maximum_bytes: 1024,
        maximum_work: 8192,
        maximum_hunks: 8,
    };

    #[test]
    fn ordered_hunks_preserve_exact_context_utf8_and_line_endings() -> Result<()> {
        assert_eq!(
            apply_update(
                "a\r\n🦀\r\nb\r\nc",
                "@@\n a\n-🦀\n+日本語\n@@\n-b\n+B\n",
                LIMITS
            )?,
            "a\r\n日本語\r\nB\r\nc"
        );
        assert_eq!(
            apply_update("head\ntail\n", "@@ head\n+middle\n", LIMITS)?,
            "head\nmiddle\ntail\n"
        );
        assert_eq!(
            apply_update("a\na\n", "@@\n-a\n+last\n*** End of File\n", LIMITS)?,
            "a\nlast\n"
        );
        assert_eq!(apply_update("", "@@\n+new\n", LIMITS)?, "new\n");
        assert_eq!(
            apply_update("head", "@@ head\n+next\n", LIMITS)?,
            "head\nnext\n"
        );
        assert_eq!(
            apply_update("a\nb\nc\n", "@@\n-a\n+A\n b\n@@\n-c\n+C\n", LIMITS)?,
            "A\nb\nC\n"
        );
        Ok(())
    }

    #[test]
    fn malformed_ambiguous_out_of_order_and_unbounded_patches_fail() {
        for (source, diff) in [
            ("a\na\n", "@@\n-a\n+x\n"),
            ("a \n", "@@\n-a\n+x\n"),
            ("a\n", "@@\n+unanchored\n"),
            ("a\nb\n", "@@\n-b\n+B\n@@\n-a\n+A\n"),
            ("a\n", "*** Update File: other\n@@\n-a\n+x\n"),
            ("a\n", "@@\n a\n"),
            ("a\n", "@@\n-a\n+x\n*** End of File\n+extra\n"),
        ] {
            assert!(apply_update(source, diff, LIMITS).is_err());
        }
        for limits in [
            PatchLimits {
                maximum_bytes: 0,
                ..LIMITS
            },
            PatchLimits {
                maximum_bytes: u64::MAX,
                ..LIMITS
            },
            PatchLimits {
                maximum_work: 0,
                ..LIMITS
            },
            PatchLimits {
                maximum_work: u64::MAX,
                ..LIMITS
            },
            PatchLimits {
                maximum_hunks: 0,
                ..LIMITS
            },
            PatchLimits {
                maximum_hunks: u32::MAX,
                ..LIMITS
            },
            PatchLimits {
                maximum_work: 1,
                ..LIMITS
            },
            PatchLimits {
                maximum_hunks: 1,
                ..LIMITS
            },
            PatchLimits {
                maximum_bytes: 8,
                ..LIMITS
            },
        ] {
            assert!(apply_update("a\nb\n", "@@\n-a\n+A\n@@\n-b\n+B\n", limits).is_err());
        }
        // Input and diff each fit, but their combined output exceeds the cap.
        let source = format!("a\n{}", "z".repeat(29));
        assert!(
            apply_update(
                &source,
                "@@\n-a\n+1234\n",
                PatchLimits {
                    maximum_bytes: 32,
                    ..LIMITS
                }
            )
            .is_err()
        );
    }

    #[test]
    fn bounded_single_line_oracle_rejects_every_ambiguous_source() -> Result<()> {
        for length in 0..=6 {
            for bits in 0..(1 << length) {
                let lines: Vec<_> = (0..length)
                    .map(|index| {
                        if bits & (1 << index) == 0 {
                            "a\n"
                        } else {
                            "🦀\n"
                        }
                    })
                    .collect();
                let source = lines.concat();
                for needle in ["a\n", "🦀\n"] {
                    let count = lines.iter().filter(|line| **line == needle).count();
                    let diff = format!("@@\n-{}\n+X\n", needle.trim_end_matches('\n'));
                    let result = apply_update(&source, &diff, LIMITS);
                    if count == 1 {
                        assert_eq!(result?, source.replacen(needle, "X\n", 1));
                    } else {
                        assert!(matches!(result, Err(Error::Conflict(_))));
                    }
                }
            }
        }
        Ok(())
    }
}
