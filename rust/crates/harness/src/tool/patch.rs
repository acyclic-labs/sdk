//! Exact, bounded V4A update-file transformation over immutable UTF-8.
//!
//! This accepts the `diff` fragment of an update operation, not paths or a
//! multi-file envelope. Publication stays with the caller's original task
//! context and its existing generation-checked content receipt.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};

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

fn parse(diff: &str, maximum_hunks: u32) -> Result<Vec<Hunk<'_>>> {
    let mut hunks: Vec<Hunk<'_>> = Vec::new();
    let mut ended = false;
    for raw in diff.split_inclusive('\n') {
        let line = line_text(raw);
        if ended {
            return Err(Error::Invalid(
                "patch has data after its end-of-file marker".into(),
            ));
        }
        if line == "@@" || line.starts_with("@@ ") {
            if hunks.len() as u64 >= u64::from(maximum_hunks) {
                return Err(Error::Invalid("patch exceeds hunk bound".into()));
            }
            let anchor = line.strip_prefix("@@ ");
            if anchor == Some("") {
                return Err(Error::Invalid("patch anchor is empty".into()));
            }
            hunks.push(Hunk {
                anchor,
                lines: Vec::new(),
                at_end: false,
            });
        } else {
            let hunk = hunks
                .last_mut()
                .ok_or_else(|| Error::Invalid("patch requires a hunk header".into()))?;
            if line == "*** End of File" {
                hunk.at_end = true;
                ended = true;
            } else {
                let prefix = line
                    .as_bytes()
                    .first()
                    .copied()
                    .ok_or_else(|| Error::Invalid("patch line lacks a prefix".into()))?;
                if !matches!(prefix, b' ' | b'+' | b'-') {
                    return Err(Error::Invalid(
                        "patch line has an unsupported prefix".into(),
                    ));
                }
                let text = line
                    .get(1..)
                    .ok_or_else(|| Error::Invalid("patch line prefix is invalid".into()))?;
                hunk.lines.push((prefix, text));
            }
        }
    }
    if hunks.is_empty()
        || hunks
            .iter()
            .any(|h| !h.lines.iter().any(|(kind, _)| matches!(kind, b'+' | b'-')))
    {
        return Err(Error::Invalid(
            "patch requires a change in every hunk".into(),
        ));
    }
    Ok(hunks)
}

fn same(left: &str, right: &str, remaining: &mut u64) -> Result<bool> {
    let cost = left.len().max(right.len()).max(1) as u64;
    *remaining = remaining
        .checked_sub(cost)
        .ok_or_else(|| Error::Invalid("patch exceeds context comparison work bound".into()))?;
    Ok(left == right)
}

fn unique_anchor(source: &[&str], cursor: usize, anchor: &str, work: &mut u64) -> Result<usize> {
    let mut found = None;
    for (index, line) in source.iter().enumerate().skip(cursor) {
        if same(line_text(line), anchor, work)? {
            if found.is_some() {
                return Err(Error::Conflict("patch anchor is ambiguous".into()));
            }
            found = Some(index);
        }
    }
    found
        .map(|index| index + 1)
        .ok_or_else(|| Error::Conflict("patch anchor is missing".into()))
}

fn locate(source: &[&str], cursor: usize, hunk: &Hunk<'_>, work: &mut u64) -> Result<usize> {
    let start = match hunk.anchor {
        Some(anchor) => unique_anchor(source, cursor, anchor, work)?,
        None => cursor,
    };
    let old: Vec<_> = hunk
        .lines
        .iter()
        .filter(|(kind, _)| *kind != b'+')
        .map(|(_, text)| *text)
        .collect();
    if old.is_empty() {
        // An insertion without context has no unique position except an
        // explicit anchor, EOF, or the only boundary in an empty file.
        return if hunk.at_end {
            Ok(source.len())
        } else if hunk.anchor.is_some() || source.is_empty() {
            Ok(start)
        } else {
            Err(Error::Conflict(
                "patch insertion needs context, an anchor or EOF".into(),
            ))
        };
    }
    let mut found = None;
    for offset in start..=source.len() {
        let Some(end) = offset
            .checked_add(old.len())
            .filter(|end| *end <= source.len())
        else {
            break;
        };
        if hunk.at_end && end != source.len() {
            continue;
        }
        let candidate = source
            .get(offset..end)
            .ok_or_else(|| Error::Invalid("patch source range is invalid".into()))?;
        let mut matches = true;
        for (line, expected) in candidate.iter().zip(&old) {
            if !same(line_text(line), expected, work)? {
                matches = false;
                break;
            }
        }
        if matches {
            if found.is_some() {
                return Err(Error::Conflict("patch context is ambiguous".into()));
            }
            found = Some(offset);
        }
    }
    found.ok_or_else(|| Error::Conflict("patch context is missing".into()))
}

fn append(output: &mut String, text: &str, maximum: u64) -> Result<()> {
    let length = output
        .len()
        .checked_add(text.len())
        .filter(|length| *length as u64 <= maximum)
        .ok_or_else(|| Error::Invalid("patch output exceeds byte bound".into()))?;
    output
        .try_reserve(length - output.len())
        .map_err(|_| Error::Invalid("patch output allocation failed".into()))?;
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
    if source.len() as u64 > limits.maximum_bytes || diff.len() as u64 > limits.maximum_bytes {
        return Err(Error::Invalid("patch input exceeds byte bound".into()));
    }
    let hunks = parse(diff, limits.maximum_hunks)?;
    let source_lines: Vec<_> = source.split_inclusive('\n').collect();
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
        let start = locate(&source_lines, cursor, hunk, &mut work)?;
        for line in source_lines
            .get(cursor..start)
            .ok_or_else(|| Error::Invalid("patch hunks overlap".into()))?
        {
            append(&mut output, line, limits.maximum_bytes)?;
        }
        cursor = start;
        for &(kind, text) in &hunk.lines {
            match kind {
                b' ' => {
                    let original = source_lines.get(cursor).ok_or_else(|| {
                        Error::Conflict("patch context extends beyond source".into())
                    })?;
                    append(&mut output, original, limits.maximum_bytes)?;
                    cursor += 1;
                }
                b'-' => {
                    cursor += 1;
                }
                b'+' => {
                    // Inserting after an unterminated context/anchor line
                    // creates a line boundary, never concatenates two lines.
                    if !output.is_empty() && !output.ends_with('\n') {
                        append(&mut output, ending, limits.maximum_bytes)?;
                    }
                    append(&mut output, text, limits.maximum_bytes)?;
                    append(&mut output, ending, limits.maximum_bytes)?;
                }
                _ => return Err(Error::Invalid("invalid parsed patch prefix".into())),
            }
        }
    }
    for line in source_lines
        .get(cursor..)
        .ok_or_else(|| Error::Conflict("patch extends beyond source".into()))?
    {
        append(&mut output, line, limits.maximum_bytes)?;
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
