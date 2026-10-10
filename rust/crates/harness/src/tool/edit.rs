//! Portable exact text replacement, independent of model family and storage.

use crate::{Error, Result};

/// Rejects unbounded or empty allowances at both transformation and I/O boundaries.
pub(crate) fn validate_edit_bound(maximum_bytes: u64) -> Result<()> {
    if maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(Error::Invalid(
            "edit byte bound must be positive and finite".into(),
        ));
    }
    Ok(())
}

/// Replaces one unambiguous exact occurrence within finite input/output bounds.
///
/// Matching includes overlapping occurrences. Empty needles, missing text and
/// ambiguous matches fail before allocation. Callers publish the returned text
/// through their existing generation-checked Filesystem transaction; this pure
/// transformation does not acquire authority or mutate a workspace.
pub fn exact_replace(
    source: &str,
    old_text: &str,
    new_text: &str,
    maximum_bytes: u64,
) -> Result<String> {
    validate_edit_bound(maximum_bytes)?;
    if source.len() as u64 > maximum_bytes
        || old_text.len() as u64 > maximum_bytes
        || new_text.len() as u64 > maximum_bytes
        || old_text.is_empty()
    {
        return Err(Error::Invalid(
            "exact edit has invalid text or byte bounds".into(),
        ));
    }
    let start = source
        .find(old_text)
        .ok_or_else(|| Error::Conflict("exact edit text is missing".into()))?;
    // Advance one character, rather than one match, to detect overlaps without
    // slicing inside a UTF-8 code point.
    let first_character_bytes = old_text.chars().next().map_or(0, char::len_utf8);
    let next_start = start + first_character_bytes;
    if source
        .get(next_start..)
        .is_some_and(|tail| tail.contains(old_text))
    {
        return Err(Error::Conflict("exact edit text is ambiguous".into()));
    }
    let output_bytes = source
        .len()
        .checked_sub(old_text.len())
        .and_then(|length| length.checked_add(new_text.len()))
        .filter(|length| *length as u64 <= maximum_bytes)
        .ok_or_else(|| Error::Invalid("exact edit output exceeds byte bound".into()))?;
    let mut output = String::new();
    output
        .try_reserve_exact(output_bytes)
        .map_err(|_| Error::Invalid("exact edit output allocation failed".into()))?;
    let end = start + old_text.len();
    let prefix = source
        .get(..start)
        .ok_or_else(|| Error::Invalid("exact edit match is not UTF-8 aligned".into()))?;
    let suffix = source
        .get(end..)
        .ok_or_else(|| Error::Invalid("exact edit match is not UTF-8 aligned".into()))?;
    output.push_str(prefix);
    output.push_str(new_text);
    output.push_str(suffix);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_edit_preserves_utf8_and_rejects_overlapping_matches() -> Result<()> {
        assert_eq!(
            exact_replace("before 🦀 after", "🦀", "日本語", 64)?,
            "before 日本語 after"
        );
        assert_eq!(exact_replace("a\r\nb\n", "\r\n", "\n", 8)?, "a\nb\n");
        assert!(matches!(
            exact_replace("aaa", "aa", "b", 8),
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            exact_replace("ééé", "éé", "b", 8),
            Err(Error::Conflict(_))
        ));
        assert!(exact_replace("one one", "one", "two", 8).is_err());
        assert!(exact_replace("one", "missing", "two", 8).is_err());
        assert!(exact_replace("one", "", "two", 8).is_err());
        Ok(())
    }

    #[test]
    fn exact_edit_checks_input_and_output_byte_bounds() -> Result<()> {
        assert_eq!(exact_replace("abc", "b", "", 3)?, "ac");
        assert_eq!(exact_replace("abc", "abc", "xyz", 3)?, "xyz");
        assert!(exact_replace("abc", "b", "long", 4).is_err());
        assert!(exact_replace("abc", "abc", "a", 2).is_err());
        assert!(exact_replace("abc", "a", "a", 0).is_err());
        assert!(exact_replace("abc", "a", "a", u64::MAX).is_err());
        assert!(exact_replace("é", "é", "🦀", 3).is_err());
        Ok(())
    }

    #[test]
    fn bounded_matching_agrees_with_all_utf8_start_positions() {
        // Exhaust all binary strings through six characters, including repeated
        // multibyte characters. The oracle counts every legal starting position.
        for alphabet in [['a', 'b'], ['é', '🦀']] {
            for length in 1..=6 {
                for bits in 0..(1 << length) {
                    let source: String = (0..length)
                        .map(|position| alphabet[(bits >> position) & 1])
                        .collect();
                    for start in 0..length {
                        for end in start + 1..=length {
                            let needle: String =
                                source.chars().skip(start).take(end - start).collect();
                            let matches: Vec<usize> = source
                                .char_indices()
                                .filter_map(|(offset, _)| {
                                    source
                                        .get(offset..)
                                        .filter(|tail| tail.starts_with(&needle))
                                        .map(|_| offset)
                                })
                                .collect();
                            let result = exact_replace(&source, &needle, "X", 64);
                            if matches.len() == 1 {
                                assert_eq!(result.ok(), Some(source.replacen(&needle, "X", 1)));
                            } else {
                                assert!(matches!(result, Err(Error::Conflict(_))));
                            }
                        }
                    }
                }
            }
        }
    }
}
