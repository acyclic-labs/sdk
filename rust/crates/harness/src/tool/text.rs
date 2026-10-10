//! Bounded UTF-8 selections and literal search, independent of storage or models.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};

fn finite(value: u64, field: &str) -> Result<()> {
    if value == 0 || value > crate::conversation::MAX_EXACT_JS_INTEGER {
        return Err(Error::Invalid(format!(
            "{field} must be positive and exactly representable"
        )));
    }
    Ok(())
}

/// Exact UTF-8 byte interval; offsets refer to the original immutable source.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TextRange {
    /// Inclusive byte offset.
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub start: u64,
    /// Exclusive byte offset.
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub end: u64,
}

/// Consumer-selected ceilings for an explicit partial read.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadOptions {
    /// Maximum verified source bytes, checked before source I/O by adapters.
    #[schemars(range(min = 1, max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub maximum_input_bytes: u64,
    /// Maximum selected UTF-8 bytes, independent of the source allowance.
    #[schemars(range(min = 1, max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub maximum_text_bytes: u64,
}

impl ReadOptions {
    /// Validates positive portable finite allowances.
    pub fn validate(self) -> Result<()> {
        finite(self.maximum_input_bytes, "read input byte bound")?;
        finite(self.maximum_text_bytes, "read text byte bound")
    }
}

/// One explicit partial read, including the exact omitted source intervals.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TextSelection {
    /// Selected source interval.
    pub range: TextRange,
    /// Exact selected UTF-8 with original line endings.
    pub text: String,
    /// Bytes excluded before the selection.
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub omitted_before: u64,
    /// Bytes excluded after the selection.
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub omitted_after: u64,
}

/// Selects exactly the requested interval, rejecting invalid UTF-8 boundaries.
/// No implicit rounding, truncation or normalization changes the request.
pub fn read_range(source: &str, range: TextRange, options: ReadOptions) -> Result<TextSelection> {
    options.validate()?;
    let source_bytes = source.len() as u64;
    let selected = range
        .end
        .checked_sub(range.start)
        .ok_or_else(|| Error::Invalid("read range is reversed".into()))?;
    if source_bytes > options.maximum_input_bytes
        || range.end > source_bytes
        || selected > options.maximum_text_bytes
    {
        return Err(Error::Invalid(
            "read range exceeds its explicit byte bounds".into(),
        ));
    }
    let start = usize::try_from(range.start)
        .map_err(|_| Error::Invalid("read start cannot be represented".into()))?;
    let end = usize::try_from(range.end)
        .map_err(|_| Error::Invalid("read end cannot be represented".into()))?;
    let text = source
        .get(start..end)
        .ok_or_else(|| Error::Invalid("read range splits a UTF-8 character".into()))?;
    Ok(TextSelection {
        range,
        text: text.to_owned(),
        omitted_before: range.start,
        omitted_after: source_bytes - range.end,
    })
}

/// Consumer-selected finite literal-search allowances.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchOptions {
    /// Maximum verified source bytes, checked before I/O by adapters.
    #[schemars(range(min = 1, max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub maximum_input_bytes: u64,
    /// Maximum nonempty literal needle bytes.
    #[schemars(range(min = 1, max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub maximum_query_bytes: u64,
    /// Maximum individual byte comparisons across all candidates.
    #[schemars(range(min = 1, max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub maximum_work: u64,
    /// Maximum retained match intervals; remaining matches are counted.
    #[schemars(range(min = 1, max = 4294967294_u32))]
    pub maximum_matches: u32,
}

impl SearchOptions {
    /// Validates finite work, source, query and retained-result allowances.
    pub fn validate(self) -> Result<()> {
        finite(self.maximum_input_bytes, "search input byte bound")?;
        finite(self.maximum_query_bytes, "search query byte bound")?;
        finite(self.maximum_work, "search comparison bound")?;
        if self.maximum_matches == 0 || self.maximum_matches == u32::MAX {
            return Err(Error::Invalid(
                "search match bound must be positive and finite".into(),
            ));
        }
        Ok(())
    }
}

/// Complete literal-search accounting with bounded retained match positions.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchMatches {
    /// Ordered, possibly overlapping exact UTF-8 match intervals.
    pub matches: Vec<TextRange>,
    /// All matches in the verified source, including omitted intervals.
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub total_matches: u64,
    /// Exact count of additional matches excluded from the retained result.
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub omitted_matches: u64,
    /// Individual byte comparisons actually performed.
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[cfg_attr(feature = "wasm", tsify(type = "number"))]
    pub work: u64,
}

/// Searches every legal candidate under an actual comparison counter.
///
/// Matching is case-sensitive, literal and includes overlaps. The source must
/// be fully scanned to return successful complete accounting. Exhausted work
/// returns an explicit error, never an incomplete result reported as complete.
pub fn literal_search(source: &str, query: &str, options: SearchOptions) -> Result<SearchMatches> {
    options.validate()?;
    if source.len() as u64 > options.maximum_input_bytes
        || query.is_empty()
        || query.len() as u64 > options.maximum_query_bytes
    {
        return Err(Error::Invalid(
            "search input or query exceeds its byte contract".into(),
        ));
    }
    let mut result = SearchMatches {
        matches: Vec::new(),
        total_matches: 0,
        omitted_matches: 0,
        work: 0,
    };
    if query.len() > source.len() {
        return Ok(result);
    }
    let last = source.len() - query.len();
    for offset in 0..=last {
        if !source.is_char_boundary(offset) {
            continue;
        }
        let end = offset + query.len();
        let candidate = source
            .as_bytes()
            .get(offset..end)
            .ok_or_else(|| Error::Invalid("search range cannot be represented".into()))?;
        let mut matched = true;
        for (actual, expected) in candidate.iter().zip(query.as_bytes()) {
            if result.work >= options.maximum_work {
                return Err(Error::Invalid(
                    "search exhausted its comparison allowance".into(),
                ));
            }
            result.work += 1;
            if actual != expected {
                matched = false;
                break;
            }
        }
        if matched {
            result.total_matches += 1;
            if result.matches.len() as u64 >= u64::from(options.maximum_matches) {
                result.omitted_matches += 1;
            } else {
                result.matches.push(TextRange {
                    start: offset as u64,
                    end: end as u64,
                });
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEARCH: SearchOptions = SearchOptions {
        maximum_input_bytes: 64,
        maximum_query_bytes: 16,
        maximum_work: 4096,
        maximum_matches: 1,
    };
    const READ: ReadOptions = ReadOptions {
        maximum_input_bytes: 64,
        maximum_text_bytes: 8,
    };

    #[test]
    fn read_records_omissions_and_preserves_exact_utf8_and_crlf() -> Result<()> {
        assert_eq!(
            read_range("a\r\n🦀\r\nb", TextRange { start: 3, end: 9 }, READ)?,
            TextSelection {
                range: TextRange { start: 3, end: 9 },
                text: "🦀\r\n".into(),
                omitted_before: 3,
                omitted_after: 1
            }
        );
        for range in [
            TextRange { start: 4, end: 9 },
            TextRange { start: 3, end: 6 },
            TextRange { start: 9, end: 8 },
            TextRange { start: 0, end: 11 },
        ] {
            assert!(read_range("a\r\n🦀\r\nb", range, READ).is_err());
        }
        assert_eq!(
            read_range("abc", TextRange { start: 3, end: 3 }, READ)?.text,
            ""
        );
        assert!(
            read_range(
                "abc",
                TextRange { start: 0, end: 3 },
                ReadOptions {
                    maximum_text_bytes: 2,
                    ..READ
                }
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn overlaps_omissions_and_work_are_explicit() -> Result<()> {
        assert_eq!(
            literal_search("aaa", "aa", SEARCH)?,
            SearchMatches {
                matches: vec![TextRange { start: 0, end: 2 }],
                total_matches: 2,
                omitted_matches: 1,
                work: 4
            }
        );
        assert_eq!(literal_search("🦀🦀", "🦀", SEARCH)?.total_matches, 2);
        assert!(
            literal_search(
                "aaa",
                "aa",
                SearchOptions {
                    maximum_work: 3,
                    ..SEARCH
                }
            )
            .is_err()
        );
        assert_eq!(literal_search("a", "long", SEARCH)?.work, 0);
        assert_eq!(literal_search("ABC", "abc", SEARCH)?.total_matches, 0);
        for options in [
            SearchOptions {
                maximum_input_bytes: 0,
                ..SEARCH
            },
            SearchOptions {
                maximum_query_bytes: 0,
                ..SEARCH
            },
            SearchOptions {
                maximum_work: u64::MAX,
                ..SEARCH
            },
            SearchOptions {
                maximum_matches: 0,
                ..SEARCH
            },
            SearchOptions {
                maximum_matches: u32::MAX,
                ..SEARCH
            },
        ] {
            assert!(literal_search("a", "a", options).is_err());
        }
        assert!(literal_search("a", "", SEARCH).is_err());
        Ok(())
    }

    #[test]
    fn bounded_search_agrees_with_all_legal_start_positions() -> Result<()> {
        for length in 0..=6 {
            for bits in 0..(1 << length) {
                let source: String = (0..length)
                    .map(|index| if bits & (1 << index) == 0 { "a" } else { "é" })
                    .collect();
                for query in ["a", "é", "aa", "aé", "éa", "éé", "aaé"] {
                    let expected: Vec<_> = source
                        .char_indices()
                        .filter_map(|(offset, _)| {
                            source
                                .get(offset..)
                                .filter(|tail| tail.starts_with(query))
                                .map(|_| TextRange {
                                    start: offset as u64,
                                    end: (offset + query.len()) as u64,
                                })
                        })
                        .collect();
                    let actual = literal_search(&source, query, SEARCH)?;
                    assert_eq!(actual.total_matches, expected.len() as u64);
                    assert_eq!(
                        actual.matches,
                        expected.iter().take(1).copied().collect::<Vec<_>>()
                    );
                    assert_eq!(
                        actual.omitted_matches,
                        expected.len().saturating_sub(1) as u64
                    );
                    assert!(actual.work <= SEARCH.maximum_work);
                }
            }
        }
        Ok(())
    }
}
