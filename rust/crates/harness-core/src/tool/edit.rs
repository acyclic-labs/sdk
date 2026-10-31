//! Portable exact text replacement.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode::{Conflict, Invalid};
use crate::{Error, NativeCustody, Result};

/// A definitive exact-match refusal from the shared edit engine.
/// Native presentation retains the original cause rather than interpreting
/// diagnostic text or repeating the edit against a later source.
#[derive(Debug)]
pub struct EditRejection {
    occurrences: usize,
    presentation: Option<(String, EditPresentation)>,
    _custody: NativeCustody,
}

pub(crate) type EditPresentation =
    fn(usize, &str, &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

impl EditRejection {
    fn error(occurrences: usize) -> Error {
        Error::ExactEdit(Self {
            occurrences,
            presentation: None,
            _custody: None,
        })
    }

    pub(crate) fn present(
        mut self,
        path: &str,
        format: EditPresentation,
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        let mut custody = allocation(work, 0)?;
        let mut owned = String::new();
        reserve_string(&mut custody, &mut owned, path.len())?;
        charge(&custody, path.len() as u64, 0)?;
        owned.push_str(path);
        self.presentation = Some((owned, format));
        self._custody = custody;
        Ok(self)
    }
}

impl std::fmt::Display for EditRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.presentation {
            Some((path, format)) => format(self.occurrences, path, formatter),
            None if self.occurrences == 0 => formatter.write_str("exact edit text is missing"),
            None => formatter.write_str("exact edit text is ambiguous"),
        }
    }
}

impl std::error::Error for EditRejection {}

#[doc(hidden)]
pub fn allocation(work: crate::NativeWork<'_>, bytes: usize) -> Result<NativeCustody> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        return Ok(Some(work.claim(
            bytes as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?));
    }
    let _ = (work, bytes);
    Ok(None)
}

pub(super) fn invalid(work: &NativeCustody, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::ToolRejected {
            code: Invalid,
            reason,
        };
    }
    let _ = work;
    Error::Invalid(reason.into())
}
pub(super) fn conflict(work: &NativeCustody, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::ToolRejected {
            code: Conflict,
            reason,
        };
    }
    let _ = work;
    Error::Conflict(reason.into())
}

fn capacity_overflow(work: &NativeCustody) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: Invalid,
            reason: "tool vector capacity overflow",
        };
    }
    invalid(work, "tool vector capacity overflow")
}

pub(super) fn retain(work: &mut NativeCustody, bytes: usize) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(custody) = work {
        let total = custody
            .bytes()
            .checked_add(bytes as u64)
            .ok_or(Error::NativeBoundary {
                code: Invalid,
                reason: "tool logical ownership overflow",
            })?;
        custody.resize(total)?;
    }
    let _ = (work, bytes);
    Ok(())
}

fn admit(work: &mut NativeCustody, bytes: usize) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(custody) = work {
        custody.grow(bytes as u64)?;
    }
    let _ = (work, bytes);
    Ok(())
}

fn capacity(work: &mut NativeCustody, requested: usize, actual: usize, old: usize) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(custody) = work {
        // The allocator retired the old buffer before capacity reconciliation.
        let bytes = custody
            .bytes()
            .checked_sub(requested as u64)
            .and_then(|n| n.checked_add(actual as u64))
            .and_then(|n| n.checked_sub(old as u64))
            .ok_or(Error::NativeBoundary {
                code: Invalid,
                reason: "tool allocation capacity overflow",
            })?;
        custody.reconcile_capacity(bytes)?;
    }
    let _ = (work, requested, actual, old);
    Ok(())
}

fn allocation_error(
    work: &NativeCustody,
    cause: std::collections::TryReserveError,
    reason: &'static str,
) -> Error {
    if work.is_some() {
        Error::Allocation(cause)
    } else {
        invalid(work, reason)
    }
}

#[doc(hidden)]
pub fn reserve_vec<T>(work: &mut NativeCustody, value: &mut Vec<T>, length: usize) -> Result<()> {
    if length <= value.capacity() {
        return Ok(());
    }
    let length = length.max(value.capacity().checked_mul(2).unwrap_or(length));
    let element = std::mem::size_of::<T>();
    let requested = length
        .checked_mul(element)
        .ok_or_else(|| capacity_overflow(work))?;
    let old = value
        .capacity()
        .checked_mul(element)
        .ok_or_else(|| capacity_overflow(work))?;
    admit(work, requested)?;
    value
        .try_reserve_exact(length - value.len())
        .map_err(|cause| allocation_error(work, cause, "tool vector allocation failed"))?;
    capacity(work, requested, value.capacity() * element, old)
}

#[doc(hidden)]
pub fn reserve_string(work: &mut NativeCustody, value: &mut String, length: usize) -> Result<()> {
    if length <= value.capacity() {
        return Ok(());
    }
    let length = length.max(value.capacity().checked_mul(2).unwrap_or(length));
    let old = value.capacity();
    admit(work, length)?;
    value
        .try_reserve_exact(length - value.len())
        .map_err(|cause| allocation_error(work, cause, "tool text allocation failed"))?;
    capacity(work, length, value.capacity(), old)
}

#[doc(hidden)]
pub fn charge(work: &NativeCustody, bytes_copied: u64, items_examined: u64) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(custody) = work {
        custody.work().charge(acyclic_fs::WorkCounters {
            bytes_copied,
            items_examined,
            ..Default::default()
        })?;
    }
    let _ = (work, bytes_copied, items_examined);
    Ok(())
}

/// Rejects unbounded or empty allowances at both transformation and I/O boundaries.
pub(crate) fn validate_edit_bound(maximum_bytes: u64) -> Result<()> {
    if maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(Error::Invalid(
            "edit byte bound must be positive and finite".into(),
        ));
    }
    Ok(())
}

/// The shared replacement engine for unique and all-occurrence edits.
pub(crate) fn replace(
    source: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
    maximum_bytes: u64,
    work: &mut NativeCustody,
) -> Result<String> {
    replace_sequence(
        source,
        std::iter::once(Replacement {
            old_text,
            new_text,
            replace_all,
        }),
        maximum_bytes,
        work,
    )
}

/// One borrowed transformation, interpreted against the preceding result.
#[derive(Clone, Copy)]
pub(crate) struct Replacement<'a> {
    pub old_text: &'a str,
    pub new_text: &'a str,
    pub replace_all: bool,
}

/// Plans the complete sequence without I/O. A failed later transformation
/// cannot expose an intermediate result. Single edits allocate one output;
/// longer sequences alternate two buffers, independent of edit count.
pub(crate) fn replace_sequence<'a>(
    source: &str,
    replacements: impl IntoIterator<Item = Replacement<'a>>,
    maximum_bytes: u64,
    work: &mut NativeCustody,
) -> Result<String> {
    let mut replacements = replacements.into_iter();
    let first = replacements
        .next()
        .ok_or_else(|| invalid(work, "exact edit sequence is empty"))?;
    let mut output = String::new();
    let mut scratch = String::new();
    replace_into(source, first, maximum_bytes, work, &mut output)?;
    for replacement in replacements {
        replace_into(&output, replacement, maximum_bytes, work, &mut scratch)?;
        std::mem::swap(&mut output, &mut scratch);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let retired = scratch.capacity() as u64;
    drop(scratch);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(custody) = work {
        let retained = custody
            .bytes()
            .checked_sub(retired)
            .ok_or(Error::NativeBoundary {
                code: Invalid,
                reason: "exact edit scratch ownership underflow",
            })?;
        custody.resize(retained)?;
    }
    Ok(output)
}

fn replace_into(
    source: &str,
    Replacement {
        old_text,
        new_text,
        replace_all,
    }: Replacement<'_>,
    maximum_bytes: u64,
    work: &mut NativeCustody,
    output: &mut String,
) -> Result<()> {
    if maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(invalid(work, "edit byte bound must be positive and finite"));
    }
    if source.len() as u64 > maximum_bytes
        || old_text.len() as u64 > maximum_bytes
        || new_text.len() as u64 > maximum_bytes
        || old_text.is_empty()
    {
        return Err(invalid(work, "exact edit has invalid text or byte bounds"));
    }
    charge(work, 0, source.len() as u64)?;
    let (start, count) = if replace_all {
        let mut matches = source.match_indices(old_text);
        let (start, _) = matches.next().ok_or_else(|| EditRejection::error(0))?;
        (start, 1 + matches.count())
    } else {
        let start = source
            .find(old_text)
            .ok_or_else(|| EditRejection::error(0))?;
        // Unique replacement rejects overlapping matches too. Advancing one
        // complete character preserves UTF-8 and finds every later start.
        let next_start = start + old_text.chars().next().map_or(0, char::len_utf8);
        charge(work, 0, (source.len() - next_start) as u64)?;
        if source[next_start..].contains(old_text) {
            // Only the refusal path counts all non-overlapping occurrences,
            // for the native harness's exact diagnostic. Successful edits keep
            // their original search/copy work. Overlap-only ambiguity retains
            // the stronger shared unique-match contract.
            charge(work, 0, source.len() as u64)?;
            let occurrences = source.match_indices(old_text).count();
            return Err(if occurrences > 1 {
                EditRejection::error(occurrences)
            } else {
                conflict(work, "exact edit text has overlapping matches")
            });
        }
        (start, 1)
    };
    let output_bytes = source
        .len()
        .checked_sub(
            old_text
                .len()
                .checked_mul(count)
                .ok_or_else(|| invalid(work, "exact edit size overflow"))?,
        )
        .and_then(|length| {
            new_text
                .len()
                .checked_mul(count)
                .and_then(|added| length.checked_add(added))
        })
        .filter(|length| *length as u64 <= maximum_bytes)
        .ok_or_else(|| invalid(work, "exact edit output exceeds byte bound"))?;
    reserve_string(work, output, output_bytes)?;
    output.clear();
    charge(work, (output_bytes) as u64, 0)?;
    let mut cursor = 0;
    if replace_all {
        charge(work, 0, source.len() as u64)?;
        for (position, _) in source.match_indices(old_text) {
            output.push_str(&source[cursor..position]);
            output.push_str(new_text);
            cursor = position + old_text.len();
        }
    } else {
        output.push_str(&source[..start]);
        output.push_str(new_text);
        cursor = start + old_text.len();
    }
    output.push_str(&source[cursor..]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn edit_sequences_retain_only_their_output_and_refuse_invalid_later_edits() -> Result<()> {
        for length in [1, 2, 33] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(64 * 1024, 64 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let mut custody = allocation(Some(&work), 0)?;
            let edits = (0..length).map(|index| Replacement {
                old_text: if index % 2 == 0 { "one" } else { "two" },
                new_text: if index % 2 == 0 { "two" } else { "one" },
                replace_all: false,
            });
            let output = replace_sequence("one", edits, 64, &mut custody)?;
            assert_eq!(output, if length % 2 == 0 { "one" } else { "two" });
            // An allocation retained for every prior intermediate result would
            // fail this same bound as the sequence grows.
            assert_eq!(
                work.live_allocation_bytes(),
                baseline + output.capacity() as u64
            );
            drop(output);
            drop(custody);
            assert_eq!(work.live_allocation_bytes(), baseline);

            let invalid = [
                Replacement {
                    old_text: "one",
                    new_text: "two",
                    replace_all: false,
                },
                Replacement {
                    old_text: "missing",
                    new_text: "three",
                    replace_all: false,
                },
            ];
            let result = {
                let mut custody = allocation(Some(&work), 0)?;
                replace_sequence("one", invalid, 64, &mut custody)
            };
            assert!(result.is_err());
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[test]
    fn exact_edit_preserves_utf8_and_rejects_overlapping_matches() -> Result<()> {
        let mut work = None;
        assert_eq!(
            replace("before 🦀 after", "🦀", "日本語", false, 64, &mut work)?,
            "before 日本語 after"
        );
        assert_eq!(
            replace("a\r\nb\n", "\r\n", "\n", false, 8, &mut work)?,
            "a\nb\n"
        );
        assert!(matches!(
            replace("aaa", "aa", "b", false, 8, &mut work),
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            replace("ééé", "éé", "b", false, 8, &mut work),
            Err(Error::Conflict(_))
        ));
        assert!(replace("one one", "one", "two", false, 8, &mut work).is_err());
        assert!(replace("one", "missing", "two", false, 8, &mut work).is_err());
        assert!(replace("one", "", "two", false, 8, &mut work).is_err());
        Ok(())
    }

    #[test]
    fn exact_edit_checks_input_and_output_byte_bounds() -> Result<()> {
        let mut work = None;
        assert_eq!(replace("abc", "b", "", false, 3, &mut work)?, "ac");
        assert_eq!(replace("abc", "abc", "xyz", false, 3, &mut work)?, "xyz");
        assert!(replace("abc", "b", "long", false, 4, &mut work).is_err());
        assert!(replace("abc", "abc", "a", false, 2, &mut work).is_err());
        assert!(replace("abc", "a", "a", false, 0, &mut work).is_err());
        assert!(replace("abc", "a", "a", false, u64::MAX, &mut work).is_err());
        assert!(replace("é", "é", "🦀", false, 3, &mut work).is_err());
        Ok(())
    }

    #[test]
    fn bounded_matching_agrees_with_all_utf8_start_positions() {
        let mut work = None;
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
                            let result = replace(&source, &needle, "X", false, 64, &mut work);
                            assert_eq!(
                                replace(&source, &needle, "X", true, 64, &mut work).unwrap(),
                                source.replace(&needle, "X")
                            );
                            if matches.len() == 1 {
                                assert_eq!(result.ok(), Some(source.replacen(&needle, "X", 1)));
                            } else {
                                assert_eq!(
                                    result.unwrap_err().code(),
                                    crate::wire::ErrorCode::Conflict
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
