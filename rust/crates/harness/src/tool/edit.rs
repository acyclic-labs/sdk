//! Portable exact text replacement.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode::{Conflict, Invalid};
use crate::{Error, NativeCustody, Result};

pub(crate) fn allocation(work: crate::NativeWork<'_>, bytes: usize) -> Result<NativeCustody> {
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
        return Error::NativeBoundary {
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
        return Error::NativeBoundary {
            code: Conflict,
            reason,
        };
    }
    let _ = work;
    Error::Conflict(reason.into())
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

pub(crate) fn reserve_vec<T>(
    work: &mut NativeCustody,
    value: &mut Vec<T>,
    length: usize,
) -> Result<()> {
    if length <= value.capacity() {
        return Ok(());
    }
    let length = length.max(value.capacity().checked_mul(2).unwrap_or(length));
    let slots = std::mem::size_of::<T>();
    let requested = length
        .checked_mul(slots)
        .ok_or_else(|| invalid(work, "tool vector capacity overflow"))?;
    let old = value
        .capacity()
        .checked_mul(slots)
        .ok_or_else(|| invalid(work, "tool vector capacity overflow"))?;
    admit(work, requested)?;
    value
        .try_reserve_exact(length - value.len())
        .map_err(|cause| allocation_error(work, cause, "tool vector allocation failed"))?;
    capacity(work, requested, value.capacity() * slots, old)
}

pub(crate) fn reserve_string(
    work: &mut NativeCustody,
    value: &mut String,
    length: usize,
) -> Result<()> {
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

pub(crate) fn charge(work: &NativeCustody, bytes_copied: u64, items_examined: u64) -> Result<()> {
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

/// Replaces one unambiguous exact occurrence within finite input/output bounds.
pub(crate) fn exact_replace(
    source: &str,
    old_text: &str,
    new_text: &str,
    maximum_bytes: u64,
    work: &mut NativeCustody,
) -> Result<String> {
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
    charge(work, 0, 1)?;
    let start = source
        .find(old_text)
        .ok_or_else(|| conflict(work, "exact edit text is missing"))?;
    let first_character_bytes = old_text.chars().next().map_or(0, char::len_utf8);
    let next_start = start + first_character_bytes;
    charge(work, 0, 1)?;
    if source
        .get(next_start..)
        .is_some_and(|tail| tail.contains(old_text))
    {
        return Err(conflict(work, "exact edit text is ambiguous"));
    }
    let output_bytes = source
        .len()
        .checked_sub(old_text.len())
        .and_then(|length| length.checked_add(new_text.len()))
        .filter(|length| *length as u64 <= maximum_bytes)
        .ok_or_else(|| invalid(work, "exact edit output exceeds byte bound"))?;
    let mut output = String::new();
    reserve_string(work, &mut output, output_bytes)?;
    let end = start + old_text.len();
    let prefix = source
        .get(..start)
        .ok_or_else(|| invalid(work, "exact edit match is not UTF-8 aligned"))?;
    let suffix = source
        .get(end..)
        .ok_or_else(|| invalid(work, "exact edit match is not UTF-8 aligned"))?;
    charge(work, (output_bytes) as u64, 0)?;
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
        let mut work = None;
        assert_eq!(
            exact_replace("before 🦀 after", "🦀", "日本語", 64, &mut work)?,
            "before 日本語 after"
        );
        assert_eq!(
            exact_replace("a\r\nb\n", "\r\n", "\n", 8, &mut work)?,
            "a\nb\n"
        );
        assert!(matches!(
            exact_replace("aaa", "aa", "b", 8, &mut work),
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            exact_replace("ééé", "éé", "b", 8, &mut work),
            Err(Error::Conflict(_))
        ));
        assert!(exact_replace("one one", "one", "two", 8, &mut work).is_err());
        assert!(exact_replace("one", "missing", "two", 8, &mut work).is_err());
        assert!(exact_replace("one", "", "two", 8, &mut work).is_err());
        Ok(())
    }

    #[test]
    fn exact_edit_checks_input_and_output_byte_bounds() -> Result<()> {
        let mut work = None;
        assert_eq!(exact_replace("abc", "b", "", 3, &mut work)?, "ac");
        assert_eq!(exact_replace("abc", "abc", "xyz", 3, &mut work)?, "xyz");
        assert!(exact_replace("abc", "b", "long", 4, &mut work).is_err());
        assert!(exact_replace("abc", "abc", "a", 2, &mut work).is_err());
        assert!(exact_replace("abc", "a", "a", 0, &mut work).is_err());
        assert!(exact_replace("abc", "a", "a", u64::MAX, &mut work).is_err());
        assert!(exact_replace("é", "é", "🦀", 3, &mut work).is_err());
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
                            let result = exact_replace(&source, &needle, "X", 64, &mut work);
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
