//! Portable exact text replacement, independent of model family and storage.

use crate::{Error, Result};

/// Shared allocation hooks; ordinary transformations use the same engine.
pub(super) struct AlgorithmWork {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

impl AlgorithmWork {
    pub(super) fn ordinary() -> Self {
        Self {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            custody: None,
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn native(work: &acyclic_fs::NativeMaterializationWork, bytes: usize) -> Result<Self> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        io.claim(bytes as u64)?;
        Ok(Self { custody: Some(io.into_native_custody(bytes as u64)?) })
    }

    pub(super) fn invalid(&self, reason: &'static str) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.custody.is_some() {
            return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
        }
        Error::Invalid(reason.into())
    }

    pub(super) fn conflict(&self, reason: &'static str) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.custody.is_some() {
            return Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason };
        }
        Error::Conflict(reason.into())
    }

    fn allocation_error(&self, cause: std::collections::TryReserveError, reason: &'static str) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.custody.is_some() { return Error::Allocation(cause); }
        let _ = cause;
        self.invalid(reason)
    }

    pub(super) fn retain(&mut self, bytes: usize) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &mut self.custody {
            let total = custody.bytes().checked_add(bytes as u64).ok_or_else(|| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "tool logical ownership overflow",
            })?;
            custody.resize_retained(total).map_err(native_error)?;
        }
        let _ = bytes;
        Ok(())
    }

    fn admit_allocation(&mut self, bytes: usize) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &mut self.custody {
            custody.grow(bytes as u64).map_err(native_error)?;
        }
        let _ = bytes;
        Ok(())
    }

    fn record_capacity(&mut self, requested: usize, actual: usize, old: usize) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &mut self.custody {
            // Record the completed allocator capacity before observing late
            // cancellation; the old allocation has now physically retired.
            let bytes = custody.bytes().checked_sub(requested as u64)
                .and_then(|n| n.checked_add(actual as u64))
                .and_then(|n| n.checked_sub(old as u64))
                .ok_or_else(|| Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid, reason: "tool allocation capacity overflow",
                })?;
            custody.reconcile_capacity(bytes).map_err(native_error)?;
        }
        let _ = (requested, actual, old);
        Ok(())
    }

    pub(super) fn reserve_vec<T>(&mut self, value: &mut Vec<T>, length: usize) -> Result<()> {
        if length <= value.capacity() { return Ok(()); }
        let length = length.max(value.capacity().checked_mul(2).unwrap_or(length));
        let slots = std::mem::size_of::<T>();
        let requested = length.checked_mul(slots)
            .ok_or_else(|| self.invalid("tool vector capacity overflow"))?;
        let old = value.capacity().checked_mul(slots)
            .ok_or_else(|| self.invalid("tool vector capacity overflow"))?;
        self.admit_allocation(requested)?;
        value.try_reserve_exact(length - value.len())
            .map_err(|cause| self.allocation_error(cause, "tool vector allocation failed"))?;
        self.record_capacity(requested, value.capacity() * slots, old)
    }

    pub(super) fn reserve_string(&mut self, value: &mut String, length: usize) -> Result<()> {
        if length <= value.capacity() { return Ok(()); }
        let length = length.max(value.capacity().checked_mul(2).unwrap_or(length));
        let old = value.capacity();
        self.admit_allocation(length)?;
        value.try_reserve_exact(length - value.len())
            .map_err(|cause| self.allocation_error(cause, "tool text allocation failed"))?;
        self.record_capacity(length, value.capacity(), old)
    }

    pub(super) fn examine(&self, items: u64) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &self.custody {
            custody.work().charge(acyclic_fs::WorkCounters {
                items_examined: items, ..Default::default()
            }).map_err(native_error)?;
        }
        let _ = items;
        Ok(())
    }

    pub(super) fn copy(&self, bytes: usize) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &self.custody {
            custody.work().charge(acyclic_fs::WorkCounters {
                bytes_copied: bytes as u64, ..Default::default()
            }).map_err(native_error)?;
        }
        let _ = bytes;
        Ok(())
    }

}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
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
    exact_replace_engine(source, old_text, new_text, maximum_bytes, &mut AlgorithmWork::ordinary())
}

pub(super) fn exact_replace_engine(
    source: &str,
    old_text: &str,
    new_text: &str,
    maximum_bytes: u64,
    work: &mut AlgorithmWork,
) -> Result<String> {
    if maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(work.invalid("edit byte bound must be positive and finite"));
    }
    if source.len() as u64 > maximum_bytes
        || old_text.len() as u64 > maximum_bytes
        || new_text.len() as u64 > maximum_bytes
        || old_text.is_empty()
    {
        return Err(work.invalid("exact edit has invalid text or byte bounds"));
    }
    // These are logical source examinations; std's opaque substring-search
    // implementation is not falsely reported as one SDK comparison per byte.
    work.examine(1)?;
    let start = source
        .find(old_text)
        .ok_or_else(|| work.conflict("exact edit text is missing"))?;
    // Advance one character, rather than one match, to detect overlaps without
    // slicing inside a UTF-8 code point.
    let first_character_bytes = old_text.chars().next().map_or(0, char::len_utf8);
    let next_start = start + first_character_bytes;
    work.examine(1)?;
    if source
        .get(next_start..)
        .is_some_and(|tail| tail.contains(old_text))
    {
        return Err(work.conflict("exact edit text is ambiguous"));
    }
    let output_bytes = source
        .len()
        .checked_sub(old_text.len())
        .and_then(|length| length.checked_add(new_text.len()))
        .filter(|length| *length as u64 <= maximum_bytes)
        .ok_or_else(|| work.invalid("exact edit output exceeds byte bound"))?;
    let mut output = String::new();
    work.reserve_string(&mut output, output_bytes)?;
    let end = start + old_text.len();
    let prefix = source
        .get(..start)
        .ok_or_else(|| work.invalid("exact edit match is not UTF-8 aligned"))?;
    let suffix = source
        .get(end..)
        .ok_or_else(|| work.invalid("exact edit match is not UTF-8 aligned"))?;
    work.copy(output_bytes)?;
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
