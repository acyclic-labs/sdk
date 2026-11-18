//! Canonical wire grammar for a flat Session filter.

use prost::Message as _;

use crate::{headers::MAX_RECORD_KEY_BYTES, wire};

/// Tests a key against sorted, disjoint ranges produced by canonicalization.
#[must_use]
pub fn contains_key(ranges: &[wire::KeyRange], key: &[u8]) -> bool {
    ranges
        .partition_point(|range| range.start.as_ref() <= key)
        .checked_sub(1)
        .and_then(|index| ranges.get(index))
        .is_some_and(|range| range.end.is_empty() || key < range.end.as_ref())
}

/// A filter cannot be represented by the wire contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterError {
    /// A clause, range or attribute is malformed.
    Invalid,
    /// The serialized filter exceeds the command budget.
    Limit,
}

/// Validates and normalizes a flat filter before matching.
///
/// # Errors
/// Rejects malformed filters and filters over the command budget.
pub fn canonical(mut filter: wire::SessionFilter) -> Result<wire::SessionFilter, FilterError> {
    if !filter.all_of.is_empty() {
        return Err(FilterError::Invalid);
    }
    if filter.encoded_len() > wire::StreamLimit::MaxCommandBytes as usize {
        return Err(FilterError::Limit);
    }
    for range in &filter.keys {
        if !range.end.is_empty() && range.start >= range.end {
            return Err(FilterError::Invalid);
        }
    }
    filter.keys.sort_unstable_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
    });
    let mut ranges: Vec<wire::KeyRange> = Vec::new();
    for range in filter.keys {
        if let Some(last) = ranges.last_mut()
            && (last.end.is_empty() || range.start <= last.end)
        {
            if !last.end.is_empty() && (range.end.is_empty() || range.end > last.end) {
                last.end = range.end;
            }
        } else {
            ranges.push(range);
        }
    }
    filter.keys = ranges;
    filter
        .attributes
        .sort_unstable_by(|left, right| left.name.cmp(&right.name));
    if filter
        .attributes
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left.name == right.name))
    {
        return Err(FilterError::Invalid);
    }
    for attribute in &mut filter.attributes {
        if attribute.name.is_empty()
            || attribute.name.chars().any(char::is_control)
            || attribute.any_of.is_empty()
        {
            return Err(FilterError::Invalid);
        }
        attribute.any_of.sort_unstable();
        attribute.any_of.dedup();
    }
    Ok(filter)
}

/// Returns the exclusive upper bound selecting exactly one legal record key.
/// An empty bound is unbounded when the input is the greatest legal key.
///
/// # Errors
/// Returns a size error for a key beyond the wire limit.
pub fn exclusive_key_end(key: &[u8]) -> Result<Vec<u8>, FilterError> {
    if key.len() > MAX_RECORD_KEY_BYTES {
        return Err(FilterError::Limit);
    }
    let mut end = key.to_vec();
    if end.len() < MAX_RECORD_KEY_BYTES {
        end.push(0);
    } else if let Some(index) = end.iter().rposition(|byte| *byte != u8::MAX) {
        if let Some(byte) = end.get_mut(index) {
            *byte += 1;
        }
        end.truncate(index + 1);
    } else {
        // No longer keys are admitted; the greatest legal key is last.
        end.clear();
    }
    Ok(end)
}
