//! Borrowed, fail-closed decoding of bounded native USN pages.
use crate::demand::{DemandError, SourceDemandReadWork};
use crate::performance::WorkCounters;

pub(crate) const PAGE_BYTES: usize = 8192;

/// One native change record. File IDs retain all 128 bits and names retain
/// their original UTF-16 code units, including unpaired surrogates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowsUsnRecord<'a> {
    /// Native record layout, 2 for 64-bit references or 3 for 128-bit references.
    pub major_version: u16,
    /// File reference in native little-endian order; V2 upper bytes are zero.
    pub file_id: [u8; 16],
    /// Parent reference, with the same representation as the file reference.
    pub parent_id: [u8; 16],
    /// This record's update sequence number.
    pub usn: u64,
    /// Native reason flags, including create/delete and both sides of rename.
    pub reason: u32,
    /// Native file attributes, including the directory bit.
    pub attributes: u32,
    /// Original filename bytes. Unprivileged journal reads can omit the name;
    /// an empty name must be resolved by identity, never treated as no change.
    pub name_utf16le: &'a [u8],
}

pub(crate) struct UsnPage<'a> {
    records: &'a [u8],
    pub(crate) next_usn: u64,
}

fn u16_at(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}
fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}
fn u64_at(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn record(bytes: &[u8]) -> Option<(WindowsUsnRecord<'_>, usize)> {
    let length = usize::try_from(u32_at(bytes, 0)?).ok()?;
    if length % 8 != 0 || u16_at(bytes, 6)? != 0 {
        return None;
    }
    let bytes = bytes.get(..length)?;
    let major = u16_at(bytes, 4)?;
    let (header, width, usn_at, reason_at, attributes_at, name_at) = match major {
        2 => (60, 8, 24, 40, 52, 56),
        3 => (76, 16, 40, 56, 68, 72),
        _ => return None,
    };
    if length < header {
        return None;
    }
    let mut file_id = [0; 16];
    let mut parent_id = [0; 16];
    file_id[..width].copy_from_slice(bytes.get(8..8 + width)?);
    parent_id[..width].copy_from_slice(bytes.get(8 + width..8 + 2 * width)?);
    let name_length = usize::from(u16_at(bytes, name_at)?);
    let name_offset = usize::from(u16_at(bytes, name_at + 2)?);
    if name_length % 2 != 0
        || name_offset % 2 != 0
        || (name_offset < header && !(name_length == 0 && name_offset == 0))
    {
        return None;
    }
    let name_utf16le = bytes.get(name_offset..name_offset.checked_add(name_length)?)?;
    Some((
        WindowsUsnRecord {
            major_version: major,
            file_id,
            parent_id,
            usn: u64_at(bytes, usn_at)?,
            reason: u32_at(bytes, reason_at)?,
            attributes: u32_at(bytes, attributes_at)?,
            name_utf16le,
        },
        length,
    ))
}

impl<'a> UsnPage<'a> {
    pub(crate) fn validate<W: SourceDemandReadWork>(
        bytes: &'a [u8],
        start_usn: u64,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        work.check()?;
        if bytes.len() > PAGE_BYTES {
            return Err(DemandError::InvalidRequest);
        }
        let next_usn = u64_at(bytes, 0).ok_or(DemandError::InvalidRequest)?;
        if next_usn > i64::MAX as u64 || next_usn < start_usn {
            return Err(DemandError::InvalidRequest);
        }
        let records = bytes.get(8..).ok_or(DemandError::InvalidRequest)?;
        let mut remaining = records;
        let mut previous = None;
        while !remaining.is_empty() {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let (entry, length) = record(remaining).ok_or(DemandError::InvalidRequest)?;
            if entry.usn < start_usn
                || entry.usn >= next_usn
                || previous.is_some_and(|last| entry.usn <= last)
            {
                return Err(DemandError::InvalidRequest);
            }
            previous = Some(entry.usn);
            remaining = &remaining[length..];
        }
        // No cursor or record is exposed until the entire page is validated.
        Ok(Self { records, next_usn })
    }

    pub(crate) fn records(&self) -> impl Iterator<Item = WindowsUsnRecord<'a>> {
        let mut remaining = self.records;
        std::iter::from_fn(move || {
            if remaining.is_empty() {
                return None;
            }
            let (entry, length) = record(remaining).expect("validated immutable USN record");
            remaining = &remaining[length..];
            Some(entry)
        })
    }

    /// The cursor advances only after every record before the fence is
    /// accepted. Failed applications can be retried and must be idempotent.
    pub(crate) fn apply<W: SourceDemandReadWork, E: From<DemandError>>(
        &self,
        cursor: &mut u64,
        fence: u64,
        work: &mut W,
        mut accept: impl FnMut(WindowsUsnRecord<'a>, &mut W) -> Result<(), E>,
    ) -> Result<u64, E> {
        work.check()?;
        let next = self.next_usn.min(fence);
        if next <= *cursor || fence < *cursor {
            return Err(DemandError::InvalidRequest.into());
        }
        let mut count = 0;
        for record in self.records().take_while(|record| record.usn < fence) {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            accept(record, work)?;
            count += 1;
        }
        work.check()?;
        *cursor = next;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(major: u16, usn: u64, name: &[u16]) -> Vec<u8> {
        let (header, usn_at, name_at) = if major == 2 {
            (60, 24, 56)
        } else {
            (76, 40, 72)
        };
        let length = (header + name.len() * 2).div_ceil(8) * 8;
        let mut bytes = vec![0; length];
        bytes[..4].copy_from_slice(&(length as u32).to_le_bytes());
        bytes[4..6].copy_from_slice(&major.to_le_bytes());
        bytes[usn_at..usn_at + 8].copy_from_slice(&usn.to_le_bytes());
        bytes[name_at..name_at + 2].copy_from_slice(&((name.len() * 2) as u16).to_le_bytes());
        if !name.is_empty() {
            bytes[name_at + 2..name_at + 4].copy_from_slice(&(header as u16).to_le_bytes());
            for (out, unit) in bytes[header..].chunks_exact_mut(2).zip(name) {
                out.copy_from_slice(&unit.to_le_bytes());
            }
        }
        bytes
    }
    fn page(entries: &[Vec<u8>]) -> Vec<u8> {
        1000u64
            .to_le_bytes()
            .into_iter()
            .chain(entries.iter().flatten().copied())
            .collect()
    }
    fn valid(bytes: &[u8]) -> bool {
        UsnPage::validate(bytes, 1, &mut crate::native_host::ordinary_work()).is_ok()
    }

    #[test]
    fn name_search_usn_pages_preserve_mixed_ids_nameless_records_and_raw_names() {
        let mut v2 = entry(2, 10, &[b'a' as u16, 0xd800]);
        v2[8..16].copy_from_slice(&7u64.to_le_bytes());
        v2[16..24].copy_from_slice(&9u64.to_le_bytes());
        let mut v3 = entry(3, 11, &[]);
        v3[8..24].fill(0x7a);
        v3[24..40].fill(0x9b);
        let bytes = page(&[v2, v3]);
        let decoded =
            UsnPage::validate(&bytes, 1, &mut crate::native_host::ordinary_work()).unwrap();
        let entries: Vec<_> = decoded.records().collect();
        assert_eq!(decoded.next_usn, 1000);
        assert_eq!(entries.len(), 2);
        assert_eq!(&entries[0].file_id[..8], &7u64.to_le_bytes());
        assert_eq!(&entries[0].file_id[8..], &[0; 8]);
        assert_eq!(entries[1].file_id, [0x7a; 16]);
        assert_eq!(entries[1].parent_id, [0x9b; 16]);
        assert_eq!(entries[0].name_utf16le, &[b'a', 0, 0, 0xd8]);
        assert!(entries[1].name_utf16le.is_empty());
        // Negative controls: a UTF-8 decoder loses native spellings; a 64-bit
        // file ID projection loses the V3 identity's upper half.
        assert!(String::from_utf16(&[b'a' as u16, 0xd800]).is_err());
        assert_ne!(&entries[1].file_id[8..], &[0; 8]);
    }

    #[test]
    fn name_search_usn_rejects_every_malformed_tail_before_exposing_a_cursor() {
        let original = page(&[entry(2, 10, &[b'x' as u16]), entry(3, 20, &[])]);
        assert!(valid(&original));
        for end in 0..original.len() {
            // A whole first record is itself a valid shorter journal page.
            if matches!(end, 8 | 72) {
                continue;
            }
            assert!(!valid(&original[..end]), "truncated at {end}");
        }
        for (offset, value) in [
            (8, 0),
            (8, 65),
            (12, 4),
            (14, 1),
            (64, 3),
            (66, 2),
            (72, 0),
            (76, 4),
        ] {
            let mut bytes = original.clone();
            bytes[offset] = value;
            assert!(!valid(&bytes), "bad field at {offset}");
        }
        for next in [0, 10, i64::MAX as u64 + 1] {
            let mut bytes = original.clone();
            bytes[..8].copy_from_slice(&next.to_le_bytes());
            assert!(!valid(&bytes));
        }
        let mut reversed = original.clone();
        reversed[112..120].copy_from_slice(&9u64.to_le_bytes());
        assert!(!valid(&reversed));
        // Negative control: reading just the first eight bytes would advance
        // past the malformed tail and permanently lose its changes.
        assert_eq!(u64_at(&reversed, 0), Some(1000));
    }

    #[test]
    fn name_search_usn_decode_has_a_fixed_page_bound_and_honors_denied_work() {
        let entries: Vec<_> = (1..=127).map(|usn| entry(2, usn, &[])).collect();
        let bytes = page(&entries);
        assert_eq!(bytes.len(), PAGE_BYTES - 56);
        let mut work = crate::native_host::ordinary_work();
        let decoded = UsnPage::validate(&bytes, 1, &mut work).unwrap();
        assert_eq!(decoded.records().count(), 127);
        assert_eq!(work.receipt().items_examined, 127);
        assert!(std::mem::size_of::<UsnPage<'_>>() <= 32);
        let mut too_large = bytes.clone();
        too_large.resize(PAGE_BYTES + 1, 0);
        assert!(!valid(&too_large));
        let mut denied = crate::demand::DemandWork::new(
            crate::WorkBudget {
                items_examined: 1,
                ..crate::WorkBudget::UNBOUNDED
            },
            &crate::CancellationToken::new(),
        );
        assert!(UsnPage::validate(&bytes, 1, &mut denied).is_err());
        assert_eq!(denied.receipt().items_examined, 1);
        // Negative control: collecting an unbounded volume's journal records
        // has no such fixed storage bound, even with zero candidate contents.
        assert!(1_000_000 * std::mem::size_of::<WindowsUsnRecord<'_>>() > PAGE_BYTES);
    }

    #[test]
    fn name_search_usn_cursor_commits_only_after_every_callback_and_cancel_check() {
        let bytes = page(&[entry(2, 10, &[]), entry(3, 20, &[]), entry(2, 30, &[])]);
        let mut work = crate::native_host::ordinary_work();
        let page = UsnPage::validate(&bytes, 1, &mut work).unwrap();
        let mut cursor = 1;
        let mut accepted = Vec::new();
        let failure: Result<_, DemandError> =
            page.apply(&mut cursor, 25, &mut work, |record, _| {
                if record.usn == 20 {
                    return Err(DemandError::InvalidRequest);
                }
                accepted.push(record.usn);
                Ok(())
            });
        assert!(failure.is_err());
        assert_eq!(cursor, 1);
        assert_eq!(accepted, [10]);
        // Retry repeats accepted records; coalescing consumers must therefore
        // be idempotent. Records beyond the captured fence are never applied.
        let count: Result<_, DemandError> = page.apply(&mut cursor, 25, &mut work, |record, _| {
            accepted.push(record.usn);
            Ok(())
        });
        assert_eq!(count.unwrap(), 2);
        assert_eq!(cursor, 25);
        assert_eq!(accepted, [10, 10, 20]);
        // Negative control: publishing the page's raw next-USN (1000) would
        // skip the record at 30 that was outside this replay fence.
        assert_ne!(cursor, page.next_usn);

        let stop = crate::CancellationToken::new();
        let mut cancelled = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &stop);
        cursor = 1;
        let result: Result<_, DemandError> = page.apply(&mut cursor, 15, &mut cancelled, |_, _| {
            stop.cancel();
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(
            cursor, 1,
            "cancellation after the last callback cannot commit"
        );

        let stalled = 1u64.to_le_bytes();
        let empty = UsnPage::validate(&stalled, 1, &mut work).unwrap();
        let result: Result<_, DemandError> = empty.apply(&mut cursor, 25, &mut work, |_, _| Ok(()));
        assert!(
            result.is_err(),
            "a stalled read cannot certify the replay fence"
        );
        assert_eq!(cursor, 1);
    }
}
