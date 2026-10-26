//! The claim record: which process, and which of its claims, owns a store.
//!
//! Every field is length-prefixed and the whole record is checksummed, so any
//! path or identity round-trips exactly, and a record cut short by a crash is
//! recognized as such.

const MAGIC: &[u8] = b"acyclic-claim-v2\0";
const CHECKSUM_BYTES: usize = 8;
/// A record is a few short fields; anything longer is not one.
pub(crate) const RECORD_LIMIT: usize = 4096;

/// The process that owns a claim, as other processes find it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Process {
    pub(super) pid: u32,
    /// The process's start identity, empty where the platform has none.
    pub(super) start: Vec<u8>,
    /// The process lock's path, in the platform's own encoding.
    pub(super) lock: Vec<u8>,
    /// The process lock's file identity, so a different file at that path is
    /// never mistaken for it.
    pub(super) lock_identity: Vec<u8>,
}

/// One claim: its owning process and that claim's token, unique within it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Record {
    pub(super) process: Process,
    pub(super) token: u64,
}

/// What a claim file holds.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Contents {
    /// No owner was ever recorded, or the last one released.
    Empty,
    /// Bytes that are not a whole record. Records are written only under the
    /// claim file's lock, and a writer whose write fails never owns, so these
    /// come only from a writer that died mid-write: no owner.
    Torn,
    /// More bytes than any record: read only in part, so never decoded.
    Oversized,
    Record(Record),
}

/// FNV-1a: stable across builds, which a record must be, and enough to tell a
/// torn record from a whole one.
fn checksum(bytes: &[u8]) -> [u8; CHECKSUM_BYTES] {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash.to_le_bytes()
}

fn field(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(bytes);
}

impl Record {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        field(&mut out, &self.process.pid.to_le_bytes());
        field(&mut out, &self.process.start);
        field(&mut out, &self.process.lock);
        field(&mut out, &self.process.lock_identity);
        field(&mut out, &self.token.to_le_bytes());
        let sum = checksum(&out);
        out.extend_from_slice(&sum);
        out
    }
}

/// Reads fields front to back; any shortfall is a torn record.
struct Fields<'a>(&'a [u8]);

impl<'a> Fields<'a> {
    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        let (head, rest) = self.0.split_at_checked(count)?;
        self.0 = rest;
        Some(head)
    }

    fn field(&mut self) -> Option<&'a [u8]> {
        let length = u32::from_le_bytes(self.take(4)?.try_into().ok()?);
        self.take(usize::try_from(length).ok()?)
    }
}

impl Contents {
    pub(super) fn decode(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            return Self::Empty;
        }
        if bytes.len() > RECORD_LIMIT {
            return Self::Oversized;
        }
        Self::record(bytes).map_or(Self::Torn, Self::Record)
    }

    fn record(bytes: &[u8]) -> Option<Record> {
        let body = bytes.len().checked_sub(CHECKSUM_BYTES)?;
        let (body, sum) = bytes.split_at_checked(body)?;
        if checksum(body) != sum {
            return None;
        }
        let mut fields = Fields(body);
        if fields.take(MAGIC.len())? != MAGIC {
            return None;
        }
        let pid = u32::from_le_bytes(fields.field()?.try_into().ok()?);
        let start = fields.field()?.to_vec();
        let lock = fields.field()?.to_vec();
        let lock_identity = fields.field()?.to_vec();
        let token = u64::from_le_bytes(fields.field()?.try_into().ok()?);
        fields.0.is_empty().then_some(Record {
            process: Process {
                pid,
                start,
                lock,
                lock_identity,
            },
            token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(lock: &[u8]) -> Record {
        Record {
            process: Process {
                pid: 7,
                start: b"boot:1".to_vec(),
                lock: lock.to_vec(),
                lock_identity: vec![1, 2, 3],
            },
            token: 9,
        }
    }

    #[test]
    fn any_path_round_trips_including_newlines_and_separators() {
        for lock in [
            &b"/run/user/1000/acyclic-owners-1000/1-2.lock"[..],
            b"/run/a\nb/acyclic-claim-v1\n12\n\n/x.lock",
            b"\0\n\r:/\xff",
            b"",
        ] {
            let record = record(lock);
            assert_eq!(Contents::decode(&record.encode()), Contents::Record(record));
        }
    }

    /// A record whose encoding is exactly `length` bytes.
    fn record_of_length(length: usize) -> Record {
        let mut record = record(b"");
        let lock = length.saturating_sub(record.encode().len());
        record.process.lock = vec![b'/'; lock];
        record
    }

    #[test]
    fn a_record_at_the_bound_decodes_and_one_past_it_never_does() {
        let whole = record_of_length(RECORD_LIMIT);
        let encoded = whole.encode();
        assert_eq!(encoded.len(), RECORD_LIMIT);
        assert_eq!(Contents::decode(&encoded), Contents::Record(whole));
        let past = record_of_length(RECORD_LIMIT + 1).encode();
        assert_eq!(Contents::decode(&past), Contents::Oversized);
        // A whole record followed by anything is no longer that record.
        let mut padded = encoded;
        padded.push(0);
        assert_eq!(Contents::decode(&padded), Contents::Oversized);
        // Nor is any read cut at one past the bound.
        let longer = record_of_length(2 * RECORD_LIMIT).encode();
        assert_eq!(
            Contents::decode(longer.get(..=RECORD_LIMIT).unwrap_or_default()),
            Contents::Oversized
        );
    }

    #[test]
    fn every_cut_of_a_record_is_torn_never_another_record() {
        let encoded = record(b"/run/a\nb/x.lock").encode();
        for cut in 1..encoded.len() {
            assert_eq!(
                Contents::decode(encoded.get(..cut).unwrap_or_default()),
                Contents::Torn,
                "cut {cut}"
            );
        }
        assert_eq!(Contents::decode(&[]), Contents::Empty);
    }
}
