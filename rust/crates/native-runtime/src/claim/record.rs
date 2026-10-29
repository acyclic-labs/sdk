//! The claim record: which process, and which of its claims, owns a store.
//!
//! Every field is length-prefixed and the whole record is checksummed, so any
//! path or identity round-trips exactly, and a record cut short by a crash is
//! recognized as such.

const MAGIC: &[u8] = b"acyclic-claim-v1\0";
const CHECKSUM_BYTES: usize = 8;

/// The process that owns a claim, as other processes find it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Process<B = Vec<u8>> {
    pub(super) pid: u32,
    /// The process's start identity, empty where the platform has none.
    pub(super) start: B,
    /// The process lock's path, in the platform's own encoding.
    pub(super) lock: B,
    /// The process lock's file identity, so a different file at that path is
    /// never mistaken for it.
    pub(super) lock_identity: B,
}

impl<B: AsRef<[u8]>> Process<B> {
    pub(super) fn borrowed(&self) -> Process<&[u8]> {
        Process {
            pid: self.pid,
            start: self.start.as_ref(),
            lock: self.lock.as_ref(),
            lock_identity: self.lock_identity.as_ref(),
        }
    }
}

/// One claim: its owning process and that claim's token, unique within it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Record<B = Vec<u8>> {
    pub(super) process: Process<B>,
    pub(super) token: u64,
}

/// What a claim file holds.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Contents<'a> {
    /// No owner was ever recorded, or the last one released.
    Empty,
    /// Bytes that are not a whole record. Records are written only under the
    /// claim file's lock, and a writer whose write fails never owns, so these
    /// come only from a writer that died mid-write: no owner.
    Torn,
    /// Intact framing or an incompatible header contradicts a claim record.
    Malformed,
    Record(Record<&'a [u8]>),
}

/// Complete record storage; its buffer is destroyed before its original credit.
pub(super) struct ReadRecord {
    bytes: Vec<u8>,
    status: Option<Status>,
    _allocation: Option<crate::ReadAllocation>,
}

#[derive(Clone, Copy)]
enum Status {
    Empty,
    Torn,
    Malformed,
    Verified,
}

impl ReadRecord {
    #[cfg(all(test, loom))]
    pub(super) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            status: None,
            _allocation: None,
        }
    }

    pub(super) fn contents(&self) -> Contents<'_> {
        match self.status {
            Some(Status::Empty) => Contents::Empty,
            Some(Status::Torn) => Contents::Torn,
            Some(Status::Malformed) => Contents::Malformed,
            Some(Status::Verified) => Contents::decode_inner(&self.bytes, false),
            None => Contents::decode(&self.bytes),
        }
    }

    // Inspect framing and checksum in constant scratch space before allocating.
    // A large sparse/corrupt claim file therefore cannot request its file size
    // from the allocator. The same original meter pays for each actual read.
    pub(super) fn read(
        file: &mut (impl std::io::Read + std::io::Seek),
        meter: Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
    ) -> std::io::Result<Self> {
        use std::io::{self, SeekFrom};
        let mut result = Self {
            bytes: Vec::new(),
            status: None,
            _allocation: None,
        };
        let length = native_operation(meter, || file.seek(SeekFrom::End(0)))?;
        native_operation(meter, || file.seek(SeekFrom::Start(0)))?;
        if length == 0 {
            result.status = Some(Status::Empty);
            return Ok(result);
        }
        let mut magic = [0_u8; MAGIC.len()];
        let prefix = usize::try_from(length.min(MAGIC.len() as u64))
            .map_err(|_| io::ErrorKind::InvalidData)?;
        read_exact(file, &mut magic[..prefix], meter)?;
        if magic[..prefix] != MAGIC[..prefix] {
            result.status = Some(Status::Malformed);
            return Ok(result);
        }
        if prefix < MAGIC.len() {
            result.status = Some(Status::Torn);
            return Ok(result);
        }
        let mut position = MAGIC.len() as u64;
        for index in 0..5 {
            if length.saturating_sub(position) < 4 {
                result.status = Some(Status::Torn);
                return Ok(result);
            }
            let mut size = [0_u8; 4];
            read_exact(file, &mut size, meter)?;
            position += 4;
            let size = u64::from(u32::from_le_bytes(size));
            if (index == 0 && size != 4) || (index == 4 && size != 8) {
                result.status = Some(Status::Malformed);
                return Ok(result);
            }
            if size > length.saturating_sub(position) {
                result.status = Some(Status::Torn);
                return Ok(result);
            }
            position += size;
            native_operation(meter, || file.seek(SeekFrom::Start(position)))?;
        }
        match length
            .saturating_sub(position)
            .cmp(&(CHECKSUM_BYTES as u64))
        {
            std::cmp::Ordering::Less => {
                result.status = Some(Status::Torn);
                return Ok(result);
            }
            std::cmp::Ordering::Greater => {
                result.status = Some(Status::Malformed);
                return Ok(result);
            }
            std::cmp::Ordering::Equal => {}
        }
        native_operation(meter, || file.seek(SeekFrom::Start(0)))?;
        let mut remaining = length - CHECKSUM_BYTES as u64;
        let mut scratch = [0_u8; 16 * 1024];
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        while remaining != 0 {
            let count = usize::try_from(remaining.min(scratch.len() as u64))
                .map_err(|_| io::ErrorKind::InvalidData)?;
            read_exact(file, &mut scratch[..count], meter)?;
            if let Some(meter) = meter {
                meter.admit_codec(0, 0, count as u64)?;
            }
            for byte in &scratch[..count] {
                hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
            if let Some(meter) = meter {
                meter.record_codec(0, 0, count as u64)?;
            }
            remaining -= count as u64;
        }
        let mut sum = [0_u8; CHECKSUM_BYTES];
        read_exact(file, &mut sum, meter)?;
        if hash.to_le_bytes() != sum {
            result.status = Some(Status::Torn);
            return Ok(result);
        }
        let count = usize::try_from(length).map_err(|_| io::ErrorKind::InvalidData)?;
        result._allocation = meter
            .map(|meter| crate::ReadAllocation::new(meter, count))
            .transpose()?;
        result
            .bytes
            .try_reserve_exact(count)
            .map_err(|error| match meter {
                Some(meter) => meter.allocation_failed(error),
                None => io::Error::from(io::ErrorKind::OutOfMemory),
            })?;
        if let Some(allocation) = &mut result._allocation {
            allocation.reconcile_capacity(result.bytes.capacity())?;
        }
        result.bytes.resize(count, 0);
        native_operation(meter, || file.seek(SeekFrom::Start(0)))?;
        read_exact(file, &mut result.bytes, meter)?;
        // Revalidate the owned snapshot too, even if an external writer ignores
        // the protocol lock. Contents then borrows only this verified buffer.
        let body = result.bytes.len() - CHECKSUM_BYTES;
        if let Some(meter) = meter {
            meter.admit_codec(0, 0, body as u64)?;
        }
        let valid = checksum(&result.bytes[..body]) == result.bytes[body..];
        if let Some(meter) = meter {
            meter.record_codec(0, 0, body as u64)?;
        }
        result.status = Some(if valid {
            Status::Verified
        } else {
            Status::Torn
        });
        Ok(result)
    }
}

pub(super) fn native_operation<T>(
    meter: Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> std::io::Result<T> {
    crate::meter::read_operation(meter, operation)?
}

fn read_exact(
    file: &mut impl std::io::Read,
    mut bytes: &mut [u8],
    meter: Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
) -> std::io::Result<()> {
    use std::io;
    while !bytes.is_empty() {
        let mut transfer = meter
            .map(|meter| {
                crate::NativeByteReservation::new(
                    meter.clone(),
                    crate::NativeOperationKind::Read,
                    bytes.len(),
                )
            })
            .transpose()?;
        let result = native_operation(meter, || file.read(bytes));
        if let Some(transfer) = &mut transfer {
            transfer.record(result.as_ref().copied().unwrap_or(0))?;
        }
        match result {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => bytes = &mut bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
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

impl<B: AsRef<[u8]>> Record<B> {
    pub(super) fn borrowed(&self) -> Record<&[u8]> {
        Record {
            process: self.process.borrowed(),
            token: self.token,
        }
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        field(&mut out, &self.process.pid.to_le_bytes());
        field(&mut out, self.process.start.as_ref());
        field(&mut out, self.process.lock.as_ref());
        field(&mut out, self.process.lock_identity.as_ref());
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

impl<'a> Contents<'a> {
    pub(super) fn decode(bytes: &'a [u8]) -> Self {
        Self::decode_inner(bytes, true)
    }

    fn decode_inner(bytes: &'a [u8], verify_checksum: bool) -> Self {
        if bytes.is_empty() {
            return Self::Empty;
        }
        if bytes.len() >= MAGIC.len() && !bytes.starts_with(MAGIC) {
            return Self::Malformed;
        }
        // Trailing bytes after complete declared fields cannot be a partial
        // write. Preserve fail-closed handling without a whole-record ceiling.
        let mut framing = Fields(bytes);
        if framing.take(MAGIC.len()).is_some()
            && (0..5).all(|_| framing.field().is_some())
            && framing.0.len() > CHECKSUM_BYTES
        {
            return Self::Malformed;
        }
        Self::record(bytes, verify_checksum).map_or(Self::Torn, Self::Record)
    }

    fn record(bytes: &'a [u8], verify_checksum: bool) -> Option<Record<&'a [u8]>> {
        let body = bytes.len().checked_sub(CHECKSUM_BYTES)?;
        let (body, sum) = bytes.split_at_checked(body)?;
        if verify_checksum && checksum(body) != sum {
            return None;
        }
        let mut fields = Fields(body);
        if fields.take(MAGIC.len())? != MAGIC {
            return None;
        }
        let pid = u32::from_le_bytes(fields.field()?.try_into().ok()?);
        let start = fields.field()?;
        let lock = fields.field()?;
        let lock_identity = fields.field()?;
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
            assert_eq!(
                Contents::decode(&record.encode()),
                Contents::Record(record.borrowed())
            );
        }
    }

    #[test]
    fn long_fields_round_trip_and_trailing_bytes_are_corrupt() {
        let record = record(&vec![b'/'; 65_537]);
        let mut bytes = record.encode();
        assert_eq!(
            Contents::decode(&bytes),
            Contents::Record(record.borrowed())
        );
        bytes.push(0);
        assert_eq!(Contents::decode(&bytes), Contents::Malformed);
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
