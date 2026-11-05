//! The claim record: which process, and which of its claims, owns a store.
//!
//! Every field is length-prefixed and the whole record is checksummed, so any
//! path or identity round-trips exactly, and a record cut short by a crash is
//! recognized as such.

pub(super) const MAGIC: &[u8] = b"acyclic-claim-v2\0";
pub(super) const CHECKSUM_BYTES: usize = 8;

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

/// FNV-1a: stable across builds, which a record must be, and enough to tell a
/// torn record from a whole one.
pub(super) fn checksum(bytes: &[u8]) -> [u8; CHECKSUM_BYTES] {
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

    pub(super) fn decode_inner(bytes: &'a [u8], verify_checksum: bool) -> Self {
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
