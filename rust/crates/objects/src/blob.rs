//! Content addresses, logical ranges, manifests and the v2 envelope encoding.
use crate::{Error, error::invalid, wire::ErrorCode};
use sha2::{Digest as _, Sha256};

/// Hashes the logical body in input order, independent of transport framing.
/// Both digests consume each bounded input segment before the next is read.
#[derive(Default)]
pub struct ContentHasher {
    blake3: blake3::Hasher,
    sha256: Sha256,
    length: u64,
}

/// Digests computed from one complete logical body. BLAKE3 is its content
/// identity; SHA-256 is a checksum for contracts that require that algorithm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentDigests {
    blake3: Digest,
    sha256: [u8; 32],
    length: u64,
}

impl ContentHasher {
    /// Adds bytes to both digests. Overflow leaves the hasher unchanged.
    pub fn update(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or(Error::from(ErrorCode::QuotaExceeded))?;
        for chunk in bytes.chunks(64 * 1024) {
            self.blake3.update(chunk);
            self.sha256.update(chunk);
        }
        self.length = length;
        Ok(())
    }

    /// Finishes both digests over the same accepted input.
    #[must_use]
    pub fn finish(self) -> ContentDigests {
        ContentDigests {
            blake3: Digest::from_bytes(*self.blake3.finalize().as_bytes()),
            sha256: self.sha256.finalize().into(),
            length: self.length,
        }
    }
}

impl ContentDigests {
    /// The BLAKE3 digest of the logical bytes.
    #[must_use]
    pub const fn blake3(&self) -> Digest {
        self.blake3
    }
    /// The SHA-256 checksum of those same bytes.
    #[must_use]
    pub const fn sha256(&self) -> &[u8; 32] {
        &self.sha256
    }
    /// The number of logical bytes hashed.
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }
}

#[cfg(test)]
mod content_digest_tests {
    use super::{ContentHasher, Digest, Manifest, Part};
    use sha2::{Digest as _, Sha256};

    #[test]
    fn logical_checksums_are_independent_of_input_frames() {
        let expected_sha256 =
            hex::decode("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                .unwrap();
        for frames in [
            &[b"abc".as_slice()][..],
            &[b"a".as_slice(), b"bc".as_slice()][..],
        ] {
            let mut hasher = ContentHasher::default();
            for frame in frames {
                hasher.update(frame).unwrap();
            }
            let result = hasher.finish();
            assert_eq!(result.blake3(), Digest::of(b"abc"));
            assert_eq!(result.sha256().as_slice(), expected_sha256);
            assert_eq!(result.length(), 3);
            let manifest = Manifest {
                parts: vec![Part {
                    digest: result.blake3(),
                    length: result.length(),
                }],
                edges: Vec::new(),
                sha256: Some(*result.sha256()),
            };
            let encoded = manifest.encode().unwrap();
            assert_eq!(Manifest::decode(&encoded).unwrap(), manifest);
        }
    }

    #[test]
    fn logical_checksums_cross_chunk_and_frame_boundaries() {
        let bytes: Vec<_> = (0..(3 * 65536 + 19))
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect();
        let expected_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        let expected_blake3 = Digest::of(&bytes);
        for boundary in [1, 65535, 65536, 65537, 131073] {
            let mut hasher = ContentHasher::default();
            hasher.update(&bytes[..boundary]).unwrap();
            hasher.update(&bytes[boundary..]).unwrap();
            let result = hasher.finish();
            assert_eq!(result.blake3(), expected_blake3);
            assert_eq!(*result.sha256(), expected_sha256);
            assert_eq!(result.length(), bytes.len() as u64);
        }
    }
}

/// BLAKE3-256 of a blob's stored bytes.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Digest([u8; 32]);

impl Digest {
    /// Hashes `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(acyclic_stream::content_digest(bytes))
    }

    /// The digest a seal proved for its bytes.
    #[must_use]
    pub const fn sealed(body: &acyclic_stream::SealedBytes) -> Self {
        Self(*body.digest())
    }

    /// Wraps an already computed BLAKE3-256 value.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32 bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hexadecimal, the form used in storage keys.
    #[must_use]
    pub fn to_hex(&self) -> String {
        self.encoded_hex().as_ref().to_owned()
    }

    pub(crate) fn encoded_hex(&self) -> impl AsRef<str> {
        blake3::Hash::from_bytes(self.0).to_hex()
    }

    /// The first two lowercase hexadecimal digits, the storage shard.
    #[must_use]
    pub fn hex_prefix(&self) -> String {
        let [first, ..] = self.0;
        hex::encode([first])
    }

    /// Parses exactly 64 lowercase or uppercase hexadecimal digits.
    pub fn from_hex(text: &str) -> Result<Self, Error> {
        let mut bytes = [0; 32];
        hex::decode_to_slice(text, &mut bytes).map_err(|_| invalid())?;
        Ok(Self(bytes))
    }

    pub(crate) fn from_slice(bytes: &[u8]) -> Result<Self, Error> {
        <[u8; 32]>::try_from(bytes).map(Self).map_err(|_| invalid())
    }
}

impl std::fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Digest({self})")
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.encoded_hex().as_ref())
    }
}

/// What a digest names.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Kind {
    /// The stored bytes are the logical bytes.
    Blob,
    /// The stored bytes are a canonical [`Manifest`]; reads concatenate its parts.
    Manifest,
}

/// Half-open logical byte range `[start, end)`; `end: None` selects to the last byte.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ByteRange {
    /// First selected byte.
    pub start: u64,
    /// One past the last selected byte, or the end of the content.
    pub end: Option<u64>,
}

impl ByteRange {
    /// Every byte.
    pub const FULL: Self = Self {
        start: 0,
        end: None,
    };

    /// Selects `[start, end)`.
    #[must_use]
    pub const fn new(start: u64, end: u64) -> Self {
        Self {
            start,
            end: Some(end),
        }
    }

    /// Resolves against a content length, refusing an empty or out-of-bounds selection
    /// of nonempty content. The full range of empty content is the empty selection.
    pub fn resolve(self, length: u64) -> Result<std::ops::Range<u64>, Error> {
        let end = self.end.unwrap_or(length);
        if end > length || self.start > end || (self.start == end && self != Self::FULL) {
            return Err(ErrorCode::RangeNotSatisfiable.into());
        }
        Ok(self.start..end)
    }
}

/// One ordered manifest part.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Part {
    /// The part's own digest; it may itself be a manifest.
    pub digest: Digest,
    /// The part's logical length.
    pub length: u64,
}

/// Ordered parts concatenating to the logical bytes, plus typed edges that the
/// manifest keeps alive without contributing bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Manifest {
    /// Parts in logical order.
    pub parts: Vec<Part>,
    /// Additional retained references.
    pub edges: Vec<Digest>,
    /// A publisher-declared SHA-256 of the concatenated logical bytes, when an
    /// external contract requires it. This is not a content address and is
    /// not verified by manifest admission.
    pub sha256: Option<[u8; 32]>,
}

const MANIFEST_MAGIC: &[u8; 20] = b"acyclic-manifest-v2\0";
const ENVELOPE_MAGIC: &[u8; 16] = b"acyclic-blob-v3\0";
const BATCH_MAGIC: &[u8] = b"acyclic-retained-v1\0";
const INCARNATION_MAGIC: &[u8] = b"acyclic-incarnation-v1\0";
const EPOCH_MAGIC: &[u8] = b"acyclic-epoch-v1\0";

/// Admission of the decoded vectors, before their allocator calls.
pub(crate) trait DecodeAllocation {
    fn admit(&self, bytes: usize) -> Result<(), Error>;
    fn capacity(&self, admitted: usize, actual: usize) -> Result<(), Error>;
}

impl DecodeAllocation for () {
    fn admit(&self, _bytes: usize) -> Result<(), Error> {
        Ok(())
    }

    fn capacity(&self, _admitted: usize, _actual: usize) -> Result<(), Error> {
        Ok(())
    }
}

/// One admission and capacity reconciliation for engine-owned vectors.
pub(crate) fn reserve_admitted<T>(
    values: &mut Vec<T>,
    additional: usize,
    allocation: &impl DecodeAllocation,
) -> Result<(), Error> {
    let needed = values
        .len()
        .checked_add(additional)
        .ok_or(Error::from(ErrorCode::QuotaExceeded))?;
    if needed <= values.capacity() {
        return Ok(());
    }
    let width = std::mem::size_of::<T>();
    let admitted = needed
        .checked_mul(width)
        .ok_or(Error::from(ErrorCode::QuotaExceeded))?;
    let held = admitted
        .checked_add(values.capacity() * width)
        .ok_or(Error::from(ErrorCode::QuotaExceeded))?;
    allocation.admit(admitted)?;
    values
        .try_reserve_exact(additional)
        .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
    // Both backings stay funded through reallocation; only the replacement
    // remains live once it succeeds.
    allocation.capacity(held, values.capacity() * width)
}

impl Manifest {
    /// Total logical length of the parts.
    pub fn length(&self) -> Result<u64, Error> {
        self.parts.iter().try_fold(0u64, |total, part| {
            total
                .checked_add(part.length)
                .ok_or_else(|| ErrorCode::QuotaExceeded.into())
        })
    }

    /// Canonical v2 encoding; its BLAKE3 is the manifest's digest.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let parts =
            u32::try_from(self.parts.len()).map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
        let edges =
            u32::try_from(self.edges.len()).map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
        let mut bytes = Vec::with_capacity(
            MANIFEST_MAGIC.len() + 41 + self.parts.len() * 40 + self.edges.len() * 32,
        );
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&parts.to_le_bytes());
        for part in &self.parts {
            bytes.extend_from_slice(part.digest.as_bytes());
            bytes.extend_from_slice(&part.length.to_le_bytes());
        }
        bytes.extend_from_slice(&edges.to_le_bytes());
        for edge in &self.edges {
            bytes.extend_from_slice(edge.as_bytes());
        }
        bytes.push(u8::from(self.sha256.is_some()));
        bytes.extend_from_slice(&self.sha256.unwrap_or_default());
        Ok(bytes)
    }

    /// Decodes exactly one canonical encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_admitted(bytes, &())
    }

    pub(crate) fn decode_admitted(
        bytes: &[u8],
        allocation: &impl DecodeAllocation,
    ) -> Result<Self, Error> {
        let mut reader = Reader(bytes);
        reader.header(MANIFEST_MAGIC)?;
        let count = reader.u32()? as usize;
        let parts = reader.vector(count, 40, allocation, |reader| {
            Ok(Part {
                digest: reader.digest()?,
                length: reader.u64()?,
            })
        })?;
        let count = reader.u32()? as usize;
        let edges = reader.vector(count, 32, allocation, |reader| reader.digest())?;
        let present = reader.take(1)?;
        let checksum: [u8; 32] = reader.take(32)?.try_into().map_err(|_| corrupt())?;
        let sha256 = match present {
            [0] if checksum == [0; 32] => None,
            [1] => Some(checksum),
            _ => return Err(corrupt()),
        };
        reader.finish()?;
        Ok(Self {
            parts,
            edges,
            sha256,
        })
    }

    /// Every outgoing reference, parts first, each digest once.
    #[must_use]
    pub fn references(&self) -> Vec<Digest> {
        let mut seen = std::collections::HashSet::new();
        self.parts
            .iter()
            .map(|part| part.digest)
            .chain(self.edges.iter().copied())
            .filter(|digest| seen.insert(*digest))
            .collect()
    }
}

/// The stored metadata beside a payload. References live here, outside the
/// payload, so marking never decodes customer bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Envelope {
    pub(crate) kind: Kind,
    pub(crate) length: u64,
    pub(crate) stored: u64,
    pub(crate) references: Vec<Digest>,
    pub(crate) sha256: Option<[u8; 32]>,
    /// Unique to the put that published this blob; names its payload and
    /// index keys, so no actor working on an earlier blob of the same digest
    /// can match anything of this one.
    pub(crate) version: [u8; 16],
    /// The deleter that owns this blob's removal, if any. Encoded at a fixed
    /// width, so marking and reviving never change the envelope's size and a
    /// full bounded medium can still free space.
    pub(crate) dying: Option<[u8; 16]>,
    /// The epoch of the collection that first found this blob unreferenced,
    /// zero for none; the collection two epochs later removes it, unless a
    /// put clears it first. Fixed width, so stamping never changes the
    /// envelope's size.
    pub(crate) condemned: u64,
}

impl Envelope {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        if self.kind == Kind::Blob && self.sha256.is_some() {
            return Err(invalid());
        }
        let references = u32::try_from(self.references.len())
            .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
        let mut bytes = Vec::with_capacity(ENVELOPE_MAGIC.len() + 95 + self.references.len() * 32);
        bytes.extend_from_slice(ENVELOPE_MAGIC);
        bytes.push(match self.kind {
            Kind::Blob => 1,
            Kind::Manifest => 2,
        });
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.stored.to_le_bytes());
        bytes.extend_from_slice(&references.to_le_bytes());
        for reference in &self.references {
            bytes.extend_from_slice(reference.as_bytes());
        }
        bytes.push(u8::from(self.sha256.is_some()));
        bytes.extend_from_slice(&self.sha256.unwrap_or_default());
        bytes.extend_from_slice(&self.version);
        bytes.push(u8::from(self.dying.is_some()));
        bytes.extend_from_slice(&self.dying.unwrap_or_default());
        bytes.extend_from_slice(&self.condemned.to_le_bytes());
        Ok(bytes)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_admitted(bytes, &())
    }

    pub(crate) fn decode_admitted(
        bytes: &[u8],
        allocation: &impl DecodeAllocation,
    ) -> Result<Self, Error> {
        let mut reader = Reader(bytes);
        reader.header(ENVELOPE_MAGIC)?;
        let kind = match reader.take(1)? {
            [1] => Kind::Blob,
            [2] => Kind::Manifest,
            _ => return Err(corrupt()),
        };
        let length = reader.u64()?;
        let stored = reader.u64()?;
        let count = reader.u32()? as usize;
        let references = reader.vector(count, 32, allocation, |reader| reader.digest())?;
        let present = reader.take(1)?;
        let checksum: [u8; 32] = reader.take(32)?.try_into().map_err(|_| corrupt())?;
        let sha256 = match present {
            [0] if checksum == [0; 32] => None,
            [1] if kind == Kind::Manifest => Some(checksum),
            _ => return Err(corrupt()),
        };
        let nonce = |reader: &mut Reader<'_>| -> Result<[u8; 16], Error> {
            reader.take(16)?.try_into().map_err(|_| corrupt())
        };
        let version = nonce(&mut reader)?;
        let dying = match reader.take(1)? {
            [0] if nonce(&mut reader)? == [0; 16] => None,
            [1] => Some(nonce(&mut reader)?),
            _ => return Err(corrupt()),
        };
        let condemned = reader.u64()?;
        reader.finish()?;
        Ok(Self {
            kind,
            length,
            stored,
            references,
            sha256,
            version,
            dying,
            condemned,
        })
    }
}

/// Encodes one exact retained reference batch.
pub(crate) fn encode_batch(references: &[Digest]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(BATCH_MAGIC.len() + references.len() * 32);
    bytes.extend_from_slice(BATCH_MAGIC);
    for reference in references {
        bytes.extend_from_slice(reference.as_bytes());
    }
    bytes
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn decode_batch(bytes: &[u8]) -> Result<Vec<Digest>, Error> {
    decode_batch_admitted(bytes, &())
}

pub(crate) fn decode_batch_admitted(
    bytes: &[u8],
    allocation: &impl DecodeAllocation,
) -> Result<Vec<Digest>, Error> {
    let mut reader = Reader(bytes);
    reader.header(BATCH_MAGIC)?;
    if reader.0.len() % 32 != 0 {
        return Err(corrupt());
    }
    reader.vector(reader.0.len() / 32, 32, allocation, |reader| {
        reader.digest()
    })
}

pub(crate) fn encode_incarnation(incarnation: u128, run: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(INCARNATION_MAGIC.len() + 24);
    bytes.extend_from_slice(INCARNATION_MAGIC);
    bytes.extend_from_slice(&incarnation.to_be_bytes());
    bytes.extend_from_slice(&run.to_be_bytes());
    bytes
}

pub(crate) fn decode_incarnation(bytes: &[u8]) -> Result<(u128, u64), Error> {
    let mut reader = Reader(bytes);
    reader.header(INCARNATION_MAGIC)?;
    let incarnation = u128::from_be_bytes(reader.take(16)?.try_into().map_err(|_| corrupt())?);
    let run = u64::from_be_bytes(reader.take(8)?.try_into().map_err(|_| corrupt())?);
    reader.finish()?;
    Ok((incarnation, run))
}

pub(crate) fn encode_epoch(epoch: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(EPOCH_MAGIC.len() + 8);
    bytes.extend_from_slice(EPOCH_MAGIC);
    bytes.extend_from_slice(&epoch.to_be_bytes());
    bytes
}

pub(crate) fn decode_epoch(bytes: &[u8]) -> Result<u64, Error> {
    let mut reader = Reader(bytes);
    reader.header(EPOCH_MAGIC)?;
    let epoch = u64::from_be_bytes(reader.take(8)?.try_into().map_err(|_| corrupt())?);
    reader.finish()?;
    Ok(epoch)
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn vector<T>(
        &mut self,
        count: usize,
        width: usize,
        allocation: &impl DecodeAllocation,
        mut decode: impl FnMut(&mut Reader<'_>) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error> {
        // Validate the complete fixed-width body before admitting an allocation.
        let bytes = self.take(count.checked_mul(width).ok_or_else(corrupt)?)?;
        let mut values = Vec::new();
        reserve_admitted(&mut values, count, allocation)?;
        let mut reader = Reader(bytes);
        for _ in 0..count {
            values.push(decode(&mut reader)?);
        }
        Ok(values)
    }

    // Domain first, then version, before any counts, allocations or body fields.
    fn header(&mut self, expected: &[u8]) -> Result<(), Error> {
        let domain_length = expected
            .len()
            .checked_sub(2)
            .ok_or_else(|| Error::from(ErrorCode::Unsupported))?;
        let (domain, version) = expected
            .split_at_checked(domain_length)
            .ok_or_else(|| Error::from(ErrorCode::Unsupported))?;
        self.0 = acyclic_stream::persistence::header(self.0, domain, version)
            .map_err(|_| Error::from(ErrorCode::Unsupported))?;
        Ok(())
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], Error> {
        if self.0.len() < count {
            return Err(corrupt());
        }
        let (head, tail) = self.0.split_at(count);
        self.0 = tail;
        Ok(head)
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| corrupt())?,
        ))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| corrupt())?,
        ))
    }
    fn digest(&mut self) -> Result<Digest, Error> {
        Digest::from_slice(self.take(32)?)
    }
    fn finish(self) -> Result<(), Error> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(corrupt())
        }
    }
}

pub(crate) fn corrupt() -> Error {
    ErrorCode::Corrupt.into()
}

#[cfg(test)]
mod tests;

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod codec_fuzz;
