//! Content addresses, logical ranges, manifests and the v1 envelope encoding.
use crate::{Error, wire::ErrorCode};

/// BLAKE3-256 of a blob's stored bytes.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Digest([u8; 32]);

impl Digest {
    /// Hashes `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
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
        hex::encode(self.0)
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
        write!(formatter, "Digest({})", self.to_hex())
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_hex())
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
}

const MANIFEST_MAGIC: &[u8; 20] = b"acyclic-manifest-v1\0";
const ENVELOPE_MAGIC: &[u8; 16] = b"acyclic-blob-v1\0";

impl Manifest {
    /// Total logical length of the parts.
    pub fn length(&self) -> Result<u64, Error> {
        self.parts.iter().try_fold(0u64, |total, part| {
            total
                .checked_add(part.length)
                .ok_or_else(|| ErrorCode::QuotaExceeded.into())
        })
    }

    /// Canonical v1 encoding; its BLAKE3 is the manifest's digest.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            MANIFEST_MAGIC.len() + 8 + self.parts.len() * 40 + self.edges.len() * 32,
        );
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&(self.parts.len() as u32).to_le_bytes());
        for part in &self.parts {
            bytes.extend_from_slice(part.digest.as_bytes());
            bytes.extend_from_slice(&part.length.to_le_bytes());
        }
        bytes.extend_from_slice(&(self.edges.len() as u32).to_le_bytes());
        for edge in &self.edges {
            bytes.extend_from_slice(edge.as_bytes());
        }
        bytes
    }

    /// Decodes exactly one canonical encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader(bytes);
        if reader.take(MANIFEST_MAGIC.len())? != MANIFEST_MAGIC {
            return Err(corrupt());
        }
        let parts = (0..reader.u32()?)
            .map(|_| {
                Ok(Part {
                    digest: reader.digest()?,
                    length: reader.u64()?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let edges = (0..reader.u32()?)
            .map(|_| reader.digest())
            .collect::<Result<Vec<_>, Error>>()?;
        reader.finish()?;
        Ok(Self { parts, edges })
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
    /// Unique to the put that published this blob; names its payload and
    /// index keys, so no actor working on an earlier blob of the same digest
    /// can match anything of this one.
    pub(crate) version: [u8; 16],
    /// The deleter that owns this blob's removal, if any.
    pub(crate) dying: Option<[u8; 16]>,
}

impl Envelope {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ENVELOPE_MAGIC.len() + 54 + self.references.len() * 32);
        bytes.extend_from_slice(ENVELOPE_MAGIC);
        bytes.push(match self.kind {
            Kind::Blob => 1,
            Kind::Manifest => 2,
        });
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.stored.to_le_bytes());
        bytes.extend_from_slice(&(self.references.len() as u32).to_le_bytes());
        for reference in &self.references {
            bytes.extend_from_slice(reference.as_bytes());
        }
        bytes.extend_from_slice(&self.version);
        match self.dying {
            None => bytes.push(0),
            Some(deleter) => {
                bytes.push(1);
                bytes.extend_from_slice(&deleter);
            }
        }
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader(bytes);
        if reader.take(ENVELOPE_MAGIC.len())? != ENVELOPE_MAGIC {
            return Err(corrupt());
        }
        let kind = match reader.take(1)? {
            [1] => Kind::Blob,
            [2] => Kind::Manifest,
            _ => return Err(corrupt()),
        };
        let length = reader.u64()?;
        let stored = reader.u64()?;
        let references = (0..reader.u32()?)
            .map(|_| reader.digest())
            .collect::<Result<Vec<_>, Error>>()?;
        let nonce = |reader: &mut Reader<'_>| -> Result<[u8; 16], Error> {
            reader.take(16)?.try_into().map_err(|_| corrupt())
        };
        let version = nonce(&mut reader)?;
        let dying = match reader.take(1)? {
            [0] => None,
            [1] => Some(nonce(&mut reader)?),
            _ => return Err(corrupt()),
        };
        reader.finish()?;
        Ok(Self {
            kind,
            length,
            stored,
            references,
            version,
            dying,
        })
    }
}

/// Encodes one exact retained reference batch.
pub(crate) fn encode_batch(references: &[Digest]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(references.len() * 32);
    for reference in references {
        bytes.extend_from_slice(reference.as_bytes());
    }
    bytes
}

pub(crate) fn decode_batch(bytes: &[u8]) -> Result<Vec<Digest>, Error> {
    if bytes.len() % 32 != 0 {
        return Err(corrupt());
    }
    bytes.chunks_exact(32).map(Digest::from_slice).collect()
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
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

pub(crate) fn invalid() -> Error {
    ErrorCode::InvalidArgument.into()
}

pub(crate) fn corrupt() -> Error {
    ErrorCode::Corrupt.into()
}
