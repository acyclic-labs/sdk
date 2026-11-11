//! Shared content-addressed manifest codec and bounded content trees.
//!
//! Objects and staged Stream writes use the same persisted manifest format.
//! A page size splits work; it never bounds the logical content length.

use crate::{StreamError, content_digest};

pub mod tree;

/// Canonical Objects manifest v1 domain, including its version terminator.
pub const MANIFEST_MAGIC: &[u8; 20] = b"acyclic-manifest-v1\0";
const PART_BYTES: usize = 40;

/// One ordered content reference; lengths count logical bytes, not stored bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Part {
    /// BLAKE3-256 of the referenced stored bytes.
    pub digest: [u8; 32],
    /// Logical byte length, including all descendants of a manifest.
    pub length: u64,
}

/// Validated borrowed manifest. Decoding allocates nothing and follows no edges.
#[derive(Clone, Copy)]
pub struct Manifest<'a> {
    parts: &'a [u8],
    edges: &'a [u8],
}

impl<'a> Manifest<'a> {
    /// Validates the version, counts, exact frame length and all byte ranges.
    pub fn decode(input: &'a [u8]) -> Result<Self, StreamError> {
        let input = crate::persistence::header(input, b"acyclic-manifest-v", b"1\0")?;
        let (parts, input) = section(input, PART_BYTES)?;
        let (edges, rest) = section(input, 32)?;
        if !rest.is_empty() {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self { parts, edges })
    }

    /// Canonical ordered part bytes, for an adapter with its own allocation claims.
    pub const fn part_bytes(&self) -> &'a [u8] {
        self.parts
    }

    /// Canonical retained-edge bytes, for an adapter with its own allocation claims.
    pub const fn edge_bytes(&self) -> &'a [u8] {
        self.edges
    }

    /// Ordered references without allocating a decoded vector.
    pub fn parts(&self) -> impl ExactSizeIterator<Item = Part> + 'a {
        self.parts.chunks_exact(PART_BYTES).map(|part| {
            let mut digest = [0; 32];
            digest.copy_from_slice(&part[..32]);
            let mut length = [0; 8];
            length.copy_from_slice(&part[32..]);
            Part {
                digest,
                length: u64::from_le_bytes(length),
            }
        })
    }

    /// Additional retained references, without allocating a decoded vector.
    pub fn edges(&self) -> impl ExactSizeIterator<Item = [u8; 32]> + 'a {
        self.edges.chunks_exact(32).map(|bytes| {
            let mut digest = [0; 32];
            digest.copy_from_slice(bytes);
            digest
        })
    }

    /// Total logical byte length, checked before traversing any child.
    pub fn length(&self) -> Result<u64, StreamError> {
        self.parts().try_fold(0_u64, |sum, part| {
            sum.checked_add(part.length).ok_or(StreamError::Capacity)
        })
    }
}

fn section(input: &[u8], width: usize) -> Result<(&[u8], &[u8]), StreamError> {
    let (count, input) = input
        .split_at_checked(4)
        .ok_or(StreamError::InvalidArgument)?;
    let mut encoded = [0; 4];
    encoded.copy_from_slice(count);
    let length = usize::try_from(u32::from_le_bytes(encoded))
        .ok()
        .and_then(|count| count.checked_mul(width))
        .ok_or(StreamError::InvalidArgument)?;
    input
        .split_at_checked(length)
        .ok_or(StreamError::InvalidArgument)
}

/// Encodes one physical manifest. Larger logical lists are trees of manifests.
/// Returns capacity refusal rather than truncating an unrepresentable count.
pub fn encode_manifest(
    parts: impl ExactSizeIterator<Item = Part>,
    edges: impl ExactSizeIterator<Item = [u8; 32]>,
) -> Result<Vec<u8>, StreamError> {
    let count = u32::try_from(parts.len()).map_err(|_| StreamError::Capacity)?;
    let edge_count = u32::try_from(edges.len()).map_err(|_| StreamError::Capacity)?;
    let length = parts
        .len()
        .checked_mul(PART_BYTES)
        .and_then(|length| edges.len().checked_mul(32)?.checked_add(length))
        .and_then(|length| length.checked_add(MANIFEST_MAGIC.len() + 8))
        .ok_or(StreamError::Capacity)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| StreamError::Capacity)?;
    output.extend_from_slice(MANIFEST_MAGIC);
    output.extend_from_slice(&count.to_le_bytes());
    for part in parts {
        output.extend_from_slice(&part.digest);
        output.extend_from_slice(&part.length.to_le_bytes());
    }
    output.extend_from_slice(&edge_count.to_le_bytes());
    for edge in edges {
        output.extend_from_slice(&edge);
    }
    Ok(output)
}

/// Verifies stored bytes before a reader interprets any referenced content.
pub fn verify(digest: &[u8; 32], bytes: &[u8]) -> Result<(), StreamError> {
    if content_digest(bytes) == *digest {
        Ok(())
    } else {
        Err(StreamError::InvalidResponse)
    }
}
