//! On-demand checks of the stored codecs through their canonical implementation.
use super::{
    BATCH_MAGIC, DecodeAllocation, ENVELOPE_MAGIC, EPOCH_MAGIC, Envelope, INCARNATION_MAGIC,
    MANIFEST_MAGIC, decode_batch, decode_batch_admitted, decode_epoch, decode_incarnation,
    encode_batch, encode_epoch, encode_incarnation,
};
use crate::{ByteRange, Digest, Kind, Manifest, Part};
use crate::{Error, wire::ErrorCode};
use std::cell::Cell;

struct Admission<'a> {
    expected: &'a [usize],
    next: Cell<usize>,
    completed: Cell<usize>,
    refuse: Option<usize>,
}

impl DecodeAllocation for Admission<'_> {
    fn admit(&self, bytes: usize) -> Result<(), Error> {
        let next = self.next.get();
        self.next.set(next + 1);
        if self.expected.get(next) != Some(&bytes) || self.refuse == Some(next) {
            return Err(ErrorCode::QuotaExceeded.into());
        }
        Ok(())
    }

    fn capacity(&self, admitted: usize, actual: usize) -> Result<(), Error> {
        let completed = self.completed.get();
        if self.expected.get(completed) != Some(&admitted) || actual < admitted {
            return Err(ErrorCode::QuotaExceeded.into());
        }
        self.completed.set(completed + 1);
        Ok(())
    }
}

fn check_admission<T: PartialEq>(
    written: &T,
    bytes: &[u8],
    expected: &[usize],
    decode: impl Fn(&[u8], &Admission<'_>) -> Result<T, Error>,
) -> bool {
    let expected: Vec<_> = expected
        .iter()
        .copied()
        .filter(|&bytes| bytes != 0)
        .collect();
    // Each allocator boundary must be reached exactly once, and refusal must
    // stop before its capacity callback or any later allocator boundary.
    for refuse in (0..expected.len()).map(Some).chain([None]) {
        let admission = Admission {
            expected: &expected,
            next: Cell::new(0),
            completed: Cell::new(0),
            refuse,
        };
        let decoded = decode(bytes, &admission);
        if let Some(refuse) = refuse {
            if !decoded.is_err_and(|error| error.code == ErrorCode::QuotaExceeded)
                || admission.next.get() != refuse + 1
                || admission.completed.get() != refuse
            {
                return false;
            }
        } else if !decoded.is_ok_and(|decoded| &decoded == written)
            || admission.next.get() != expected.len()
            || admission.completed.get() != expected.len()
        {
            return false;
        }
    }
    true
}

/// Prepare an obsolete value using the original local compaction chunk size.
#[cfg(not(target_arch = "wasm32"))]
pub fn compaction_chunk_bytes() -> Result<usize, Error> {
    usize::try_from(crate::local::COMPACTION_CHUNK)
        .map_err(|source| Error::from(ErrorCode::Unavailable).with_source(source))
}

/// Reads one little-endian generator word from `fields` at `start`.
fn word(fields: &[u8], start: usize) -> u64 {
    let mut bytes = [0; 8];
    for (slot, byte) in bytes.iter_mut().zip(fields.iter().skip(start)) {
        *slot = *byte;
    }
    u64::from_le_bytes(bytes)
}

/// Reads one little-endian generator array from `fields` at `start`.
fn wide(fields: &[u8], start: usize) -> [u8; 16] {
    let mut bytes = [0; 16];
    for (slot, byte) in bytes.iter_mut().zip(fields.iter().skip(start)) {
        *slot = *byte;
    }
    bytes
}

fn changed(mut bytes: Vec<u8>, edits: &[(usize, u8)]) -> Option<Vec<u8>> {
    for &(offset, value) in edits {
        *bytes.get_mut(offset)? = value;
    }
    Some(bytes)
}

fn check_codec<T: PartialEq>(
    bytes: &[u8],
    magic: &[u8],
    written: &T,
    decode: impl Fn(&[u8]) -> Result<T, Error>,
    encode: impl Fn(&T) -> Result<Vec<u8>, Error>,
    valid_prefix: impl Fn(usize) -> bool,
) -> bool {
    // Arbitrary malformed bytes may be refused, but the actual writer's value
    // must be admitted unchanged. Otherwise a reject-all decoder passes fuzzing.
    let Ok(encoded_written) = encode(written) else {
        return false;
    };
    if !decode(&encoded_written).is_ok_and(|decoded| &decoded == written) {
        return false;
    }
    let Ok(value) = decode(bytes) else {
        return true;
    };
    let Ok(mut encoded) = encode(&value) else {
        return false;
    };
    if encoded != bytes {
        return false;
    }
    let refused = |input: &[u8], code| {
        decode(input).is_err_and(|error| error.code == code && error.stopped.is_none())
    };
    // A fixed number of selected prefixes keeps total work linear in the input.
    let last = bytes.len().saturating_sub(1);
    for end in [
        0,
        magic.len().saturating_sub(1),
        magic.len(),
        bytes.len() / 2,
        last,
    ] {
        let Some(prefix) = bytes.get(..end) else {
            return false;
        };
        if valid_prefix(end) {
            let Ok(value) = decode(prefix) else {
                return false;
            };
            let Ok(encoded) = encode(&value) else {
                return false;
            };
            if encoded.as_slice() != prefix {
                return false;
            }
        } else if !refused(
            prefix,
            if end < magic.len() {
                ErrorCode::Unsupported
            } else {
                ErrorCode::Corrupt
            },
        ) {
            return false;
        }
    }
    // A complete valid body must not admit another format domain.
    let Some(first) = encoded.first_mut() else {
        return false;
    };
    *first ^= 0x20;
    let foreign_domain = refused(&encoded, ErrorCode::Unsupported);
    let Some(first) = encoded.first_mut() else {
        return false;
    };
    *first ^= 0x20;
    if !foreign_domain {
        return false;
    }
    // Refuse another version before any body, using the admitted writer header.
    let Some(version_index) = magic.len().checked_sub(2) else {
        return false;
    };
    let Some(version_byte) = magic.get(version_index).copied() else {
        return false;
    };
    for version in *b"0129" {
        if version != version_byte {
            let Some(slot) = encoded.get_mut(version_index) else {
                return false;
            };
            *slot = version;
            let Some(prefix) = encoded.get(..magic.len()) else {
                return false;
            };
            if !refused(prefix, ErrorCode::Unsupported) {
                return false;
            }
        }
    }
    let Some(slot) = encoded.get_mut(version_index) else {
        return false;
    };
    *slot = version_byte;
    encoded.push(0);
    refused(&encoded, ErrorCode::Corrupt)
}

/// Exercise canonical identity and typed framing refusal through the original
/// stored Objects decoders, including the metadata behind HEAD.
pub fn check(bytes: &[u8]) -> bool {
    if let Ok(text) = std::str::from_utf8(bytes) {
        let parsed = Digest::from_hex(text);
        if text.len() == 64 && text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            if !parsed.is_ok_and(|digest| digest.to_hex() == text.to_ascii_lowercase()) {
                return false;
            }
        } else if !parsed.is_err_and(|error| error.code == ErrorCode::InvalidArgument) {
            return false;
        }
    }
    // The original storage-key writer must preserve its independently held
    // digest, including uppercase spelling. Reject-all and altered-value
    // decoders cannot satisfy this admission check.
    let digest = Digest::of(bytes);
    let written = digest.to_hex();
    for text in [&written, &written.to_ascii_uppercase()] {
        if Digest::from_hex(text) != Ok(digest) {
            return false;
        }
    }
    let mut invalid = written.clone();
    invalid.replace_range(..1, "!");
    let Some(short) = written.get(..63) else {
        return false;
    };
    for text in [short, &format!("{written}0"), &invalid] {
        if !Digest::from_hex(text).is_err_and(|error| error.code == ErrorCode::InvalidArgument) {
            return false;
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    if !crate::local::codec_fuzz::check(bytes) {
        return false;
    }
    // These are generator fields, not a second persisted-format decoder.
    let mut fields = [0; 64];
    for (field, byte) in fields.iter_mut().zip(bytes) {
        *field = *byte;
    }
    // Sorted input-derived bounds give valid selections independently of the
    // resolver. Fixed counterexamples cover empty, reversed and oversized ones.
    let mut bounds = [word(&fields, 0), word(&fields, 8), word(&fields, 16)];
    bounds.sort_unstable();
    let [start, end, length] = bounds;
    if ![
        (ByteRange::FULL, 0, Some(0..0)),
        (ByteRange::FULL, length, Some(0..length)),
        (
            ByteRange {
                start: length / 2,
                end: None,
            },
            length,
            Some(length / 2..length),
        ),
        (
            ByteRange {
                start: 1,
                end: None,
            },
            0,
            None,
        ),
        (
            ByteRange::new(start, end),
            length,
            (start < end).then_some(start..end),
        ),
        (ByteRange::new(length, length), length, None),
        (ByteRange::new(1, 0), length, None),
        (ByteRange::new(0, u64::MAX), 0, None),
    ]
    .into_iter()
    .all(
        |(range, length, expected)| match (range.resolve(length), expected) {
            (Ok(actual), Some(expected)) => actual == expected,
            (Err(error), None) => {
                error.code == ErrorCode::RangeNotSatisfiable && error.stopped.is_none()
            }
            _ => false,
        },
    ) {
        return false;
    }
    let references: Vec<_> = bytes
        .as_chunks::<32>()
        .0
        .iter()
        .map(|chunk| Digest::from_bytes(*chunk))
        .collect();
    let Some(parts) = references
        .iter()
        .map(|&digest| {
            let length = u64::from_le_bytes(*digest.as_bytes().first_chunk::<8>()?);
            Some(Part { digest, length })
        })
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let manifest = Manifest {
        parts,
        edges: references.clone(),
        sha256: (fields[58] & 1 != 0).then(|| *digest.as_bytes()),
    };
    // Wide arithmetic independently checks the public logical-length boundary.
    let length: u128 = manifest
        .parts
        .iter()
        .map(|part| u128::from(part.length))
        .sum();
    match (manifest.length(), u64::try_from(length)) {
        (Ok(actual), Ok(expected)) if actual == expected => {}
        (Err(error), Err(_))
            if error.code == ErrorCode::QuotaExceeded && error.stopped.is_none() => {}
        _ => return false,
    }
    // Part and edge custody is independent of their repeated wire positions.
    let mut edge = *digest.as_bytes();
    edge[0] ^= 1;
    let edge = Digest::from_bytes(edge);
    let part = Part {
        digest,
        length: bytes.len() as u64,
    };
    if (Manifest {
        parts: vec![part, part],
        edges: vec![edge, digest, edge],
        sha256: None,
    })
    .references()
    .as_slice()
        != [digest, edge].as_slice()
    {
        return false;
    }
    let kind = if fields[56] & 1 == 0 {
        Kind::Blob
    } else {
        Kind::Manifest
    };
    let mut envelope = Envelope {
        kind,
        length: word(&fields, 0),
        stored: word(&fields, 8),
        references: references.clone(),
        sha256: (kind == Kind::Manifest && fields[58] & 1 != 0).then(|| *digest.as_bytes()),
        version: wide(&fields, 16),
        dying: (fields[57] & 1 != 0).then(|| wide(&fields, 32)),
        condemned: word(&fields, 48),
    };
    let Ok(mut encoded) = envelope.encode() else {
        return false;
    };
    let Ok(manifest_bytes) = manifest.encode() else {
        return false;
    };
    let Some(manifest_tag) = manifest_bytes.len().checked_sub(33) else {
        return false;
    };
    let Some(envelope_tag) = encoded.len().checked_sub(74) else {
        return false;
    };
    let Some(invalid_manifest_tag) = changed(manifest_bytes.clone(), &[(manifest_tag, 2)]) else {
        return false;
    };
    let Some(nonzero_absent_manifest) = changed(
        manifest_bytes.clone(),
        &[(manifest_tag, 0), (manifest_tag + 1, 1)],
    ) else {
        return false;
    };
    if ![invalid_manifest_tag, nonzero_absent_manifest]
        .iter()
        .all(|bytes| Manifest::decode(bytes).is_err_and(|error| error.code == ErrorCode::Corrupt))
    {
        return false;
    }
    let Some(invalid_envelope_tag) = changed(encoded.clone(), &[(envelope_tag, 2)]) else {
        return false;
    };
    let Some(nonzero_absent_envelope) =
        changed(encoded.clone(), &[(envelope_tag, 0), (envelope_tag + 1, 1)])
    else {
        return false;
    };
    let Some(checksum_on_blob) = changed(
        encoded.clone(),
        &[(ENVELOPE_MAGIC.len(), 1), (envelope_tag, 1)],
    ) else {
        return false;
    };
    if ![
        invalid_envelope_tag,
        nonzero_absent_envelope,
        checksum_on_blob,
    ]
    .iter()
    .all(|bytes| Envelope::decode(bytes).is_err_and(|error| error.code == ErrorCode::Corrupt))
    {
        return false;
    }
    if !check_admission(
        &manifest,
        &manifest_bytes,
        &[
            manifest.parts.len() * std::mem::size_of::<Part>(),
            manifest.edges.len() * std::mem::size_of::<Digest>(),
        ],
        |bytes, admission| Manifest::decode_admitted(bytes, admission),
    ) || !check_admission(
        &envelope,
        &encoded,
        &[envelope.references.len() * std::mem::size_of::<Digest>()],
        |bytes, admission| Envelope::decode_admitted(bytes, admission),
    ) || !check_admission(
        &references,
        &encode_batch(&references),
        &[references.len() * std::mem::size_of::<Digest>()],
        |bytes, admission| decode_batch_admitted(bytes, admission),
    ) {
        return false;
    }
    let width = encoded.len();
    envelope.dying = match envelope.dying {
        Some(_) => None,
        None => Some(wide(&fields, 32)),
    };
    envelope.condemned = !envelope.condemned;
    let Ok(restamped) = envelope.encode() else {
        return false;
    };
    if restamped.len() != width
        || !Envelope::decode(&restamped).is_ok_and(|decoded| decoded == envelope)
    {
        return false;
    }
    // Mutate the complete writer body: presence byte, 16-byte nonce, u64 epoch.
    // Truncating it first would let a missing-byte refusal hide a bad tag.
    let Some(truncated_end) = width.checked_sub(8) else {
        return false;
    };
    let Some(truncated_bytes) = encoded.get(..truncated_end) else {
        return false;
    };
    let truncated = Envelope::decode(truncated_bytes);
    let kind_index = ENVELOPE_MAGIC.len();
    let Some(kind) = encoded.get(kind_index).copied() else {
        return false;
    };
    let Some(slot) = encoded.get_mut(kind_index) else {
        return false;
    };
    *slot = 0;
    let unknown_kind = Envelope::decode(&encoded);
    let Some(slot) = encoded.get_mut(kind_index) else {
        return false;
    };
    *slot = kind;
    let Some(presence) = width.checked_sub(25) else {
        return false;
    };
    let Some(slot) = encoded.get_mut(presence) else {
        return false;
    };
    *slot = 2;
    let unknown_presence = Envelope::decode(&encoded);
    let Some(slot) = encoded.get_mut(presence) else {
        return false;
    };
    *slot = 0;
    let Some(nonce) = width.checked_sub(24) else {
        return false;
    };
    let Some(slot) = encoded.get_mut(nonce) else {
        return false;
    };
    *slot = 1;
    if ![
        truncated,
        unknown_kind,
        unknown_presence,
        Envelope::decode(&encoded),
    ]
    .into_iter()
    .all(|result| {
        result.is_err_and(|error| error.code == ErrorCode::Corrupt && error.stopped.is_none())
    }) {
        return false;
    }
    check_codec(
        bytes,
        MANIFEST_MAGIC,
        &manifest,
        Manifest::decode,
        Manifest::encode,
        |_| false,
    ) && check_codec(
        bytes,
        ENVELOPE_MAGIC,
        &envelope,
        Envelope::decode,
        Envelope::encode,
        |_| false,
    ) && check_codec(
        bytes,
        BATCH_MAGIC,
        &references,
        decode_batch,
        |value| Ok(encode_batch(value)),
        |end| end >= BATCH_MAGIC.len() && (end - BATCH_MAGIC.len()).is_multiple_of(32),
    ) && check_codec(
        bytes,
        INCARNATION_MAGIC,
        &(u128::from_le_bytes(wide(&fields, 0)), word(&fields, 16)),
        decode_incarnation,
        |&(incarnation, run)| Ok(encode_incarnation(incarnation, run)),
        |_| false,
    ) && check_codec(
        bytes,
        EPOCH_MAGIC,
        &word(&fields, 24),
        decode_epoch,
        |&epoch| Ok(encode_epoch(epoch)),
        |_| false,
    )
}

/// Capture actual SDK encoder output; names identify the producing codec.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Error> {
    let digest = Digest::of(b"persisted codec corpus");
    let mut seeds = vec![
        ("manifest-empty", Manifest::default().encode()?),
        (
            "manifest-part-edge",
            Manifest {
                parts: vec![Part { digest, length: 22 }],
                edges: vec![digest],
                sha256: None,
            }
            .encode()?,
        ),
        ("retained-batch", encode_batch(&[digest])),
        ("incarnation", encode_incarnation(7, 9)),
        ("epoch", encode_epoch(9)),
    ];
    for (name, dying, sha256) in [
        ("head-v3-live", None, None),
        ("head-v3-dying", Some([2; 16]), Some(*digest.as_bytes())),
    ] {
        seeds.push((
            name,
            Envelope {
                kind: Kind::Manifest,
                length: 22,
                stored: 64,
                references: vec![digest],
                sha256,
                version: [1; 16],
                dying,
                condemned: 9,
            }
            .encode()?,
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    seeds.extend(crate::local::codec_fuzz::seeds()?);
    Ok(seeds)
}

#[cfg(test)]
mod tests {
    #[test]
    fn stored_writer_corpus_satisfies_the_fuzz_oracle() -> Result<(), Box<dyn std::error::Error>> {
        for (name, bytes) in super::seeds()? {
            assert!(super::check(&bytes), "persisted fuzz oracle: {name}");
        }
        for name in ["head-live", "head-dying"] {
            let bytes =
                acyclic_proto_build::golden::frame(&format!("corpus/object_persistence/{name}"))?;
            assert!(super::check(&bytes), "persisted fuzz oracle: {name}");
        }
        Ok(())
    }
}
