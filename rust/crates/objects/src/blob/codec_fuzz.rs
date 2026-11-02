//! On-demand checks of the stored codecs through their canonical implementation.
use super::{
    BATCH_MAGIC, ENVELOPE_MAGIC, EPOCH_MAGIC, Envelope, INCARNATION_MAGIC, MANIFEST_MAGIC,
    decode_batch, decode_epoch, decode_incarnation, encode_batch, encode_epoch, encode_incarnation,
};
use crate::{Digest, Kind, Manifest, Part};
use crate::{Error, wire::ErrorCode};

/// Prepare an obsolete value using the original local compaction chunk size.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub fn compaction_chunk_bytes() -> usize {
    usize::try_from(crate::local::COMPACTION_CHUNK)
        .expect("local compaction chunk fits a native allocation")
}

fn check_codec<T>(
    bytes: &[u8],
    magic: &[u8],
    decode: impl Fn(&[u8]) -> Result<T, Error>,
    encode: impl Fn(&T) -> Vec<u8>,
    valid_prefix: impl Fn(usize) -> bool,
) -> bool {
    let Ok(value) = decode(bytes) else {
        return true;
    };
    let mut encoded = encode(&value);
    if encoded != bytes {
        return false;
    }
    let refused = |input: &[u8], code| {
        decode(input).is_err_and(|error| error.code == code && error.stopped.is_none())
    };
    // A fixed number of selected prefixes keeps total work linear in the input.
    for end in [
        0,
        magic.len() - 1,
        magic.len(),
        bytes.len() / 2,
        bytes.len() - 1,
    ] {
        if valid_prefix(end) {
            if !decode(&bytes[..end]).is_ok_and(|value| encode(&value) == bytes[..end]) {
                return false;
            }
        } else if !refused(
            &bytes[..end],
            if end < magic.len() {
                ErrorCode::Unsupported
            } else {
                ErrorCode::Corrupt
            },
        ) {
            return false;
        }
    }
    // Refuse another version before any body, using the admitted writer header.
    for version in [b'0', b'1', b'2', b'9'] {
        if version != magic[magic.len() - 2] {
            encoded[magic.len() - 2] = version;
            if !refused(&encoded[..magic.len()], ErrorCode::Unsupported) {
                return false;
            }
        }
    }
    encoded[magic.len() - 2] = magic[magic.len() - 2];
    encoded.push(0);
    refused(&encoded, ErrorCode::Corrupt)
}

/// Exercise canonical identity and typed framing refusal through the original
/// stored Objects decoders, including the metadata behind HEAD.
pub fn check(bytes: &[u8]) -> bool {
    check_codec(
        bytes,
        MANIFEST_MAGIC,
        Manifest::decode,
        Manifest::encode,
        |_| false,
    ) && check_codec(
        bytes,
        ENVELOPE_MAGIC,
        Envelope::decode,
        Envelope::encode,
        |_| false,
    ) && check_codec(
        bytes,
        BATCH_MAGIC,
        decode_batch,
        |value| encode_batch(value),
        |end| end >= BATCH_MAGIC.len() && (end - BATCH_MAGIC.len()) % 32 == 0,
    ) && check_codec(
        bytes,
        INCARNATION_MAGIC,
        decode_incarnation,
        |&(incarnation, run)| encode_incarnation(incarnation, run),
        |_| false,
    ) && check_codec(
        bytes,
        EPOCH_MAGIC,
        decode_epoch,
        |&epoch| encode_epoch(epoch),
        |_| false,
    )
}

/// Capture actual SDK encoder output; names identify the producing codec.
pub fn seeds() -> Vec<(&'static str, Vec<u8>)> {
    let digest = Digest::of(b"persisted codec corpus");
    let mut seeds = vec![
        ("manifest-empty", Manifest::default().encode()),
        (
            "manifest-part-edge",
            Manifest {
                parts: vec![Part { digest, length: 22 }],
                edges: vec![digest],
            }
            .encode(),
        ),
        ("retained-batch", encode_batch(&[digest])),
        ("incarnation", encode_incarnation(7, 9)),
        ("epoch", encode_epoch(9)),
    ];
    for (name, dying) in [("head-v2-live", None), ("head-v2-dying", Some([2; 16]))] {
        seeds.push((
            name,
            Envelope {
                kind: Kind::Manifest,
                length: 22,
                stored: 64,
                references: vec![digest],
                version: [1; 16],
                dying,
                condemned: 9,
            }
            .encode(),
        ));
    }
    seeds
}
