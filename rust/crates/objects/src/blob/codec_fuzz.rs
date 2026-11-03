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

fn check_codec<T: PartialEq>(
    bytes: &[u8],
    magic: &[u8],
    written: T,
    decode: impl Fn(&[u8]) -> Result<T, Error>,
    encode: impl Fn(&T) -> Vec<u8>,
    valid_prefix: impl Fn(usize) -> bool,
) -> bool {
    // Arbitrary malformed bytes may be refused, but the actual writer's value
    // must be admitted unchanged. Otherwise a reject-all decoder passes fuzzing.
    if !decode(&encode(&written)).is_ok_and(|decoded| decoded == written) {
        return false;
    }
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
    for text in [&written[..63], &format!("{written}0"), &invalid] {
        if !Digest::from_hex(text).is_err_and(|error| error.code == ErrorCode::InvalidArgument) {
            return false;
        }
    }
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    if !crate::local::codec_fuzz::check(bytes) {
        return false;
    }
    // These are generator fields, not a second persisted-format decoder.
    let mut fields = [0; 64];
    for (field, byte) in fields.iter_mut().zip(bytes) {
        *field = *byte;
    }
    let word =
        |start| u64::from_le_bytes(fields[start..start + 8].try_into().expect("generator word"));
    let references: Vec<_> = bytes
        .chunks_exact(32)
        .map(|chunk| Digest::from_bytes(chunk.try_into().expect("generator digest")))
        .collect();
    let manifest = Manifest {
        parts: references
            .iter()
            .map(|&digest| Part {
                digest,
                length: u64::from_le_bytes(
                    digest.as_bytes()[..8].try_into().expect("generator length"),
                ),
            })
            .collect(),
        edges: references.clone(),
    };
    let envelope = Envelope {
        kind: if fields[56] & 1 == 0 {
            Kind::Blob
        } else {
            Kind::Manifest
        },
        length: word(0),
        stored: word(8),
        references: references.clone(),
        version: fields[16..32].try_into().expect("generator version"),
        dying: (fields[57] & 1 != 0).then(|| fields[32..48].try_into().expect("generator deleter")),
        condemned: word(48),
    };
    check_codec(
        bytes,
        MANIFEST_MAGIC,
        manifest,
        Manifest::decode,
        Manifest::encode,
        |_| false,
    ) && check_codec(
        bytes,
        ENVELOPE_MAGIC,
        envelope,
        Envelope::decode,
        Envelope::encode,
        |_| false,
    ) && check_codec(
        bytes,
        BATCH_MAGIC,
        references,
        decode_batch,
        |value| encode_batch(value),
        |end| end >= BATCH_MAGIC.len() && (end - BATCH_MAGIC.len()) % 32 == 0,
    ) && check_codec(
        bytes,
        INCARNATION_MAGIC,
        (
            u128::from_le_bytes(fields[..16].try_into().expect("generator incarnation")),
            word(16),
        ),
        decode_incarnation,
        |&(incarnation, run)| encode_incarnation(incarnation, run),
        |_| false,
    ) && check_codec(
        bytes,
        EPOCH_MAGIC,
        word(24),
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
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    seeds.extend(crate::local::codec_fuzz::seeds());
    seeds
}

#[cfg(test)]
mod tests {
    #[test]
    fn stored_writer_corpus_satisfies_the_fuzz_oracle() {
        for (name, bytes) in super::seeds() {
            assert!(super::check(&bytes), "persisted fuzz oracle: {name}");
        }
        for name in ["head-live", "head-dying"] {
            let bytes =
                acyclic_proto_build::golden::frame(&format!("corpus/object_persistence/{name}"))
                    .expect("retained historical writer capture");
            assert!(super::check(&bytes), "persisted fuzz oracle: {name}");
        }
    }
}
