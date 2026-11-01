//! On-demand checks of the stored codecs through their canonical implementation.
use crate::blob::{
    Envelope, decode_batch, decode_epoch, decode_incarnation, encode_batch, encode_epoch,
    encode_incarnation,
};
use crate::{Digest, Kind, Manifest, Part};

/// Prepare an obsolete value using the original local compaction chunk size.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub fn compaction_chunk_bytes() -> usize {
    usize::try_from(crate::local::COMPACTION_CHUNK)
        .expect("local compaction chunk fits a native allocation")
}

/// Exercise every stored Objects decoder, including the metadata behind HEAD.
pub fn check(bytes: &[u8]) -> bool {
    if let Ok(value) = Manifest::decode(bytes) {
        if value.encode() != bytes {
            return false;
        }
    }
    if let Ok(value) = Envelope::decode(bytes) {
        if value.encode() != bytes {
            return false;
        }
    }
    if let Ok(value) = decode_batch(bytes) {
        if encode_batch(&value) != bytes {
            return false;
        }
    }
    if let Ok((incarnation, run)) = decode_incarnation(bytes) {
        if encode_incarnation(incarnation, run) != bytes {
            return false;
        }
    }
    if let Ok(epoch) = decode_epoch(bytes) {
        if encode_epoch(epoch) != bytes {
            return false;
        }
    }
    true
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
