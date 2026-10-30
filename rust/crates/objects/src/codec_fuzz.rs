//! On-demand checks of the stored codecs through their canonical implementation.
use crate::blob::{Envelope, decode_batch, encode_batch};
use crate::{Digest, Kind, Manifest, Part};

/// Exercise every stored Objects decoder, including the metadata behind HEAD.
pub fn check(bytes: &[u8]) -> bool {
    if let Ok(value) = Manifest::decode(bytes) {
        if value.encode() != bytes {
            return false;
        }
    }
    if let Ok(value) = Envelope::decode(bytes) {
        // Older accepted envelopes can normalize on first encoding.
        let canonical = value.encode();
        if Envelope::decode(&canonical) != Ok(value) {
            return false;
        }
    }
    if let Ok(value) = decode_batch(bytes) {
        if encode_batch(&value) != bytes {
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
    ];
    for (name, dying) in [("head-live", None), ("head-dying", Some([2; 16]))] {
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
