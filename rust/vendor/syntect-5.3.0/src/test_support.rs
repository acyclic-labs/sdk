//! Invariants shared by real dump corpus tests and the syntax-data fuzzer.

use crate::{
    dumps,
    highlighting::ThemeSet,
    parsing::{LazyContexts, SyntaxSet},
};
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use serde::{de::DeserializeOwned, Serialize};
use std::io::{Read, Write};

fn check_type<T: DeserializeOwned + Serialize>(data: &[u8], compressed: bool) -> Option<T> {
    let value: T = if compressed {
        dumps::from_reader(data)
    } else {
        dumps::from_uncompressed_data(data)
    }
    .ok()?;
    let encoded = dumps::dump_binary(&value);
    let restored: T = dumps::from_reader(&encoded[..]).unwrap();
    assert_eq!(
        serde_json::to_value(&value).unwrap(),
        serde_json::to_value(restored).unwrap()
    );

    let raw = if compressed {
        let mut raw = Vec::new();
        ZlibDecoder::new(data).read_to_end(&mut raw).unwrap();
        raw
    } else {
        data.to_vec()
    };
    check_rejected::<T>(&raw[..raw.len() - 1], "truncated");
    let mut changed = raw.clone();
    changed[..4].copy_from_slice(&0u32.to_be_bytes());
    check_rejected::<T>(&changed, "unsupported version");
    changed[..4].copy_from_slice(&raw[..4]);
    changed.push(0);
    check_rejected::<T>(&changed, "trailing bytes");
    changed = raw;
    changed[4] = 0xc1; // MessagePack's reserved, invalid value marker.
    check_rejected::<T>(&changed, "invalid value");
    Some(value)
}

fn check_rejected<T: DeserializeOwned>(raw: &[u8], fault: &str) {
    for compressed in [true, false] {
        let result: std::io::Result<T> = if compressed {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
            encoder.write_all(raw).unwrap();
            dumps::from_reader(&encoder.finish().unwrap()[..])
        } else {
            dumps::from_uncompressed_data(raw)
        };
        assert!(
            result.is_err(),
            "accepted {fault} in {} syntax data for {}",
            if compressed {
                "compressed"
            } else {
                "uncompressed"
            },
            std::any::type_name::<T>()
        );
    }
}

/// Checks round trips and malformed decoded envelopes through the public dump
/// readers, including each original compressed lazy grammar in a syntax set.
/// Inputs that do not decode as a supported dump type have no round-trip claim.
pub fn check_syntax_data(data: &[u8]) {
    if let Some(syntaxes) = check_type::<SyntaxSet>(data, false) {
        for syntax in syntaxes.syntaxes() {
            let _ = check_type::<LazyContexts>(&syntax.serialized_lazy_contexts, true);
        }
    }
    let _ = check_type::<ThemeSet>(data, true);
}
