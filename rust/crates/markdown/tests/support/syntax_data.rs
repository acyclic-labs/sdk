//! The dump decoder's real bundled syntax, lazy grammar and theme formats.
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};
use syntect::{dumps, highlighting::ThemeSet, parsing::SyntaxSet};

fn compressed(data: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn check_type<T: DeserializeOwned + Serialize>(data: &[u8], is_compressed: bool) -> Option<T> {
    let value = if is_compressed {
        dumps::from_reader::<T, _>(data)
    } else {
        dumps::from_uncompressed_data::<T>(data)
    }
    .ok()?;
    let encoded = dumps::dump_binary(&value);
    let restored: T = dumps::from_reader(&encoded[..]).unwrap();
    assert_eq!(
        serde_json::to_value(&value).unwrap(),
        serde_json::to_value(restored).unwrap()
    );
    let mut raw = Vec::new();
    ZlibDecoder::new(&encoded[..])
        .read_to_end(&mut raw)
        .unwrap();
    let mut changed = raw.clone();
    changed[..4].copy_from_slice(&0u32.to_be_bytes());
    let mut invalid_value = raw.clone();
    invalid_value[4] = 0xc1; // MessagePack's reserved, invalid marker.
    let mut trailing = raw.clone();
    trailing.push(0);
    for malformed in [&raw[..raw.len() - 1], &changed, &invalid_value, &trailing] {
        assert!(dumps::from_uncompressed_data::<T>(malformed).is_err());
        assert!(
            dumps::from_reader::<T, _>(&compressed(malformed)[..]).is_err(),
            "accepted malformed compressed syntax data"
        );
    }
    Some(value)
}

fn check_syntax(data: &[u8]) -> bool {
    let Some(syntaxes) = check_type::<SyntaxSet>(data, false) else {
        return false;
    };
    for syntax in syntaxes.syntaxes() {
        // Read the original retained compressed grammar through its serialized
        // field, without exposing private parser types or re-encoding it.
        let value = serde_json::to_value(syntax).unwrap();
        let bytes: Vec<u8> =
            serde_json::from_value(value["serialized_lazy_contexts"].clone()).unwrap();
        assert!(check_type::<serde_json::Value>(&bytes, true).is_some());
    }
    true
}

/// Arbitrary fuzz bytes may be rejected. Expected seeds require their
/// corresponding result to be true, so stale data cannot skip the checks.
pub fn check(data: &[u8]) -> (bool, bool) {
    (
        check_syntax(data),
        check_type::<ThemeSet>(data, true).is_some(),
    )
}
