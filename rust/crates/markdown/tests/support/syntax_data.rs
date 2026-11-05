//! The dump decoder's real bundled syntax and theme formats.
use serde::{Serialize, de::DeserializeOwned};
use syntect::{dumps, highlighting::ThemeSet, parsing::SyntaxSet};

fn check_type<T: DeserializeOwned + Serialize>(data: &[u8], compressed: bool) {
    let decoded = if compressed {
        dumps::from_reader::<T, _>(data)
    } else {
        dumps::from_uncompressed_data::<T>(data)
    };
    let Ok(value) = decoded else { return };
    let encoded = dumps::dump_binary(&value);
    let restored: T = dumps::from_reader(&encoded[..]).unwrap();
    assert_eq!(
        serde_json::to_value(&value).unwrap(),
        serde_json::to_value(restored).unwrap()
    );
    if !compressed {
        assert!(
            dumps::from_uncompressed_data::<T>(&data[..data.len() - 1]).is_err(),
            "accepted truncated syntax data"
        );
        let mut changed = data.to_vec();
        changed
            .get_mut(..4)
            .unwrap()
            .copy_from_slice(&0u32.to_be_bytes());
        assert!(
            dumps::from_uncompressed_data::<T>(&changed).is_err(),
            "accepted unsupported syntax data version"
        );
        changed.get_mut(..4).unwrap().copy_from_slice(&data[..4]);
        changed.push(0);
        assert!(
            dumps::from_uncompressed_data::<T>(&changed).is_err(),
            "accepted trailing syntax data"
        );
    }
}

pub fn check(data: &[u8]) {
    check_type::<SyntaxSet>(data, false);
    check_type::<ThemeSet>(data, true);
}
