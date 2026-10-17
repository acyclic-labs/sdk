use crate::{file_bytes::FileBytes, input_files::InputFile};
use serde::{Serialize, de::DeserializeOwned};

fn round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(
    bytes: &[u8],
) -> Option<(T, T)> {
    let value = serde_json::from_slice::<T>(bytes).ok()?;
    let encoded = serde_json::to_vec(&value).unwrap();
    let decoded = serde_json::from_slice::<T>(&encoded).unwrap();
    assert_eq!(value, decoded);
    Some((value, decoded))
}

pub fn check(bytes: &[u8]) {
    if let Some((original, decoded)) = round_trip::<FileBytes>(bytes) {
        assert_eq!(original.as_bytes(), decoded.as_bytes());
    }
    let _ = round_trip::<Vec<InputFile>>(bytes);
}
