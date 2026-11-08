//! The production reference decoder and read-only store-binding boundary.
use acyclic_native_runtime::signing_key::{SigningCustody, SigningKeyReference};
use std::path::Path;

pub fn check(bytes: &[u8]) {
    let Ok(reference) = serde_json::from_slice::<SigningKeyReference>(bytes) else {
        return;
    };
    let encoded = serde_json::to_vec(&reference).unwrap();
    assert_eq!(
        serde_json::from_slice::<SigningKeyReference>(&encoded).unwrap(),
        reference
    );
    let first = reference.validate_custody(SigningCustody::PrivateFile(Path::new(
        "/reference-oracle/first",
    )));
    let second = reference.validate_custody(SigningCustody::PrivateFile(Path::new(
        "/reference-oracle/second",
    )));
    assert!(
        !(first.is_ok() && second.is_ok()),
        "one signing reference admitted different custody stores"
    );
    let mut unknown: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    unknown["unrecognized"] = true.into();
    assert!(serde_json::from_value::<SigningKeyReference>(unknown).is_err());
    let mut future: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    let value = future.as_object_mut().unwrap().remove("v1").unwrap();
    future["v2"] = value;
    assert!(serde_json::from_value::<SigningKeyReference>(future).is_err());
}
