use super::valid;
use proptest::prelude::*;
use serde_json::Value;

proptest! {
    // Property: canonical documents round trip exactly, while changing any
    // executable/platform/version binding rejects the previously valid document.
    #[test]
    fn certification_round_trip_preserves_exact_binary_and_platform_bindings(
        version in "[a-zA-Z0-9_.-]{1,24}", digest in "[0-9a-f]{64}",
    ) {
        let mut value: Value = serde_json::from_str(include_str!("../../tests/fixtures/native-certification.json"))?;
        let object = value.as_object_mut().ok_or_else(|| std::io::Error::other("fixture object"))?;
        object.insert("release_version".into(), Value::String(version.clone()));
        object.insert("executable_blake3".into(), Value::String(digest.clone()));
        prop_assert!(valid(&value, &digest, "linux", "x86_64", &version));
        let round_trip: Value = serde_json::from_slice(&serde_json::to_vec(&value)?)?;
        prop_assert_eq!(&round_trip, &value);
        prop_assert!(valid(&round_trip, &digest, "linux", "x86_64", &version));
        let altered_digest = format!("{digest}0");
        let altered_version = format!("{version}-other");
        prop_assert!(!valid(&value, &altered_digest, "linux", "x86_64", &version));
        prop_assert!(!valid(&value, &digest, "windows", "x86_64", &version));
        prop_assert!(!valid(&value, &digest, "linux", "aarch64", &version));
        prop_assert!(!valid(&value, &digest, "linux", "x86_64", &altered_version));
        value.as_object_mut().ok_or_else(|| std::io::Error::other("fixture object"))?.insert("passed".into(), Value::Bool(false));
        prop_assert!(!valid(&value, &digest, "linux", "x86_64", &version));
    }
}
