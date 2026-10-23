use super::*;
use proptest::{
    prelude::*,
    test_runner::{Config, TestRunner},
};

fn tuple() -> impl Strategy<Value = (Vec<u8>, ResourceKind, Vec<u8>)> {
    (
        prop::collection::vec(any::<u8>(), 0..=MAX_ACCOUNT_BYTES),
        prop::sample::select(vec![
            ResourceKind::Bucket,
            ResourceKind::Stream,
            ResourceKind::Actor,
            ResourceKind::Worker,
            ResourceKind::FsWorkspace,
            ResourceKind::Machine,
        ]),
        prop::collection::vec(any::<u8>(), 0..=MAX_NAME_BYTES),
    )
}

proptest! {
    #![proptest_config(Config::with_cases(512))]
    #[test]
    fn arbitrary_byte_tuples_roundtrip((account, kind, name) in tuple()) {
        let value = ResourceName::new(&account, kind, &name)?;
        let encoded = value.encode();
        prop_assert_eq!(encoded.len(), value.encoded_len());
        prop_assert_eq!(ResourceName::parse(&encoded)?, value);
        let address = value.address();
        let digest = blake3::hash(&encoded);
        prop_assert_eq!(address.as_bytes(), digest.as_bytes());
    }

    #[test]
    fn distinct_tuples_have_distinct_preimages(left in tuple(), right in tuple()) {
        prop_assume!(left != right);
        let a = ResourceName::new(&left.0, left.1, &left.2)?.encode();
        let b = ResourceName::new(&right.0, right.1, &right.2)?.encode();
        prop_assert_ne!(a, b);
    }

    #[test]
    fn parser_never_accepts_noncanonical_bytes(bytes in prop::collection::vec(any::<u8>(), 0..10000)) {
        if let Ok(value) = ResourceName::parse(&bytes) {
            prop_assert_eq!(value.encode(), bytes);
        }
    }

    #[test]
    fn digest_has_one_text_presentation(bytes in any::<[u8; 32]>()) {
        let address = ResourceAddress::from_bytes(bytes);
        let text = address.to_string();
        prop_assert_eq!(text.parse::<ResourceAddress>()?, address);
        prop_assert_eq!(text.len(), 64);
    }
}

// Exercise the same injectivity assertion on adversarial boundary shifts. The
// naïve encoder fails for arbitrary prefixes/suffixes, not just one known tuple.
fn boundary_shift_injectivity(encode: impl Fn(&ResourceName<'_>) -> Vec<u8>) -> bool {
    let strategy = (
        prop::collection::vec(any::<u8>(), 0..32),
        prop::collection::vec(any::<u8>(), 0..32),
        1_u8..=6,
    );
    let mut runner = TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(&strategy, |(prefix, suffix, code)| {
            let kind = ResourceKind::try_from(code)?;
            let mut left_name = vec![code];
            left_name.extend_from_slice(&suffix);
            let mut right_account = prefix.clone();
            right_account.push(code);
            let a = ResourceName::new(&prefix, kind, &left_name)?;
            let b = ResourceName::new(&right_account, kind, &suffix)?;
            prop_assert_ne!(a, b);
            prop_assert_ne!(encode(&a), encode(&b));
            Ok(())
        })
        .is_ok()
}

#[test]
fn negative_control_plain_concatenation_fails_injectivity() {
    assert!(boundary_shift_injectivity(|value| value.encode()));
    assert!(!boundary_shift_injectivity(|value| {
        [value.account(), &[value.kind() as u8], value.name()].concat()
    }));
}

#[test]
fn rejects_bounds_unknown_kind_versions_and_trailing_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    for digest in [
        "a".repeat(63),
        "a".repeat(65),
        "A".repeat(64),
        "g".repeat(64),
        "é".repeat(32),
    ] {
        assert_eq!(
            digest.parse::<ResourceAddress>(),
            Err(AddressError::InvalidDigest)
        );
    }
    let oversized = vec![0; MAX_NAME_BYTES + 1];
    assert_eq!(
        ResourceName::new(b"a", ResourceKind::Actor, &oversized),
        Err(AddressError::NameTooLong)
    );
    assert_eq!(
        ResourceName::new(&oversized, ResourceKind::Actor, b"n"),
        Err(AddressError::AccountTooLong)
    );
    assert_eq!(ResourceKind::try_from(0), Err(AddressError::UnknownKind(0)));
    assert_eq!(ResourceKind::try_from(7), Err(AddressError::UnknownKind(7)));
    let encoded = ResourceName::new(b"a", ResourceKind::Bucket, b"n")?.encode();
    for length in 0..encoded.len() {
        assert!(ResourceName::parse(encoded.get(..length).ok_or("prefix")?).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(ResourceName::parse(&trailing), Err(AddressError::Malformed));
    let mut unsupported = encoded;
    *unsupported
        .get_mut(8 + DOMAIN.len() + 8)
        .ok_or("version offset")? = 2;
    assert_eq!(
        ResourceName::parse(&unsupported),
        Err(AddressError::UnsupportedVersion)
    );
    Ok(())
}

fn accepts_only_canonical_labels(validate: impl Fn(&[u8]) -> bool) -> bool {
    let valid = ["a", "0", "worker-1"];
    let invalid = [
        "",
        "A",
        "a.b",
        "a_b",
        "-a",
        "a-",
        "a.",
        "a:443",
        "é",
        "a\0",
        "a/b",
        "*.example",
    ];
    valid.iter().all(|label| validate(label.as_bytes()))
        && invalid.iter().all(|label| !validate(label.as_bytes()))
        && validate(&[b'a'; 63])
        && !validate(&[b'a'; 64])
}

#[test]
fn negative_control_unvalidated_labels_fail_dns_contract() {
    assert!(accepts_only_canonical_labels(|label| validate_dns_label(
        label
    )
    .is_ok()));
    assert!(!accepts_only_canonical_labels(|_| true));
}

#[test]
fn host_rejects_tls_and_signature_aliases() -> Result<(), DnsError> {
    let expected = "worker.account.example.com";
    assert_eq!(CanonicalHost::parse(expected)?.as_str(), expected);
    for host in [
        "Worker.account.example.com",
        "worker.account.example.com.",
        "worker.account.example.com:443",
        "worker@account.example.com",
        "worker..example.com",
        "work%65r.account.example.com",
        "worker.account.example.com/",
        "worker.account.example.com\n",
        "127.0.0.1",
        "0x7f000001",
        "0177.1",
    ] {
        assert!(CanonicalHost::parse(host).is_err(), "{host:?}");
    }
    assert!(CanonicalHost::parse(&vec!["a".repeat(63); 4].join(".")).is_err());
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn unhex(value: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return Err("odd-length hex".into());
    }
    pairs
        .iter()
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect()
}

#[test]
fn checked_in_golden_vectors() -> Result<(), Box<dyn std::error::Error>> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/resource-address-v1.json"
    ))?;
    assert_eq!(
        vectors.get("hash").and_then(serde_json::Value::as_str),
        Some("blake3-256")
    );
    for vector in vectors
        .get("addresses")
        .and_then(serde_json::Value::as_array)
        .ok_or("addresses")?
    {
        let account = unhex(
            vector
                .get("account_hex")
                .and_then(serde_json::Value::as_str)
                .ok_or("account_hex")?,
        )?;
        let name = unhex(
            vector
                .get("name_hex")
                .and_then(serde_json::Value::as_str)
                .ok_or("name_hex")?,
        )?;
        let kind = ResourceKind::try_from(u8::try_from(
            vector
                .get("kind")
                .and_then(serde_json::Value::as_u64)
                .ok_or("kind")?,
        )?)?;
        let resource = ResourceName::new(&account, kind, &name)?;
        assert_eq!(
            Some(hex(&resource.encode()).as_str()),
            vector
                .get("preimage_hex")
                .and_then(serde_json::Value::as_str)
        );
        assert_eq!(
            Some(resource.address().to_string().as_str()),
            vector
                .get("address_hex")
                .and_then(serde_json::Value::as_str)
        );
        assert_eq!(ResourceName::parse(&resource.encode())?, resource);
    }
    Ok(())
}
