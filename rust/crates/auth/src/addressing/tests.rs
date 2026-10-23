use super::*;
use proptest::{
    prelude::*,
    test_runner::{Config, TestRunner},
};
use serde::Deserialize;

fn pair() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
    (
        prop::collection::vec(any::<u8>(), 1..=65_535),
        prop::collection::vec(any::<u8>(), 1..=65_535),
    )
}
proptest! {
    #![proptest_config(Config::with_cases(512))]
    #[test]
    fn arbitrary_byte_pairs_roundtrip((account, id) in pair()) {
        let value = ResourceIdentity::new(&account, &id)?;
        let encoded = value.encode();
        prop_assert_eq!(encoded.len(), value.encoded_len());
        prop_assert_eq!(ResourceIdentity::decode(&encoded)?, value);
        let (original_account, original_id) = ResourceIdentity::decode(&encoded)?.into_parts();
        prop_assert_eq!(original_account.into_bytes(), account);
        prop_assert_eq!(original_id.into_bytes(), id);
    }
    #[test]
    fn distinct_pairs_have_distinct_encodings(left in pair(), right in pair()) {
        let mut right = right;
        if left == right {
            right.1.push(0);
        }
        prop_assert_ne!(&left, &right);
        prop_assert_ne!(ResourceIdentity::new(&left.0, &left.1)?.encode(),
                        ResourceIdentity::new(&right.0, &right.1)?.encode());
    }
    #[test]
    fn hostname_retains_direct_components(account in "[a-z][a-z0-9]{0,62}", id in "[a-z][a-z0-9]{0,62}") {
        let identity = ResourceIdentity::new(account.as_bytes(), id.as_bytes())?;
        let host = identity.hostname("example.com")?;
        prop_assert_eq!(host.as_str(), format!("{id}.{account}.example.com"));
        prop_assert_eq!(CanonicalHost::parse(host.as_str())?, host);
    }
    #[test]
    fn admitted_hosts_retain_their_url_authority(labels in prop::collection::vec("[a-z0-9][a-z0-9-]{0,61}[a-z0-9]", 1..=4)) {
        let text = labels.join(".");
        if let Ok(host) = CanonicalHost::parse(&text) {
            let url = url::Url::parse(&format!("https://{}", host.as_str()))?;
            prop_assert_eq!(url.host_str(), Some(host.as_str()));
        }
    }
    #[test]
    fn parser_accepts_only_canonical_bytes(bytes in prop::collection::vec(any::<u8>(), 0..10000)) {
        if let Ok(value) = ResourceIdentity::decode(&bytes) {
            prop_assert_eq!(value.encode(), bytes);
        }
    }
}
fn boundary_shift_injectivity(encode: impl Fn(&ResourceIdentity) -> Vec<u8>) -> bool {
    let strategy = (
        prop::collection::vec(any::<u8>(), 1..32),
        any::<u8>(),
        prop::collection::vec(any::<u8>(), 1..32),
    );
    TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        ..Config::default()
    })
    .run(&strategy, |(prefix, middle, suffix)| {
        let mut left_id = vec![middle];
        left_id.extend_from_slice(&suffix);
        let mut right_account = prefix.clone();
        right_account.push(middle);
        let left = ResourceIdentity::new(&prefix, &left_id)?;
        let right = ResourceIdentity::new(&right_account, &suffix)?;
        prop_assert_ne!(&left, &right);
        prop_assert_ne!(encode(&left), encode(&right));
        Ok(())
    })
    .is_ok()
}
#[test]
fn negative_control_concatenation_fails_injectivity() {
    assert!(boundary_shift_injectivity(ResourceIdentity::encode));
    assert!(!boundary_shift_injectivity(|value| [
        value.account().as_bytes(),
        value.id().as_bytes()
    ]
    .concat()));
}
#[test]
fn rejects_malformed_storage_and_wire() -> Result<(), Box<dyn std::error::Error>> {
    let encoded = ResourceIdentity::new(b"a", b"b")?.encode();
    for length in 0..encoded.len() {
        assert!(ResourceIdentity::decode(encoded.get(..length).ok_or("prefix")?).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        ResourceIdentity::decode(&trailing),
        Err(IdentityError::Malformed)
    );
    let mut impossible_length = encoded;
    impossible_length.get_mut(..8).ok_or("length")?.fill(255);
    assert_eq!(
        ResourceIdentity::decode(&impossible_length),
        Err(IdentityError::Malformed)
    );
    assert_eq!(AccountId::new(b""), Err(IdentityError::EmptyAccount));
    assert_eq!(ResourceId::new(b""), Err(IdentityError::EmptyId));
    let pair = ResourceIdentity::from_parts(AccountId::new(b"a")?, ResourceId::new(b"b")?);
    assert_eq!(pair, ResourceIdentity::new(b"a", b"b")?);
    Ok(())
}
fn label_contract(validate: impl Fn(&[u8]) -> bool) -> bool {
    ["a", "0", "worker-1"]
        .iter()
        .all(|s| validate(s.as_bytes()))
        && [
            "", "A", "a.b", "a_b", "-a", "a-", "a:443", "é", "a\0", "*.x",
        ]
        .iter()
        .all(|s| !validate(s.as_bytes()))
        && validate(&[b'a'; 63])
        && !validate(&[b'a'; 64])
}
#[test]
fn negative_control_unvalidated_labels_fail_dns_contract() {
    assert!(label_contract(|label| validate_dns_label(label).is_ok()));
    assert!(!label_contract(|_| true));
}
#[test]
fn rejects_tls_and_signature_aliases() -> Result<(), DnsError> {
    let host = "worker.account.example.com";
    assert_eq!(CanonicalHost::parse(host)?.as_str(), host);
    for alias in [
        "Worker.account.example.com",
        "worker.account.example.com.",
        "worker.account.example.com:443",
        "x@worker.account.example.com",
        "work%65r.account.example.com",
        "worker..example.com",
        "127.0.0.1",
        "0x7f000001",
    ] {
        assert!(CanonicalHost::parse(alias).is_err(), "{alias:?}");
    }
    Ok(())
}

#[test]
fn invalid_a_labels_cannot_cross_the_url_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let invalid = "xn--a.account.example.com";
    // The former LDH-only path accepts every label; the URL authority refuses it.
    assert!(
        invalid
            .split('.')
            .all(|label| validate_dns_label(label.as_bytes()).is_ok())
    );
    assert!(url::Url::parse(&format!("https://{invalid}")).is_err());
    assert_eq!(
        CanonicalHost::parse(invalid),
        Err(DnsError::InvalidCharacter)
    );
    let valid = "xn--caf-dma.account.example.com";
    assert_eq!(CanonicalHost::parse(valid)?.as_str(), valid);
    assert_eq!(
        url::Url::parse(&format!("https://{valid}"))?.host_str(),
        Some(valid)
    );
    Ok(())
}

#[test]
fn canonical_host_bytes_are_the_signature_input() -> Result<(), Box<dyn std::error::Error>> {
    use ed25519_dalek::{Signer, SigningKey};

    let host = ResourceIdentity::new(b"account", b"worker")?.hostname("example.com")?;
    let signer = SigningKey::from_bytes(&[7; 32]);
    // This is a byte-preservation control, not a new HTTP signing protocol.
    let signature = signer.sign(host.as_str().as_bytes());
    crate::crypto::verify_signature(
        &signer.verifying_key(),
        b"worker.account.example.com",
        &signature.to_bytes(),
    )?;
    // Bypassing the canonical host boundary changes the signed bytes.
    for alias in ["Worker.account.example.com", "worker.account.example.com."] {
        assert!(CanonicalHost::parse(alias).is_err());
        assert!(
            crate::crypto::verify_signature(
                &signer.verifying_key(),
                alias.as_bytes(),
                &signature.to_bytes(),
            )
            .is_err()
        );
    }
    Ok(())
}

#[derive(Deserialize)]
struct Vectors {
    encoding: String,
    component_format: String,
    pairs: Vec<PairVector>,
    invalid: Vec<InvalidVector>,
    hosts: Vec<HostVector>,
}
#[derive(Deserialize)]
struct PairVector {
    account_hex: String,
    id_hex: String,
    encoded_hex: String,
}
#[derive(Deserialize)]
struct InvalidVector {
    account_hex: String,
    id_hex: String,
    error: String,
}
#[derive(Deserialize)]
struct HostVector {
    account_hex: String,
    id_hex: String,
    domain: String,
    hostname: Option<String>,
    error: Option<String>,
}
fn unhex(value: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return Err("odd hex length".into());
    }
    pairs
        .iter()
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect()
}
#[test]
fn shared_golden_pairs_and_hostnames() -> Result<(), Box<dyn std::error::Error>> {
    let vectors: Vectors = serde_json::from_str(include_str!(
        "../../tests/fixtures/resource-identity-v1.json"
    ))?;
    assert_eq!(vectors.encoding, "u64be-length-account-u64be-length-id");
    assert_eq!(
        vectors.component_format,
        "nonempty-opaque-bytes; product limits apply; DNS labels at most 63 bytes"
    );
    for row in vectors.pairs {
        let account = unhex(&row.account_hex)?;
        let id = unhex(&row.id_hex)?;
        let identity = ResourceIdentity::new(&account, &id)?;
        let encoded = unhex(&row.encoded_hex)?;
        assert_eq!(identity.encode(), encoded);
        assert_eq!(ResourceIdentity::decode(&encoded)?, identity);
    }
    for row in vectors.invalid {
        let result = ResourceIdentity::new(&unhex(&row.account_hex)?, &unhex(&row.id_hex)?);
        assert_eq!(
            result.err().map(IdentityError::code),
            Some(row.error.as_str())
        );
    }
    for row in vectors.hosts {
        let identity = ResourceIdentity::new(&unhex(&row.account_hex)?, &unhex(&row.id_hex)?)?;
        let host = identity.hostname(&row.domain);
        match row.hostname {
            Some(expected) => assert_eq!(host?.as_str(), expected),
            None => assert_eq!(host.err().map(DnsError::code), row.error.as_deref()),
        }
    }
    Ok(())
}
