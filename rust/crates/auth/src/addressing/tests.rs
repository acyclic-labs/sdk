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
    #[test]
    fn actor_triples_are_injective_and_disjoint(
        account in prop::collection::vec(any::<u8>(), 1..256),
        namespace in "[A-Za-z0-9_.-]{1,128}", id in "[A-Za-z0-9_.-]{1,128}",
        other_account in prop::collection::vec(any::<u8>(), 1..256),
        other_namespace in "[A-Za-z0-9_.-]{1,128}", other_id in "[A-Za-z0-9_.-]{1,128}"
    ) {
        let value = ActorReference::new(&account, namespace.as_bytes(), id.as_bytes())?;
        let bytes = value.encode();
        prop_assert_eq!(&ActorReference::decode(&bytes)?, &value);
        prop_assert!(ResourceIdentity::decode(&bytes).is_err());
        let pair = ResourceIdentity::new(&account, id.as_bytes())?.encode();
        prop_assert!(ActorReference::decode(&pair).is_err());
        let changed = ActorReference::new(&other_account, other_namespace.as_bytes(), other_id.as_bytes())?;
        if value != changed {
            prop_assert_ne!(value.encode(), changed.encode());
        }
    }
    #[test]
    fn actor_parser_accepts_only_canonical_bytes(bytes in prop::collection::vec(any::<u8>(), 0..10000)) {
        if let Ok(value) = ActorReference::decode(&bytes) {
            prop_assert_eq!(value.encode(), bytes);
        }
    }
    #[test]
    fn multi_label_hosts_roundtrip(account in "[a-z][a-z0-9]{0,62}", id in "[a-z][a-z0-9]{0,62}") {
        let value = ResourceIdentity::new(account.as_bytes(), id.as_bytes())?;
        let host = value.hostname("run.cell-1.example")?;
        prop_assert_eq!(ResourceIdentity::from_hostname(host.as_str(), "run.cell-1.example")?, value);
    }
}

#[test]
fn actor_reference_framing_and_legacy_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let legacy = ActorReference::from_legacy_fields(b"account", None, "existing.Actor_1")?;
    assert_eq!(legacy.namespace().as_str(), DEFAULT_ACTOR_NAMESPACE);
    assert_eq!(legacy.id(), "existing.Actor_1");
    assert_eq!(
        legacy,
        ActorReference::new(b"account", b"__default", b"existing.Actor_1")?
    );
    assert!(ActorReference::from_legacy_fields(b"account", Some(""), "id").is_err());
    assert_eq!(
        NamespaceId::for_deployment(b"__default"),
        Err(IdentityError::ReservedNamespace)
    );
    let bytes = legacy.encode();
    for length in 0..bytes.len() {
        assert!(ActorReference::decode(bytes.get(..length).ok_or("prefix")?).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(ActorReference::decode(&trailing).is_err());
    let mut version = bytes;
    *version.get_mut(20).ok_or("version")? = 2;
    assert!(ActorReference::decode(&version).is_err());
    // Without field boundaries these distinct valid triples alias.
    let first = ActorReference::new(b"a", b"bc", b"d")?;
    let second = ActorReference::new(b"ab", b"c", b"d")?;
    assert_eq!(
        [b"a".as_slice(), b"bc", b"d"].concat(),
        [b"ab".as_slice(), b"c", b"d"].concat()
    );
    assert_ne!(first.encode(), second.encode());
    Ok(())
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
    actors: Vec<ActorVector>,
    invalid_actors: Vec<InvalidActorVector>,
    host_parsing: Vec<HostParsingVector>,
}
#[derive(Deserialize)]
struct ActorVector {
    account_hex: String,
    namespace_hex: String,
    id_hex: String,
    encoded_hex: String,
    legacy: bool,
}
#[derive(Deserialize)]
struct InvalidActorVector {
    account_hex: String,
    namespace_hex: String,
    id_hex: String,
    error: String,
}
#[derive(Deserialize)]
struct HostParsingVector {
    host: String,
    domain: String,
    valid: bool,
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
    for row in vectors.actors {
        let account = unhex(&row.account_hex)?;
        let namespace = unhex(&row.namespace_hex)?;
        let id = unhex(&row.id_hex)?;
        let value = ActorReference::new(&account, &namespace, &id)?;
        let bytes = unhex(&row.encoded_hex)?;
        assert_eq!(value.encode(), bytes);
        assert_eq!(ActorReference::decode(&bytes)?, value);
        assert!(ResourceIdentity::decode(&bytes).is_err());
        if row.legacy {
            assert_eq!(
                ActorReference::from_legacy_fields(&account, None, std::str::from_utf8(&id)?)?,
                value
            );
        }
    }
    for row in vectors.invalid_actors {
        assert_eq!(
            ActorReference::new(
                &unhex(&row.account_hex)?,
                &unhex(&row.namespace_hex)?,
                &unhex(&row.id_hex)?
            )
            .err()
            .map(IdentityError::code),
            Some(row.error.as_str())
        );
    }
    for row in vectors.host_parsing {
        let parsed = ResourceIdentity::from_hostname(&row.host, &row.domain);
        assert_eq!(parsed.is_ok(), row.valid, "{} / {}", row.host, row.domain);
        if let Ok(value) = parsed {
            assert_eq!(value.hostname(&row.domain)?.as_str(), row.host);
        }
    }
    Ok(())
}
#[test]
fn shared_worker_route_vectors_and_specificity() -> Result<(), Box<dyn std::error::Error>> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/resource-identity-v1.json"
    ))?;
    for row in vectors
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .ok_or("route rows")?
    {
        let text = |key| {
            row.get(key)
                .and_then(serde_json::Value::as_str)
                .ok_or("route text")
        };
        let pattern = RoutePattern::parse(text("pattern")?)?;
        assert_eq!(pattern.as_str(), text("canonical")?);
        assert_eq!(
            pattern.matches(text("url")?)?,
            row.get("matches")
                .and_then(serde_json::Value::as_bool)
                .ok_or("route match")?
        );
    }
    for row in vectors
        .get("invalid_routes")
        .and_then(serde_json::Value::as_array)
        .ok_or("invalid rows")?
    {
        assert!(RoutePattern::parse(row.as_str().ok_or("invalid pattern")?).is_err());
    }
    for row in vectors
        .get("route_selection")
        .and_then(serde_json::Value::as_array)
        .ok_or("selection rows")?
    {
        let patterns = row
            .get("patterns")
            .and_then(serde_json::Value::as_array)
            .ok_or("patterns")?;
        let targets = row
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .ok_or("targets")?;
        let routes = patterns
            .iter()
            .zip(targets)
            .map(|(p, t)| {
                Ok(WorkerRoute {
                    pattern: RoutePattern::parse(p.as_str().ok_or("pattern")?)?,
                    target: t.as_str().map(str::to_owned),
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        let url = row
            .get("url")
            .and_then(serde_json::Value::as_str)
            .ok_or("url")?;
        assert_eq!(
            select_route(&routes, url)?,
            Some(row.get("target").and_then(serde_json::Value::as_str))
        );
        // Negative control: declaration order cannot replace specificity.
        let mut reverse = routes.clone();
        reverse.reverse();
        assert_eq!(select_route(&reverse, url)?, select_route(&routes, url)?);
    }
    let same = RoutePattern::parse("example.com/*")?;
    assert_eq!(
        select_route(
            &[
                WorkerRoute {
                    pattern: same.clone(),
                    target: Some("a".into())
                },
                WorkerRoute {
                    pattern: same,
                    target: Some("b".into())
                }
            ],
            "https://example.com/"
        ),
        Err(RouteError::Ambiguous)
    );
    Ok(())
}

#[test]
fn route_ambiguity_only_applies_to_the_winning_specificity()
-> Result<(), Box<dyn std::error::Error>> {
    let broad = RoutePattern::parse("example.com/*")?;
    let narrow = RoutePattern::parse("example.com/api/*")?;
    let routes = [
        WorkerRoute {
            pattern: broad.clone(),
            target: Some("a".into()),
        },
        WorkerRoute {
            pattern: broad,
            target: Some("b".into()),
        },
        WorkerRoute {
            pattern: narrow,
            target: Some("api".into()),
        },
    ];
    // Negative control: rejecting a losing tie early makes declaration order observable.
    for permutation in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let reordered = permutation
            .map(|index| routes.get(index).cloned().ok_or("missing route"))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            select_route(&reordered, "https://example.com/api/x")?,
            Some(Some("api"))
        );
        assert_eq!(
            select_route(&reordered, "https://example.com/x"),
            Err(RouteError::Ambiguous)
        );
    }
    for target in ["", "Upper", "bad_name", "-leading"] {
        let invalid = WorkerRoute {
            pattern: RoutePattern::parse("example.com/*")?,
            target: Some(target.to_owned()),
        };
        assert_eq!(
            select_route(&[invalid], "https://example.com/x"),
            Err(RouteError::Target)
        );
    }
    Ok(())
}
