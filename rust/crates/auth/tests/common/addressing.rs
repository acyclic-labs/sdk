//! Addresses must retain the identities and route decisions they carry.
use acyclic_auth::addressing::{
    AccountId, ActorReference, CanonicalHost, DnsError, IdentityError, NamespaceId, ResourceId,
    ResourceIdentity, RouteError, RoutePattern, WorkerRoute, select_route, validate_actor_id,
    validate_dns_label,
};

const DOMAIN: &str = "run.example.com";
type CheckResult = Result<(), Box<dyn std::error::Error>>;

fn host_url_boundary() -> CheckResult {
    let invalid = "xn--a.account.example.com";
    assert!(
        invalid
            .split('.')
            .all(|label| validate_dns_label(label.as_bytes()).is_ok())
    );
    assert!(url::Url::parse(&format!("https://{invalid}")).is_err());
    assert_eq!(
        CanonicalHost::parse(invalid),
        Err(DnsError::InvalidCharacter),
        "URL-incompatible A-label must retain typed refusal"
    );
    let valid = "xn--caf-dma.account.example.com";
    assert_eq!(CanonicalHost::parse(valid)?.as_str(), valid);
    assert_eq!(
        url::Url::parse(&format!("https://{valid}"))?.host_str(),
        Some(valid)
    );
    Ok(())
}

fn header<T>(
    bytes: &[u8],
    offset: usize,
    invalid: IdentityError,
    decode: impl Fn(&[u8]) -> Result<T, IdentityError>,
) -> CheckResult {
    let mut header = bytes
        .get(..=offset)
        .ok_or("missing identity header")?
        .to_vec();
    for other in [0, 2, u8::MAX] {
        *header.get_mut(offset).ok_or("missing identity version")? = other;
        assert!(
            matches!(decode(&header), Err(IdentityError::UnsupportedVersion)),
            "identity version must refuse before fields"
        );
    }
    header.pop();
    assert!(matches!(decode(&header), Err(error) if error == invalid));
    let mut alias = bytes.to_vec();
    *alias.first_mut().ok_or("missing identity domain")? ^= 1;
    for value in [
        alias.as_slice(),
        bytes.get(offset + 1..).ok_or("missing identity body")?,
    ] {
        assert!(
            matches!(decode(value), Err(error) if error == invalid),
            "identity domain must retain typed refusal"
        );
    }
    Ok(())
}

/// Require exact framing of admitted bytes through the owning production decoder.
pub fn framing<T>(bytes: &[u8], decode: impl Fn(&[u8]) -> Result<T, IdentityError>) {
    let mut alias = bytes.to_vec();
    alias.push(0);
    assert!(
        matches!(decode(&alias), Err(IdentityError::Malformed)),
        "identity framing must refuse trailing bytes"
    );
    alias.pop();
    alias.pop();
    assert!(
        matches!(decode(&alias), Err(IdentityError::Malformed)),
        "identity framing must refuse truncated fields"
    );
}

fn label(bytes: &[u8]) -> CheckResult {
    // Construct an LDH label; this is a generator, not another admission parser.
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-";
    let edge = *ALPHABET
        .get(usize::from(bytes.first().copied().unwrap_or(0)) % 36)
        .ok_or("missing DNS edge character")?;
    let mut value = vec![edge];
    for byte in bytes.iter().take(61) {
        value.push(
            *ALPHABET
                .get(usize::from(*byte) % ALPHABET.len())
                .ok_or("missing DNS character")?,
        );
    }
    value.push(edge);
    for valid in [value.as_slice(), std::slice::from_ref(&edge)] {
        assert!(
            validate_dns_label(valid).is_ok(),
            "generated DNS label must admit"
        );
    }
    for (index, byte) in [
        (0, b'-'),
        (value.len() - 1, b'-'),
        (value.len() / 2, b'.'),
        (0, b'A'),
        (0, b'_'),
        (0, b':'),
        (0, b'*'),
        (0, 0),
        (0, 255),
    ] {
        let mut alias = value.clone();
        *alias
            .get_mut(index)
            .ok_or("missing DNS mutation position")? = byte;
        assert!(
            validate_dns_label(&alias).is_err(),
            "DNS label aliases must refuse"
        );
    }
    assert!(
        validate_dns_label(&[]).is_err(),
        "empty DNS label must refuse"
    );
    value.resize(63, b'a');
    assert!(
        validate_dns_label(&value).is_ok(),
        "maximum DNS label must admit"
    );
    value.resize(64, b'a');
    assert!(
        validate_dns_label(&value).is_err(),
        "oversized DNS label must refuse"
    );
    value.truncate(63);
    *value.first_mut().ok_or("missing DNS first character")? = b'a';
    *value
        .get_mut(60)
        .ok_or("missing DNS final label boundary")? = b'a';
    let label = std::str::from_utf8(&value)?;
    // Four valid labels of 63/63/63/61 bytes reach the 253-byte host boundary.
    let mut maximum = format!(
        "{label}.{label}.{label}.{}",
        label.get(..61).ok_or("missing DNS label prefix")?
    );
    assert_eq!(
        CanonicalHost::parse(&maximum)?.as_str(),
        maximum,
        "maximum DNS host must admit"
    );
    maximum.push('a');
    assert_eq!(
        CanonicalHost::parse(&maximum),
        Err(DnsError::HostTooLong),
        "oversized DNS host must refuse"
    );
    let number = bytes.first().copied().unwrap_or(0);
    for tail in [number.to_string(), format!("0x{number:x}")] {
        assert_eq!(
            CanonicalHost::parse(&format!("{label}.{tail}")),
            Err(DnsError::NumericHost),
            "numeric DNS host must retain typed refusal"
        );
    }
    let _ = validate_dns_label(bytes);
    Ok(())
}

/// Check the real hostname producer against canonical and identity admission.
pub fn identity(value: &ResourceIdentity) {
    if let Ok(host) = value.hostname(DOMAIN) {
        assert_eq!(
            ResourceIdentity::from_hostname(host.as_str(), DOMAIN),
            Ok(value.clone())
        );
        assert_eq!(CanonicalHost::parse(host.as_str()), Ok(host));
    }
}

fn route(pattern: &str, url: &str) -> CheckResult {
    let Ok(original) = RoutePattern::parse(pattern) else {
        return Ok(());
    };
    let canonical = RoutePattern::parse(original.as_str())?;
    assert_eq!(canonical, original);
    let matched = original.matches(url);
    assert_eq!(canonical.matches(url), matched);
    assert_eq!(
        RoutePattern::parse(&format!("{}?", original.as_str())),
        Err(RouteError::Pattern)
    );
    for refused in [
        "https://user:password@route.example/",
        "https://route.example/#fragment",
        "ws://route.example/",
    ] {
        assert_eq!(
            original.matches(refused),
            Err(RouteError::Url),
            "routing must refuse credentials, fragments and unsupported schemes"
        );
    }
    let first = WorkerRoute {
        pattern: original,
        target: Some("first".into()),
    };
    let mut routes = vec![first.clone()];
    let expected = matched.map(|matched| matched.then_some(Some("first")));
    assert_eq!(select_route(&routes, url), expected);
    routes.push(first);
    assert_eq!(select_route(&routes, url), expected);
    routes.get_mut(1).ok_or("missing second route")?.target = Some("second".into());
    assert_eq!(
        select_route(&routes, url),
        match matched {
            Ok(true) => Err(RouteError::Ambiguous),
            other => other.map(|_| None),
        }
    );
    routes.get_mut(1).ok_or("missing second route")?.target = None;
    routes.remove(0);
    assert_eq!(
        select_route(&routes, url),
        matched.map(|matched| matched.then_some(None))
    );
    Ok(())
}

/// Check binary identities, hostnames and route tuples through production APIs.
pub fn check(bytes: &[u8]) {
    let result = checked(bytes);
    assert!(result.is_ok(), "addressing oracle failed: {result:?}");
}

fn checked(bytes: &[u8]) -> CheckResult {
    host_url_boundary()?;
    assert_eq!(
        AccountId::new(b""),
        Err(IdentityError::EmptyAccount),
        "empty account must retain typed refusal"
    );
    assert_eq!(
        ResourceId::new(b""),
        Err(IdentityError::EmptyId),
        "empty resource id must retain typed refusal"
    );
    if !bytes.is_empty() {
        let value = ResourceIdentity::new(bytes, bytes)?;
        assert_eq!(
            ResourceIdentity::decode(&value.encode()),
            Ok(value),
            "resource writer output must admit"
        );
        // Generate a legal actor name; the account still retains arbitrary bytes.
        let name: Vec<_> = bytes
            .iter()
            .take(128)
            .map(|byte| b'a' + byte % 26)
            .collect();
        for invalid in [b"".as_slice(), &[b'a'; 129], b"/", b"\0", b"\xff"] {
            assert_eq!(
                NamespaceId::new(invalid),
                Err(IdentityError::InvalidNamespace),
                "namespace name aliases must refuse"
            );
            assert_eq!(
                validate_actor_id(invalid),
                Err(IdentityError::InvalidActorId),
                "actor name aliases must refuse"
            );
        }
        let value = ActorReference::new(bytes, b"fuzz", &name)?;
        assert_eq!(
            ActorReference::decode(&value.encode()),
            Ok(value),
            "actor writer output must admit"
        );
    }
    if let Ok(value) = ResourceIdentity::decode(bytes) {
        header(
            bytes,
            super::RESOURCE_DOMAIN.len(),
            IdentityError::UnsupportedVersion,
            ResourceIdentity::decode,
        )?;
        framing(bytes, ResourceIdentity::decode);
        identity(&value);
        assert_eq!(
            value.encode(),
            bytes,
            "resource bytes must roundtrip exactly"
        );
        assert_eq!(ResourceIdentity::decode(&value.encode()), Ok(value));
    }
    if let Ok(value) = ActorReference::decode(bytes) {
        header(
            bytes,
            super::actor::HEADER.len() - 1,
            IdentityError::Malformed,
            ActorReference::decode,
        )?;
        framing(bytes, ActorReference::decode);
        assert_eq!(value.encode(), bytes, "actor bytes must roundtrip exactly");
        assert_eq!(ActorReference::decode(&value.encode()), Ok(value));
        assert!(ResourceIdentity::decode(bytes).is_err());
    }
    label(bytes)?;
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Ok(());
    };
    if let Ok(host) = CanonicalHost::parse(text) {
        assert_eq!(host.as_str(), text);
        assert_eq!(CanonicalHost::parse(host.as_str()), Ok(host));
        assert!(CanonicalHost::parse(&format!("{text}.")).is_err());
        let alias = text.to_ascii_uppercase();
        if alias != text {
            assert!(CanonicalHost::parse(&alias).is_err());
        }
    }
    if let Ok(value) = ResourceIdentity::from_hostname(text, DOMAIN) {
        assert_eq!(value.hostname(DOMAIN)?.as_str(), text);
        assert!(ResourceIdentity::from_hostname(text, "foreign.example.com").is_err());
        identity(&value);
    }
    // The separator is only a fuzz input tuple, never a product format.
    let (pattern, url) = text.split_once('\0').unwrap_or((text, text));
    route(pattern, url)
}

/// Canonical SDK host/pattern output, with caller-owned URL context for routes.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
    let value = ResourceIdentity::new(b"account", b"resource")?;
    let host = value.hostname(DOMAIN)?;
    let mut seeds = vec![
        (
            "resource-hostname".into(),
            host.as_str().as_bytes().to_vec(),
        ),
        (
            "canonical-alabel-hostname".into(),
            CanonicalHost::parse("xn--bcher-kva.example")?
                .as_str()
                .as_bytes()
                .to_vec(),
        ),
    ];
    let long_path = format!("/{}", "p".repeat(4096));
    for (index, (pattern, path)) in [
        (format!("https://{}/", host.as_str()), "/"),
        (format!("http://{}/data*", host.as_str()), "/data?q=1"),
        (format!("*.{}/data*", host.as_str()), "/data?q=1"),
        ("*/*".into(), "/data?q=1"),
        (format!("{}/*", host.as_str()), "/data?q=1"),
        (format!("https://{}/data", host.as_str()), "/data?q=1"),
        (
            format!("https://{}{long_path}*", host.as_str()),
            long_path.as_str(),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let value = RoutePattern::parse(&pattern)?;
        let mut bytes = value.as_str().as_bytes().to_vec();
        bytes.push(0);
        bytes.extend_from_slice(format!("https://{}{path}", host.as_str()).as_bytes());
        check(&bytes);
        seeds.push((format!("route-{index}"), bytes));
    }
    check(host.as_str().as_bytes());
    Ok(seeds)
}
