//! Textual addresses must retain the identities and route decisions they carry.
use acyclic_auth::addressing::{
    CanonicalHost, ResourceIdentity, RouteError, RoutePattern, WorkerRoute, select_route,
};

const DOMAIN: &str = "run.example.com";

pub(super) fn identity(value: &ResourceIdentity) {
    if let Ok(host) = value.hostname(DOMAIN) {
        assert_eq!(
            ResourceIdentity::from_hostname(host.as_str(), DOMAIN),
            Ok(value.clone())
        );
        assert_eq!(CanonicalHost::parse(host.as_str()), Ok(host));
    }
}

fn route(pattern: &str, url: &str) {
    let Ok(original) = RoutePattern::parse(pattern) else {
        return;
    };
    let canonical = RoutePattern::parse(original.as_str()).unwrap();
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
        assert_eq!(original.matches(refused), Err(RouteError::Url));
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
    routes[1].target = Some("second".into());
    assert_eq!(
        select_route(&routes, url),
        match matched {
            Ok(true) => Err(RouteError::Ambiguous),
            other => other.map(|_| None),
        }
    );
    routes[1].target = None;
    routes.remove(0);
    assert_eq!(
        select_route(&routes, url),
        matched.map(|matched| matched.then_some(None))
    );
}

pub(super) fn check(bytes: &[u8]) {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
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
        assert_eq!(value.hostname(DOMAIN).unwrap().as_str(), text);
        assert!(ResourceIdentity::from_hostname(text, "foreign.example.com").is_err());
        identity(&value);
    }
    // The separator is only a fuzz input tuple, never a product format.
    let (pattern, url) = text.split_once('\0').unwrap_or((text, text));
    route(pattern, url);
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
    for (index, (pattern, path)) in [
        (format!("https://{}/", host.as_str()), "/"),
        (format!("http://{}/data*", host.as_str()), "/data?q=1"),
        (format!("*.{}/data*", host.as_str()), "/data?q=1"),
        ("*/*".into(), "/data?q=1"),
        (format!("{}/*", host.as_str()), "/data?q=1"),
        (format!("https://{}/data", host.as_str()), "/data?q=1"),
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
