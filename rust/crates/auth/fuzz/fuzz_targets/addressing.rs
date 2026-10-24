#![no_main]
use acyclic_auth::addressing::{
    ActorReference, CanonicalHost, ResourceIdentity, validate_dns_label,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(route) = acyclic_auth::addressing::RoutePattern::parse(text)
    {
        assert_eq!(
            acyclic_auth::addressing::RoutePattern::parse(route.as_str()),
            Ok(route.clone())
        );
        let _ = route.matches(text);
    }
    if let Ok(reference) = ActorReference::decode(data) {
        assert_eq!(reference.encode(), data);
        assert!(ResourceIdentity::decode(data).is_err());
    }
    if let Ok(identity) = ResourceIdentity::decode(data) {
        assert_eq!(identity.encode(), data);
        if let Ok(host) = identity.hostname("example.com") {
            assert_eq!(CanonicalHost::parse(host.as_str()), Ok(host));
        }
    }
    let _ = validate_dns_label(data);
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(host) = CanonicalHost::parse(text)
    {
        assert_eq!(host.as_str(), text);
        let _ = ResourceIdentity::from_hostname(text, "run.cell-1.example");
    }
});
