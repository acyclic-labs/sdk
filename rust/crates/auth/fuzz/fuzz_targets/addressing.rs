#![no_main]
use acyclic_auth::addressing::{CanonicalHost, ResourceAddress, ResourceName, validate_dns_label};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(resource) = ResourceName::parse(data) {
        assert_eq!(resource.encode(), data);
        assert_eq!(resource.address().as_bytes(), blake3::hash(data).as_bytes());
    }
    let _ = validate_dns_label(data);
    if let Ok(text) = std::str::from_utf8(data) {
        if let Ok(address) = text.parse::<ResourceAddress>() {
            assert_eq!(address.to_string(), text);
        }
        if let Ok(host) = CanonicalHost::parse(text) {
            assert_eq!(host.as_str(), text);
            for label in host.as_str().split('.') {
                assert!(validate_dns_label(label.as_bytes()).is_ok());
            }
        }
    }
});
