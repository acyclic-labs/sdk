#![no_main]
use acyclic_auth::addressing::{ActorReference, ResourceIdentity, testing, validate_dns_label};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    testing::check(data);
    if let Ok(reference) = ActorReference::decode(data) {
        assert_eq!(reference.encode(), data);
        assert!(ResourceIdentity::decode(data).is_err());
    }
    if let Ok(identity) = ResourceIdentity::decode(data) {
        assert_eq!(identity.encode(), data);
        testing::identity(&identity);
    }
    let _ = validate_dns_label(data);
});
