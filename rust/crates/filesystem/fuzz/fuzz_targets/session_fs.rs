#![no_main]
use acyclic_stream::{FollowFilter, wire};
use libfuzzer_sys::fuzz_target;
use prost::Message;
fuzz_target!(|data: &[u8]| {
    if let Ok(value) = wire::SessionSubscription::decode(data) {
        let encoded = value.encode_to_vec();
        assert_eq!(
            wire::SessionSubscription::decode(encoded.as_slice()).unwrap(),
            value
        );
        if let Some(filter) = value.filter {
            let _ = FollowFilter::new(filter);
        }
    }
    if let Ok(value) = wire::ChangeRecord::decode(data) {
        assert_eq!(
            wire::ChangeRecord::decode(value.encode_to_vec().as_slice()).unwrap(),
            value
        );
    }
    if let Some((head, tail)) = data.split_at_checked(16) {
        let id: [u8; 16] = head.try_into().unwrap();
        if let Ok(path) = std::str::from_utf8(tail)
            && let Ok(resource) = acyclic_fs::session::resource(acyclic_fs::VolumeId::from_bytes(id), path)
        {
            assert!(acyclic_auth::valid_resource(&resource));
        }
    }
});
