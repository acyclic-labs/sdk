#![no_main]
use acyclic_objects::v1::{request, wire};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|bytes: &[u8]| {
    let result = request::validate_binary("objects/restore", bytes, 0);
    if bytes.len() > 16 * 1024 * 1024 {
        assert_eq!(result.unwrap_err().code, wire::ErrorCode::QuotaExceeded);
        return;
    }
    if let Ok(query) = wire::RestoreObjectsRequest::decode(bytes) {
        let canonical = query.encode_to_vec();
        assert_eq!(
            wire::RestoreObjectsRequest::decode(canonical.as_slice()).unwrap(),
            query
        );
        assert_eq!(result, request::restore_digest(&query).map(|_| ()));
    } else {
        assert!(result.is_err());
    }
});
