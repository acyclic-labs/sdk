#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_objects::v1::{request, wire};
use prost::Message;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let result = request::validate_binary("objects/restore", bytes, 0);
    if bytes.len() > 16 * 1024 * 1024 {
        assert_eq!(result.unwrap_err().code, wire::ErrorCode::QuotaExceeded);
        return;
    }
    if let Some(query) =
        fuzz_support::FuzzInput::parse(fuzz_input, wire::RestoreObjectsRequest::decode)
    {
        let canonical = query.encode_to_vec();
        let _ = fuzz_support::round_trip(
            &(query),
            wire::RestoreObjectsRequest::decode(canonical.as_slice()),
        );
        assert_eq!(result, request::restore_digest(&query).map(|_| ()));
    } else {
        assert!(result.is_err());
    }
});
