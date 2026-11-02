#![no_main]

use acyclic_objects::{Error, wire::ErrorCode};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (i32, u16)| {
    let (grpc, http) = input;
    let grpc_failure = Error::from_grpc_status(grpc);
    let http_failure = Error::from_http_status(http);
    // Unknown protocol statuses cannot invent a definitive service refusal.
    if !(0..=16).contains(&grpc) {
        assert_eq!(grpc_failure.code, ErrorCode::Unavailable);
    }
    if !(400..600).contains(&http) {
        assert_eq!(http_failure.code, ErrorCode::Unavailable);
    }
    // Terminal capability/authentication refusals must never turn into an
    // outage because that would authorize a transport retry.
    if matches!(grpc, 7 | 16) {
        assert_eq!(grpc_failure.code, ErrorCode::AccessDenied);
    }
    if grpc == 12 {
        assert_eq!(grpc_failure.code, ErrorCode::Unsupported);
    }
    if matches!(http, 401 | 403) {
        assert_eq!(http_failure.code, ErrorCode::AccessDenied);
    }
    if http == 501 {
        assert_eq!(http_failure.code, ErrorCode::Unsupported);
    }
});
