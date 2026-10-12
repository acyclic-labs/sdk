use super::*;

#[test]
fn stream_error_codes_are_closed_and_stable() {
    for error in StreamError::ALL {
        assert!(is_stream_error_code(error.code()));
    }
    assert!(!is_stream_error_code("stream_not_found"));
    assert!(!is_stream_error_code("unknown"));
}

#[test]
fn hosted_http_errors_use_the_public_route_aware_vocabulary() {
    assert_eq!(
        public_http_error_code("not_found", "tail").as_deref(),
        Some("stream_not_found")
    );
    assert_eq!(
        public_http_error_code("not_found", "commits/read").as_deref(),
        Some("commit_not_found")
    );
    assert_eq!(
        public_http_error_code("out_of_range", "read").as_deref(),
        Some("out_of_range")
    );
    assert_eq!(
        public_http_error_code("capacity", "append").as_deref(),
        Some("capacity_exhausted")
    );
    assert_eq!(
        public_http_error_code("stream_not_found", "read").as_deref(),
        Some("stream_not_found")
    );
    assert_eq!(public_http_error_code("commit_not_found", "read"), None);
    assert_eq!(public_http_error_code("unknown", "read"), None);
}
