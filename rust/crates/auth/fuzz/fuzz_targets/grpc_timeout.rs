#![no_main]
use acyclic_auth::grpc_timeout::{Deadline, read};
use libfuzzer_sys::fuzz_target;
use tonic::codegen::http::{HeaderMap, HeaderValue, Request};

fuzz_target!(|bytes: &[u8]| {
    let Ok(value) = HeaderValue::from_bytes(bytes) else {
        return;
    };
    let mut headers = HeaderMap::new();
    headers.insert("grpc-timeout", value.clone());
    let decoded = read(&headers);
    let mut request = Request::new(());
    *request.headers_mut() = headers.clone();
    let admitted = Deadline::of(&request);
    if decoded.is_err() {
        assert!(admitted.is_err());
    }
    if let (Ok(Some(original)), Ok(deadline)) = (decoded, admitted) {
        assert!(deadline.remaining().unwrap() <= original);
        deadline.forward(&mut headers).unwrap();
        assert!(read(&headers).unwrap().unwrap() <= original);
        request.extensions_mut().insert(deadline);
        assert_eq!(Deadline::of(&request).unwrap().end(), deadline.end());
    }
    // A second value never silently replaces the first admitted budget.
    headers.append("grpc-timeout", value);
    assert!(read(&headers).is_err());
});
