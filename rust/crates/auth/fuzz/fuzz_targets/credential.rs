#![no_main]
use acyclic_auth::credential::{CredentialError, CredentialProvider, valid_credential};
use libfuzzer_sys::fuzz_target;
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fuzz_target!(|value: &str| {
    let admitted = valid_credential(value);
    let token = value.to_owned();
    let provider = CredentialProvider::new(move || std::future::ready(Ok(token.clone())));
    let mut resolution = std::pin::pin!(provider.resolve());
    // A ready original callback has no outstanding event to wait on.
    match resolution
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(request)) => {
            assert!(admitted);
            assert_eq!(request.as_str(), value);
        }
        Poll::Ready(Err(CredentialError::Invalid)) => assert!(!admitted),
        Poll::Ready(Err(error)) => panic!("ready credential admission failed: {error}"),
        Poll::Pending => panic!("ready credential admission lost progress"),
    }
});
