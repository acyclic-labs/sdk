//! Transport custody admits opaque bytes without interpreting token claims.
use acyclic_auth::credential::{
    CredentialError, CredentialProvider, ResolvedCredential, bearer_bytes, valid_credential,
};
use futures::FutureExt;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn resolve(value: &str) -> Result<ResolvedCredential, CredentialError> {
    let token = value.to_owned();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let provider = CredentialProvider::new(move || {
        observed.fetch_add(1, Ordering::Relaxed);
        // The producer is completed on this path, so Pending would be a
        // progress defect rather than an invitation to wait or retry.
        std::future::ready(Ok(token.clone()))
    });
    let result = provider
        .resolve()
        .now_or_never()
        .expect("ready credential producer completes");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    result
}

pub(super) fn check(bytes: &[u8]) {
    let Ok(value) = std::str::from_utf8(bytes) else {
        return;
    };
    let resolved = resolve(value);
    if !valid_credential(value) {
        assert!(matches!(resolved, Err(CredentialError::Invalid)));
        return;
    }
    let request = resolved.expect("admitted opaque credential resolves");
    assert_eq!(request.as_str().as_bytes(), bytes);
    request
        .check()
        .now_or_never()
        .expect("raw custody check completes")
        .unwrap();
    let encoded = bearer_bytes(request.as_str());
    let rest = encoded
        .strip_prefix(b"Bearer ")
        .expect("bearer transport prefix");
    assert_eq!(rest, bytes);
    assert_eq!(std::str::from_utf8(rest).unwrap(), value);
    // No trimming, normalization or token decoding may hide an injected
    // transport delimiter, including one after an otherwise admitted token.
    for delimiter in ['\r', '\n', '\0'] {
        let mut forged = value.to_owned();
        forged.push(delimiter);
        assert!(matches!(resolve(&forged), Err(CredentialError::Invalid)));
    }
    assert_eq!(request.as_str().as_bytes(), bytes);
}

/// Capture the actual bearer writer, using only public SDK test signing keys.
pub fn seed(root: &[u8]) -> Vec<u8> {
    let token = hex::encode(root);
    let encoded = bearer_bytes(&token);
    check(&encoded);
    encoded
}
