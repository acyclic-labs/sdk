//! Transport custody admits opaque bytes without interpreting token claims.
use acyclic_auth::credential::{
    CredentialError, CredentialProvider, ResolvedCredential, bearer_bytes, capability_from_text,
    capability_serde, capability_text, valid_credential,
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

fn capability_format(bytes: &[u8]) {
    let text = capability_text(bytes);
    assert_eq!(capability_from_text(&text).unwrap(), bytes);
    let mut json = Vec::new();
    capability_serde::serialize(bytes, &mut serde_json::Serializer::new(&mut json)).unwrap();
    assert_eq!(serde_json::from_slice::<String>(&json).unwrap(), text);
    let mut input = serde_json::Deserializer::from_slice(&json);
    assert_eq!(capability_serde::deserialize(&mut input).unwrap(), bytes);
    for suffix in ['=', '!', '\0'] {
        let mut invalid = text.clone();
        invalid.push(suffix);
        assert!(capability_from_text(&invalid).is_err());
        let invalid_json = serde_json::to_vec(&invalid).unwrap();
        let mut input = serde_json::Deserializer::from_slice(&invalid_json);
        assert!(capability_serde::deserialize(&mut input).is_err());
    }
    // An arbitrary accepted text has exactly its original canonical spelling.
    if let Ok(text) = std::str::from_utf8(bytes)
        && let Ok(decoded) = capability_from_text(text)
    {
        assert_eq!(capability_text(&decoded), text);
    }
    // Raw JSON reaches the actual field adapter, including non-string inputs.
    let mut input = serde_json::Deserializer::from_slice(bytes);
    if let Ok(decoded) = capability_serde::deserialize(&mut input) {
        let mut original = serde_json::Deserializer::from_slice(bytes);
        let text: String = serde::Deserialize::deserialize(&mut original).unwrap();
        assert_eq!(capability_text(&decoded), text);
    }
}

/// Check real text codecs, provider admission, progress and exact bearer bytes.
pub fn check(bytes: &[u8]) {
    capability_format(bytes);
    let Ok(value) = std::str::from_utf8(bytes) else {
        return;
    };
    // Independent header-contract oracle, checked through the real provider.
    let expected = value.len() <= 12 * 1024
        && value.chars().any(|character| !character.is_whitespace())
        && value.bytes().all(|byte| ![0, 10, 13].contains(&byte));
    assert_eq!(valid_credential(value), expected);
    let resolved = resolve(value);
    if !expected {
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
        assert!(
            matches!(resolve(&forged), Err(CredentialError::Invalid)),
            "credential transport delimiter must refuse"
        );
    }
    assert_eq!(request.as_str().as_bytes(), bytes);
}
