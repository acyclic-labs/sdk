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
        .ok_or_else(|| CredentialError::Provider("credential producer did not complete".into()))?;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    result
}

fn capability_format(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let text = capability_text(bytes);
    assert_eq!(capability_from_text(&text)?, bytes);
    let mut json = Vec::new();
    capability_serde::serialize(bytes, &mut serde_json::Serializer::new(&mut json))?;
    assert_eq!(serde_json::from_slice::<String>(&json)?, text);
    let mut input = serde_json::Deserializer::from_slice(&json);
    assert_eq!(
        capability_serde::deserialize(&mut input)?,
        bytes,
        "capability JSON must retain original bytes"
    );
    for suffix in ['=', '!', '\0'] {
        let mut invalid = text.clone();
        invalid.push(suffix);
        assert!(capability_from_text(&invalid).is_err());
        let invalid_json = serde_json::to_vec(&invalid)?;
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
        let text: String = serde::Deserialize::deserialize(&mut original)?;
        assert_eq!(capability_text(&decoded), text);
    }
    Ok(())
}

/// Check real text codecs, provider admission, progress and exact bearer bytes.
pub fn check(bytes: &[u8]) {
    let result = checked(bytes);
    assert!(result.is_ok(), "credential oracle failed: {result:?}");
}

fn checked(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    capability_format(bytes)?;
    let Ok(value) = std::str::from_utf8(bytes) else {
        return Ok(());
    };
    // Independent header-contract oracle, checked through the real provider.
    let expected = value.chars().any(|character| !character.is_whitespace())
        && value.bytes().all(|byte| ![0, 10, 13].contains(&byte));
    assert_eq!(valid_credential(value), expected);
    let resolved = resolve(value);
    if !expected {
        assert!(matches!(resolved, Err(CredentialError::Invalid)));
        return Ok(());
    }
    let request = resolved?;
    assert_eq!(request.as_str().as_bytes(), bytes);
    request
        .check()
        .now_or_never()
        .ok_or("raw custody check did not complete")??;
    let encoded = bearer_bytes(request.as_str());
    let rest = encoded
        .strip_prefix(b"Bearer ")
        .ok_or("missing bearer transport prefix")?;
    assert_eq!(rest, bytes);
    assert_eq!(std::str::from_utf8(rest)?, value);
    let header = acyclic_auth::credential::bearer_header(value);
    let representable = http::HeaderValue::from_bytes(&encoded).is_ok();
    assert_eq!(header.is_ok(), representable);
    if let Ok(header) = header {
        assert!(header.is_sensitive());
        let retained = header.clone();
        assert_eq!(header.as_bytes().as_ptr(), retained.as_bytes().as_ptr());
        drop(header);
        assert_eq!(retained.as_bytes(), encoded.as_ref());
    }
    #[cfg(all(not(target_arch = "wasm32"), feature = "grpc-metadata"))]
    {
        let metadata = acyclic_auth::credential::bearer_metadata(value);
        let expected =
            tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(encoded.clone());
        assert_eq!(metadata.is_ok(), expected.is_ok());
        if let Ok(metadata) = metadata {
            assert!(metadata.is_sensitive());
            let retained = metadata.clone();
            assert_eq!(
                metadata.as_encoded_bytes().as_ptr(),
                retained.as_encoded_bytes().as_ptr()
            );
            drop(metadata);
            assert_eq!(retained.as_encoded_bytes(), encoded.as_ref());
        }
    }
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
    Ok(())
}
