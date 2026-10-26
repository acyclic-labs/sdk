//! Authentication comparisons use the shared cryptographic implementation.
use acyclic_auth::crypto::{authentication_eq, verify_signature};
use ed25519_dalek::{Signer as _, SigningKey};

#[test]
fn strict_signature_verification_rejects_every_changed_byte_and_prefix() {
    let signing = SigningKey::from_bytes(&[37; 32]);
    let key = signing.verifying_key();
    let message = b"shared runtime metadata and capability verifier";
    let signature = signing.sign(message).to_bytes();
    assert!(verify_signature(&key, message, &signature).is_ok());
    for index in 0..signature.len() {
        let mut changed = signature;
        for byte in changed.iter_mut().skip(index).take(1) {
            *byte ^= 1;
        }
        assert!(verify_signature(&key, message, &changed).is_err());
        let (prefix, _) = signature.split_at(index);
        assert!(verify_signature(&key, message, prefix).is_err());
    }
    assert!(verify_signature(&key, b"another payload", &signature).is_err());
    let foreign = SigningKey::from_bytes(&[38; 32]).verifying_key();
    assert!(verify_signature(&foreign, message, &signature).is_err());
}

#[test]
fn authentication_comparison_checks_the_complete_value() {
    let original = [39; 64];
    assert!(authentication_eq(&original, &original));
    for index in 0..original.len() {
        let mut changed = original;
        for byte in changed.iter_mut().skip(index).take(1) {
            *byte ^= 1;
        }
        assert!(!authentication_eq(&original, &changed));
        let (prefix, _) = original.split_at(index);
        assert!(!authentication_eq(&original, prefix));
    }
    // A prefix-only comparison would accept this planted partial signature.
    let truncated = &original[..32];
    assert!(original.starts_with(truncated));
    assert!(!authentication_eq(&original, truncated));
}
