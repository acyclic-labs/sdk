//! Transport custody shares the Auth provider conformance oracle.
pub(super) use acyclic_auth::credential::testing::check;

/// Capture the actual bearer writer, using only public SDK test signing keys.
pub fn seed(root: &[u8]) -> Vec<u8> {
    let token = hex::encode(root);
    let encoded = acyclic_auth::credential::bearer_bytes(&token);
    check(&encoded);
    encoded.to_vec()
}
