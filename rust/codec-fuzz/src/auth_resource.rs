//! Opaque resource components use the same canonical grammar as grants.
pub(super) fn check(bytes: &[u8]) {
    use acyclic_auth::{decode_component, encode_component, objects_resource, valid_resource};
    let mut encoded = encode_component(bytes);
    assert_eq!(decode_component(&encoded).unwrap(), bytes);
    // An incomplete escape cannot become a second canonical spelling.
    encoded.push('%');
    assert!(matches!(
        decode_component(&encoded),
        Err(acyclic_auth::Error::Invalid)
    ));
    let resource = objects_resource(b"bucket", bytes);
    assert!(valid_resource(&resource));
    let (_, opaque_key) = resource
        .rsplit_once('/')
        .expect("actual resource writer separator");
    assert_eq!(decode_component(opaque_key).unwrap(), bytes);
    if let Ok(text) = std::str::from_utf8(bytes) {
        if let Ok(decoded) = decode_component(text) {
            assert_eq!(encode_component(&decoded), text);
        }
        let _ = valid_resource(text);
        let _ = acyclic_auth::valid_action(text);
    }
}
