//! Cross-language protobuf fixture for the Machines create contract.

use acyclic_machines::wire;
use prost::Message;

// Produced by a language-neutral protobuf encoder for the typed request in
// sdk-examples/src/machines_scenarios.rs. Keeping the bytes here catches wire
// drift that a Rust-to-Rust re-encode comparison would miss.
const CREATE_MACHINE_FIXTURE_HEX: &str = concat!(
    "0a0408011001",
    "12120a1001010101010101010101010101010101",
    "1a2408021a200707070707070707070707070707070707070707070707070707070707070707",
    "22020801",
    "3203109875",
    "3a020801",
    "42200808080808080808080808080808080808080808080808080808080808080808",
    "4a00"
);

#[test]
fn create_fixture_preserves_presence_enums_and_wire_tags() {
    let bytes = hex::decode(CREATE_MACHINE_FIXTURE_HEX).expect("fixture hex");
    let request = wire::CreateMachineRequest::decode(bytes.as_slice())
        .expect("cross-language CreateMachineRequest fixture");

    assert_eq!(
        request.protocol,
        Some(wire::ProtocolVersion { major: 1, minor: 1 })
    );
    assert_eq!(
        request
            .idempotency_key
            .as_ref()
            .map(|key| key.value.as_slice()),
        Some([1; 16].as_slice())
    );
    let image = request.image.as_ref().expect("image presence");
    assert_eq!(image.kind, wire::ImageKind::Custom as i32);
    assert_eq!(
        image.immutable_reference,
        Some(wire::image::ImmutableReference::CustomDigest(vec![7; 32]))
    );
    assert_eq!(
        request.compatibility.as_ref().map(|policy| policy.mode),
        Some(wire::CompatibilityMode::BestEffort as i32)
    );
    assert!(request.suspension.is_some());
    assert!(request.expiration.is_some());
    assert!(request.budgets.is_some());
    assert_eq!(request.network_policy_digest, vec![8; 32]);
    assert_eq!(request.encode_to_vec(), bytes);
}
