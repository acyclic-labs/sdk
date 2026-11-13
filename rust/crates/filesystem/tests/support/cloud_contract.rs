use acyclic_fs_wire::FILE_DESCRIPTOR_SET;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;

pub fn handshake() -> acyclic_fs_wire::wire::filesystem::v2::HandshakeRequest {
    use acyclic_fs_wire::wire::{filesystem::v2 as wire, protocol::v1 as protocol};
    wire::HandshakeRequest {
        protocol: Some(protocol::HandshakeRequest {
            protocol: Some(protocol::ProtocolIdentity {
                version: acyclic_fs::FILESYSTEM_PROTOCOL_VERSION.to_owned(),
                descriptor_digest: acyclic_fs::descriptor_digest(),
            }),
            required: Some(protocol::CapabilitySet {
                capabilities: vec![protocol::Capability {
                    name: "filesystem".to_owned(),
                    version: acyclic_fs::FILESYSTEM_PROTOCOL_VERSION.to_owned(),
                }],
            }),
        }),
    }
}

pub fn json<M: Message>(name: &str, message: &M) -> Result<Value, Box<dyn std::error::Error>> {
    let descriptor = DescriptorPool::decode(FILE_DESCRIPTOR_SET)?
        .get_message_by_name(&format!("acyclic.filesystem.v2.{name}"))
        .ok_or("missing filesystem descriptor")?;
    let message = DynamicMessage::decode(descriptor, message.encode_to_vec().as_slice())?;
    Ok(serde_json::to_value(message)?)
}
