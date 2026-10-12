use acyclic_fs::FILE_DESCRIPTOR_SET;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;

pub fn json<M: Message>(name: &str, message: &M) -> Result<Value, Box<dyn std::error::Error>> {
    let descriptor = DescriptorPool::decode(FILE_DESCRIPTOR_SET)?
        .get_message_by_name(&format!("acyclic.filesystem.v2.{name}"))
        .ok_or("missing filesystem descriptor")?;
    let message = DynamicMessage::decode(descriptor, message.encode_to_vec().as_slice())?;
    Ok(serde_json::to_value(message)?)
}
