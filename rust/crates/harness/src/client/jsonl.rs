//! Canonical protobuf JSON, shared by stdin, stdout and service adapters.

use crate::{Error, Result, wire};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde::de::DeserializeSeed;
use std::sync::OnceLock;

fn descriptor(name: &str) -> Result<prost_reflect::MessageDescriptor> {
    static POOL: OnceLock<std::result::Result<DescriptorPool, String>> = OnceLock::new();
    let pool = POOL
        .get_or_init(|| {
            DescriptorPool::decode(crate::FILE_DESCRIPTOR_SET).map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| Error::Invalid(error.clone()))?;
    pool.get_message_by_name(name)
        .ok_or_else(|| Error::Invalid(format!("missing wire descriptor {name}")))
}

fn encode(message: &impl Message, name: &str) -> Result<String> {
    let dynamic = DynamicMessage::decode(descriptor(name)?, message.encode_to_vec().as_slice())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    serde_json::to_string(&dynamic).map_err(|error| Error::Invalid(error.to_string()))
}

fn decode<M: Message + Default>(line: &str, name: &str) -> Result<M> {
    let mut decoder = serde_json::Deserializer::from_str(line);
    let dynamic = descriptor(name)?
        .deserialize(&mut decoder)
        .map_err(|error| Error::Invalid(format!("invalid wire JSON: {error}")))?;
    decoder
        .end()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    M::decode(dynamic.encode_to_vec().as_slice()).map_err(|error| Error::Invalid(error.to_string()))
}

/// Renders the exact service frame as one line, without a trailing newline.
pub fn server_frame(frame: &wire::ServerFrame) -> Result<String> {
    encode(frame, "acyclic.harness.v2.ServerFrame")
}

/// Parses one service frame, including protobuf bytes and string-valued u64s.
pub fn parse_server_frame(line: &str) -> Result<wire::ServerFrame> {
    decode(line, "acyclic.harness.v2.ServerFrame")
}

/// Renders one wire command frame.
pub fn client_frame(frame: &wire::ClientFrame) -> Result<String> {
    encode(frame, "acyclic.harness.v2.ClientFrame")
}

/// Parses `--input-format jsonl` input using the service's schema.
pub fn parse_client_frame(line: &str) -> Result<wire::ClientFrame> {
    decode(line, "acyclic.harness.v2.ClientFrame")
}
