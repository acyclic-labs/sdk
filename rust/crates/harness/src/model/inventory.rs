use super::*;
use std::mem::size_of;

pub(super) fn overflow() -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { Error::NativeBoundary { code: Invalid, reason: "closed model payload ownership overflow" } }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { Error::Invalid("closed model payload ownership overflow".into()) }
}
pub(super) fn add(left: u64, right: u64) -> Result<u64> { left.checked_add(right).ok_or_else(overflow) }
pub(super) fn slots(capacity: usize, size: usize) -> Result<u64> { (capacity as u64).checked_mul(size as u64).ok_or_else(overflow) }

impl FileProjectionPolicy {
    fn payload_bytes(&self) -> Result<u64> {
        let Self::Native(policy) = self else { return Ok(0); };
        let mut bytes = size_of::<NativeMediaPolicy>() as u64;
        if let Some(binding) = &policy.configuration {
            bytes = add(bytes, binding.source.authority.id.capacity() as u64)?;
            bytes = add(bytes, binding.configuration.extension.name.capacity() as u64)?;
            bytes = add(bytes, binding.configuration.content.logical_owned_bytes()?)?;
        }
        Ok(bytes)
    }
}
impl ModelContentPart {
    fn payload_bytes(&self) -> Result<u64> {
        match self {
            Self::Text { text } => Ok(text.capacity() as u64),
            Self::File { file, policy } => add(file.logical_owned_bytes()?, policy.payload_bytes()?),
            Self::ToolCall { call_id, name, arguments } => add(add(call_id.capacity() as u64, name.capacity() as u64)?, crate::contract::json_value_logical_owned_bytes(arguments)?),
            Self::ToolResult { call_id, name, content } => add(add(call_id.capacity() as u64, name.capacity() as u64)?, content.payload_bytes()?),
        }
    }
}
impl ToolResultContent {
    pub(super) fn payload_bytes(&self) -> Result<u64> {
        match self {
            Self::Json { value } => crate::contract::json_value_logical_owned_bytes(value),
            Self::Parts { parts } => parts.iter().try_fold(slots(parts.capacity(), size_of::<ModelDataPart>())?, |bytes, part| {
                let payload = match part {
                    ModelDataPart::Text { text } => text.capacity() as u64,
                    ModelDataPart::File { file, policy } => add(file.logical_owned_bytes()?, policy.payload_bytes()?)?,
                };
                add(bytes, payload)
            }),
        }
    }
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> { add(size_of::<Self>() as u64, self.payload_bytes()?) }
}
impl ModelContent {
    fn payload_bytes(&self) -> Result<u64> {
        match self {
            Self::Text(text) => Ok(text.capacity() as u64), Self::Part(part) => part.payload_bytes(),
            Self::Parts(parts) => parts.iter().try_fold(slots(parts.capacity(), size_of::<ModelContentPart>())?, |bytes, part| add(bytes, part.payload_bytes()?)),
        }
    }
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> { add(size_of::<Self>() as u64, self.payload_bytes()?) }
}
impl ModelRequest {
    fn payload_bytes(&self) -> Result<u64> {
        let mut bytes = crate::contract::json_value_logical_owned_bytes(&self.model.options)?;
        for capacity in [self.model.provider.capacity(), self.model.name.capacity(), self.model.revision.capacity()] { bytes = add(bytes, capacity as u64)?; }
        let messages = self.messages.iter().try_fold(slots(self.messages.capacity(), size_of::<ModelMessage>())?, |bytes, message| add(bytes, message.content.payload_bytes()?))?;
        bytes = add(bytes, messages)?;
        bytes = add(bytes, slots(self.tools.capacity(), size_of::<crate::tool::ToolDefinition>())?)?;
        for tool in &self.tools {
            for capacity in [tool.name.capacity(), tool.revision.capacity(), tool.description.capacity()] { bytes = add(bytes, capacity as u64)?; }
            for value in [&tool.input_schema, &tool.output_schema, &tool.projection_schema] { bytes = add(bytes, crate::contract::json_value_logical_owned_bytes(value)?)?; }
        }
        Ok(bytes)
    }
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> { add(size_of::<Self>() as u64, self.payload_bytes()?) }
}
impl PreparedModelRequest {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> { add(size_of::<Self>() as u64, self.request().payload_bytes()?) }
}
