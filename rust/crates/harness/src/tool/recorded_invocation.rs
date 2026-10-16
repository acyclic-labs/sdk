use super::*;

/// Durable invocation authority and the model's opaque continuation payload.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordedInvocation<I = ToolInvocation, M = Value> {
    pub invocation: I,
    pub metadata: M,
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for RecordedInvocation {
    fn diagnostic_label_bytes() -> u64 {
        TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES
    }
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<Self>().max(std::mem::size_of::<(String, Value)>()) as u64
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for RecordedInvocation {
    fn diagnostic_label_bytes() -> u64 {
        TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let invocation = crate::filesystem::UploadRecord::logical_owned_bytes(&self.invocation)?;
        invocation
            .checked_add(crate::executor::json::json_value_logical_owned_bytes(
                &self.metadata,
            )?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "recorded invocation ownership overflow",
            })
    }
}

impl ToolInvocation {
    /// Admits the complete saved record under the same byte, depth and literal JSON bounds as replay.
    pub fn validate_recorded(
        &self,
        metadata: &Value,
        limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| {
            limits.validate(work)?;
            crate::executor::json::json_byte_length_at_depth(
                &RecordedInvocation {
                    invocation: self,
                    metadata,
                },
                limits.file_bytes.min(limits.render_bytes),
                crate::executor::json::RECORD_MAXIMUM_DEPTH,
                work,
            )?;
            Self::validate_identity(&self.call_id, Some(&self.name), work)?;
            crate::conversation::validate_json_value(&self.arguments, work)?;
            crate::conversation::validate_json_value(metadata, work)
        })
    }
}
