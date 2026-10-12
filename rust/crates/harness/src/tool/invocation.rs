use super::*;

/// One admitted invocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvocation {
    /// Runtime-owned identity for execution and reconciliation.
    pub operation_id: OperationId,
    /// Model-owned call identity.
    pub call_id: String,
    /// Registered tool name.
    pub name: String,
    /// Schema-validated arguments.
    pub arguments: Value,
}

impl ToolInvocation {
    /// Binds a model call to one deterministic operation within its parent turn.
    pub fn for_model_call(
        parent: OperationId,
        step: u32,
        call_id: String,
        name: String,
        arguments: Value,
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let hashed = (b"harness:tool-call:v2".len() as u64)
                .checked_add(16 + 4)
                .and_then(|bytes| bytes.checked_add(call_id.len() as u64))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool call hash length overflow",
                })?;
            work.charge(acyclic_fs::WorkCounters {
                bytes_hashed: hashed,
                items_examined: 4,
                ..Default::default()
            })?;
        }
        let _ = work;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"harness:tool-call:v2");
        hasher.update(&parent.into_bytes());
        hasher.update(&step.to_be_bytes());
        hasher.update(call_id.as_bytes());
        let digest = hasher.finalize();
        let mut identity = [0_u8; 16];
        identity.copy_from_slice(&digest.as_bytes()[..16]);
        Ok(Self {
            operation_id: OperationId::from_bytes(identity),
            call_id,
            name,
            arguments,
        })
    }

    /// Validates a call ID and its tool name when already selected.
    pub fn validate_identity(
        call_id: &str,
        name: Option<&str>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if work.is_some() {
            let items = (call_id.len() as u64)
                .checked_add(name.map_or(0, |name| name.len() as u64))
                .ok_or_else(|| {
                    crate::effect_host::effect_error(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "tool identity scan overflow",
                    )
                })?;
            work.as_deref_mut()
                .expect("identity observer")
                .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items })?;
        }
        if name.is_some_and(|name| !crate::contract::is_valid_component_label(name)) {
            return Err(crate::effect_host::effect_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "tool name is invalid",
            ));
        }
        if !valid_call_id(call_id) {
            return Err(crate::effect_host::effect_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "tool call identity is invalid",
            ));
        }
        Ok(())
    }
}

fn valid_call_id(call_id: &str) -> bool {
    !(call_id.is_empty()
        || call_id.len() as u64 > crate::COMPONENT_LABEL_MAX_BYTES as u64
        || call_id.chars().any(char::is_control)
        || call_id.contains('/')
        || call_id.contains('\\')
        || call_id == "."
        || call_id == "..")
}

/// Result returned by a tool executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Schema-validated structured value.
    pub value: Value,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for ToolInvocation {
    fn diagnostic_label_bytes() -> u64 {
        tool_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let deep = crate::contract::json_value_logical_owned_bytes(&self.arguments)?;
        (self.call_id.capacity() as u64)
            .checked_add(self.name.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(deep))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool invocation ownership overflow",
            })
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ToolInvocation {
    fn diagnostic_label_bytes() -> u64 {
        tool_serde_diagnostic_label_bytes()
    }
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<Self>().max(std::mem::size_of::<(String, Value)>()) as u64
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for ToolResult {
    fn diagnostic_label_bytes() -> u64 {
        tool_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::contract::json_value_logical_owned_bytes(&self.value)
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ToolResult {
    fn diagnostic_label_bytes() -> u64 {
        tool_serde_diagnostic_label_bytes()
    }
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<Self>().max(std::mem::size_of::<(String, Value)>()) as u64
    }
}
