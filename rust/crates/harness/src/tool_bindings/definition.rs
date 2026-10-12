use super::*;

/// Model-visible tool definition with immutable schemas.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ToolDefinition {
    /// Stable bounded tool name; local names and explicit namespaces are both valid.
    pub name: String,
    /// Immutable revision of definition, executor, and projection semantics.
    pub revision: String,
    /// Model-visible description.
    pub description: String,
    /// JSON Schema for invocation arguments.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonSchema"))]
    pub input_schema: Value,
    /// JSON Schema for the canonical successful executor value.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonSchema"))]
    pub output_schema: Value,
    /// JSON Schema for the model-facing projection, independently of the result.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonSchema"))]
    pub projection_schema: Value,
}

impl ToolDefinition {
    /// Constructs a definition from borrowed schema literals on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments, reason = "the complete pinned tool definition")]
    pub fn new(
        name: &str,
        revision: &str,
        description: &str,
        input_schema_json: &str,
        output_schema_json: &str,
        projection_schema_json: &str,
        maximum_schema_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        crate::executor::with_history_io(Some(&io), |work| Self::validate_identity_fields(name, revision, work))?;
        for schema in [input_schema_json, output_schema_json, projection_schema_json] {
            if schema.len() as u64 > maximum_schema_bytes {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool schema exceeds its admitted byte limit",
                });
            }
        }
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let definition = Self {
            name: crate::executor::stock_string_copy(name, Some(&io))?,
            revision: crate::executor::stock_string_copy(revision, Some(&io))?,
            description: crate::executor::stock_string_copy(description, Some(&io))?,
            input_schema: crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<Value>,
                input_schema_json.as_bytes(), crate::wire::ErrorCode::Invalid)?),
            output_schema: crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<Value>,
                output_schema_json.as_bytes(), crate::wire::ErrorCode::Invalid)?),
            projection_schema: crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<Value>,
                projection_schema_json.as_bytes(), crate::wire::ErrorCode::Invalid)?),
        };
        crate::executor::with_history_io(Some(&io), |work| definition.validate_schemas(work))?;
        use crate::filesystem::UploadRecord as _;
        let retained = (std::mem::size_of::<Self>() as u64)
            .checked_add(definition.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool definition ownership overflow",
            })?;
        let custody = io.into_native_custody(retained)?;
        Ok((definition, custody))
    }

    /// Validates the name and all three independently pinned schemas.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        Self::validate_identity_fields(&self.name, &self.revision, work)?;
        self.validate_schemas(work)
    }

    fn validate_schemas(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        for schema in [
            &self.input_schema,
            &self.output_schema,
            &self.projection_schema,
        ] {
            crate::contract::compile_json_schema(schema, "tool", work)?;
        }
        Ok(())
    }

    fn validate_identity_fields(
        name: &str,
        revision: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::Work {
                bytes: 0, items: (name.len() as u64).saturating_add(revision.len() as u64).saturating_add(1),
            })?;
        }
        for (value, reason) in [(name, "tool name is invalid"), (revision, "tool revision is invalid")] {
            if !crate::contract::is_valid_component_label(value) {
                return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid, reason));
            }
        }
        if name.contains('@') || revision.contains('@') {
            return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid,
                "tool name and revision cannot contain the version separator"));
        }
        Ok(())
    }

    /// Validates and decodes the complete model-visible projection envelope.
    pub(crate) fn validate_projection(
        &self,
        value: &Value,
        limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<crate::model::ToolResultContent> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io && let Some(work) = io.original_native_work() {
            let decode = crate::filesystem::PreparationIo::from_native_work(work)?;
            crate::executor::with_history_io(Some(&decode), |work| crate::contract::validate_json_schema_value(&self.projection_schema, value, "tool projection", work))?;
            let content: crate::model::ToolResultContent =
                decode.value_decode(value, crate::wire::ErrorCode::Invalid)?;
            crate::executor::with_history_io(Some(&decode), |work| content.validate_limits(limits, work))?;
            let retained = content.logical_owned_bytes()?;
            io.absorb_native_custody(decode.into_native_custody(retained)?)?;
            return Ok(content);
        }
        crate::executor::with_history_io(io, |work| crate::contract::validate_json_schema_value(&self.projection_schema, value, "tool projection", work))?;
        let content = crate::model::ToolResultContent::deserialize(value).map_err(|error| {
            Error::Invalid(format!("tool projection envelope is invalid: {error}"))
        })?;
        crate::executor::with_history_io(io, |work| content.validate_limits(limits, work))?;
        Ok(content)
    }

    /// Immutable approval identity for the exact model-visible definition.
    pub fn digest(&self) -> Result<[u8; 32]> {
        self.validate(&mut None)?;
        crate::contract::canonical_json_digest(self)
    }
}

/// Declares an explicit JSON result envelope for the supplied value schema.
#[must_use]
pub fn json_projection_schema(value_schema: Value) -> Value {
    let properties = serde_json::Map::from_iter([
        ("kind".into(), serde_json::json!({"const":"json"})),
        ("value".into(), value_schema),
    ]);
    Value::Object(serde_json::Map::from_iter([
        ("type".into(), Value::String("object".into())),
        ("properties".into(), Value::Object(properties)),
        ("required".into(), serde_json::json!(["kind", "value"])),
        ("additionalProperties".into(), Value::Bool(false)),
    ]))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for ToolDefinition {
    fn diagnostic_label_bytes() -> u64 { tool_serde_diagnostic_label_bytes() }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = 0_u64;
        for text in [&self.name, &self.revision, &self.description] {
            bytes = bytes.checked_add(text.capacity() as u64).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool definition ownership overflow",
            })?;
        }
        for value in [&self.input_schema, &self.output_schema, &self.projection_schema] {
            bytes = bytes.checked_add(crate::contract::json_value_logical_owned_bytes(value)?)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "tool definition schema ownership overflow",
                })?;
        }
        Ok(bytes)
    }
}
