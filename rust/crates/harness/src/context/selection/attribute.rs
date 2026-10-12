use super::*;

/// An authoritative attribute state, shared by prompt rebuilds and updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ContextAttribute {
    /// Registered, namespaced application type (roles are ordinary attributes).
    pub type_name: String,
    /// Exact type/renderer definition revision.
    pub type_revision: String,
    /// Schema of this attribute type.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonSchema"))]
    pub schema: Value,
    /// Exact authoritative state revision.
    pub state_revision: String,
    /// State satisfying the registered schema.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonValue"))]
    pub value: Value,
}

impl ContextAttribute {
    /// Captures a native typed value without changing its authoritative state.
    pub fn typed<T: Serialize>(
        type_name: String,
        type_revision: String,
        schema: Value,
        state_revision: String,
        value: &T,
    ) -> Result<Self> {
        let attribute = Self {
            type_name,
            type_revision,
            schema,
            state_revision,
            value: serde_json::to_value(value)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        attribute.validate(&mut None)?;
        Ok(attribute)
    }

    /// Validates type identity, revision and state using the existing schema boundary.
    pub fn validate(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        self.preflight(work)?;
        self.validate_schema(work)
    }
    pub(super) fn preflight(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        for label in [&self.type_name, &self.type_revision, &self.state_revision] {
            if !crate::is_valid_component_label(label) {
                return Err(boundary(work, "context attribute identity is invalid"));
            }
        }
        observe_value(&self.schema, work)?;
        observe_value(&self.value, work)
    }
    pub(super) fn validate_schema(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        crate::contract::validate_trusted_json_byte_bound(self, u64::MAX, work)?;
        crate::contract::validate_json_schema_value_with_work(
            &self.schema,
            &self.value,
            "context attribute",
            work,
        )
    }
}
