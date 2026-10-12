use super::*;

/// A pinned input; a file reference conveys no read authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextSourceValue {
    /// History, instructions, skills, catalogs or resources in immutable storage.
    File {
        /// Exact generation and descriptor.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        file: FileRef,
    },
    /// Schema-validated application state.
    Attribute {
        /// Immutable typed state record.
        attribute: ContextAttribute,
    },
}

/// A declared representation; summary creation remains an admitted model operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextRepresentation {
    /// Render the selected content.
    Full,
    /// Render a separately supplied, pinned summary.
    Summary,
    /// Render an explicit reference.
    Reference,
}

/// Explicit bounded selection from a pinned source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextExtent {
    /// The complete source.
    Whole,
    /// Half-open byte span of a file. Renderers define decoding semantics.
    Span {
        /// Inclusive start.
        start: u64,
        /// Exclusive end.
        end: u64,
    },
}

/// Host-approved selection and representation. Model suggestions need host policy approval.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ContextSelection {
    /// Pinned authoritative input.
    pub source: ContextSourceValue,
    /// Exact selected extent.
    pub extent: ContextExtent,
    /// Requested representation.
    pub representation: ContextRepresentation,
}

impl ContextSelection {
    /// Validates structure without granting authority or executing a renderer.
    pub fn validate(&self, limits: Limits, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        limits.validate(work)?;
        match &self.source {
            ContextSourceValue::File { file } => {
                limits.validate_file(file, work)?;
                if let ContextExtent::Span { start, end } = self.extent
                    && (start >= end || end > file.descriptor().byte_length())
                {
                    return Err(boundary(work, "context span is outside pinned file"));
                }
            }
            ContextSourceValue::Attribute { attribute } => {
                attribute.preflight(work)?;
                if self.extent != ContextExtent::Whole {
                    return Err(boundary(work, "attributes require whole selection"));
                }
            }
        }
        crate::contract::validate_trusted_json_byte_bound(self, limits.render_bytes, work)?;
        if let ContextSourceValue::Attribute { attribute } = &self.source {
            attribute.validate_schema(work)?;
        }
        Ok(())
    }
}
