//! Final-output projection using the executor's shared JSON Schema validator.
use crate::{Error, Result};
use std::sync::Arc;

/// An admitted final-output schema. Admission compiles once; every output must
/// parse and validate before it can be exported as a successful structured value.
/// Provider adapters may use the same schema for native constrained generation.
#[derive(Clone)]
pub struct OutputSchema {
    schema: serde_json::Value,
    validator: Arc<jsonschema::Validator>,
}

impl OutputSchema {
    /// Admit a user-supplied schema before any model work is sent.
    pub fn new(schema: serde_json::Value) -> Result<Self> {
        let validator = crate::executor::compile_json_schema(&schema, "final output", &mut None)?;
        Ok(Self { schema, validator })
    }

    /// Provider-neutral schema, unchanged from admission.
    pub fn schema(&self) -> &serde_json::Value {
        &self.schema
    }

    /// Parse one complete final artifact, then validate it without coercion,
    /// fenced-code stripping or repair. Failure never produces a success value.
    pub fn project(&self, text: &str) -> Result<serde_json::Value> {
        let value = serde_json::from_str(text)
            .map_err(|error| Error::Invalid(format!("final output is not JSON: {error}")))?;
        self.validator.validate(&value).map_err(|error| {
            Error::Invalid(format!("final output failed schema validation: {error}"))
        })?;
        Ok(value)
    }
}
