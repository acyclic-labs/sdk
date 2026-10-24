use super::*;

impl ToolDefinition {
    #[cfg(feature = "filesystem")]
    pub(super) fn check_scoped_projection(
        &self,
        value: &Value,
        limits: crate::conversation::Limits,
        original: Option<&crate::executor::PreparationIo>,
    ) -> Result<Result<crate::model::ToolResultContent>> {
        // All fallible preparation lives in a private pool on the original
        // ledger. Declare it before the result so failure destroys the result
        // before releasing its allocation, on finite and native paths alike.
        let scope = crate::filesystem::PreparationScope::new(original)?;
        let io = scope.io();
        if let Err(violation) = crate::executor::with_history_io(io, |work| {
            crate::executor::check_json_schema_value(
                &self.projection_schema,
                value,
                "tool projection",
                work,
            )
        })? {
            return Ok(Err(violation));
        }
        let content = decode_projection(value, io)?;
        crate::executor::with_history_io(io, |work| content.validate_limits(limits, work))?;
        scope.finish(content.logical_owned_bytes()?)?;
        Ok(Ok(content))
    }
}

#[cfg(feature = "filesystem")]
fn decode_projection(
    value: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<crate::model::ToolResultContent> {
    let Some(io) = io else {
        return crate::model::ToolResultContent::deserialize(value).map_err(|error| {
            Error::Invalid(format!("tool projection envelope is invalid: {error}"))
        });
    };
    #[cfg(not(target_arch = "wasm32"))]
    if io.original_native_work().is_some() {
        return io.value_decode(value, crate::wire::ErrorCode::Invalid);
    }
    let _bytes_hold;
    let bytes = io.encode(value, io.remaining()?.bytes_encoded, false)?;
    _bytes_hold = io.guard_bytes(&bytes)?;
    crate::filesystem::retain_decoded(io.decode(
        std::marker::PhantomData::<crate::model::ToolResultContent>,
        &bytes,
        crate::wire::ErrorCode::Invalid,
    )?)
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests;
