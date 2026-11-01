//! Pinned sources and bounded context rendering.
use super::{
    Context, ContextMessages, ContextPlacement, append_context_messages, copy_context,
    validate_projected_context,
};
use crate::{
    Error, Result,
    conversation::{FileRef, Limits},
    executor::PreparationIo,
    model::ModelMessage,
    store::history::HistoryIo,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
mod attribute;
pub use attribute::ContextAttribute;
mod input;
pub use input::*;
mod renderer;
pub use renderer::*;
mod composition;
pub use composition::apply_context_projection;
pub(super) use composition::{place_context, place_snapshot};
#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod tests;
fn boundary(work: &Option<&mut dyn HistoryIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    Error::Invalid(reason.into())
}
fn observe_value(value: &Value, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = work
        .as_deref_mut()
        .filter(|io| io.original_native_work().is_some())
    {
        let remaining = io.remaining()?;
        crate::executor::json::json_owned_value_shape_bounded(
            value,
            crate::executor::json::JsonBounds {
                maximum_bytes: Some(remaining.bytes_encoded),
                maximum_items: Some(remaining.items_examined),
                maximum_depth: 256,
            },
            crate::executor::json::JsonValueSchema::None,
            &mut |charge| io.codec_charge(charge),
        )?;
    }
    let _ = (value, work);
    Ok(())
}
