//! Pinned sources and bounded context rendering.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use super::context_contract_native;
use super::{
    Context, ContextInput, ContextMessages, ContextPlacement, ContextStage,
    append_context_messages, copy_context, restrict_context_limits, validate_projected_context,
};
use crate::{
    Error, Result,
    conversation::{ContentResidencyVerifier, FileRef, Limits},
    executor::PreparationIo,
    model::ModelMessage,
    store::history::HistoryIo,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
mod attribute;
pub use attribute::ContextAttribute;
mod input;
pub use input::*;
mod renderer;
pub use renderer::*;
mod composition;
pub use composition::apply_context_projection;
pub(super) use composition::{place_context, place_snapshot};
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests;
fn has_native_work(work: &Option<&mut dyn HistoryIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        work.as_deref()
            .is_some_and(|io| io.original_native_work().is_some())
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = work;
        false
    }
}
fn boundary(work: &Option<&mut dyn HistoryIo>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if has_native_work(work) {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    Error::Invalid(reason.into())
}
fn observe_value(value: &Value, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    if !has_native_work(work) {
        return Ok(());
    }
    let io = work.as_deref_mut().expect("native selection observer");
    let remaining = io.remaining()?;
    crate::contract::json_owned_value_shape_bounded(
        value,
        crate::contract::JsonBounds {
            maximum_bytes: remaining.bytes_encoded,
            maximum_items: remaining.items_examined,
            maximum_depth: 256,
        },
        crate::contract::JsonValueSchema::None,
        &mut |charge| io.codec_charge(charge),
    )?;
    Ok(())
}
