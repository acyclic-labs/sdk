//! Provider-neutral identities for other SDK resources.
use crate::{Error, Result, store::history::HistoryIo};
use serde::{Deserialize, Deserializer, Serialize};
mod provider;
pub use provider::ProviderRef;
mod resource;
pub use resource::{ResourceKind, ResourceRef};
mod typed;
pub use typed::*;
mod generation;
#[cfg(test)]
mod tests;
fn validate_text<'a>(
    parts: impl IntoIterator<Item = &'a str>,
    work: &mut Option<&mut dyn HistoryIo>,
    reason: &'static str,
) -> Result<()> {
    for part in parts {
        for ch in std::iter::once(None).chain(part.chars().map(Some)) {
            if let Some(io) = work.as_deref_mut() {
                io.check_cancellation()?;
                io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if part.is_empty() || ch.is_some_and(char::is_control) {
                return Err(invalid(reason));
            }
        }
    }
    Ok(())
}
fn capacity_sum(parts: impl IntoIterator<Item = usize>) -> Result<u64> {
    parts.into_iter().try_fold(0_u64, |sum, n| {
        sum.checked_add(n as u64)
            .ok_or_else(|| invalid("resource ownership overflows"))
    })
}
fn invalid(reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        }
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        Error::Invalid(reason.into())
    }
}
