//! Conversation-owned effect admission and reconciliation.
#[cfg(feature = "filesystem")]
use crate::store::HistoryWork;
use crate::{
    EffectId, Error, IdempotencyKey, NativeCustody, NativeWork, OperationId, Result,
    conversation::ContentResidencyVerifier,
    core::{
        Action, Authority, AuthorityIssuer, Command, EffectAttestation, EffectState, EffectStatus,
        SchemaRegistry, Scope,
    },
    effects::EffectRegistry,
    runtime::DurableEffectObserver,
    store::{
        DEFAULT_TERMINAL_EFFECTS, StreamAggregate, default_projection_read_limits,
        guarded_boundary as boundary, history::HistoryIo,
    },
    wire_codec::CodecQuota,
};
use crate::{
    contract::capability,
    wire::ErrorCode::{Conflict, Invalid, NotFound, Unauthorized},
};
use acyclic_stream::StreamProvider;
use std::sync::Arc;
mod plan;
pub use plan::TaskEffectPlan;
#[cfg(feature = "filesystem")]
pub use plan::task_effect_id;
mod host;
pub use host::ConversationEffectHost;
#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod tests;
#[cfg(feature = "filesystem")]
fn history(work: NativeWork<'_>) -> Result<Option<HistoryWork>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        work.map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()
            .map(|io| io.map(HistoryWork::new))
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        if work.is_some() {
            return Err(Error::Invalid("native effects are unavailable".into()));
        }
        Ok(None)
    }
}
