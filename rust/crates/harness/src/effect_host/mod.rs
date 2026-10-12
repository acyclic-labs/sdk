//! Conversation-owned effect admission, dispatch and reconciliation.

use crate::contract::capability;
use crate::{
    EffectId, Error, IdempotencyKey, OperationId, Result,
    conversation::ContentResidencyVerifier,
    core::{
        Action, Authority, AuthorityIssuer, Command, EffectAttestation, EffectState, EffectStatus,
        SchemaRegistry, Scope,
    },
    effects::EffectRegistry,
    runtime::DurableEffectObserver,
    store::{
        DEFAULT_TERMINAL_EFFECTS, HistoryReadLimits, StreamAggregate,
        default_projection_read_limits,
    },
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::StreamProvider;
use std::sync::Arc;

mod plan;
pub use plan::TaskEffectPlan;
#[cfg(feature = "filesystem")]
pub use plan::task_effect_id;
mod host;
pub use host::ConversationEffectHost;
pub(crate) use host::effect_error;
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests;
