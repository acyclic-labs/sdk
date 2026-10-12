//! Deterministic, host-neutral durable substrate semantics.

use crate::contract::capability;
use crate::contract::next_revision;
use crate::store::history::HistoryIo;
use crate::{
    AgentId, Capabilities, EffectAttemptId, EffectId, Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentGrant, ConversationMessage, ConversationState, FileRef, ModelContextSelection,
        VolumeOperation, VolumeOwner, VolumeRef, is_internal_path,
    },
    fork::{ForkSeed, InheritedConversationPrefix},
    interaction::{InteractionOutcome, InteractionResolution, InteractionTicket},
    merge::ProjectMergeReceipt,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

mod authority;
mod copying;
mod effects;
mod extension_types;
mod ownership;
mod records;
mod reducer;
mod registry;
mod snapshot;
#[cfg(test)]
mod tests;
pub(in crate::core) use crate::fork::copy_seed;
pub(crate) use copying::{
    copy_authority, copy_boxed, copy_capabilities, copy_configuration, copy_copy_vector,
    copy_effect, copy_event, copy_interaction_outcome, copy_owned, copy_provider, copy_reference,
    copy_resolution, copy_resource, copy_text, copy_ticket, copy_value, copy_vector,
    reserve_copy_vector,
};
pub(crate) use ownership::inspect_metadata;
#[cfg(feature = "filesystem")]
pub(crate) use ownership::resource_creation_clone_reservation;
pub(in crate::core) use ownership::*;
pub(crate) use ownership::{event_owned_bytes, owned_buffer, owned_bytes, owned_sum, owned_vector};
pub use snapshot::Snapshot;

pub use authority::{
    AUTHORITY_ID_FORBIDDEN_EXACT, AUTHORITY_ID_FORBIDDEN_SEPARATORS, AggregateKind, Authority,
    AuthorityIssuer, AuthorityVerifier, Scope,
};

pub use effects::{EffectAttestation, EffectGuarantee, EffectState, EffectStatus};

pub use records::{
    Action, ApplyResult, Command, Event, EventPayload, EventReference, LifecycleState,
    RecordedScope,
};

pub use extension_types::{
    ExtensionAdmission, ExtensionConfiguration, ExtensionDependency, ExtensionForkPolicy,
    ExtensionRecord, ExtensionStateMigration,
};

pub use registry::SchemaRegistry;

pub(in crate::core) use effects::{
    effect_request_digest_with_work, validate_effect_dispatch, validate_effect_observation,
};
pub(crate) use effects::{event_effect_id, settled_effect};
pub(crate) use extension_types::validate_extension_name;
pub(in crate::core) use records::{
    Subject, require_capability, validate_causal_parent_with_work, validate_interaction_operation,
    validate_interaction_transition, validate_lifecycle,
};
pub use reducer::Reducer;
pub(in crate::core) use registry::ExtensionBinding;

pub(in crate::core) use crate::contract::json_value_logical_owned_bytes;
