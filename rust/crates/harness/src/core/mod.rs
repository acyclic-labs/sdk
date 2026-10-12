//! Deterministic, host-neutral durable substrate semantics.

use crate::contract::capability;
use crate::contract::next_revision;
use crate::store::history::HistoryIo;
use crate::{
    AgentId, Capabilities, EffectAttemptId, EffectId, Error, IdempotencyKey, OperationId,
    Result,
    conversation::{
        ContentGrant, ConversationMessage, ConversationState, FileRef,
        ModelContextSelection, VolumeOperation, VolumeOwner, VolumeRef, is_internal_path,
    },
    fork::{ForkSeed, InheritedConversationPrefix},
    interaction::{InteractionOutcome, InteractionResolution, InteractionTicket},
    merge::ProjectMergeReceipt,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

mod snapshot;
mod authority;
mod effects;
mod records;
mod extension_types;
mod registry;
mod reducer;
#[cfg(test)]
mod tests;
mod inventory;
mod resource_metadata;
mod native_custody;
mod native_key;
mod copying;
mod ownership;
use native_custody::NativeComponentCustody;
use native_key::NativeComponentKey;
pub(in crate::core) use crate::fork::copy_seed;
pub use snapshot::Snapshot;
pub(crate) use inventory::{
    source_inventory,
    source_metadata_inspection_with_work,
};
pub(crate) use resource_metadata::{
    resource_preparation_owned_bytes,
};
#[cfg(feature = "filesystem")]
pub(crate) use resource_metadata::resource_creation_clone_reservation;
pub(crate) use copying::{
    copy_owned, copy_iter, copy_copy_vector, reserve_copy_vector,
    copy_text, copy_authority, copy_provider, copy_file, copy_resource, copy_vector,
    copy_boxed, copy_configuration, copy_value, copy_capabilities, copy_reference,
    copy_effect, copy_resolution, copy_ticket, copy_interaction_outcome, copy_event,
};
pub(crate) use ownership::{owned_revision, owned_capture, owned_seed, owned_sum,
    owned_vector, owned_buffer, event_owned_bytes, value_owned_bytes};
pub(in crate::core) use copying::*;
pub(in crate::core) use ownership::*;

pub use authority::{AggregateKind, AUTHORITY_ID_FORBIDDEN_EXACT, AUTHORITY_ID_FORBIDDEN_SEPARATORS, Authority, Scope, AuthorityIssuer, AuthorityVerifier};

pub use effects::{EffectGuarantee, EffectStatus, EffectAttestation, EffectState};

pub use records::{LifecycleState, EventPayload, EventReference, RecordedScope, Event, Command, Action, ApplyResult};

pub use extension_types::{ExtensionForkPolicy, ExtensionRecord, ExtensionStateMigration, ExtensionDependency, ExtensionConfiguration, ExtensionAdmission};

pub use registry::{SchemaRegistry};

pub use reducer::{Reducer};
pub(crate) use effects::{settled_effect, event_effect_id};
pub(in crate::core) use effects::{effect_request_digest_with_work, validate_effect_dispatch, validate_effect_observation};
pub(crate) use extension_types::validate_extension_name;
pub(in crate::core) use registry::{ExtensionBinding};
pub(in crate::core) use records::{Subject, validate_interaction_transition, validate_interaction_operation, require_capability, validate_causal_parent_with_work, validate_lifecycle};
