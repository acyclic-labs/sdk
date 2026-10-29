//! Deterministic, host-neutral durable substrate semantics.

use crate::store::history::HistoryIo;
use crate::wire_codec::CodecQuota;
use crate::{
    AgentId, Capabilities, Error, Result,
    conversation::{FileRef, VolumeOperation, VolumeOwner, is_internal_path},
};
#[cfg(test)]
use crate::{
    conversation::{ConversationMessage, ModelContextSelection},
    interaction::{InteractionResolution, InteractionTicket},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

mod authority;
mod capability_bytes;
mod copying;
#[cfg(test)]
mod element_sizes;
mod extension_types;
mod filesystem_access;
pub(crate) use filesystem_access::{
    ReadGrant, directory_read_granted_with_work, file_read_grant, filesystem_scope_label,
    volume_write_granted_with_work,
};
pub(crate) mod ownership;
mod records;
mod registry;
pub(crate) use authority::VerifiedScope;
#[cfg(feature = "filesystem")]
pub(crate) use authority::until_turn_stopped;
#[cfg(feature = "filesystem")]
pub(crate) use copying::copy_resource;
pub(crate) use copying::{
    copy, copy_boxed, copy_boxed_text, copy_copy_vector, copy_provider, copy_text, copy_vector,
    reserve_copy_vector,
};
pub(crate) use ownership::inspect_metadata;
pub(crate) use ownership::{OwnedBytes, owned_buffer, owned_sum};

pub use authority::{
    AUTHORITY_ID_FORBIDDEN_EXACT, AUTHORITY_ID_FORBIDDEN_SEPARATORS, AdmittedAuthority,
    AggregateKind, Authority, AuthorityIssuer, AuthorityOperations, AuthorityRoot,
    AuthorityVerifier, CheckpointKind, RuntimeAuthority, Scope, ScopeDerivation,
};

pub use records::EventReference;

pub use extension_types::{
    ExtensionAdmission, ExtensionConfiguration, ExtensionDependency, ExtensionForkPolicy,
    ExtensionRecord, ExtensionStateMigration,
};

pub use registry::SchemaRegistry;

pub(crate) use extension_types::validate_extension_name;
