//! Deterministic, host-neutral durable substrate semantics.

use crate::store::history::HistoryIo;
use crate::wire_codec::CodecQuota;
use crate::{
    AgentId, Capabilities, Error, Result,
    conversation::{FileRef, VolumeOperation, VolumeOwner},
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

/// Truncate a finite nonnegative float to a count, returning None on overflow.
pub(crate) fn truncated_u64(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let bits = value.to_bits();
    let exponent = (bits >> 52) & 0x7ff;
    let Some(power) = exponent.checked_sub(1023) else {
        return Some(0);
    };
    // The encoded fraction omits the leading bit of a normal number.
    let significand = (1 << 52) | (bits & ((1 << 52) - 1));
    match power {
        0..=52 => Some(significand >> (52 - power)),
        53..=63 => Some(significand << (power - 52)),
        _ => None,
    }
}

mod authority;
mod copying;
#[cfg(test)]
mod element_sizes;
mod extension_types;
mod filesystem_access;
pub use filesystem_access::volume_operation_granted_with_work;
pub(crate) use filesystem_access::{
    ReadGrant, contains_content_grant_with_work, directory_read_granted_with_work, file_read_grant,
    filesystem_scope_label, journal_input_requires_selection, journal_turn_input_directory,
    structured_grant,
};
pub(crate) mod ownership;
mod records;
mod registry;
pub(crate) use authority::VerifiedScope;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use authority::check_filesystem_refinement;
pub(crate) use authority::until_turn_stopped;
pub(crate) use copying::copy_resource;
pub(crate) use copying::{
    copy, copy_admitted, copy_boxed, copy_boxed_text, copy_copy_vector, copy_provider, copy_text,
    copy_vector, reserve_copy_vector,
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
