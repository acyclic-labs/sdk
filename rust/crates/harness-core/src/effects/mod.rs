//! Replaceable external effect providers with explicit guarantees and reconciliation.

use crate::{
    EffectAttemptId, EffectId, Error, Result,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{AuthorityIssuer, EffectAttestation},
    core::{EffectGuarantee, EffectState, EffectStatus},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

mod records;
pub use records::{EffectDispatch, EffectObservation};
mod provider;
pub use provider::EffectProvider;
mod catalog;
pub use catalog::EffectRegistry;
mod validation;
pub(crate) use validation::{validate_result_bytes, validate_schema_bytes};
mod attestation;
#[cfg(test)]
mod tests;
