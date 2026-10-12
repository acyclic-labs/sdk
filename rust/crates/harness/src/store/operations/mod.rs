//! Atomic operation and message indexes over canonical events.

use crate::{
    Error, OperationId, Result,
    core::{Authority, AuthorityVerifier, Event},
};

use acyclic_stream::{
    CommitCondition, CommitMutation, CommitRequest, CommittedEnvelope, CommittedMutation,
    IdempotencyKey, OwnedBytes, Record, StreamPath, StreamProvider,
};

use serde::{Deserialize, Serialize};


mod codec;
pub(crate) use codec::*;
mod paths;
pub(crate) use paths::*;
mod publication;
pub(crate) use publication::*;
mod sdk;
pub(crate) use sdk::*;
mod lookup;
pub(crate) use lookup::*;
#[cfg(all(test, feature = "native-execution", not(target_arch = "wasm32")))]
mod native_only_provider_tests;
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_publication_custody_tests;
