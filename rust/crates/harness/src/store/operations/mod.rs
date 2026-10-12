//! Atomic operation and message indexes over canonical events.

use crate::{
    Error, OperationId, Result,
    core::{Authority, AuthorityVerifier, Event},
};

use acyclic_stream::{
    CommitCondition, CommittedEnvelope, CommittedMutation, IdempotencyKey, OwnedBytes, Record,
    StreamPath, StreamProvider,
};

#[cfg(test)]
use acyclic_stream::{CommitMutation, CommitRequest};

use serde::{Deserialize, Serialize};

use crate::distributed::{SdkCallError, sdk_error, sdk_source};

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
