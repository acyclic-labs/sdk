use super::*;
use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::foundation::Digest;
use crate::storage::FenceOutcome;
use crate::{AuthorityWork, CancellationToken};
use acyclic_stream::OwnedBytes;
use std::error::Error;
use std::sync::Arc;

#[macro_use]
#[path = "../../../tests/support/provider_receipt.rs"]
mod provider_receipt;

mod authority;
#[cfg(not(target_arch = "wasm32"))]
mod native_objects;
#[cfg(not(target_arch = "wasm32"))]
mod native_publication;
mod objects;
