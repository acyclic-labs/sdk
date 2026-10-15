use super::*;
use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::foundation::Digest;
use crate::storage::FenceOutcome;
use crate::{AuthorityWork, CancellationToken};
use acyclic_stream::OwnedBytes;
use std::error::Error;
use std::sync::Arc;

macro_rules! provider {
    ($store:expr, create_authority($id:expr, $epoch:expr), $budget:expr) => {{
        let cancel = CancellationToken::new();
        let mut work = AuthorityWork::new($budget, &cancel);
        let result = $store.create_authority($id, $epoch, &mut work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity));
        work.receipt(result)
    }};
    ($store:expr, compare_and_append($id:expr, $epoch:expr, $head:expr, $commit:expr), $budget:expr) => {{
        let cancel = CancellationToken::new();
        let mut work = AuthorityWork::new($budget, &cancel);
        let result = $store.compare_and_append(crate::GuardedAppend {
            authority_id: $id, epoch: $epoch, expected: $head, commit: $commit, permit: crate::PublicationPermit::Unrestricted,
        }, &mut work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity));
        work.receipt(result)
    }};
    ($store:expr, $method:ident($($arg:expr),*), $budget:expr) => {{
        let cancel = CancellationToken::new();
        let mut work = AuthorityWork::new($budget, &cancel);
        let result = provider!(@call $store, $method($($arg),*), work);
        work.receipt(result)
    }};
    (@call $store:expr, replay($($arg:expr),*), $work:ident) => { $store.replay($($arg,)* &mut $work).await.map(|(value, _)| value) };
    (@call $store:expr, read_many($($arg:expr),*), $work:ident) => { $store.read_many($($arg,)* &mut $work).await.map(|(value, _)| value) };
    (@call $store:expr, $method:ident($($arg:expr),*), $work:ident) => { $store.$method($($arg,)* &mut $work).await };
}

mod authority;
#[cfg(not(target_arch = "wasm32"))]
mod native_objects;
#[cfg(not(target_arch = "wasm32"))]
mod native_publication;
mod objects;
