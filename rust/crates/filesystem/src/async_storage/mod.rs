//! Runtime-neutral asynchronous storage contracts for browser and remote use.

use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Epoch, GenerationId, Head, OperationId, ProposedCommit, Sequence,
};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, CreateAuthorityOutcome, FenceOutcome,
    GuardedAppend, ObjectId, ObjectRead, ObjectReadRequest, ObjectResult, ObjectWrite,
    PublicationPermit, PublicationReservation, ReplayLimit, ReservationOutcome,
};
use acyclic_stream::{OwnedBytes, ProviderPlatform, ProviderTask};
#[cfg(target_arch = "wasm32")]
use crate::storage::AuthorityResult;
use std::any::{Any, TypeId};
use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;

mod cache;
pub(crate) use cache::observe_read;
pub use cache::{DecodedCacheKey, DecodedCacheValue, DecodedCacheAdmission};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use cache::{native_read_batch, owned_read};
mod fork;
pub use fork::{GenerationFork, GenerationForkSource, WorkspaceForkCommit, WorkspaceForkOutcome};
pub(crate) use fork::{seal, workspace_fork, first_record};
mod work;
pub use work::{AuthorityWork, AuthorityError, AuthorityGuard, AuthorityAllocation};
#[cfg(not(target_arch = "wasm32"))]
pub use work::NativeAuthorityGuard;
mod authority;
pub use authority::AsyncAuthorityStore;
mod objects;
pub use objects::{AsyncObjectStore, PublicationScope};
mod read_batch;
pub use read_batch::read_many_sequential_async;
#[cfg(not(target_arch = "wasm32"))]
pub use read_batch::read_many_sequential_with_native_work;
pub(crate) use read_batch::{merge_completed_object_read, object_error};

#[cfg(all(test, feature = "memory"))]
#[path = "../tests/async_storage.rs"]
mod tests;
