//! Owner-authenticated resource preparation and original allocation access.
mod access;
#[cfg(feature = "filesystem")]
mod codec;
mod creation;
mod helpers;
mod provider;
mod request;
mod root_bootstrap;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::durable_host::TaskVolumeSnapshot;
#[cfg(feature = "filesystem")]
use crate::runtime::TaskAdmissionRecord;
use crate::scheduler::diagnostic;
use crate::wire::ErrorCode::Invalid;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode::{Unauthorized, Unsupported};
use crate::{Capabilities, Error, OperationId, Result, TaskId};
use crate::{
    core::Authority,
    resources::{ProviderRef, ResourceRef},
    store::HistoryIo,
};
pub use access::CreatedResourceAccess;
pub(crate) use creation::CreatedTaskLease;
pub use creation::ResourceCreationOrigin;
#[cfg(feature = "filesystem")]
pub use creation::TaskResourcePreparation;
pub use helpers::created_resource_capability;
use helpers::*;
#[cfg(feature = "filesystem")]
pub use provider::ChildScratchCreationProof;
pub use provider::ResourceCreationProofRef;
#[cfg(feature = "filesystem")]
pub(crate) use provider::ScratchCreationIdentity;
pub use provider::{CreatedContentBinding, CreatedResourceReceipt, ResourceCreationProvider};
pub use request::ResourcePreparationRequest;
#[cfg(feature = "filesystem")]
pub use root_bootstrap::RootBootstrapContentContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;
