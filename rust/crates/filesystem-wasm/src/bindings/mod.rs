macro_rules! unbounded_future {
    ($owner:expr, $operation:ident($($argument:expr),* $(,)?)) => {
        ($owner).$operation(
            $($argument,)*
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
    };
}

macro_rules! sdk_value {
    ($future:expr) => {
        ($future)
            .await
            .map_err(|failure| js_error(failure.error))
            .map(|receipt| receipt.value)
    };
}

macro_rules! browser_call {
    ($owner:ident, $operation:ident($($argument:expr),* $(,)?)) => {{
        $owner.engine.$operation(
            $($argument,)*
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &CancellationToken::default(),
        ).await.map_err(js_error)
    }};
}

macro_rules! compat_json {
    ($name:ident, $codec:ident, $js:ident, $doc:literal) => {
        #[doc = $doc]
        #[wasm_bindgen(js_name = $js)]
        pub fn $name(value_json: &str) -> Result<String, JsValue> {
            compat_wire::$codec(value_json).map_err(wire_error)
        }
    };
}

mod authored;
pub use authored::*;
mod checkout;
pub use checkout::*;
mod context;
pub use context::*;
mod directory;
mod engine;
pub use engine::*;
mod extents;
mod git;
pub use git::*;
mod history;
pub use history::*;
mod merge;
pub use merge::*;
mod metadata;
mod namespace;
mod options;
pub use options::*;
mod publication;
pub use publication::*;
mod read;
pub use read::*;
mod records;
pub use records::*;
mod resolved;
pub use resolved::*;
mod speculation;
pub use speculation::*;
#[cfg(test)]
mod tests;
mod transaction;
pub use transaction::*;
mod transfer;
pub use transfer::*;
mod volume;
pub use volume::*;
mod wire;
pub use wire::*;
mod workspace;
pub use workspace::*;
mod backend;
mod operation_windows;
mod write;

use super::{IndexedDbAuthorityStore, IndexedDbObjectStore, OpfsAcceleratedObjectStore};
use acyclic_fs::compat_wire;
use acyclic_fs::kernel::{
    AttributeClass, AttributeName, DecodeLimits, ExtentKind, ExtentSeekTarget, FileKind,
    FileMetadata, FilePayload, FilePayloadKind, FileRecord, LogicalName, NameEncoding,
    NamespacePath, RebaseDecision, TransferCursor, TreeEntry, decode_file_metadata,
    encode_file_metadata,
};
use acyclic_fs::model::{
    AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode, FilesystemProfile,
    GenerationSelector, Lifecycle, MutationMode, UnicodePolicy, VolumeConfig, VolumeConfigError,
    VolumeLimits,
};
use acyclic_fs::path::PortablePath;
use acyclic_fs::workspace_context_wire;
use acyclic_fs::{
    ApplyOptions, AuthoredMutation, ByteRange, CachedObjectStore, CancellationToken, ChangeSet,
    Checkout, LivePublicationObservation, DependencyRegion, DependencyState, DependencyUse, Digest,
    DirectoryPage, EmbeddedCapabilities, FileCloneRequest, FileId, ForkOptions, Fs, Generation,
    GenerationExportManifest, GitCommand, GitCompatRepository, IdempotencyKey, JoinHistory,
    JoinOutcome, JoinPlan, LiveMutationOutcome, LogicalObjectStore, MemoryGitCompatStore,
    MemoryWorkspaceContextStore, MergeConflict, MergePreparation, NamedAttributeWriteMode,
    ObjectCacheOptions, ObjectId, ObjectReadRequest, ObjectResidency, OperationId,
    PromotionAdmission, PromotionDestination, PromotionSpeculatorOptions, RebaseConflict,
    ResidencyAdmission, ResidencyHint, ResidencyReason, ResidencySpeculatorOptions, ResolvedFile,
    SpeculationController, SpeculationOptions, StorageLocationId, StorageTier,
    StreamAuthorityStore, Transaction, TransactionCommit, TransactionRebase, Volume, VolumeId,
    Workspace, WorkspaceContextId, WorkspaceContextRegistry, WorkspaceDelete, WorkspaceExtentKind,
    WorkspaceExtentPlan, WorkspaceId, WorkspaceMetadata, WorkspaceRebase, WorkspaceRootId,
    WorkspaceStat, canonicalize_git_output_json, canonicalize_git_pending_transition_json,
    decode_generation_export_manifest, encode_generation_export_manifest, parse_git_public_command,
};
use backend::{BrowserAuthority, BrowserObjects};
pub use operation_windows::BrowserOperationWindowCoordinator;
use operation_windows::browser_publication_permit;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tsify::{Ts, Tsify};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
