//! Optional native command composition over existing task, effect and workspace owners.

pub(super) use super::root_binding::{
    allocate, boundary, encode_record, native_codec_error_custody, native_owned_bytes_failure,
    native_root_output_binding_operation,
};

use super::{
    FilesystemHost, NativeVolumeNamespace, NativeViewManifest, NativeViewOptions,
    NativeVolumeMapping,
};
use crate::{
    Error, Result,
    conversation::{ContentGrant, ContentPublisher, ContentResidencyVerifier, FileRef,
        VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    durable_host::TaskJournalOwner,
    resources::GenerationRef,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, MaterializationJournal, MaterializationJournalStore,
    MaterializationPhase, NativeMaterializationWork, NativeWorkingSet, PublicationPermit,
};
use acyclic_stream::StreamProvider;
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};



mod root_output;
mod working_view;
mod publication;
mod lazy_volume;
mod preparation;
mod prepare;
mod view;
mod journal;
use root_output::*;
use working_view::*;
use publication::*;
use lazy_volume::*;
use preparation::*;
use journal::*;
pub use root_output::NativeRootOutputSource;
pub use working_view::{NativeTaskViewDrainReceipt, NativeTaskWorkingView, NativeProcessView};
pub use publication::{NativePublicationCleanup, NativePublicationReservationFailure,
    NativePublicationFailure, NativeViewPublicationError, NativeViewPublicationReceipt};
pub use lazy_volume::NativeExistingLazyVolume;
pub use preparation::{BorrowedNativeVolumeBinding, NativePreparationRoot,
    NativePreparationReceipt, NativeRecordedPreparation, observe_preparation};
pub use view::NativeVolumeView;
pub use journal::copy_native_mapping_with_work;
pub(crate) use journal::NativePreparationCompletionTemplate;
pub(super) use publication::reserve_box;
