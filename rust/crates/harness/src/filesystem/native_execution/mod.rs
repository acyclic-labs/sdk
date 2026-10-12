//! Optional native command composition over existing task, effect and workspace owners.

pub(super) use super::root_binding::{
    allocate, boundary, encode_record, native_codec_error_custody, native_owned_bytes_failure,
    native_root_output_binding_operation,
};

use super::{
    FilesystemHost, NativeViewManifest, NativeViewOptions, NativeVolumeMapping,
    NativeVolumeNamespace,
};
use crate::{
    Error, Result,
    conversation::{
        ContentGrant, ContentPublisher, ContentResidencyVerifier, FileRef, VolumeOperation,
        VolumeRef,
    },
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

mod journal;
mod lazy_volume;
mod preparation;
mod prepare;
mod publication;
mod root_output;
mod view;
mod working_view;
pub(crate) use journal::NativePreparationCompletionTemplate;
use journal::*;
pub use lazy_volume::NativeExistingLazyVolume;
use lazy_volume::*;
use preparation::*;
pub use preparation::{
    BorrowedNativeVolumeBinding, NativePreparationReceipt, NativePreparationRoot,
    NativeRecordedPreparation, observe_preparation,
};
pub(super) use publication::reserve_box;
use publication::*;
pub use publication::{
    NativePublicationCleanup, NativePublicationFailure, NativePublicationReservationFailure,
    NativeViewPublicationError, NativeViewPublicationReceipt,
};
pub use root_output::NativeRootOutputSource;
pub use view::NativeVolumeView;
use working_view::*;
pub use working_view::{NativeProcessView, NativeTaskViewDrainReceipt, NativeTaskWorkingView};
