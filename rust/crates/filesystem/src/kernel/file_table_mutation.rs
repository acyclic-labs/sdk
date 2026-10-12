//! Sparse persistent path-independent file-table mutation.

use super::allocation::AllocationError;
use super::codec::DecodedPageShape;
use super::file_table::{file_record_encoded_length, file_table_page_decode_shape};
use super::persistent_btree::{self, Child, Format, Page, PageRef};
use super::{
    CanonicalDecodeError, DecodeLimits, FileRecord,
};
use crate::foundation::FileId;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};
use thiserror::Error;

pub(crate) struct FileTableFormat;

impl Format for FileTableFormat {
    type Key = FileId;
    type Value = FileRecord;

    fn kind() -> ObjectKind {
        ObjectKind::FileTablePage
    }

    fn key(value: &Self::Value) -> &Self::Key {
        &value.file_id
    }

    fn decode<E: From<CanonicalDecodeError>>(
        bytes: &[u8],
        limits: DecodeLimits,
        observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Page<Self>, E> {
        Ok(match super::file_table::decode_file_table_page(bytes, limits, observe)? {
            super::FileTablePage::Leaf(values) => Page::Leaf(values),
            super::FileTablePage::Internal(children) => Page::Internal(children),
        })
    }

    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError> {
        file_table_page_decode_shape(bytes, limits)
    }

    fn encode_with_accounting(
        page: &PageRef<'_, Self>, maximum_items: u32,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
        match page {
            PageRef::Leaf(records) => crate::kernel::codec::encode_owned(accounting, |observe, reserve| super::file_table::encode_file_table_leaf_records(records, maximum_items, None, observe, reserve)),
            PageRef::Internal(children) => crate::kernel::codec::encode_owned(accounting, |observe, reserve| super::file_table::encode_file_table_internal_children(children.iter().map(|child| (child.first, child.page)), maximum_items, observe, reserve)),
        }
    }

    fn page_encoded_length(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
    ) -> Result<usize, CanonicalDecodeError> {
        let count = match page {
            PageRef::Leaf(records) => records.len(),
            PageRef::Internal(children) => children.len(),
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(CanonicalDecodeError::LengthOverflow);
        }
        let mut bytes = 8_usize + 2 + 1 + 4;
        match page {
            PageRef::Leaf(records) => {
                for record in *records {
                    bytes = bytes
                        .checked_add(Self::leaf_item_encoded_length(record)?)
                        .ok_or(CanonicalDecodeError::LengthOverflow)?;
                }
            }
            PageRef::Internal(children) => {
                for child in *children {
                    bytes = bytes
                        .checked_add(Self::internal_item_encoded_length(&child.first)?)
                        .ok_or(CanonicalDecodeError::LengthOverflow)?;
                }
            }
        }
        Ok(bytes)
    }

    fn leaf_item_encoded_length(value: &Self::Value) -> Result<usize, CanonicalDecodeError> {
        file_record_encoded_length(*value)
    }

    fn internal_item_encoded_length(_key: &Self::Key) -> Result<usize, CanonicalDecodeError> {
        Ok(16 + 32)
    }

    fn key_nested_bytes(_key: &Self::Key) -> u64 {
        0
    }

    fn value_nested_bytes(_value: &Self::Value) -> u64 {
        0
    }

    fn key_owned_bytes(_key: &Self::Key) -> u64 { 0 }

    fn value_owned_bytes(_value: &Self::Value) -> u64 { 0 }

    fn try_clone_key<E: From<CanonicalDecodeError>>(
        key: &Self::Key,
        _maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Key, E> {
        let copied_bytes = std::mem::size_of::<Self::Key>() as u64;
        steps(super::codec::ReadAllocationStep::BeforeCopy(copied_bytes))?;
        let copied = *key;
        steps(super::codec::ReadAllocationStep::Copied(copied_bytes))?;
        Ok(copied)
    }

    fn try_clone_value<E: From<CanonicalDecodeError>>(
        value: &Self::Value,
        _maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Value, E> {
        let copied_bytes = std::mem::size_of::<Self::Value>() as u64;
        steps(super::codec::ReadAllocationStep::BeforeCopy(copied_bytes))?;
        let copied = *value;
        steps(super::codec::ReadAllocationStep::Copied(copied_bytes))?;
        Ok(copied)
    }
}

/// One ordered path-independent record mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileTableMutation {
    /// Inserts a record only if its identity is absent.
    Insert(FileRecord),
    /// Removes an existing record with an optional complete-state precondition.
    Remove {
        /// Stable file identity.
        file_id: FileId,
        /// Exact expected record, or any record with this identity.
        expected: Option<FileRecord>,
    },
    /// Replaces a record after checking its complete prior state.
    Replace {
        /// Exact prior record.
        expected: FileRecord,
        /// Complete replacement with the same stable identity.
        replacement: FileRecord,
    },
}

impl FileTableMutation {
    fn validate(self) -> Result<(), FileTableMutationError> {
        match self {
            Self::Insert(record) => record
                .validate()
                .map_err(|_| FileTableMutationError::InvalidRecord),
            Self::Remove {
                file_id,
                expected: Some(expected),
            } => {
                expected
                    .validate()
                    .map_err(|_| FileTableMutationError::InvalidRecord)?;
                if expected.file_id != file_id {
                    return Err(FileTableMutationError::IdentityMismatch);
                }
                Ok(())
            }
            Self::Replace {
                expected,
                replacement,
            } => {
                expected
                    .validate()
                    .map_err(|_| FileTableMutationError::InvalidRecord)?;
                replacement
                    .validate()
                    .map_err(|_| FileTableMutationError::InvalidRecord)?;
                if expected.file_id != replacement.file_id {
                    return Err(FileTableMutationError::IdentityMismatch);
                }
                Ok(())
            }
            Self::Remove { expected: None, .. } => Ok(()),
        }
    }
}

impl persistent_btree::Mutation<FileTableFormat> for FileTableMutation {
    type Error = FileTableMutationError;

    fn key(&self) -> &FileId {
        match self {
            Self::Insert(record) => &record.file_id,
            Self::Remove { file_id, .. } => file_id,
            Self::Replace { replacement, .. } => &replacement.file_id,
        }
    }

    fn changes_cardinality(&self) -> bool {
        matches!(self, Self::Insert(_) | Self::Remove { .. })
    }

    fn replacement<'a>(&'a self, current: Option<&FileRecord>) -> Result<Option<&'a FileRecord>, Self::Error> {
        match self {
            Self::Insert(_) if current.is_some() => Err(FileTableMutationError::AlreadyExists),
            Self::Insert(record) => Ok(Some(record)),
            Self::Remove { .. } | Self::Replace { .. } if current.is_none() => Err(FileTableMutationError::Missing),
            Self::Remove { expected, .. } => {
                let existing = current.ok_or(FileTableMutationError::Missing)?;
                if expected.as_ref().is_some_and(|value| existing != value) { return Err(FileTableMutationError::StateConflict); }
                Ok(None)
            }
            Self::Replace { expected, replacement } => {
                let existing = current.ok_or(FileTableMutationError::Missing)?;
                if existing != expected { return Err(FileTableMutationError::StateConflict); }
                Ok(Some(replacement))
            }
        }
    }

    fn map_error(error: persistent_btree::Error<Self::Error>) -> crate::FsError { map_error(error).into() }
}

/// Candidate file-table root and exact sparse path-copy work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileTableMutationReceipt {
    /// Candidate immutable file-table root.
    pub root: ObjectId,
    /// Exact work; no authority publication is included.
    pub work: WorkCounters,
}

/// File-table mutation failure retaining all work and possible orphan writes.
pub type FileTableMutationFailure = OperationFailure<FileTableMutationError>;

/// Applies one bounded file-table mutation batch.
pub async fn apply_file_table_mutations<S: crate::AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    mutations: Vec<FileTableMutation>,
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<FileTableMutationReceipt, FileTableMutationFailure> {
    let mut accounting = super::allocation::KernelAccounting::standalone(budget, cancellation);
    apply_file_table_mutations_with_accounting(store, root, &mutations, maximum_mutations, limits, &mut accounting, cancellation)
        .await.map_err(|error| OperationFailure::new(project_file_table_error(error), accounting.spent()))
}

pub(crate) async fn apply_file_table_mutations_with_accounting<S: crate::AsyncObjectStore>(
    store: &S, root: ObjectId, mutations: &[FileTableMutation], maximum_mutations: u32,
    limits: DecodeLimits, accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<FileTableMutationReceipt, crate::FsError> {
    accounting.check()?;
    for mutation in mutations { mutation.validate().map_err(crate::FsError::from)?; }
    let receipt = persistent_btree::apply_with_accounting::<S, FileTableFormat, FileTableMutation>(
        store, root, mutations, maximum_mutations, limits, accounting, cancellation,
    ).await?;
    Ok(FileTableMutationReceipt { root: receipt.value, work: receipt.work })
}

pub(super) fn project_file_table_error(error: crate::FsError) -> FileTableMutationError {
    match error {
        crate::FsError::MergeFileTable(error) => error,
        crate::FsError::Work(error) => FileTableMutationError::Work(error),
        crate::FsError::Object(error) => FileTableMutationError::Storage(error),
        crate::FsError::PendingMutationAllocationFailed | crate::FsError::HandleAllocationFailed
        | crate::FsError::Decode(CanonicalDecodeError::AllocationFailed) => FileTableMutationError::AllocationFailed,
        crate::FsError::Decode(error) => FileTableMutationError::Decode(error),
        crate::FsError::Cancelled(_) => FileTableMutationError::Storage(ObjectStoreError::Cancelled),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => FileTableMutationError::Storage(ObjectStoreError::Native(error)),
        _ => unreachable!("file-table mutation engine returns only its typed kernel errors"),
    }
}

fn map_error(error: persistent_btree::Error<FileTableMutationError>) -> FileTableMutationError {
    match error {
        persistent_btree::Error::Empty => FileTableMutationError::Empty,
        persistent_btree::Error::TooManyMutations => FileTableMutationError::TooManyMutations,
        persistent_btree::Error::WrongRootKind => FileTableMutationError::WrongRootKind,
        persistent_btree::Error::InvalidLimits => FileTableMutationError::InvalidLimits,
        persistent_btree::Error::PageItemTooLarge => FileTableMutationError::PageItemTooLarge,
        persistent_btree::Error::HeightExceeded => FileTableMutationError::HeightExceeded,
        persistent_btree::Error::CycleOrAlias => FileTableMutationError::CycleOrAlias,
        persistent_btree::Error::ChildBoundsMismatch => FileTableMutationError::ChildBoundsMismatch,
        persistent_btree::Error::MutationContract => FileTableMutationError::MutationContract,
        persistent_btree::Error::Allocation(AllocationError::Work(error)) => {
            FileTableMutationError::Work(error)
        }
        persistent_btree::Error::AllocationFailed | persistent_btree::Error::Allocation(_) => {
            FileTableMutationError::AllocationFailed
        }
        persistent_btree::Error::Semantic(error) => error,
        persistent_btree::Error::Storage(error) => FileTableMutationError::Storage(error),
        persistent_btree::Error::Decode(error) => FileTableMutationError::Decode(error),
        persistent_btree::Error::Work(error) => FileTableMutationError::Work(error),
    }
}

/// Persistent file-table path-copy failures.
#[derive(Debug, Error)]
pub enum FileTableMutationError {
    /// Batch contains no mutations.
    #[error("file-table mutation batch is empty")]
    Empty,
    /// Batch exceeds its bound.
    #[error("file-table mutation count exceeds its bound")]
    TooManyMutations,
    /// Root has the wrong object class.
    #[error("file-table mutation root has the wrong kind")]
    WrongRootKind,
    /// Page width/height cannot support mutation.
    #[error("file-table mutation limits are invalid")]
    InvalidLimits,
    /// One encoded file record cannot fit under the page byte bound.
    #[error("file-table mutation item exceeds its page byte bound")]
    PageItemTooLarge,
    /// Traversal or root growth exceeds the height bound.
    #[error("file-table mutation exceeds its height bound")]
    HeightExceeded,
    /// Authenticated graph cycles or aliases.
    #[error("file-table mutation encountered a cycle or alias")]
    CycleOrAlias,
    /// Parent and child routing bounds differ.
    #[error("file-table child bounds mismatch")]
    ChildBoundsMismatch,
    /// A private mutation implementation violated plan invariants.
    #[error("file-table mutation violated its physical-plan contract")]
    MutationContract,
    /// Bounded scratch allocation could not be satisfied.
    #[error("file-table mutation scratch allocation failed")]
    AllocationFailed,
    /// Record violates canonical invariants.
    #[error("file-table mutation record is invalid")]
    InvalidRecord,
    /// Mutation identities differ.
    #[error("file-table mutation identities differ")]
    IdentityMismatch,
    /// Insert found an existing identity.
    #[error("file-table record already exists")]
    AlreadyExists,
    /// Remove/replace found no identity.
    #[error("file-table record is missing")]
    Missing,
    /// Complete-state precondition failed.
    #[error("file-table record state precondition failed")]
    StateConflict,
    /// Immutable object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical encoding/decoding failed.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/file_table_mutation.rs"]
mod tests;
