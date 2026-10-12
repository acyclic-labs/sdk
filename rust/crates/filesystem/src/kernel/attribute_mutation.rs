//! Sparse persistent named-attribute mutation over the shared B+tree engine.

use super::allocation::AllocationError;
use super::attribute::{
    attribute_internal_item_encoded_length, attribute_internal_page_encoded_length,
    attribute_leaf_item_encoded_length, attribute_leaf_page_encoded_length,
    attribute_page_decode_shape,
};
use super::codec::DecodedPageShape;
use super::persistent_btree::{self, Format, Page, PageRef};
use super::{
    AttributeEntry, AttributeName, CanonicalDecodeError, DecodeLimits,
};
use crate::foundation::usize_to_u64;
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};
use thiserror::Error;

pub(crate) struct AttributeFormat;

impl Format for AttributeFormat {
    type Key = AttributeName;
    type Value = AttributeEntry;

    fn kind() -> ObjectKind {
        ObjectKind::AttributePage
    }

    fn key(value: &Self::Value) -> &Self::Key {
        &value.name
    }

    fn decode<E: From<CanonicalDecodeError>>(
        bytes: &[u8],
        limits: DecodeLimits,
        observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Page<Self>, E> {
        Ok(match super::attribute::decode_attribute_page(bytes, limits, observe)? {
            super::AttributePage::Leaf(values) => Page::Leaf(values),
            super::AttributePage::Internal(children) => Page::Internal(children),
        })
    }

    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError> {
        attribute_page_decode_shape(bytes, limits)
    }

    fn encode_with_accounting(
        page: &PageRef<'_, Self>, maximum_items: u32,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
        match page {
            PageRef::Leaf(entries) => crate::kernel::codec::encode_owned(accounting, |observe, reserve| super::attribute::encode_attribute_leaf_entries(entries, maximum_items, observe, reserve)),
            PageRef::Internal(children) => crate::kernel::codec::encode_owned(accounting, |observe, reserve| super::attribute::encode_attribute_internal_children(children.iter().map(|child| (&child.first, child.page)), maximum_items, observe, reserve)),
        }
    }

    fn page_encoded_length(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
    ) -> Result<usize, CanonicalDecodeError> {
        match page {
            PageRef::Leaf(entries) => attribute_leaf_page_encoded_length(entries, maximum_items),
            PageRef::Internal(children) => attribute_internal_page_encoded_length(
                children.iter().map(|child| (&child.first, child.page)),
                maximum_items,
            ),
        }
    }

    fn leaf_item_encoded_length(value: &Self::Value) -> Result<usize, CanonicalDecodeError> {
        attribute_leaf_item_encoded_length(value)
    }

    fn internal_item_encoded_length(key: &Self::Key) -> Result<usize, CanonicalDecodeError> {
        attribute_internal_item_encoded_length(key)
    }

    fn key_nested_bytes(key: &Self::Key) -> u64 {
        usize_to_u64(key.as_bytes().len())
    }

    fn value_nested_bytes(value: &Self::Value) -> u64 {
        Self::key_nested_bytes(&value.name)
    }

    fn key_owned_bytes(key: &Self::Key) -> u64 {
        usize_to_u64(key.retained_bytes())
    }

    fn value_owned_bytes(value: &Self::Value) -> u64 {
        Self::key_owned_bytes(&value.name)
    }

    fn try_clone_key<E: From<CanonicalDecodeError>>(
        key: &Self::Key,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Key, E> {
        key.try_clone(maximum_bytes, steps)
    }

    fn try_clone_value<E: From<CanonicalDecodeError>>(
        value: &Self::Value,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Value, E> {
        Ok(AttributeEntry {
            name: Self::try_clone_key(&value.name, maximum_bytes, steps)?,
            value_bytes: value.value_bytes,
            value: value.value,
        })
    }
}

/// One ordered named-attribute mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttributeMutation {
    /// Inserts an attribute only when its name is absent.
    Insert(AttributeEntry),
    /// Removes an attribute with an optional complete-state precondition.
    Remove {
        /// Exact canonical attribute name.
        name: AttributeName,
        /// Required prior entry, or any entry with this name.
        expected: Option<AttributeEntry>,
    },
    /// Replaces an attribute after checking its complete prior state.
    Replace {
        /// Required prior entry.
        expected: AttributeEntry,
        /// Complete replacement with the same name.
        replacement: AttributeEntry,
    },
}

impl AttributeMutation {
    fn validate(&self) -> Result<(), AttributeMutationError> {
        match self {
            Self::Insert(entry) => validate_entry(entry),
            Self::Remove {
                name,
                expected: Some(expected),
            } => {
                validate_entry(expected)?;
                if &expected.name != name {
                    return Err(AttributeMutationError::NameMismatch);
                }
                Ok(())
            }
            Self::Replace {
                expected,
                replacement,
            } => {
                validate_entry(expected)?;
                validate_entry(replacement)?;
                if expected.name != replacement.name {
                    return Err(AttributeMutationError::NameMismatch);
                }
                Ok(())
            }
            Self::Remove { expected: None, .. } => Ok(()),
        }
    }
}

fn validate_entry(entry: &AttributeEntry) -> Result<(), AttributeMutationError> {
    if entry.value.kind != ObjectKind::Blob {
        return Err(AttributeMutationError::InvalidEntry);
    }
    Ok(())
}

impl persistent_btree::Mutation<AttributeFormat> for AttributeMutation {
    type Error = AttributeMutationError;

    fn key(&self) -> &AttributeName {
        match self {
            Self::Insert(entry) => &entry.name,
            Self::Remove { name, .. } => name,
            Self::Replace { replacement, .. } => &replacement.name,
        }
    }

    fn changes_cardinality(&self) -> bool {
        matches!(self, Self::Insert(_) | Self::Remove { .. })
    }

    fn replacement<'a>(&'a self, current: Option<&AttributeEntry>) -> Result<Option<&'a AttributeEntry>, Self::Error> {
        match self {
            Self::Insert(_) if current.is_some() => Err(AttributeMutationError::AlreadyExists),
            Self::Insert(entry) => Ok(Some(entry)),
            Self::Remove { .. } | Self::Replace { .. } if current.is_none() => Err(AttributeMutationError::Missing),
            Self::Remove { expected, .. } => {
                if expected.as_ref().is_some_and(|expected| current != Some(expected)) {
                    return Err(AttributeMutationError::StateConflict);
                }
                Ok(None)
            }
            Self::Replace { expected, replacement } => {
                if current != Some(expected) { return Err(AttributeMutationError::StateConflict); }
                Ok(Some(replacement))
            }
        }
    }

    fn map_error(error: persistent_btree::Error<Self::Error>) -> crate::FsError { map_error(error).into() }
}

/// Attribute mutation failure retaining work and safe orphan writes.
pub type AttributeMutationFailure = OperationFailure<AttributeMutationError>;

/// Applies one bounded ordered attribute mutation batch.
pub async fn apply_attribute_mutations<S: crate::AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    mutations: Vec<AttributeMutation>,
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<OperationReceipt<ObjectId>, AttributeMutationFailure> {
    let mut accounting = super::allocation::KernelAccounting::standalone(budget, cancellation);
    apply_attribute_mutations_with_accounting(store, root, &mutations, maximum_mutations, limits, &mut accounting, cancellation)
        .await.map_err(|error| OperationFailure::new(standalone_error(error), accounting.spent()))
}

pub(crate) async fn apply_attribute_mutations_with_accounting<S: crate::AsyncObjectStore>(
    store: &S, root: ObjectId, mutations: &[AttributeMutation], maximum_mutations: u32,
    limits: DecodeLimits, accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<OperationReceipt<ObjectId>, crate::FsError> {
    accounting.check()?;
    for mutation in mutations { mutation.validate().map_err(crate::FsError::from)?; }
    persistent_btree::apply_with_accounting::<S, AttributeFormat, AttributeMutation>(
        store, root, mutations, maximum_mutations, limits, accounting, cancellation,
    ).await
}

fn standalone_error(error: crate::FsError) -> AttributeMutationError {
    match error {
        crate::FsError::AttributeMutation(error) => error,
        crate::FsError::Work(error) => AttributeMutationError::Work(error),
        crate::FsError::Object(error) => AttributeMutationError::Storage(error),
        crate::FsError::PendingMutationAllocationFailed | crate::FsError::HandleAllocationFailed
        | crate::FsError::Decode(CanonicalDecodeError::AllocationFailed) => AttributeMutationError::AllocationFailed,
        crate::FsError::Decode(error) => AttributeMutationError::Decode(error),
        crate::FsError::Cancelled(_) => AttributeMutationError::Storage(ObjectStoreError::Cancelled),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => AttributeMutationError::Storage(ObjectStoreError::Native(error)),
        _ => unreachable!("attribute mutation engine returns only its typed kernel errors"),
    }
}

fn map_error(error: persistent_btree::Error<AttributeMutationError>) -> AttributeMutationError {
    match error {
        persistent_btree::Error::Empty => AttributeMutationError::Empty,
        persistent_btree::Error::TooManyMutations => AttributeMutationError::TooManyMutations,
        persistent_btree::Error::WrongRootKind => AttributeMutationError::WrongRootKind,
        persistent_btree::Error::InvalidLimits => AttributeMutationError::InvalidLimits,
        persistent_btree::Error::PageItemTooLarge => AttributeMutationError::PageItemTooLarge,
        persistent_btree::Error::HeightExceeded => AttributeMutationError::HeightExceeded,
        persistent_btree::Error::CycleOrAlias => AttributeMutationError::CycleOrAlias,
        persistent_btree::Error::ChildBoundsMismatch => AttributeMutationError::ChildBoundsMismatch,
        persistent_btree::Error::MutationContract => AttributeMutationError::MutationContract,
        persistent_btree::Error::Allocation(AllocationError::Work(error)) => {
            AttributeMutationError::Work(error)
        }
        persistent_btree::Error::AllocationFailed | persistent_btree::Error::Allocation(_) => {
            AttributeMutationError::AllocationFailed
        }
        persistent_btree::Error::Semantic(error) => error,
        persistent_btree::Error::Storage(error) => AttributeMutationError::Storage(error),
        persistent_btree::Error::Decode(error) => AttributeMutationError::Decode(error),
        persistent_btree::Error::Work(error) => AttributeMutationError::Work(error),
    }
}

/// Persistent attribute path-copy failures.
#[derive(Debug, Error)]
pub enum AttributeMutationError {
    /// Batch is empty.
    #[error("attribute mutation batch is empty")]
    Empty,
    /// Batch exceeds its admitted operation count.
    #[error("attribute mutation count exceeds its bound")]
    TooManyMutations,
    /// Root has the wrong object kind.
    #[error("attribute root has the wrong object kind")]
    WrongRootKind,
    /// Page or traversal limits are invalid.
    #[error("attribute mutation limits are invalid")]
    InvalidLimits,
    /// One item or required fanout cannot fit the page-byte bound.
    #[error("attribute item exceeds its page-byte bound")]
    PageItemTooLarge,
    /// Traversal exceeds the page-height bound.
    #[error("attribute tree exceeds its height bound")]
    HeightExceeded,
    /// Authenticated page graph cycles or aliases.
    #[error("attribute tree contains a cycle or alias")]
    CycleOrAlias,
    /// Parent routing bounds mismatch child contents.
    #[error("attribute child bounds mismatch")]
    ChildBoundsMismatch,
    /// Mutation implementation violated its cardinality contract.
    #[error("attribute mutation contract was violated")]
    MutationContract,
    /// Fallible bounded scratch allocation failed.
    #[error("attribute mutation scratch allocation failed")]
    AllocationFailed,
    /// Entry references a non-blob payload.
    #[error("attribute entry is invalid")]
    InvalidEntry,
    /// Mutation precondition and replacement names differ.
    #[error("attribute mutation names differ")]
    NameMismatch,
    /// Insert expected an absent name.
    #[error("attribute already exists")]
    AlreadyExists,
    /// Remove or replace expected an existing name.
    #[error("attribute is missing")]
    Missing,
    /// Existing complete entry differs from the precondition.
    #[error("attribute state precondition failed")]
    StateConflict,
    /// Immutable object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical page encoding or decoding failed.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Work accounting overflowed or exceeded its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/attribute_mutation.rs"]
mod tests;
