//! Sparse persistent named-attribute mutation over the shared B+tree engine.

use super::allocation::AllocationError;
use super::attribute::{
    attribute_internal_item_encoded_length, attribute_internal_page_encoded_length,
    attribute_leaf_item_encoded_length, attribute_leaf_page_encoded_length,
    attribute_page_decode_shape, encode_attribute_internal_children, encode_attribute_leaf_entries,
};
use super::codec::DecodedPageShape;
use super::persistent_btree::{self, Child, Format, Page, PageRef};
use super::{
    AttributeEntry, AttributeName, AttributePage, CanonicalDecodeError, DecodeLimits,
    decode_attribute_page,
};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
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

    fn decode(bytes: &[u8], limits: DecodeLimits) -> Result<Page<Self>, CanonicalDecodeError> {
        Ok(match decode_attribute_page(bytes, limits)? {
            AttributePage::Leaf(entries) => Page::Leaf(entries),
            AttributePage::Internal(children) => Page::Internal(
                children
                    .into_iter()
                    .map(|child| Child {
                        first: child.first_name,
                        page: child.page,
                    })
                    .collect(),
            ),
        })
    }

    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError> {
        attribute_page_decode_shape(bytes, limits)
    }

    fn encode(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
    ) -> Result<Vec<u8>, CanonicalDecodeError> {
        match page {
            PageRef::Leaf(entries) => encode_attribute_leaf_entries(entries, maximum_items),
            PageRef::Internal(children) => encode_attribute_internal_children(
                children.iter().map(|child| (&child.first, child.page)),
                maximum_items,
            ),
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
        Ok(attribute_leaf_item_encoded_length(value))
    }

    fn internal_item_encoded_length(key: &Self::Key) -> Result<usize, CanonicalDecodeError> {
        Ok(attribute_internal_item_encoded_length(key))
    }

    fn key_nested_bytes(key: &Self::Key) -> u64 {
        u64::try_from(key.as_bytes().len()).unwrap_or(u64::MAX)
    }

    fn value_nested_bytes(value: &Self::Value) -> u64 {
        Self::key_nested_bytes(&value.name)
    }

    fn try_clone_key(
        key: &Self::Key,
        maximum_bytes: u32,
    ) -> Result<Self::Key, CanonicalDecodeError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(key.as_bytes().len())
            .map_err(|_| CanonicalDecodeError::AllocationFailed)?;
        bytes.extend_from_slice(key.as_bytes());
        AttributeName::new(key.class(), bytes, maximum_bytes)
            .map_err(|error| CanonicalDecodeError::Invariant(error.to_string()))
    }

    fn try_clone_value(
        value: &Self::Value,
        maximum_bytes: u32,
    ) -> Result<Self::Value, CanonicalDecodeError> {
        Ok(AttributeEntry {
            name: Self::try_clone_key(&value.name, maximum_bytes)?,
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
    fn validate(&self) -> Result<(), AttributeSemanticError> {
        match self {
            Self::Insert(entry) => validate_entry(entry),
            Self::Remove {
                name,
                expected: Some(expected),
            } => {
                validate_entry(expected)?;
                if &expected.name != name {
                    return Err(AttributeSemanticError::NameMismatch);
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
                    return Err(AttributeSemanticError::NameMismatch);
                }
                Ok(())
            }
            Self::Remove { expected: None, .. } => Ok(()),
        }
    }
}

fn validate_entry(entry: &AttributeEntry) -> Result<(), AttributeSemanticError> {
    if entry.value.kind != ObjectKind::Blob {
        return Err(AttributeSemanticError::InvalidEntry);
    }
    Ok(())
}

impl persistent_btree::Mutation<AttributeFormat> for AttributeMutation {
    type Error = AttributeSemanticError;

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

    fn apply_current(&self, current: &mut Option<AttributeEntry>) -> Result<(), Self::Error> {
        match self {
            Self::Insert(_) if current.is_some() => Err(AttributeSemanticError::AlreadyExists),
            Self::Insert(entry) => {
                *current = Some(entry.clone());
                Ok(())
            }
            Self::Remove { .. } | Self::Replace { .. } if current.is_none() => {
                Err(AttributeSemanticError::Missing)
            }
            Self::Remove { expected, .. } => {
                if expected
                    .as_ref()
                    .is_some_and(|expected| current.as_ref() != Some(expected))
                {
                    return Err(AttributeSemanticError::StateConflict);
                }
                *current = None;
                Ok(())
            }
            Self::Replace {
                expected,
                replacement,
            } => {
                if current.as_ref() != Some(expected) {
                    return Err(AttributeSemanticError::StateConflict);
                }
                *current = Some(replacement.clone());
                Ok(())
            }
        }
    }
}

/// Candidate attribute root and exact sparse path-copy work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttributeMutationReceipt {
    /// Candidate immutable attribute-tree root.
    pub root: ObjectId,
    /// Exact work; authority publication is not included.
    pub work: WorkCounters,
}

/// Attribute mutation failure retaining work and safe orphan writes.
pub type AttributeMutationFailure = OperationFailure<AttributeMutationError>;

/// Applies one bounded ordered attribute mutation batch.
///
/// # Errors
///
/// Rejects malformed entries and preconditions before backend access, then
/// fails closed on semantic, routing, storage, allocation, or work failures.
pub fn apply_attribute_mutations<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    mutations: Vec<AttributeMutation>,
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<AttributeMutationReceipt, AttributeMutationFailure> {
    for mutation in &mutations {
        mutation
            .validate()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
    }
    let receipt = persistent_btree::apply::<S, AttributeFormat, AttributeMutation>(
        store,
        root,
        mutations,
        maximum_mutations,
        limits,
        budget,
    )
    .map_err(|failure| OperationFailure::new(map_error(failure.error), *failure.work))?;
    Ok(AttributeMutationReceipt {
        root: receipt.root,
        work: receipt.work,
    })
}

/// Asynchronously applies the same iterative attribute path-copy machine.
///
/// # Errors
///
/// Returns the same failures as [`apply_attribute_mutations`], including
/// cooperative cancellation at object-store boundaries.
pub async fn apply_attribute_mutations_async<S: crate::AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    mutations: Vec<AttributeMutation>,
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<AttributeMutationReceipt, AttributeMutationFailure> {
    for mutation in &mutations {
        mutation
            .validate()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
    }
    let receipt = persistent_btree::apply_async::<S, AttributeFormat, AttributeMutation>(
        store,
        root,
        mutations,
        maximum_mutations,
        limits,
        budget,
        cancellation,
    )
    .await
    .map_err(|failure| OperationFailure::new(map_error(failure.error), *failure.work))?;
    Ok(AttributeMutationReceipt {
        root: receipt.root,
        work: receipt.work,
    })
}

fn map_error(error: persistent_btree::Error<AttributeSemanticError>) -> AttributeMutationError {
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
        persistent_btree::Error::Semantic(error) => error.into(),
        persistent_btree::Error::Storage(error) => AttributeMutationError::Storage(error),
        persistent_btree::Error::Decode(error) => AttributeMutationError::Decode(error),
        persistent_btree::Error::Work(error) => AttributeMutationError::Work(error),
    }
}

/// Attribute mutation semantic failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AttributeSemanticError {
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
    /// Semantic mutation failed.
    #[error(transparent)]
    Semantic(#[from] AttributeSemanticError),
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
