//! Sparse persistent directory-tree mutation with shared-ancestor rewrites.

use super::allocation::AllocationError;
use super::codec::DecodedPageShape;
use super::persistent_btree::{self, Child, Format, Page, PageRef};
use super::tree::{
    encode_tree_internal_children, encode_tree_leaf_entries, tree_page_decode_shape,
};
use super::{
    CanonicalDecodeError, DecodeLimits, LogicalName, TreeEntry, TreePage, decode_tree_page,
};
use crate::foundation::FileId;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};
use thiserror::Error;

pub(crate) struct TreeFormat;

impl Format for TreeFormat {
    type Key = LogicalName;
    type Value = TreeEntry;

    fn kind() -> ObjectKind {
        ObjectKind::TreePage
    }

    fn key(value: &Self::Value) -> &Self::Key {
        &value.name
    }

    fn decode(bytes: &[u8], limits: DecodeLimits) -> Result<Page<Self>, CanonicalDecodeError> {
        Ok(match decode_tree_page(bytes, limits)? {
            TreePage::Leaf(entries) => Page::Leaf(entries),
            TreePage::Internal(children) => Page::Internal(
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
        tree_page_decode_shape(bytes, limits)
    }

    fn encode(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
    ) -> Result<Vec<u8>, CanonicalDecodeError> {
        match page {
            PageRef::Leaf(entries) => encode_tree_leaf_entries(entries, maximum_items),
            PageRef::Internal(children) => encode_tree_internal_children(
                children.iter().map(|child| (&child.first, child.page)),
                maximum_items,
            ),
        }
    }

    fn page_encoded_length(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
    ) -> Result<usize, CanonicalDecodeError> {
        let count = match page {
            PageRef::Leaf(entries) => entries.len(),
            PageRef::Internal(children) => children.len(),
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(CanonicalDecodeError::LengthOverflow);
        }
        let mut bytes = 8_usize + 2 + 1 + 4;
        match page {
            PageRef::Leaf(entries) => {
                for entry in *entries {
                    bytes = bytes
                        .checked_add(Self::leaf_item_encoded_length(entry)?)
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
        value
            .name
            .as_bytes()
            .len()
            .checked_add(1 + 4 + 16 + 1)
            .ok_or(CanonicalDecodeError::LengthOverflow)
    }

    fn internal_item_encoded_length(key: &Self::Key) -> Result<usize, CanonicalDecodeError> {
        key.as_bytes()
            .len()
            .checked_add(1 + 4 + 32)
            .ok_or(CanonicalDecodeError::LengthOverflow)
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
        LogicalName::new(key.encoding(), bytes, maximum_bytes)
            .map_err(|error| CanonicalDecodeError::Invariant(error.to_string()))
    }

    fn try_clone_value(
        value: &Self::Value,
        maximum_bytes: u32,
    ) -> Result<Self::Value, CanonicalDecodeError> {
        Ok(TreeEntry {
            name: Self::try_clone_key(&value.name, maximum_bytes)?,
            file_id: value.file_id,
            kind: value.kind,
        })
    }
}

/// One ordered mutation to a directory name binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreeMutation {
    /// Inserts a binding only if the name is absent.
    Insert(TreeEntry),
    /// Removes an existing binding, optionally checking its stable file identity.
    Remove {
        /// Exact canonical name.
        name: LogicalName,
        /// Exact identity precondition, or any existing identity.
        expected_file_id: Option<FileId>,
    },
    /// Replaces one existing binding after checking its identity.
    Replace {
        /// Complete replacement entry; its name is unchanged.
        entry: TreeEntry,
        /// Exact existing identity.
        expected_file_id: FileId,
    },
}

impl persistent_btree::Mutation<TreeFormat> for TreeMutation {
    type Error = TreeSemanticError;

    fn key(&self) -> &LogicalName {
        match self {
            Self::Insert(entry) | Self::Replace { entry, .. } => &entry.name,
            Self::Remove { name, .. } => name,
        }
    }

    fn changes_cardinality(&self) -> bool {
        matches!(self, Self::Insert(_) | Self::Remove { .. })
    }

    fn apply_current(&self, current: &mut Option<TreeEntry>) -> Result<(), Self::Error> {
        match self {
            Self::Insert(_) if current.is_some() => Err(TreeSemanticError::AlreadyExists),
            Self::Insert(entry) => {
                *current = Some(entry.clone());
                Ok(())
            }
            Self::Remove { .. } | Self::Replace { .. } if current.is_none() => {
                Err(TreeSemanticError::Missing)
            }
            Self::Remove {
                expected_file_id, ..
            } => {
                let existing = current.as_ref().ok_or(TreeSemanticError::Missing)?;
                if expected_file_id.is_some_and(|expected| existing.file_id != expected) {
                    return Err(TreeSemanticError::FileIdentityConflict);
                }
                *current = None;
                Ok(())
            }
            Self::Replace {
                entry,
                expected_file_id,
            } => {
                let existing = current.as_ref().ok_or(TreeSemanticError::Missing)?;
                if existing.file_id != *expected_file_id {
                    return Err(TreeSemanticError::FileIdentityConflict);
                }
                *current = Some(entry.clone());
                Ok(())
            }
        }
    }
}

/// New immutable directory root and exact path-copy work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeMutationReceipt {
    /// Candidate root. It is not authoritative until its generation is published.
    pub root: ObjectId,
    /// Exact authenticated reads, canonical writes, and backend work.
    pub work: WorkCounters,
}

/// Sparse tree mutation failure preserving all work and orphan writes.
pub type TreeMutationFailure = OperationFailure<TreeMutationError>;

/// Applies one ordered mutation batch by path-copying only intersecting pages.
///
/// Mutations are stably grouped by name, preserving order for repeated names.
/// One touched page and each shared ancestor are rewritten at most once.
/// Untouched child identities are copied verbatim. A failure returns no root;
/// already-written immutable objects are harmless orphans.
///
/// # Errors
///
/// Rejects empty/excessive batches, invalid page bounds, failed preconditions,
/// malformed/cyclic trees, storage errors, or unadmitted work.
pub fn apply_tree_mutations<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    mutations: Vec<TreeMutation>,
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<TreeMutationReceipt, TreeMutationFailure> {
    let receipt = persistent_btree::apply::<S, TreeFormat, TreeMutation>(
        store,
        root,
        mutations,
        maximum_mutations,
        limits,
        budget,
    )
    .map_err(|failure| OperationFailure::new(map_error(failure.error), *failure.work))?;
    Ok(TreeMutationReceipt {
        root: receipt.root,
        work: receipt.work,
    })
}

/// Asynchronously applies one ordered sparse directory mutation batch.
///
/// Native and browser execution drive the same iterative path-copy machine;
/// only immutable-object suspension differs.
///
/// # Errors
///
/// Returns the same semantic, validation, storage, cancellation, and work
/// failures as [`apply_tree_mutations`].
pub async fn apply_tree_mutations_async<S: crate::AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    mutations: Vec<TreeMutation>,
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<TreeMutationReceipt, TreeMutationFailure> {
    let receipt = persistent_btree::apply_async::<S, TreeFormat, TreeMutation>(
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
    Ok(TreeMutationReceipt {
        root: receipt.root,
        work: receipt.work,
    })
}

fn map_error(error: persistent_btree::Error<TreeSemanticError>) -> TreeMutationError {
    match error {
        persistent_btree::Error::Empty => TreeMutationError::Empty,
        persistent_btree::Error::TooManyMutations => TreeMutationError::TooManyMutations,
        persistent_btree::Error::WrongRootKind => TreeMutationError::WrongRootKind,
        persistent_btree::Error::InvalidLimits => TreeMutationError::InvalidLimits,
        persistent_btree::Error::PageItemTooLarge => TreeMutationError::PageItemTooLarge,
        persistent_btree::Error::HeightExceeded => TreeMutationError::HeightExceeded,
        persistent_btree::Error::CycleOrAlias => TreeMutationError::CycleOrAlias,
        persistent_btree::Error::ChildBoundsMismatch => TreeMutationError::ChildBoundsMismatch,
        persistent_btree::Error::MutationContract => TreeMutationError::MutationContract,
        persistent_btree::Error::Allocation(AllocationError::Work(error)) => {
            TreeMutationError::Work(error)
        }
        persistent_btree::Error::AllocationFailed | persistent_btree::Error::Allocation(_) => {
            TreeMutationError::AllocationFailed
        }
        persistent_btree::Error::Semantic(error) => error.into(),
        persistent_btree::Error::Storage(error) => TreeMutationError::Storage(error),
        persistent_btree::Error::Decode(error) => TreeMutationError::Decode(error),
        persistent_btree::Error::Work(error) => TreeMutationError::Work(error),
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum TreeSemanticError {
    #[error("tree mutation name already exists")]
    AlreadyExists,
    #[error("tree mutation name is absent")]
    Missing,
    #[error("tree mutation file identity precondition failed")]
    FileIdentityConflict,
}

/// Persistent directory mutation failures.
#[derive(Debug, Error)]
pub enum TreeMutationError {
    /// Batch contains no mutation.
    #[error("tree mutation batch is empty")]
    Empty,
    /// Batch count is zero-bounded, excessive, or unrepresentable.
    #[error("tree mutation count exceeds its admitted bound")]
    TooManyMutations,
    /// Root object has the wrong class.
    #[error("tree mutation root is not a tree page")]
    WrongRootKind,
    /// Page width/height cannot support a persistent tree.
    #[error("tree mutation limits are invalid")]
    InvalidLimits,
    /// One encoded binding cannot fit under the page byte bound.
    #[error("tree mutation item exceeds its page byte bound")]
    PageItemTooLarge,
    /// Traversal or root growth exceeds the page-height bound.
    #[error("tree mutation exceeds its admitted height")]
    HeightExceeded,
    /// Authenticated page graph cycles or aliases.
    #[error("tree mutation encountered a cycle or page alias")]
    CycleOrAlias,
    /// Parent routing bounds do not match a child page.
    #[error("tree mutation child bounds mismatch")]
    ChildBoundsMismatch,
    /// A private mutation implementation violated plan invariants.
    #[error("tree mutation violated its physical-plan contract")]
    MutationContract,
    /// Bounded scratch allocation could not be satisfied.
    #[error("tree mutation scratch allocation failed")]
    AllocationFailed,
    /// Insert expected an absent name.
    #[error("tree mutation name already exists")]
    AlreadyExists,
    /// Remove/replace expected an existing name.
    #[error("tree mutation name is absent")]
    Missing,
    /// Existing file identity differs from the mutation precondition.
    #[error("tree mutation file identity precondition failed")]
    FileIdentityConflict,
    /// Immutable object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical page encoding/decoding failed.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

impl From<TreeSemanticError> for TreeMutationError {
    fn from(value: TreeSemanticError) -> Self {
        match value {
            TreeSemanticError::AlreadyExists => Self::AlreadyExists,
            TreeSemanticError::Missing => Self::Missing,
            TreeSemanticError::FileIdentityConflict => Self::FileIdentityConflict,
        }
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/tree_mutation.rs"]
mod tests;
