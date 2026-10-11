//! Sparse persistent directory-tree mutation with shared-ancestor rewrites.

use super::allocation::AllocationError;
use super::codec::DecodedPageShape;
use super::persistent_btree::{self, Child, Format, Page, PageRef};
use super::tree::tree_page_decode_shape;
use super::{
    CanonicalDecodeError, DecodeLimits, LogicalName, TreeEntry,
};
use crate::foundation::{FileId, usize_to_u64};
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
        Self::decode_with_steps(bytes, limits, &mut |_| Ok(()))
    }

    fn decode_with_steps<E: From<CanonicalDecodeError>>(
        bytes: &[u8],
        limits: DecodeLimits,
        observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Page<Self>, E> {
        super::tree::decode_tree_page_mapped_with_steps(
            bytes, limits, observe, Page::Leaf, Page::Internal,
            |first, page| Child { first, page },
            |child| &child.first,
        )
    }

    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError> {
        tree_page_decode_shape(bytes, limits)
    }


    fn encode_with_accounting(
        page: &PageRef<'_, Self>, maximum_items: u32,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, super::allocation::KernelOwnedAllocation), crate::FsError> {
        match page {
            PageRef::Leaf(entries) => super::tree::encode_tree_leaf_entries_with_accounting(
                entries, maximum_items, accounting, None, None,
            ),
            PageRef::Internal(children) => super::tree::encode_tree_internal_children_with_accounting(
                children.iter().map(|child| (&child.first, child.page)),
                maximum_items, accounting,
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

    fn try_clone_key(
        key: &Self::Key,
        maximum_bytes: u32,
    ) -> Result<Self::Key, CanonicalDecodeError> {
        Self::try_clone_key_with_steps(key, maximum_bytes, &mut |_| Ok(()))
    }

    fn try_clone_key_with_steps<E: From<CanonicalDecodeError>>(
        key: &Self::Key,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Key, E> {
        key.try_clone_with_steps(maximum_bytes, steps)
    }

    fn try_clone_value(
        value: &Self::Value,
        maximum_bytes: u32,
    ) -> Result<Self::Value, CanonicalDecodeError> {
        Self::try_clone_value_with_steps(value, maximum_bytes, &mut |_| Ok(()))
    }

    fn try_clone_value_with_steps<E: From<CanonicalDecodeError>>(
        value: &Self::Value,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Value, E> {
        Ok(TreeEntry {
            name: Self::try_clone_key_with_steps(&value.name, maximum_bytes, steps)?,
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

    fn replacement<'a>(&'a self, current: Option<&TreeEntry>) -> Result<Option<&'a TreeEntry>, Self::Error> {
        match self {
            Self::Insert(_) if current.is_some() => Err(TreeSemanticError::AlreadyExists),
            Self::Insert(entry) => Ok(Some(entry)),
            Self::Remove { .. } | Self::Replace { .. } if current.is_none() => Err(TreeSemanticError::Missing),
            Self::Remove { expected_file_id, .. } => {
                let existing = current.ok_or(TreeSemanticError::Missing)?;
                if expected_file_id.is_some_and(|expected| existing.file_id != expected) {
                    return Err(TreeSemanticError::FileIdentityConflict);
                }
                Ok(None)
            }
            Self::Replace { entry, expected_file_id } => {
                let existing = current.ok_or(TreeSemanticError::Missing)?;
                if existing.file_id != *expected_file_id { return Err(TreeSemanticError::FileIdentityConflict); }
                Ok(Some(entry))
            }
        }
    }

    fn map_error(error: persistent_btree::Error<Self::Error>) -> crate::FsError { map_error(error).into() }
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
    let cancellation = crate::CancellationToken::new();
    crate::async_storage::poll_immediate(apply_tree_mutations_async(
        store, root, mutations, maximum_mutations, limits, budget, &cancellation,
    ))
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
    let mut accounting = super::allocation::KernelAccounting::standalone(budget, cancellation);
    apply_tree_mutations_with_accounting(store, root, &mutations, maximum_mutations, limits, &mut accounting, cancellation)
        .await.map_err(|error| OperationFailure::new(project_tree_error(error), accounting.spent()))
}

/// Borrows caller-owned mutation storage and uses the original invocation AL.
pub(crate) async fn apply_tree_mutations_with_accounting<S: crate::AsyncObjectStore>(
    store: &S, root: ObjectId, mutations: &[TreeMutation], maximum_mutations: u32,
    limits: DecodeLimits, accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<TreeMutationReceipt, crate::FsError> {
    let receipt = persistent_btree::apply_with_accounting::<S, TreeFormat, TreeMutation>(
        store, root, mutations, maximum_mutations, limits, accounting, cancellation,
    ).await?;
    Ok(TreeMutationReceipt { root: receipt.root, work: receipt.work })
}

pub(super) fn project_tree_error(error: crate::FsError) -> TreeMutationError {
    match error {
        crate::FsError::MergeTree(error) => error,
        crate::FsError::Work(error) => TreeMutationError::Work(error),
        crate::FsError::Object(error) => TreeMutationError::Storage(error),
        crate::FsError::PendingMutationAllocationFailed | crate::FsError::HandleAllocationFailed
        | crate::FsError::Decode(CanonicalDecodeError::AllocationFailed) => TreeMutationError::AllocationFailed,
        crate::FsError::Decode(error) => TreeMutationError::Decode(error),
        crate::FsError::Cancelled(_) => TreeMutationError::Storage(ObjectStoreError::Cancelled),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => TreeMutationError::Storage(ObjectStoreError::Native(error)),
        _ => unreachable!("tree mutation engine returns only its typed kernel errors"),
    }
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
