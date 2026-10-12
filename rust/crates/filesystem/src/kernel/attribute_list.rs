//! Bounded authenticated named-attribute pagination.

use super::attribute_mutation::AttributeFormat;
use super::persistent_pagination;
use super::{AttributeEntry, AttributeName, DecodeLimits, DirectoryReadFailure};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{WorkBudget, WorkCounters};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::ObjectId;
use std::ops::Bound;

/// One bounded ordered named-attribute page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeListing {
    /// Entries strictly after the supplied exact cursor.
    pub entries: Vec<AttributeEntry>,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
    /// Exact unvisited metadata successor exposed by authenticated traversal.
    pub next_residency: Option<ResidencyHint>,
    /// Exact backend, traversal, copy, and logical-allocation work.
    pub work: WorkCounters,
}

/// Lists one bounded attribute page through the shared persistent-tree cursor.
pub async fn list_attributes<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    after: Option<&AttributeName>,
    maximum_entries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<AttributeListing, DirectoryReadFailure> {
    persistent_pagination::paginate::<S, AttributeFormat>(
        store,
        root,
        after.map_or(Bound::Unbounded, Bound::Excluded),
        maximum_entries,
        limits,
        budget,
        cancellation,
        None,
    )
    .await
    .map(to_listing)
}

pub(crate) async fn list_attributes_async_with_accounting<S: AsyncObjectStore>(
    store: &S, root: ObjectId, after: Option<&AttributeName>, maximum_entries: u32,
    limits: DecodeLimits, accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<(AttributeListing, super::allocation::KernelOwnedAllocation), crate::FsError> {
    persistent_pagination::paginate_with_accounting::<S, AttributeFormat>(
        store, root, after.map_or(Bound::Unbounded, Bound::Excluded), maximum_entries, limits, accounting, cancellation, data_custody,
    ).await.map(|(receipt, allocation)| (to_listing(receipt), allocation))
        .map_err(|error| match error {
            crate::FsError::Directory(error) => crate::FsError::AttributeList(error),
            error => error,
        })
}

fn to_listing(receipt: persistent_pagination::Receipt<AttributeEntry>) -> AttributeListing {
    AttributeListing {
        entries: receipt.values,
        has_more: receipt.has_more,
        next_residency: receipt.next_request.map(|request| ResidencyHint {
            request,
            reason: ResidencyReason::MetadataSuccessor,
        }),
        work: receipt.work,
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/attribute_list.rs"]
mod tests;
