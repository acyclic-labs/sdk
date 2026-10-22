//! Exact observation and mutation dependencies for no-scan safe rebasing.

use super::{ExtentSeekTarget, LogicalName};
use crate::cancellation::CancellationToken;
use crate::foundation::{Digest, FileId, GenerationId};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;

/// One exact semantic region whose state can affect a checkout.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DependencyRegion {
    /// Complete path-independent file record, including kind and payload roots.
    FileRecord(FileId),
    /// Complete canonical metadata object selected by one file record.
    Metadata(FileId),
    /// Exact logical length of one regular file.
    FileLength(FileId),
    /// A non-empty bounded logical content range.
    ContentRange {
        /// Stable file identity.
        file_id: FileId,
        /// Inclusive logical offset.
        offset: u64,
        /// Positive byte length.
        length: u64,
    },
    /// One exact sparse seek observation.
    SparseSeek {
        /// Stable file identity.
        file_id: FileId,
        /// Inclusive logical starting offset.
        offset: u64,
        /// Sparse class requested by the caller.
        target: ExtentSeekTarget,
    },
    /// One exact name lookup, including a negative lookup when state is absent.
    DirectoryName {
        /// Stable directory identity.
        directory_id: FileId,
        /// Canonical queried name.
        name: LogicalName,
    },
    /// A bounded directory cursor interval used to prevent phantom-safe rebases.
    DirectoryRange {
        /// Stable directory identity.
        directory_id: FileId,
        /// Exclusive lower cursor; `None` starts before the first name.
        after: Option<LogicalName>,
        /// Positive maximum entries requested by the observed page.
        maximum_entries: u32,
    },
}

impl DependencyRegion {
    /// Validates range arithmetic and directory cursor ordering.
    pub fn validate(&self) -> Result<(), DependencyError> {
        match self {
            Self::ContentRange { offset, length, .. } => {
                if *length == 0 {
                    return Err(DependencyError::EmptyContentRange);
                }
                offset
                    .checked_add(*length)
                    .ok_or(DependencyError::RangeOverflow)?;
            }
            Self::DirectoryRange {
                maximum_entries: 0, ..
            } => return Err(DependencyError::ZeroDirectoryPageLimit),
            Self::FileRecord(_)
            | Self::Metadata(_)
            | Self::FileLength(_)
            | Self::SparseSeek { .. }
            | Self::DirectoryName { .. }
            | Self::DirectoryRange { .. } => {}
        }
        Ok(())
    }
}

/// Canonical semantic state of one exact dependency region.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyState {
    /// The exact queried name/record/region does not exist.
    Absent,
    /// Domain-separated digest of the complete bounded semantic result.
    Present(Digest),
}

/// One dependency captured while evaluating or mutating a checkout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dependency {
    /// Exact semantic region.
    pub region: DependencyRegion,
    /// State authenticated against the checkout's base generation.
    pub expected: DependencyState,
}

/// Whether a conflicting dependency came from observation, mutation, or both.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyUse {
    /// A read, stat, listing, or negative lookup observed this region.
    Observation,
    /// A local mutation uses this region as its compare-before-write base.
    Mutation,
    /// The same region was both observed and mutated.
    ObservationAndMutation,
}

impl DependencyUse {
    fn with(self, other: Self) -> Self {
        if self == other {
            self
        } else {
            Self::ObservationAndMutation
        }
    }
}

#[derive(Clone, Debug)]
struct CapturedState {
    expected: DependencyState,
    usage: DependencyUse,
    data: ProofDataClaim,
}
impl PartialEq for CapturedState {
    fn eq(&self, other: &Self) -> bool {
        self.expected == other.expected && self.usage == other.usage
    }
}
impl Eq for CapturedState {}

#[derive(Clone, Debug, Default)]
struct CapturedMap {
    entries: BTreeMap<DependencyRegion, CapturedState>,
    _control: ProofDataClaim,
}

impl PartialEq for CapturedMap {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl Eq for CapturedMap {}

/// Validated, sorted, and deduplicated checkout dependency proof.
#[derive(Clone, Debug)]
pub struct CheckoutDependencies {
    captured: Arc<CapturedMap>,
    name_data_bytes: u64,
    cloned_name_data_bytes: u64,
    data_generation: u64,
}

impl PartialEq for CheckoutDependencies {
    fn eq(&self, other: &Self) -> bool {
        self.captured == other.captured
    }
}

impl Eq for CheckoutDependencies {}

fn dependency_name_bytes(region: &DependencyRegion, cloned: bool) -> u64 {
    match region {
        DependencyRegion::DirectoryName { name, .. }
        | DependencyRegion::DirectoryRange {
            after: Some(name), ..
        } => {
            if cloned {
                name.as_bytes().len() as u64
            } else {
                name.retained_bytes() as u64
            }
        }
        _ => 0,
    }
}

/// Backend-independent exact-region resolver.
pub trait RebaseProbe {
    /// Stable backend-specific failure.
    type Error: std::error::Error;

    /// Authenticates one dependency region.
    fn probe(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
    ) -> Result<ProbeReceipt, OperationFailure<Self::Error>>;
}

/// Runtime-neutral nonblocking exact-region resolver.
pub trait AsyncRebaseProbe {
    /// Stable backend-specific failure.
    type Error: std::error::Error;

    /// Authenticates one dependency region asynchronously.
    fn probe_async(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<ProbeReceipt, OperationFailure<Self::Error>>>;
}

/// One exact-region result and the work required to authenticate it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeReceipt {
    /// Canonical semantic state in the selected generation.
    pub value: DependencyState,
    /// Exact backend and semantic work.
    pub work: WorkCounters,
}

/// One region-specific reason automatic advancement is unsafe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebaseConflict {
    /// Exact changed region.
    pub region: DependencyRegion,
    /// How the checkout depended on it.
    pub usage: DependencyUse,
    /// State captured from the old base.
    pub expected: DependencyState,
    /// State resolved from the candidate generation.
    pub actual: DependencyState,
}

/// Bounded safe-rebase result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebaseDecision {
    /// Candidate generation can become the new base without replay ambiguity.
    Safe {
        /// New immutable base.
        generation: GenerationId,
    },
    /// Candidate changed one or more exact dependencies.
    Conflicted {
        /// Region-specific conflicts, bounded by the caller.
        conflicts: Vec<RebaseConflict>,
        /// More conflicts existed than the caller admitted returning.
        truncated: bool,
    },
}

/// Successful decision with exact probe work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebaseReceipt {
    /// Safe or region-conflicted result.
    pub decision: RebaseDecision,
    /// Exact semantic and backend work.
    pub work: WorkCounters,
}

/// Safe-rebase classification failures.
#[derive(Debug, Error)]
pub enum RebaseError<E: std::error::Error> {
    /// Cooperative cancellation occurred before the next region probe.
    #[error("safe rebase classification was cancelled")]
    Cancelled,
    /// Conflict output must have a positive hard bound.
    #[error("maximum rebase conflicts must be non-zero")]
    ZeroConflictLimit,
    /// Exact region resolution failed.
    #[error("rebase region probe failed: {0}")]
    Probe(E),
    /// Work exceeded or overflowed the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// A bounded conflict result could not be allocated.
    #[error("safe rebase conflict allocation failed")]
    AllocationFailed,
}

/// Dependency-proof construction errors.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DependencyError {
    /// Dependency admission bound must be non-zero.
    #[error("maximum dependencies must be non-zero")]
    ZeroLimit,
    /// Content ranges must contain at least one byte.
    #[error("content dependency range is empty")]
    EmptyContentRange,
    /// Content range arithmetic overflowed.
    #[error("content dependency range overflowed")]
    RangeOverflow,
    /// Directory listing observations require a positive page bound.
    #[error("directory dependency page limit must be non-zero")]
    ZeroDirectoryPageLimit,
    /// The same exact region was captured with two different base states.
    #[error("dependency region has contradictory captured states")]
    ContradictoryState,
    /// Distinct dependency count exceeded its hard bound.
    #[error("dependency count exceeds maximum {maximum}")]
    TooManyDependencies {
        /// Configured maximum.
        maximum: u32,
    },
}

mod classification;
mod custody;
mod dependencies;
use super::{allocation, persistent_btree, persistent_io, persistent_point, tree_mutation};
pub(crate) use classification::{
    AccountingRebaseProbe, ClassifyRebaseFailure, Settled, Settlement,
    classify_rebase_settling_with_accounting, classify_rebase_with_accounting,
};
pub use classification::{classify_rebase, classify_rebase_async};
pub(crate) use custody::{ProofDataClaim, ProofDataCustody};

#[cfg(test)]
#[path = "../tests/rebase.rs"]
mod tests;
