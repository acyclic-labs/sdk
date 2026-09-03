//! Exact observation and mutation dependencies for no-scan safe rebasing.

use super::{ExtentSeekTarget, LogicalName};
use crate::cancellation::CancellationToken;
use crate::foundation::{Digest, FileId, GenerationId};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use std::collections::BTreeMap;
use std::future::Future;
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
    ///
    /// # Errors
    ///
    /// Rejects empty/overflowing content ranges and inverted directory windows.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CapturedState {
    expected: DependencyState,
    usage: DependencyUse,
}

/// Validated, sorted, and deduplicated checkout dependency proof.
///
/// Construction pays normalization cost once. Equal-generation refresh can
/// therefore return in constant work regardless of dependency count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckoutDependencies {
    captured: BTreeMap<DependencyRegion, CapturedState>,
}

impl CheckoutDependencies {
    /// Validates and normalizes exact observations and mutation preconditions.
    ///
    /// # Errors
    ///
    /// Rejects excessive, malformed, or contradictory dependencies.
    pub fn new(
        observations: impl IntoIterator<Item = Dependency>,
        mutations: impl IntoIterator<Item = Dependency>,
        maximum_dependencies: u32,
    ) -> Result<Self, DependencyError> {
        if maximum_dependencies == 0 {
            return Err(DependencyError::ZeroLimit);
        }
        let mut captured: BTreeMap<DependencyRegion, CapturedState> = BTreeMap::new();
        for (dependency, usage) in observations
            .into_iter()
            .map(|value| (value, DependencyUse::Observation))
            .chain(
                mutations
                    .into_iter()
                    .map(|value| (value, DependencyUse::Mutation)),
            )
        {
            dependency.region.validate()?;
            if let Some(existing) = captured.get_mut(&dependency.region) {
                if existing.expected != dependency.expected {
                    return Err(DependencyError::ContradictoryState);
                }
                if existing.usage != usage {
                    existing.usage = DependencyUse::ObservationAndMutation;
                }
                continue;
            }
            if u32::try_from(captured.len()).unwrap_or(u32::MAX) >= maximum_dependencies {
                return Err(DependencyError::TooManyDependencies {
                    maximum: maximum_dependencies,
                });
            }
            captured.insert(
                dependency.region,
                CapturedState {
                    expected: dependency.expected,
                    usage,
                },
            );
        }
        Ok(Self { captured })
    }

    /// Number of distinct exact regions in the proof.
    #[must_use]
    pub fn len(&self) -> usize {
        self.captured.len()
    }

    /// Whether no operation has observed or mutated a region.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.captured.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.captured.clear();
    }

    pub(crate) fn clear_mutations(&mut self) {
        self.captured.retain(|_, state| match state.usage {
            DependencyUse::Observation => true,
            DependencyUse::Mutation => false,
            DependencyUse::ObservationAndMutation => {
                state.usage = DependencyUse::Observation;
                true
            }
        });
    }

    pub(crate) fn extend_observations(
        &mut self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
    ) -> Result<(), DependencyError> {
        self.extend(
            dependencies,
            maximum_dependencies,
            DependencyUse::Observation,
        )
    }

    pub(crate) fn extend_mutations(
        &mut self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
    ) -> Result<(), DependencyError> {
        self.extend(dependencies, maximum_dependencies, DependencyUse::Mutation)
    }

    fn extend(
        &mut self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
        usage: DependencyUse,
    ) -> Result<(), DependencyError> {
        if maximum_dependencies == 0 {
            return Err(DependencyError::ZeroLimit);
        }
        let mut staged = BTreeMap::new();
        for dependency in dependencies {
            dependency.region.validate()?;
            if let Some(existing) = self.captured.get(&dependency.region)
                && existing.expected != dependency.expected
            {
                return Err(DependencyError::ContradictoryState);
            }
            match staged.get(&dependency.region) {
                Some(expected) if *expected != dependency.expected => {
                    return Err(DependencyError::ContradictoryState);
                }
                Some(_) => {}
                None => {
                    staged.insert(dependency.region, dependency.expected);
                }
            }
        }
        let distinct_new = staged
            .keys()
            .filter(|region| !self.captured.contains_key(*region))
            .count();
        if self.captured.len().saturating_add(distinct_new)
            > usize::try_from(maximum_dependencies).unwrap_or(usize::MAX)
        {
            return Err(DependencyError::TooManyDependencies {
                maximum: maximum_dependencies,
            });
        }
        for (region, expected) in staged {
            match self.captured.get_mut(&region) {
                Some(existing) => {
                    if existing.usage != usage
                        && existing.usage != DependencyUse::ObservationAndMutation
                    {
                        existing.usage = DependencyUse::ObservationAndMutation;
                    }
                }
                None => {
                    self.captured
                        .insert(region, CapturedState { expected, usage });
                }
            }
        }
        Ok(())
    }
}

/// Backend-independent exact-region resolver.
///
/// Implementations must derive `DependencyState` from canonical authenticated
/// generation objects, not from mutable projections or cache presence.
pub trait RebaseProbe {
    /// Stable backend-specific failure.
    type Error: std::error::Error;

    /// Resolves one exact semantic region in one immutable generation.
    ///
    /// # Errors
    ///
    /// Returns a backend-specific failure with exact work performed before the
    /// region could be authenticated.
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

    /// Resolves one exact semantic region without blocking the calling runtime.
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
}

/// Compares only captured regions against a candidate generation.
///
/// Equal roots return with zero work and no probes. Otherwise each distinct
/// dependency is resolved once; untouched volume regions are never enumerated.
///
/// # Errors
///
/// Fails with retained work when conflict bounds, probe execution, or work
/// admission fail.
pub fn classify_rebase<P: RebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    budget: WorkBudget,
) -> Result<RebaseReceipt, OperationFailure<RebaseError<P::Error>>> {
    if base == candidate {
        return Ok(RebaseReceipt {
            decision: RebaseDecision::Safe {
                generation: candidate,
            },
            work: WorkCounters::default(),
        });
    }
    if maximum_conflicts == 0 {
        return Err(OperationFailure::before_work(
            RebaseError::ZeroConflictLimit,
        ));
    }
    let mut work = WorkCounters::default();
    let mut conflicts = Vec::new();
    let mut truncated = false;
    for (region, captured) in &dependencies.captured {
        let semantic = WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        };
        work = work
            .checked_add(semantic)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let remaining = work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let actual = probe
            .probe(candidate, region, remaining)
            .map_err(|failure| failure.map_with_prior_work(work, RebaseError::Probe))?;
        work = work
            .checked_add(actual.work)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if actual.value != captured.expected {
            if u32::try_from(conflicts.len()).unwrap_or(u32::MAX) < maximum_conflicts {
                conflicts.push(RebaseConflict {
                    region: region.clone(),
                    usage: captured.usage,
                    expected: captured.expected,
                    actual: actual.value,
                });
            } else {
                truncated = true;
            }
        }
    }
    let decision = if conflicts.is_empty() {
        RebaseDecision::Safe {
            generation: candidate,
        }
    } else {
        RebaseDecision::Conflicted {
            conflicts,
            truncated,
        }
    };
    Ok(RebaseReceipt { decision, work })
}

/// Nonblocking safe-rebase classification over the same exact dependencies as
/// [`classify_rebase`].
///
/// # Errors
///
/// Fails with retained exact work on cancellation, probe failure, invalid
/// conflict bounds, or work-budget exhaustion.
pub async fn classify_rebase_async<P: AsyncRebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<RebaseReceipt, OperationFailure<RebaseError<P::Error>>> {
    if base == candidate {
        return Ok(RebaseReceipt {
            decision: RebaseDecision::Safe {
                generation: candidate,
            },
            work: WorkCounters::default(),
        });
    }
    if maximum_conflicts == 0 {
        return Err(OperationFailure::before_work(
            RebaseError::ZeroConflictLimit,
        ));
    }
    let mut work = WorkCounters::default();
    let mut conflicts = Vec::new();
    let mut truncated = false;
    for (region, captured) in &dependencies.captured {
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(RebaseError::Cancelled, work))?;
        work = work
            .checked_add(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let remaining = work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let actual = probe
            .probe_async(candidate, region, remaining, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, RebaseError::Probe))?;
        work = work
            .checked_add(actual.work)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if actual.value != captured.expected {
            if u32::try_from(conflicts.len()).unwrap_or(u32::MAX) < maximum_conflicts {
                conflicts.push(RebaseConflict {
                    region: region.clone(),
                    usage: captured.usage,
                    expected: captured.expected,
                    actual: actual.value,
                });
            } else {
                truncated = true;
            }
        }
    }
    let decision = if conflicts.is_empty() {
        RebaseDecision::Safe {
            generation: candidate,
        }
    } else {
        RebaseDecision::Conflicted {
            conflicts,
            truncated,
        }
    };
    Ok(RebaseReceipt { decision, work })
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

#[cfg(test)]
#[path = "tests/rebase.rs"]
mod tests;
