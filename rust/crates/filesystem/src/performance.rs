//! Exact implementation-work budgets and receipts for complexity qualification.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// One successful measured operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationReceipt<T> {
    /// Semantic operation result.
    pub value: T,
    /// Exact work performed to produce the result.
    pub work: WorkCounters,
}

/// One failed measured operation retaining all work spent before rejection.
#[derive(Debug, Error)]
#[error("{error}")]
pub struct OperationFailure<E> {
    /// Stable semantic/backend failure.
    pub error: E,
    /// Exact work performed before the failure became known.
    pub work: Box<WorkCounters>,
}

impl<E> OperationFailure<E> {
    /// Constructs a failure with an exact work receipt.
    #[must_use]
    pub fn new(error: E, work: WorkCounters) -> Self {
        Self {
            error,
            work: Box::new(work),
        }
    }

    /// Constructs a failure discovered before measurable work began.
    #[must_use]
    pub fn before_work(error: E) -> Self {
        Self::new(error, WorkCounters::default())
    }

    /// Prepends exact work performed by a containing operation while mapping
    /// the nested error into the containing error domain.
    ///
    /// Accounting overflow takes precedence over the nested error because a
    /// caller must never receive an inexact or sentinel work receipt.
    pub fn map_with_prior_work<T>(
        self,
        prior: WorkCounters,
        map_error: impl FnOnce(E) -> T,
    ) -> OperationFailure<T>
    where
        T: From<WorkError>,
    {
        match prior.checked_add(*self.work) {
            Ok(combined) => OperationFailure::new(map_error(self.error), combined),
            Err(error) => OperationFailure::new(error.into(), prior),
        }
    }
}

/// Success or failure where failure always preserves spent work.
pub type MeasuredResult<T, E> = Result<T, OperationFailure<E>>;

/// Exact backend and memory work performed by one filesystem operation.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct WorkCounters {
    /// Authority records read.
    pub authority_records_read: u64,
    /// Authority records appended.
    pub authority_records_appended: u64,
    /// Canonical authority payload bytes read.
    pub authority_bytes_read: u64,
    /// Canonical authority payload bytes appended.
    pub authority_bytes_written: u64,
    /// Immutable-object metadata probes.
    pub object_probes: u64,
    /// Backend read calls, independent of bytes returned.
    pub backend_read_operations: u64,
    /// Backend write calls, independent of bytes admitted.
    pub backend_write_operations: u64,
    /// Durability barriers such as file or authority-log synchronization.
    pub durability_operations: u64,
    /// Authenticated tree, extent, or file-table pages read.
    pub page_reads: u64,
    /// Authenticated pages created.
    pub page_writes: u64,
    /// Canonical object bytes read.
    pub object_bytes_read: u64,
    /// Canonical object bytes written.
    pub object_bytes_written: u64,
    /// Bytes supplied to cryptographic hashes.
    pub bytes_hashed: u64,
    /// Bytes copied between owned buffers.
    pub bytes_copied: u64,
    /// Canonical bytes produced by encoders.
    pub bytes_encoded: u64,
    /// Bytes consumed from an import, capture, or application source.
    pub source_bytes_read: u64,
    /// Components in requested source paths.
    pub source_path_components: u64,
    /// Source entries observed by demand or native capture requests.
    pub source_entries_visited: u64,
    /// Bytes returned to an application or export sink.
    pub output_bytes: u64,
    /// Canonical items examined by searches, filters, diffs, or merges.
    pub items_examined: u64,
    /// Canonical items returned by bounded APIs.
    pub items_returned: u64,
    /// Owned allocation operations attributable to the request.
    pub allocation_operations: u64,
    /// Peak simultaneously owned allocation bytes attributable to the operation.
    pub peak_allocation_bytes: u64,
    /// Files or extents physically materialized.
    pub materializations: u64,
}

/// Invokes `$apply` with every additive counter of [`WorkCounters`], in
/// declaration order. `peak_allocation_bytes` is a maximum, not a sum, and is
/// handled by each caller.
macro_rules! additive_counters {
    ($apply:ident) => {
        $apply!(
            authority_records_read,
            authority_records_appended,
            authority_bytes_read,
            authority_bytes_written,
            object_probes,
            backend_read_operations,
            backend_write_operations,
            durability_operations,
            page_reads,
            page_writes,
            object_bytes_read,
            object_bytes_written,
            bytes_hashed,
            bytes_copied,
            bytes_encoded,
            source_bytes_read,
            source_path_components,
            source_entries_visited,
            output_bytes,
            items_examined,
            items_returned,
            allocation_operations,
            materializations
        )
    };
}

impl WorkCounters {
    /// No work at all, as a constant: [`WorkCounters::default`] usable in
    /// `const` charges such as one page read.
    pub(crate) const UNCHARGED: Self = Self {
        authority_records_read: 0,
        authority_records_appended: 0,
        authority_bytes_read: 0,
        authority_bytes_written: 0,
        object_probes: 0,
        backend_read_operations: 0,
        backend_write_operations: 0,
        durability_operations: 0,
        page_reads: 0,
        page_writes: 0,
        object_bytes_read: 0,
        object_bytes_written: 0,
        bytes_hashed: 0,
        bytes_copied: 0,
        bytes_encoded: 0,
        source_bytes_read: 0,
        source_path_components: 0,
        source_entries_visited: 0,
        output_bytes: 0,
        items_examined: 0,
        items_returned: 0,
        allocation_operations: 0,
        peak_allocation_bytes: 0,
        materializations: 0,
    };

    /// An explicit unbounded permit for tests and administrative tooling.
    pub const UNBOUNDED: Self = Self {
        authority_records_read: u64::MAX,
        authority_records_appended: u64::MAX,
        authority_bytes_read: u64::MAX,
        authority_bytes_written: u64::MAX,
        object_probes: u64::MAX,
        backend_read_operations: u64::MAX,
        backend_write_operations: u64::MAX,
        durability_operations: u64::MAX,
        page_reads: u64::MAX,
        page_writes: u64::MAX,
        object_bytes_read: u64::MAX,
        object_bytes_written: u64::MAX,
        bytes_hashed: u64::MAX,
        bytes_copied: u64::MAX,
        bytes_encoded: u64::MAX,
        source_bytes_read: u64::MAX,
        source_path_components: u64::MAX,
        source_entries_visited: u64::MAX,
        output_bytes: u64::MAX,
        items_examined: u64::MAX,
        items_returned: u64::MAX,
        allocation_operations: u64::MAX,
        peak_allocation_bytes: u64::MAX,
        materializations: u64::MAX,
    };

    /// Adds one receipt while failing closed on accounting overflow.
    ///
    /// # Errors
    ///
    /// Returns [`WorkError::Overflow`] if any exact counter cannot be represented.
    #[inline]
    pub fn checked_add(self, other: Self) -> Result<Self, WorkError> {
        // Every counter is summed without branching; one overflow flag
        // decides the result, so the sum stays in registers instead of
        // unwinding through two dozen early returns.
        let mut overflow = false;
        macro_rules! sum {
            ($($field:ident),*) => {
                Self {
                    $($field: {
                        let (value, overflowed) = self.$field.overflowing_add(other.$field);
                        overflow |= overflowed;
                        value
                    },)*
                    peak_allocation_bytes: self.peak_allocation_bytes.max(other.peak_allocation_bytes),
                }
            };
        }
        let combined = additive_counters!(sum);
        if overflow {
            return Err(WorkError::Overflow);
        }
        Ok(combined)
    }

    /// Checks, without changing `self`, that adding `delta` neither overflows
    /// nor exceeds `budget`: exactly the outcome of `self.checked_add(*delta)`
    /// followed by [`Self::verify`] of that sum.
    ///
    /// # Errors
    ///
    /// Returns overflow or the first counter the sum would exceed.
    #[inline]
    pub(crate) fn admit(&self, delta: &Self, budget: &WorkBudget) -> Result<(), WorkError> {
        let mut overflow = false;
        let mut within = true;
        macro_rules! check {
            ($($field:ident),*) => {
                $({
                    let (value, overflowed) = self.$field.overflowing_add(delta.$field);
                    overflow |= overflowed;
                    within &= value <= budget.$field;
                })*
            };
        }
        additive_counters!(check);
        within &= self.peak_allocation_bytes.max(delta.peak_allocation_bytes)
            <= budget.peak_allocation_bytes;
        if overflow {
            return Err(WorkError::Overflow);
        }
        if within {
            return Ok(());
        }
        self.checked_add(*delta)?.exceeded(budget)
    }

    /// Adds `delta` in place once [`Self::admit`] accepts it: exactly
    /// `*self = self.checked_add(*delta)?` after that sum verifies against
    /// `budget`, but copies neither counter set. On failure `self` is
    /// unchanged.
    ///
    /// # Errors
    ///
    /// Returns overflow or the first counter the sum would exceed.
    #[inline]
    pub(crate) fn charge(&mut self, delta: &Self, budget: &WorkBudget) -> Result<(), WorkError> {
        self.admit(delta, budget)?;
        self.add_admitted(delta);
        Ok(())
    }

    /// Adds `delta` in place: exactly `*self = self.checked_add(*delta)?`,
    /// without copying either counter set. On overflow `self` is unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`WorkError::Overflow`] if any exact counter cannot be represented.
    #[inline]
    pub(crate) fn try_add_assign(&mut self, delta: &Self) -> Result<(), WorkError> {
        let mut overflow = false;
        macro_rules! check {
            ($($field:ident),*) => {
                $(overflow |= self.$field.checked_add(delta.$field).is_none();)*
            };
        }
        additive_counters!(check);
        if overflow {
            return Err(WorkError::Overflow);
        }
        self.add_admitted(delta);
        Ok(())
    }

    /// Adds `delta` in place after [`Self::admit`] or an equivalent overflow
    /// check has accepted exactly this `self` and `delta`.
    #[inline]
    fn add_admitted(&mut self, delta: &Self) {
        macro_rules! add_in_place {
            ($($field:ident),*) => {
                $(self.$field = self.$field.wrapping_add(delta.$field);)*
            };
        }
        additive_counters!(add_in_place);
        self.peak_allocation_bytes = self.peak_allocation_bytes.max(delta.peak_allocation_bytes);
    }

    /// Verifies every counter against `budget` by reference: exactly
    /// [`Self::verify`], without copying either counter set.
    ///
    /// # Errors
    ///
    /// Returns the first stable counter name that exceeded its bound.
    #[inline]
    pub(crate) fn verify_ref(&self, budget: &WorkBudget) -> Result<(), WorkError> {
        self.verify_within(budget)
    }

    /// Verifies every counter against an admitted hard budget.
    ///
    /// # Errors
    ///
    /// Returns the first stable counter name that exceeded its bound.
    #[inline]
    pub fn verify(self, budget: WorkBudget) -> Result<(), WorkError> {
        self.verify_within(&budget)
    }

    /// Charges `count` examined items in place.
    ///
    /// Exactly equivalent to replacing `self` with its [`Self::checked_add`]
    /// of `count` items once that sum verifies against `budget`, but copies
    /// neither counter set: hot traversals charge once per examined item.
    /// On failure `self` is unchanged.
    ///
    /// # Errors
    ///
    /// Returns overflow or the first counter exceeding `budget`.
    #[inline]
    pub fn charge_items(&mut self, count: u64, budget: &WorkBudget) -> Result<(), WorkError> {
        let prior = self.items_examined;
        self.items_examined = add(prior, count)?;
        self.verify_within(budget).inspect_err(|_| {
            self.items_examined = prior;
        })
    }

    /// Charges `operations` allocations that leave `live_bytes` owned at
    /// once, in place, with the same exactness and failure behavior as
    /// [`Self::charge_items`]: peak allocation is the maximum ever live.
    ///
    /// # Errors
    ///
    /// Returns overflow or the first counter exceeding `budget`.
    #[inline]
    pub fn charge_allocation(
        &mut self,
        operations: u64,
        live_bytes: u64,
        budget: &WorkBudget,
    ) -> Result<(), WorkError> {
        let (prior_operations, prior_peak) =
            (self.allocation_operations, self.peak_allocation_bytes);
        self.allocation_operations = add(prior_operations, operations)?;
        self.peak_allocation_bytes = prior_peak.max(live_bytes);
        self.verify_within(budget).inspect_err(|_| {
            self.allocation_operations = prior_operations;
            self.peak_allocation_bytes = prior_peak;
        })
    }

    #[inline]
    fn verify_within(&self, budget: &WorkBudget) -> Result<(), WorkError> {
        // Charged once per examined item on hot paths: compare every counter
        // without branching and name the exceeded one only on failure.
        if (self.authority_records_read <= budget.authority_records_read)
            & (self.authority_records_appended <= budget.authority_records_appended)
            & (self.authority_bytes_read <= budget.authority_bytes_read)
            & (self.authority_bytes_written <= budget.authority_bytes_written)
            & (self.object_probes <= budget.object_probes)
            & (self.backend_read_operations <= budget.backend_read_operations)
            & (self.backend_write_operations <= budget.backend_write_operations)
            & (self.durability_operations <= budget.durability_operations)
            & (self.page_reads <= budget.page_reads)
            & (self.page_writes <= budget.page_writes)
            & (self.object_bytes_read <= budget.object_bytes_read)
            & (self.object_bytes_written <= budget.object_bytes_written)
            & (self.bytes_hashed <= budget.bytes_hashed)
            & (self.bytes_copied <= budget.bytes_copied)
            & (self.bytes_encoded <= budget.bytes_encoded)
            & (self.source_bytes_read <= budget.source_bytes_read)
            & (self.source_path_components <= budget.source_path_components)
            & (self.source_entries_visited <= budget.source_entries_visited)
            & (self.output_bytes <= budget.output_bytes)
            & (self.items_examined <= budget.items_examined)
            & (self.items_returned <= budget.items_returned)
            & (self.allocation_operations <= budget.allocation_operations)
            & (self.peak_allocation_bytes <= budget.peak_allocation_bytes)
            & (self.materializations <= budget.materializations)
        {
            return Ok(());
        }
        self.exceeded(budget)
    }

    #[cold]
    fn exceeded(&self, budget: &WorkBudget) -> Result<(), WorkError> {
        let fields = [
            (
                "authority_records_read",
                self.authority_records_read,
                budget.authority_records_read,
            ),
            (
                "authority_records_appended",
                self.authority_records_appended,
                budget.authority_records_appended,
            ),
            (
                "authority_bytes_read",
                self.authority_bytes_read,
                budget.authority_bytes_read,
            ),
            (
                "authority_bytes_written",
                self.authority_bytes_written,
                budget.authority_bytes_written,
            ),
            ("object_probes", self.object_probes, budget.object_probes),
            (
                "backend_read_operations",
                self.backend_read_operations,
                budget.backend_read_operations,
            ),
            (
                "backend_write_operations",
                self.backend_write_operations,
                budget.backend_write_operations,
            ),
            (
                "durability_operations",
                self.durability_operations,
                budget.durability_operations,
            ),
            ("page_reads", self.page_reads, budget.page_reads),
            ("page_writes", self.page_writes, budget.page_writes),
            (
                "object_bytes_read",
                self.object_bytes_read,
                budget.object_bytes_read,
            ),
            (
                "object_bytes_written",
                self.object_bytes_written,
                budget.object_bytes_written,
            ),
            ("bytes_hashed", self.bytes_hashed, budget.bytes_hashed),
            ("bytes_copied", self.bytes_copied, budget.bytes_copied),
            ("bytes_encoded", self.bytes_encoded, budget.bytes_encoded),
            (
                "source_bytes_read",
                self.source_bytes_read,
                budget.source_bytes_read,
            ),
            (
                "source_path_components",
                self.source_path_components,
                budget.source_path_components,
            ),
            (
                "source_entries_visited",
                self.source_entries_visited,
                budget.source_entries_visited,
            ),
            ("output_bytes", self.output_bytes, budget.output_bytes),
            ("items_examined", self.items_examined, budget.items_examined),
            ("items_returned", self.items_returned, budget.items_returned),
            (
                "allocation_operations",
                self.allocation_operations,
                budget.allocation_operations,
            ),
            (
                "peak_allocation_bytes",
                self.peak_allocation_bytes,
                budget.peak_allocation_bytes,
            ),
            (
                "materializations",
                self.materializations,
                budget.materializations,
            ),
        ];
        for (counter, observed, maximum) in fields {
            if observed > maximum {
                return Err(WorkError::BudgetExceeded {
                    counter,
                    observed,
                    maximum,
                });
            }
        }
        Ok(())
    }

    /// Returns the remaining additive budget after this spent receipt.
    /// Peak allocation is an operation-wide maximum, so its remaining limit is
    /// unchanged and each backend allocation is checked against the same cap.
    ///
    /// # Errors
    ///
    /// Returns the exact exceeded counter when already-spent work is outside
    /// the admitted budget.
    #[inline]
    pub fn remaining(self, budget: WorkBudget) -> Result<WorkBudget, WorkError> {
        self.verify_within(&budget)?;
        Ok(WorkBudget {
            authority_records_read: budget.authority_records_read - self.authority_records_read,
            authority_records_appended: budget.authority_records_appended
                - self.authority_records_appended,
            authority_bytes_read: budget.authority_bytes_read - self.authority_bytes_read,
            authority_bytes_written: budget.authority_bytes_written - self.authority_bytes_written,
            object_probes: budget.object_probes - self.object_probes,
            backend_read_operations: budget.backend_read_operations - self.backend_read_operations,
            backend_write_operations: budget.backend_write_operations
                - self.backend_write_operations,
            durability_operations: budget.durability_operations - self.durability_operations,
            page_reads: budget.page_reads - self.page_reads,
            page_writes: budget.page_writes - self.page_writes,
            object_bytes_read: budget.object_bytes_read - self.object_bytes_read,
            object_bytes_written: budget.object_bytes_written - self.object_bytes_written,
            bytes_hashed: budget.bytes_hashed - self.bytes_hashed,
            bytes_copied: budget.bytes_copied - self.bytes_copied,
            bytes_encoded: budget.bytes_encoded - self.bytes_encoded,
            source_bytes_read: budget.source_bytes_read - self.source_bytes_read,
            source_path_components: budget.source_path_components - self.source_path_components,
            source_entries_visited: budget.source_entries_visited - self.source_entries_visited,
            output_bytes: budget.output_bytes - self.output_bytes,
            items_examined: budget.items_examined - self.items_examined,
            items_returned: budget.items_returned - self.items_returned,
            allocation_operations: budget.allocation_operations - self.allocation_operations,
            peak_allocation_bytes: budget.peak_allocation_bytes,
            materializations: budget.materializations - self.materializations,
        })
    }
}

/// Hard upper bounds admitted before one operation begins.
pub type WorkBudget = WorkCounters;

fn add(left: u64, right: u64) -> Result<u64, WorkError> {
    left.checked_add(right).ok_or(WorkError::Overflow)
}

/// Exact work-accounting failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkError {
    /// One counter could not be represented exactly.
    #[error("filesystem work accounting overflowed")]
    Overflow,
    /// Executed work exceeded its admitted bound.
    #[error("work counter {counter} observed {observed}, exceeding {maximum}")]
    BudgetExceeded {
        /// Stable counter identifier.
        counter: &'static str,
        /// Exact observed work.
        observed: u64,
        /// Admitted maximum work.
        maximum: u64,
    },
}

#[cfg(test)]
#[path = "tests/performance.rs"]
mod tests;
