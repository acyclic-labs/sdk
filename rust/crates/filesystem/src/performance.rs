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
#[serde(rename_all = "camelCase")]
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

impl WorkCounters {
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
    pub fn checked_add(self, other: Self) -> Result<Self, WorkError> {
        Ok(Self {
            authority_records_read: add(self.authority_records_read, other.authority_records_read)?,
            authority_records_appended: add(
                self.authority_records_appended,
                other.authority_records_appended,
            )?,
            authority_bytes_read: add(self.authority_bytes_read, other.authority_bytes_read)?,
            authority_bytes_written: add(
                self.authority_bytes_written,
                other.authority_bytes_written,
            )?,
            object_probes: add(self.object_probes, other.object_probes)?,
            backend_read_operations: add(
                self.backend_read_operations,
                other.backend_read_operations,
            )?,
            backend_write_operations: add(
                self.backend_write_operations,
                other.backend_write_operations,
            )?,
            durability_operations: add(self.durability_operations, other.durability_operations)?,
            page_reads: add(self.page_reads, other.page_reads)?,
            page_writes: add(self.page_writes, other.page_writes)?,
            object_bytes_read: add(self.object_bytes_read, other.object_bytes_read)?,
            object_bytes_written: add(self.object_bytes_written, other.object_bytes_written)?,
            bytes_hashed: add(self.bytes_hashed, other.bytes_hashed)?,
            bytes_copied: add(self.bytes_copied, other.bytes_copied)?,
            bytes_encoded: add(self.bytes_encoded, other.bytes_encoded)?,
            source_bytes_read: add(self.source_bytes_read, other.source_bytes_read)?,
            output_bytes: add(self.output_bytes, other.output_bytes)?,
            items_examined: add(self.items_examined, other.items_examined)?,
            items_returned: add(self.items_returned, other.items_returned)?,
            allocation_operations: add(self.allocation_operations, other.allocation_operations)?,
            peak_allocation_bytes: self.peak_allocation_bytes.max(other.peak_allocation_bytes),
            materializations: add(self.materializations, other.materializations)?,
        })
    }

    /// Verifies every counter against an admitted hard budget.
    ///
    /// # Errors
    ///
    /// Returns the first stable counter name that exceeded its bound.
    pub fn verify(self, budget: WorkBudget) -> Result<(), WorkError> {
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
    pub fn remaining(self, budget: WorkBudget) -> Result<WorkBudget, WorkError> {
        self.verify(budget)?;
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
