//! One conflict rule for proposals and changes since a proposal's base.
//!
//! Namespace paths are represented by their authenticated directory/name
//! bindings, including each ancestor lookup captured by traversal. File IDs
//! also fence aliases: distinct paths to the same inode cannot evade the rule.
//! Changing a directory's child tree alone is not a write to every descendant;
//! replacing its record or an ancestor binding is. New directories include
//! their parent binding, so competing creation of the same directory overlaps.
use super::{CheckoutDependencies, DependencyRegion};
use crate::kernel::FileKind;
use crate::kernel::{DirectoryBindingChange, FileRecordChange, GenerationDiff};
use crate::performance::WorkError;

pub(crate) struct FootprintQuery {
    pub(crate) independent: Result<bool, WorkError>,
    pub(crate) items_examined: u64,
}

struct QueryBudget {
    maximum: u64,
    examined: u64,
}

impl QueryBudget {
    fn visit(&mut self) -> Result<(), WorkError> {
        let observed = self.examined.checked_add(1).ok_or(WorkError::Overflow)?;
        if observed > self.maximum {
            return Err(WorkError::BudgetExceeded {
                counter: "items_examined",
                observed,
                maximum: self.maximum,
            });
        }
        self.examined = observed;
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CommitFootprint<'a> {
    pub(crate) reads: Option<&'a CheckoutDependencies>,
    pub(crate) writes: &'a GenerationDiff,
}

impl CommitFootprint<'_> {
    /// Symmetric read/write independence. Incomplete deltas cannot certify it.
    pub(crate) fn independent(self, other: Self, maximum_items: u64) -> FootprintQuery {
        self.independent_all([other], maximum_items)
    }

    /// Validate the immutable proposal once across an authenticated cohort.
    pub(crate) fn independent_all(
        self,
        others: impl IntoIterator<Item = Self>,
        maximum_items: u64,
    ) -> FootprintQuery {
        let mut budget = QueryBudget {
            maximum: maximum_items,
            examined: 0,
        };
        let independent = (|| {
            let mut others = others.into_iter();
            let Some(first) = others.next() else {
                return Ok(true);
            };
            if first.writes.truncated {
                return Ok(false);
            }
            let Some(left) = PreparedFootprint::new(self, &mut budget)? else {
                return Ok(false);
            };
            for other in std::iter::once(first).chain(others) {
                budget.visit()?;
                let Some(right) = PreparedFootprint::new(other, &mut budget)? else {
                    return Ok(false);
                };
                if !left.independent(right, &mut budget)? {
                    return Ok(false);
                }
            }
            Ok(true)
        })();
        FootprintQuery {
            independent,
            items_examined: budget.examined,
        }
    }
}

#[derive(Clone, Copy)]
struct PreparedFootprint<'a>(CommitFootprint<'a>);

impl<'a> PreparedFootprint<'a> {
    fn new(
        footprint: CommitFootprint<'a>,
        budget: &mut QueryBudget,
    ) -> Result<Option<Self>, WorkError> {
        if footprint.writes.truncated || !ordered(footprint.writes, budget)? {
            return Ok(None);
        }
        Ok(Some(Self(footprint)))
    }

    fn independent(self, other: Self, budget: &mut QueryBudget) -> Result<bool, WorkError> {
        if !writes_disjoint(self, other, budget)? {
            return Ok(false);
        }
        if let Some(reads) = self.0.reads
            && !reads.disjoint_from(other.0.writes, budget)?
        {
            return Ok(false);
        }
        if let Some(reads) = other.0.reads
            && !reads.disjoint_from(self.0.writes, budget)?
        {
            return Ok(false);
        }
        Ok(true)
    }
}

impl CheckoutDependencies {
    /// Queries only preconditions belonging to changed identities. There is no
    /// generation probe and no file-content read. Cost is O(changes * log reads
    /// + affected preconditions), independent of unrelated captured reads.
    fn disjoint_from(
        &self,
        changes: &GenerationDiff,
        budget: &mut QueryBudget,
    ) -> Result<bool, WorkError> {
        if changes.truncated {
            return Ok(false);
        }
        for change in &changes.files {
            budget.visit()?;
            for (region, _) in self
                .captured
                .entries
                .range(DependencyRegion::FileRecord(change.file_id)..)
                .take_while(|(region, _)| region.file_id() == change.file_id)
            {
                budget.visit()?;
                if record_overlaps(region, change) {
                    return Ok(false);
                }
            }
        }
        for change in &changes.bindings {
            budget.visit()?;
            for (region, _) in self
                .captured
                .entries
                .range(DependencyRegion::FileRecord(change.directory_id)..)
                .take_while(|(region, _)| region.file_id() == change.directory_id)
            {
                budget.visit()?;
                if binding_overlaps(region, change) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

fn record_overlaps(region: &DependencyRegion, change: &FileRecordChange) -> bool {
    match region {
        DependencyRegion::FileRecord(_) => change.before != change.after,
        DependencyRegion::Metadata(_) => {
            change.before.map(|record| (record.kind, record.metadata))
                != change.after.map(|record| (record.kind, record.metadata))
        }
        DependencyRegion::FileLength(_)
        | DependencyRegion::ContentRange { .. }
        | DependencyRegion::SparseSeek { .. } => match (change.before, change.after) {
            (Some(before), Some(after))
                if before.kind == FileKind::Regular && after.kind == FileKind::Regular =>
            {
                before.payload != after.payload
            }
            _ => change.before != change.after,
        },
        DependencyRegion::DirectoryName { .. } | DependencyRegion::DirectoryRange { .. } => {
            !matches!((change.before, change.after), (Some(before), Some(after))
                if before.kind == FileKind::Directory && after.kind == FileKind::Directory)
        }
    }
}

fn binding_overlaps(region: &DependencyRegion, change: &DirectoryBindingChange) -> bool {
    match region {
        DependencyRegion::FileRecord(_) | DependencyRegion::DirectoryRange { .. } => true,
        DependencyRegion::DirectoryName { name, .. } => *name == change.name,
        DependencyRegion::Metadata(_)
        | DependencyRegion::FileLength(_)
        | DependencyRegion::ContentRange { .. }
        | DependencyRegion::SparseSeek { .. } => false,
    }
}

fn writes_disjoint(
    left: PreparedFootprint<'_>,
    right: PreparedFootprint<'_>,
    budget: &mut QueryBudget,
) -> Result<bool, WorkError> {
    let (left, right) = (left.0.writes, right.0.writes);
    let mut left_files = left.files.iter().peekable();
    let mut right_files = right.files.iter().peekable();
    if !acyclic_collections::page::merge::overlapping(
        &mut left_files,
        &mut right_files,
        |a, b| {
            budget.visit()?;
            Ok(a.file_id.cmp(&b.file_id))
        },
        |a, b| {
            Ok(a.zip(b)
                .is_none_or(|(a, b)| a.directory_children_only() && b.directory_children_only()))
        },
    )? {
        return Ok(false);
    }
    let mut left_bindings = left.bindings.iter().peekable();
    let mut right_bindings = right.bindings.iter().peekable();
    acyclic_collections::page::merge::overlapping(
        &mut left_bindings,
        &mut right_bindings,
        |a, b| {
            budget.visit()?;
            Ok((a.directory_id, &a.name).cmp(&(b.directory_id, &b.name)))
        },
        |a, b| Ok(a.zip(b).is_none()),
    )
}

fn ordered(changes: &GenerationDiff, budget: &mut QueryBudget) -> Result<bool, WorkError> {
    for pair in changes.files.windows(2) {
        budget.visit()?;
        if !matches!(pair, [a, b] if a.file_id < b.file_id) {
            return Ok(false);
        }
    }
    for pair in changes.bindings.windows(2) {
        budget.visit()?;
        if !matches!(pair, [a, b] if (a.directory_id, &a.name) < (b.directory_id, &b.name)) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
#[path = "tests/footprint.rs"]
mod tests;
