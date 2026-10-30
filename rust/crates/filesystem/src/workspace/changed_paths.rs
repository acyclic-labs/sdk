//! Owned immutable changed-path continuation, retaining the original read frame.

use super::*;
use crate::kernel::Owned;
use crate::kernel::allocation::retained::{ReadOwner, Retained};
use crate::kernel::changed_paths::Cursor;
use crate::{OperationFailure, OperationReceipt, WorkCounters};

/// One changed path retaining its original output allocation independently of
/// the cursor. The value is destroyed before that allocation is released.
pub struct ChangedPathEntry {
    row: Retained<ChangedPath>,
}

impl ChangedPathEntry {
    /// Borrows the exact immutable endpoint facts and path.
    pub fn value(&self) -> &ChangedPath {
        &self.row.value
    }
}

/// A demand-driven authored-namespace delta retaining both exact generation pins
/// and its original work and cancellation. Rows follow the file/edge frontier,
/// with moved subtrees expanded before the next file; they are not path-sorted.
/// Dropping the cursor retires its physical state before its scratch credit.
/// Source observations and identity shadows belong to a source-backed view's
/// separate [`crate::LazySnapshotRef`].
pub struct ChangedPaths<A: 'static, O: 'static> {
    owner: ReadOwner<State<A, O>>,
}

struct State<A: 'static, O: 'static> {
    pending: Option<ChangedPathEntry>,
    // Frontier backing retires before the generation pins it reads.
    cursor: Option<Cursor<'static>>,
    checkouts: [Option<Owned<Checkout<A, O>>>; 2],
    failed: bool,
    complete: bool,
}

impl<A, O> Default for State<A, O> {
    fn default() -> Self {
        Self {
            pending: None,
            cursor: None,
            checkouts: [None, None],
            failed: false,
            complete: false,
        }
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ChangedPaths<A, O> {
    pub(super) async fn open<W: crate::facade::FsWork>(
        from: &Generation<A, O>,
        to: &Generation<A, O>,
        work: W,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Self>, WorkspaceError> {
        let accounting = work.accounting(cancellation);
        let initial = accounting.spent();
        if !from
            .workspace
            .volume
            .fs
            .same_deployment(&to.workspace.volume.fs)
        {
            return Err(OperationFailure::new(
                WorkspaceError::ForeignGeneration,
                initial,
            ));
        }
        let mut owner = ReadOwner::<State<A, O>>::new(accounting)
            .map_err(|error| OperationFailure::new(WorkspaceError::from(error), initial))?;
        let result = async {
            let (state, mut accounting) = owner.borrow();
            let cancellation = accounting.cancellation();
            state.checkouts[0] = Some(
                from.workspace
                    .open_checkout(
                        GenerationSelector::Exact(from.id),
                        CheckoutMode::read_only_pinned(),
                        &mut accounting,
                        cancellation,
                    )
                    .await?,
            );
            state.checkouts[1] = Some(
                to.workspace
                    .open_checkout(
                        GenerationSelector::Exact(to.id),
                        CheckoutMode::read_only_pinned(),
                        &mut accounting,
                        cancellation,
                    )
                    .await?,
            );
            let roots = state
                .checkouts
                .each_ref()
                .map(|checkout| checkout.as_ref().map(|checkout| checkout.value.root()));
            state.cursor = Some(Cursor::new(
                roots,
                to.workspace.volume.config(),
                &mut accounting,
            )?);
            Ok::<_, WorkspaceError>(())
        }
        .await;
        let spent = owner.borrow().1.spent();
        match result {
            Ok(()) => Ok(OperationReceipt {
                value: Self { owner },
                work: spent,
            }),
            Err(error) => {
                drop(owner);
                Err(OperationFailure::new(error, spent))
            }
        }
    }

    /// Produces one complete row without collecting or recomputing its prefix.
    /// Work in each receipt is cumulative from cursor admission. The original
    /// total budget remains in force. A failed or withdrawn call is terminal;
    /// drop this cursor to retire its retained frontier and generation pins.
    pub async fn next(
        &mut self,
    ) -> crate::MeasuredResult<OperationReceipt<Option<ChangedPathEntry>>, WorkspaceError> {
        let (state, mut accounting) = self.owner.borrow();
        let result = state.prepare_next(&mut accounting).await;
        let work = accounting.spent();
        result
            .map(|()| OperationReceipt {
                value: state.pending.take(),
                work,
            })
            .map_err(|error| OperationFailure::new(error, work))
    }

    /// Borrows the next exact row while retaining its original allocation and
    /// traversal position. An output encoder can check its actual byte credit
    /// before consuming the row with `next`. Repeated borrows do no additional
    /// source work or copying; original cancellation is checked on every call.
    pub async fn peek(
        &mut self,
    ) -> crate::MeasuredResult<OperationReceipt<Option<&ChangedPath>>, WorkspaceError> {
        let (state, mut accounting) = self.owner.borrow();
        let result = state.prepare_next(&mut accounting).await;
        let work = accounting.spent();
        result
            .map(|()| OperationReceipt {
                value: state.pending.as_ref().map(ChangedPathEntry::value),
                work,
            })
            .map_err(|error| OperationFailure::new(error, work))
    }

    /// Cumulative work charged to the original read frame, including completed
    /// calls that ended in cancellation or a typed refusal.
    pub fn work(&mut self) -> WorkCounters {
        self.owner.borrow().1.spent()
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> State<A, O> {
    async fn prepare_next(
        &mut self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), WorkspaceError> {
        if self.failed {
            return Err(FsError::InvalidDiff.into());
        }
        self.failed = true;
        if self.pending.is_none() && !self.complete {
            let roots = self
                .checkouts
                .each_ref()
                .map(|checkout| checkout.as_ref().map(|checkout| checkout.value.root()));
            let source = &self.checkouts[0]
                .as_ref()
                .ok_or(FsError::InvalidDiff)?
                .value;
            let cancellation = accounting.cancellation();
            let row = self
                .cursor
                .as_mut()
                .ok_or(FsError::InvalidDiff)?
                .next(source.objects(), roots, accounting, cancellation)
                .await?;
            self.pending = row
                .map(|row| {
                    Retained::from_owned(row, accounting).map(|row| ChangedPathEntry { row })
                })
                .transpose()?;
            self.complete = self.pending.is_none();
        } else {
            accounting.check()?;
        }
        self.failed = false;
        Ok(())
    }
}
