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

/// A demand-driven delta retaining both exact generation pins and its original
/// work and cancellation. Rows follow the deterministic file/edge frontier,
/// with moved subtrees expanded before the next file; they are not path-sorted.
/// Dropping the cursor retires its physical state before its scratch credit.
pub struct ChangedPaths<A: 'static, O: 'static> {
    owner: ReadOwner<State<A, O>>,
}

struct State<A: 'static, O: 'static> {
    // Frontier backing retires before the generation pins it reads.
    cursor: Option<Cursor<'static>>,
    checkouts: [Option<Owned<Checkout<A, O>>>; 2],
    failed: bool,
}

impl<A, O> Default for State<A, O> {
    fn default() -> Self {
        Self {
            cursor: None,
            checkouts: [None, None],
            failed: false,
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
        let result = async {
            if state.failed {
                return Err(FsError::InvalidDiff.into());
            }
            state.failed = true;
            let roots = state
                .checkouts
                .each_ref()
                .map(|checkout| checkout.as_ref().map(|checkout| checkout.value.root()));
            let source = &state.checkouts[0]
                .as_ref()
                .ok_or(FsError::InvalidDiff)?
                .value;
            let cancellation = accounting.cancellation();
            let row = state
                .cursor
                .as_mut()
                .ok_or(FsError::InvalidDiff)?
                .next(source.objects(), roots, &mut accounting, cancellation)
                .await?;
            let row = row
                .map(|row| {
                    Retained::from_owned(row, &mut accounting).map(|row| ChangedPathEntry { row })
                })
                .transpose()?;
            state.failed = false;
            Ok::<_, WorkspaceError>(row)
        }
        .await;
        let work = accounting.spent();
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    /// Cumulative work charged to the original read frame, including completed
    /// calls that ended in cancellation or a typed refusal.
    pub fn work(&mut self) -> WorkCounters {
        self.owner.borrow().1.spent()
    }
}
