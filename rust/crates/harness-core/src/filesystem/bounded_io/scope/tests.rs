use super::*;
use std::cell::Cell;

struct Witness<'a> {
    payload: String,
    io: &'a PreparationIo,
    work: &'a acyclic_fs::NativeMaterializationWork,
    baseline: u64,
    observed: &'a Cell<u64>,
}

impl Drop for Witness<'_> {
    fn drop(&mut self) {
        self.observed
            .set(if self.io.original_native_work().is_some() {
                self.work.live_allocation_bytes() - self.baseline
            } else {
                self.io.owned_bytes().unwrap_or(0)
            });
        self.payload.clear();
    }
}

#[test]
fn scope_finish_cancellation_preserves_output_custody_until_destruction() -> Result<()> {
    for native in [false, true] {
        for detached_control in [false, true] {
            let cancellation = CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let scope = PreparationScope::new(Some(&io))?;
            let observed = Cell::new(0);
            let payload =
                crate::executor::stock_string_copy(&"paid value".repeat(100), scope.io())?;
            let retained = payload.capacity() as u64;
            let value = Witness {
                payload,
                io: scope
                    .io()
                    .ok_or_else(|| Error::Invalid("missing scope".into()))?,
                work: &work,
                baseline,
                observed: &observed,
            };
            cancellation.cancel();
            if detached_control {
                // Actual previous shape: the native output leaves its owner
                // before a fallible cancellation check in substitute().
                let local = scope
                    .io()
                    .ok_or_else(|| Error::Invalid("missing scope".into()))?;
                let mut hold = local.guard_owned(local.owned_bytes()?)?;
                assert!(hold.substitute(retained).is_err());
                drop(hold);
            } else {
                assert!(scope.finish(retained).is_err());
            }
            drop(value);
            if detached_control {
                assert_eq!(observed.get(), 0);
            } else {
                assert!(
                    observed.get() >= retained,
                    "output outlived its original custody"
                );
            }
            drop(scope);
            assert_eq!(io.owned_bytes()?, 0);
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[test]
fn scope_finish_moves_only_live_output_and_releases_scratch() -> Result<()> {
    for native in [false, true] {
        let cancellation = CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let scope = PreparationScope::new(Some(&io))?;
        let value = crate::executor::stock_string_copy("retained output", scope.io())?;
        let scratch = crate::executor::stock_string_copy("discarded scratch", scope.io())?;
        let retained = value.capacity() as u64;
        drop(scratch);
        scope.finish(retained)?;
        drop(scope);
        assert_eq!(io.owned_bytes()?, retained);
        let hold = io.guard_owned(retained)?;
        drop(value);
        drop(hold);
        assert_eq!(io.owned_bytes()?, 0);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[test]
fn scope_transfer_preserves_parent_and_sibling_custody_on_one_ledger() -> Result<()> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let parent = PreparationIo::from_budget(budget, &cancellation)?;
    parent.retain_owned(41)?;
    let sibling = parent.scoped()?;
    sibling.retain_owned(73)?;
    {
        let scope = PreparationScope::new(Some(&parent))?;
        let local = scope
            .io()
            .ok_or_else(|| Error::Invalid("missing scope".into()))?;
        local.retain_owned(101)?;
        assert_eq!(parent.owned_bytes()?, 41);
        assert_eq!(sibling.owned_bytes()?, 73);
        scope.finish(89)?;
        assert_eq!(local.owned_bytes()?, 0);
        assert_eq!(parent.owned_bytes()?, 130);
    }
    drop(sibling);
    assert_eq!(parent.owned_bytes()?, 130);
    parent.release(130)?;
    assert_eq!(parent.owned_bytes()?, 0);
    assert_eq!(
        parent.remaining()?.peak_allocation_bytes,
        budget.peak_allocation_bytes
    );
    Ok(())
}

#[test]
fn standalone_transfer_rejects_another_ledger_without_releasing_either_owner() -> Result<()> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let source = PreparationIo::from_budget(budget, &cancellation)?;
    let target = PreparationIo::from_budget(budget, &cancellation)?;
    source.retain_owned(31)?;
    target.retain_owned(47)?;
    let (
        PreparationIo::Owned {
            custody: source_custody,
            ..
        },
        PreparationIo::Owned {
            custody: target_custody,
            ..
        },
    ) = (&source, &target)
    else {
        return Err(Error::Invalid("expected standalone custody".into()));
    };
    assert!(source_custody.transfer_to(target_custody).is_err());
    assert_eq!(source.owned_bytes()?, 31);
    assert_eq!(target.owned_bytes()?, 47);
    Ok(())
}
