//! Composer traversal storage stays on its original native ledger.
use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};

pub(super) type NativePath = Held<String, NativeAllocation>;

fn allocate<T>(
    work: &NativeMaterializationWork,
    bytes: u64,
    copied: u64,
    build: impl FnOnce() -> Result<T>,
    capacity: impl FnOnce(&T) -> u64,
) -> Result<Held<T, NativeAllocation>> {
    let allocation = work.claim(
        bytes,
        u64::from(bytes != 0),
        WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        },
    )?;
    let mut held = Held {
        value: build()?,
        _allocation: allocation,
    };
    held._allocation.reconcile_capacity(capacity(&held.value))?;
    Ok(held)
}

pub(super) fn path(work: &NativeMaterializationWork, parts: &[&str]) -> Result<NativePath> {
    let bytes = parts
        .iter()
        .try_fold(0usize, |bytes, part| bytes.checked_add(part.len()))
        .ok_or_else(|| Error::Invalid("composer path length overflow".into()))?;
    allocate(
        work,
        bytes as u64,
        bytes as u64,
        || {
            let mut value = String::new();
            value.try_reserve_exact(bytes).map_err(storage)?;
            for part in parts {
                value.push_str(part);
            }
            Ok(value)
        },
        |value| value.capacity() as u64,
    )
}

pub(super) struct PathFrontier {
    storage: Held<Vec<NativePath>, NativeAllocation>,
}

impl PathFrontier {
    pub(super) fn new(work: &NativeMaterializationWork) -> Result<Self> {
        Ok(Self {
            storage: allocate(work, 0, 0, || Ok(Vec::new()), |_| 0)?,
        })
    }

    pub(super) fn push(&mut self, path: NativePath) -> Result<()> {
        if self.storage.value.len() == self.storage.value.capacity() {
            let capacity = self
                .storage
                .value
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(1))
                .ok_or_else(|| Error::Invalid("composer frontier length overflow".into()))?;
            let bytes = capacity
                .checked_mul(std::mem::size_of::<NativePath>())
                .ok_or_else(|| Error::Invalid("composer frontier storage overflow".into()))?;
            let copied = self
                .storage
                .value
                .len()
                .checked_mul(std::mem::size_of::<NativePath>())
                .ok_or_else(|| Error::Invalid("composer frontier copy overflow".into()))?;
            // Admit the entire replacement while the original backing remains live.
            let mut replacement = allocate(
                self.storage._allocation.work(),
                bytes as u64,
                copied as u64,
                || {
                    let mut value = Vec::new();
                    value.try_reserve_exact(capacity).map_err(storage)?;
                    Ok(value)
                },
                |value: &Vec<NativePath>| {
                    (value.capacity() * std::mem::size_of::<NativePath>()) as u64
                },
            )?;
            replacement.value.append(&mut self.storage.value);
            std::mem::swap(&mut self.storage, &mut replacement);
            // Empty old storage is freed before its original allocation guard.
        }
        self.storage.value.push(path);
        Ok(())
    }

    pub(super) fn pop(&mut self) -> Option<NativePath> {
        self.storage.value.pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger(budget: acyclic_fs::WorkBudget) -> NativeMaterializationWork {
        NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new()).unwrap()
    }

    #[test]
    fn refused_storage_and_copy_never_invoke_the_allocator() {
        for budget in [
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
                ..acyclic_fs::performance::BOUNDARY_BUDGET
            },
            acyclic_fs::WorkBudget {
                bytes_copied: 0,
                ..acyclic_fs::performance::BOUNDARY_BUDGET
            },
        ] {
            let work = ledger(budget);
            let baseline = work.live_allocation_bytes();
            let built = std::cell::Cell::new(false);
            let result = allocate(
                &work,
                1,
                1,
                || {
                    built.set(true);
                    Ok(vec![0u8])
                },
                |v| v.capacity() as u64,
            );
            assert!(result.is_err());
            assert!(!built.get(), "refused storage was built before admission");
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }

    #[test]
    fn cancelled_storage_never_invokes_the_allocator() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .unwrap();
        let baseline = work.live_allocation_bytes();
        cancellation.cancel();
        let built = std::cell::Cell::new(false);
        let result = allocate(
            &work,
            1,
            1,
            || {
                built.set(true);
                Ok(vec![0u8])
            },
            |v| v.capacity() as u64,
        );
        assert!(result.is_err());
        assert!(!built.get());
        assert_eq!(work.live_allocation_bytes(), baseline);
    }

    #[test]
    fn frontier_and_popped_paths_keep_custody_until_their_storage_drops() -> Result<()> {
        let work = ledger(acyclic_fs::performance::BOUNDARY_BUDGET);
        let before = work.live_allocation_bytes();
        let mut frontier = PathFrontier::new(&work)?;
        for name in ["one", "two", "three"] {
            frontier.push(path(&work, &["nested/", name])?)?;
        }
        let popped = frontier.pop().expect("path");
        assert_eq!(popped.value, "nested/three");
        let retained = popped.value.capacity() as u64;
        drop(frontier);
        assert_eq!(work.live_allocation_bytes(), before + retained);
        drop(popped);
        assert_eq!(work.live_allocation_bytes(), before);
        Ok(())
    }

    #[test]
    fn refused_frontier_growth_preserves_existing_path_custody() -> Result<()> {
        let frontier_bytes =
            (3 * std::mem::size_of::<NativePath>() + "first".len() + "second".len() - 1) as u64;
        let work = ledger(acyclic_fs::WorkBudget {
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + frontier_bytes,
            ..acyclic_fs::performance::BOUNDARY_BUDGET
        });
        let baseline = work.live_allocation_bytes();
        let mut frontier = PathFrontier::new(&work)?;
        frontier.push(path(&work, &["first"])?)?;
        let original = work.live_allocation_bytes();
        assert!(frontier.push(path(&work, &["second"])?).is_err());
        assert_eq!(work.live_allocation_bytes(), original);
        assert_eq!(frontier.pop().expect("original").value, "first");
        drop(frontier);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
