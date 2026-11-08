//! Original binding preparation and physical retirement share one keyed owner.
use super::*;
use acyclic_fs::kernel::allocation::{FixedIndex, RetainedArc, RetainedWeak};
use acyclic_stream::{OperationAttempt, OperationClaim, OperationClaims, OperationTermination};

#[derive(Default)]
pub(super) struct Bindings {
    entries: crate::sync::SyncMutex<FixedIndex<RetainedWeak<Binding>, 32>>,
    preparing: OperationClaims,
}

pub(super) struct Binding {
    index: RetainedArc<Bindings>,
    key: [u8; 32],
    completed: acyclic_stream::OperationObservation,
    mounts: crate::sync::SyncMutex<Option<std::sync::Weak<Mounts>>>,
}

impl Bindings {
    fn retire(&self, key: [u8; 32], original: *const Binding) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retire_if(key, |entry| std::ptr::eq(entry.as_ptr(), original));
    }

    pub(super) fn new(cancellation: &acyclic_fs::CancellationToken) -> Result<RetainedArc<Self>> {
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        RetainedArc::new(
            Self::default(),
            &mut KernelAccounting::NativeOriginal {
                work: &work,
                cancellation,
            },
        )
        .map_err(storage)
    }

    pub(super) fn select<'a>(
        index: &'a RetainedArc<Self>,
        key: [u8; 32],
        work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<(RetainedArc<Binding>, Option<OperationClaim<'a>>)> {
        let mut entries = index
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut accounting = KernelAccounting::NativeOriginal { work, cancellation };
        if let Some(binding) = entries
            .get(key, &mut accounting)
            .map_err(storage)?
            .and_then(RetainedWeak::upgrade)
        {
            return Ok((binding, None));
        }
        // The old value may already be in Drop, waiting for this same index.
        // Remove only this dead key; its late Drop checks exact pointer identity.
        entries.retire_if(key, |_| true);
        let mut operation = [0; 48];
        operation.split_at_mut(key.len()).0.copy_from_slice(&key);
        let owner = match index.preparing.claim(
            operation,
            || {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })
                .map_err(|cause| failure(work, cause))
            },
            |bytes, allocations| {
                work.claim(bytes, allocations, Default::default())
                    .map_err(|cause| failure(work, cause))
            },
        )? {
            OperationAttempt::Owned(owner) => owner,
            OperationAttempt::Observed(_) => {
                return Err(Error::Conflict(
                    "binding producer lost its retained result owner".into(),
                ));
            }
        };
        let mut selected = None;
        entries
            .get_or_try_insert_with(key, &mut accounting, |accounting| {
                let binding = RetainedArc::new_with(accounting, || Binding {
                    index: index.clone(),
                    key,
                    completed: owner.observe(),
                    mounts: crate::sync::SyncMutex::new(None),
                })?;
                let weak = binding.downgrade();
                selected = Some(binding);
                Ok(weak)
            })
            .map_err(storage)?;
        let binding = selected
            .ok_or_else(|| Error::Conflict("binding insertion lost its original owner".into()))?;
        Ok((binding, Some(owner)))
    }
}

impl Binding {
    pub(super) fn publish(&self, mounts: &Arc<Mounts>) {
        *self
            .mounts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(mounts));
    }

    pub(super) async fn mounts(&self) -> Result<Arc<Mounts>> {
        if self.completed.terminated().await != OperationTermination::Released {
            return Err(Error::Conflict(
                "original binding producer dropped before publication".into(),
            ));
        }
        self.mounts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Error::Conflict("original binding result retired".into()))
    }
}

impl Drop for Binding {
    fn drop(&mut self) {
        self.index.retire(self.key, self);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use futures::FutureExt as _;

    #[test]
    fn original_preparation_has_finite_generation_and_credit() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let index = Bindings::new(&cancellation)?;
        let baseline = work.live_allocation_bytes();
        let key = *blake3::hash(b"original binding").as_bytes();
        let (original, producer) = Bindings::select(&index, key, &work, &cancellation)?;
        let producer =
            producer.ok_or_else(|| Error::Conflict("original producer missing".into()))?;
        let old = original.downgrade();
        let mut peers = Vec::new();
        for number in 0_u32..1024 {
            let peer = *blake3::hash(&number.to_le_bytes()).as_bytes();
            peers.push(Bindings::select(&index, peer, &work, &cancellation)?);
        }
        let before = work.receipt().items_examined;
        let (observer, duplicate) = Bindings::select(&index, key, &work, &cancellation)?;
        assert!(
            duplicate.is_none(),
            "same-key observer created another producer"
        );
        assert!(std::ptr::eq(old.as_ptr(), &*observer));
        assert!(
            work.receipt().items_examined - before <= 9 * 32 + 1,
            "binding lookup grew beyond the fixed-width path"
        );
        assert!(observer.mounts().now_or_never().is_none());
        drop(producer);
        assert!(
            matches!(
                observer.mounts().now_or_never(),
                Some(Err(Error::Conflict(_)))
            ),
            "lost original producer did not finish its observer"
        );
        drop(observer);
        drop(original);
        assert!(old.upgrade().is_none());
        let (replacement, successor) = Bindings::select(&index, key, &work, &cancellation)?;
        assert!(successor.is_some());
        index.retire(key, old.as_ptr());
        let retained = Bindings::select(&index, key, &work, &cancellation);
        assert!(
            retained.is_ok(),
            "late retirement removed the replacement owner"
        );
        let (selected, duplicate) = retained?;
        assert!(
            duplicate.is_none(),
            "late retirement removed the replacement owner"
        );
        assert!(std::ptr::eq(&*selected, &*replacement));
        drop(duplicate);
        drop(selected);
        drop(successor);
        drop(replacement);
        drop(old);
        drop(peers);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline,
            "retired binding generations retained original allocation credit"
        );
        Ok(())
    }
}

fn failure(
    work: &NativeMaterializationWork,
    cause: acyclic_fs::NativeTreeMaterializationError,
) -> Error {
    Error::NativeConstruction {
        cause,
        work: Box::new(work.receipt()),
    }
}
