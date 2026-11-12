//! Original binding preparation and physical retirement share one keyed owner.
use super::*;
use acyclic_fs::kernel::allocation::{FixedIndex, RetainedArc, RetainedWeak};
use acyclic_stream::{OperationClaim, OperationTermination};

#[derive(Default)]
pub(super) struct Bindings {
    entries: crate::sync::SyncMutex<FixedIndex<RetainedWeak<Binding>, 32>>,
}

pub(in crate::local) struct Binding {
    index: RetainedArc<Bindings>,
    pub(super) key: [u8; 32],
    // Physical views omit auxiliary mounts but retain their complete source identity.
    auxiliary: [u8; 32],
    preparing: RetainedWeak<Preparation>,
    mounts: std::sync::OnceLock<std::sync::Weak<Mounts>>,
    // One primary view/capture cursor while any returned workspace is live.
    pub(super) workspace: AsyncMutex<std::sync::Weak<LocalWorkspace>>,
}

// The preparation result has a different lifetime from physical custody.
// Selections retain it; the physical binding refers back only weakly.
struct Preparation {
    completed: acyclic_stream::OperationObservation,
    mounts: std::sync::OnceLock<Arc<Mounts>>,
}

pub(super) struct Selection {
    pub(super) binding: RetainedArc<Binding>,
    result: SelectedResult,
}

enum SelectedResult {
    Preparing(RetainedArc<Preparation>),
    Ready(Arc<Mounts>),
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
        auxiliary: [u8; 32],
        work: &NativeMaterializationWork,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<(Selection, Option<OperationClaim<'a>>)> {
        // A partially inserted selection must retire after releasing the index.
        let mut selected = None;
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
            let result = binding
                .preparing
                .upgrade()
                .map(SelectedResult::Preparing)
                .or_else(|| binding.mounts.get()?.upgrade().map(SelectedResult::Ready));
            // The last physical owner may depart after its weak upgrade. Its
            // Drop needs this same index, including on an absent-result refusal.
            drop(entries);
            let result =
                result.ok_or_else(|| Error::Conflict("original binding result retired".into()))?;
            if binding.auxiliary != auxiliary {
                return Err(Error::Conflict(
                    "project already has a different live auxiliary binding".into(),
                ));
            }
            return Ok((Selection { binding, result }, None));
        }
        // The old value may already be in Drop, waiting for this same index.
        // Remove only this dead key; its late Drop checks exact pointer identity.
        entries.retire_if(key, |_| true);
        // The binding index already excludes a second producer. Only its funded
        // completion event is needed; do not index the same key a second time.
        let owner = OperationClaim::unindexed(|bytes, allocations| {
            work.claim(bytes, allocations, Default::default())
                .map_err(|cause| failure(work, cause))
        })?;
        entries
            .get_or_try_insert_with(key, &mut accounting, |accounting| {
                let preparing = RetainedArc::new_with(accounting, || Preparation {
                    completed: owner.observe(),
                    mounts: std::sync::OnceLock::new(),
                })?;
                let binding = RetainedArc::new_with(accounting, || Binding {
                    index: index.clone(),
                    key,
                    auxiliary,
                    workspace: AsyncMutex::new(std::sync::Weak::new()),
                    preparing: preparing.downgrade(),
                    mounts: std::sync::OnceLock::new(),
                })?;
                let weak = binding.downgrade();
                selected = Some(Selection {
                    binding,
                    result: SelectedResult::Preparing(preparing),
                });
                Ok(weak)
            })
            .map_err(storage)?;
        let selection = selected
            .ok_or_else(|| Error::Conflict("binding insertion lost its original owner".into()))?;
        Ok((selection, Some(owner)))
    }
}

pub(super) fn volume_key(volume: &VolumeRef) -> Result<[u8; 32]> {
    Ok(*blake3::hash(volume.storage_name(&mut None)?.as_bytes()).as_bytes())
}

/// The complete auxiliary selection, independent of the caller's binding order.
pub(super) fn auxiliary_key(bindings: &[crate::agent::WorkspaceEntry]) -> Result<[u8; 32]> {
    let mut ordered: Vec<_> = bindings.iter().collect();
    ordered.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    // Entries contain only typed structs, enums and scalar identities: there
    // are no unordered JSON maps to canonicalize. Stream their serialization
    // directly into the shared digest instead of building and sorting a Value.
    crate::executor::json::serialized_json_digest(&ordered)
}

impl Selection {
    pub(super) fn publish(&self, mounts: &Arc<Mounts>) -> Result<()> {
        let SelectedResult::Preparing(preparing) = &self.result else {
            return Err(Error::Conflict(
                "binding result has no original producer".into(),
            ));
        };
        preparing
            .mounts
            .set(Arc::clone(mounts))
            .map_err(|_| Error::Conflict("original binding already published".into()))?;
        self.binding
            .mounts
            .set(Arc::downgrade(mounts))
            .map_err(|_| Error::Conflict("original binding already published".into()))?;
        Ok(())
    }

    pub(super) async fn mounts(self) -> Result<Arc<Mounts>> {
        let preparing = match self.result {
            SelectedResult::Ready(mounts) => return Ok(mounts),
            SelectedResult::Preparing(preparing) => preparing,
        };
        if preparing.completed.terminated().await != OperationTermination::Released {
            return Err(Error::Conflict(
                "original binding producer dropped before publication".into(),
            ));
        }
        preparing
            .mounts
            .get()
            .map(Arc::clone)
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
        let (original, producer) = Bindings::select(&index, key, [0; 32], &work, &cancellation)?;
        let producer =
            producer.ok_or_else(|| Error::Conflict("original producer missing".into()))?;
        let old = original.binding.downgrade();
        let mut peers = Vec::new();
        for number in 0_u32..1024 {
            let peer = *blake3::hash(&number.to_le_bytes()).as_bytes();
            peers.push(Bindings::select(
                &index,
                peer,
                [0; 32],
                &work,
                &cancellation,
            )?);
        }
        let before = work.receipt().items_examined;
        let (observer, duplicate) = Bindings::select(&index, key, [0; 32], &work, &cancellation)?;
        assert!(
            duplicate.is_none(),
            "same-key observer created another producer"
        );
        assert!(std::ptr::eq(old.as_ptr(), &*observer.binding));
        assert!(
            work.receipt().items_examined - before <= 9 * 32 + 1,
            "binding lookup grew beyond the fixed-width path"
        );
        {
            let mut observed = std::pin::pin!(observer.mounts());
            assert!(observed.as_mut().now_or_never().is_none());
            drop(producer);
            assert!(
                matches!(
                    observed.as_mut().now_or_never(),
                    Some(Err(Error::Conflict(_)))
                ),
                "lost original producer did not finish its observer"
            );
        }
        drop(original);
        assert!(old.upgrade().is_none());
        let (replacement, successor) =
            Bindings::select(&index, key, [0; 32], &work, &cancellation)?;
        assert!(successor.is_some());
        index.retire(key, old.as_ptr());
        let retained = Bindings::select(&index, key, [0; 32], &work, &cancellation);
        assert!(
            retained.is_ok(),
            "late retirement removed the replacement owner"
        );
        let (selected, duplicate) = retained?;
        assert!(
            duplicate.is_none(),
            "late retirement removed the replacement owner"
        );
        assert!(std::ptr::eq(&*selected.binding, &*replacement.binding));
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
