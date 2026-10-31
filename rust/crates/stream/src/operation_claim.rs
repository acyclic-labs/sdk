//! Process-local exclusion shared by every adapter of one provider owner.

mod resources;
mod termination;
pub use termination::{OperationObservation, OperationTermination};

use acyclic_collections::{FixedIndex, IndexWork};

type Storage = Box<dyn Send + Sync>;

struct Registration {
    observation: OperationObservation,
    armed: bool,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if self.armed {
            self.observation
                .publish(OperationTermination::ProducerDropped);
        }
    }
}

/// Live operation ownership independent of persisted crash-recovery records.
pub struct OperationClaims {
    owner: [u8; 16],
    active: crate::query_sync::SyncMutex<FixedIndex<Registration, Storage, 48>>,
    resources: resources::Resources,
}

impl Default for OperationClaims {
    fn default() -> Self {
        Self {
            owner: crate::new_identity(),
            active: crate::query_sync::SyncMutex::new(FixedIndex::default()),
            resources: resources::Resources::default(),
        }
    }
}

/// Exclusive ownership of one exact operation until physical guard retirement.
pub struct OperationClaim<'a> {
    claims: &'a OperationClaims,
    key: [u8; 48],
    observation: Option<OperationObservation>,
}

/// Atomic acquisition or an observation pinned to the existing producer.
pub enum OperationAttempt<'a> {
    /// Exclusive producer custody, acquired before replay observation.
    Owned(OperationClaim<'a>),
    /// The exact predecessor observed under the same exclusion lock.
    Observed(OperationObservation),
}

impl<'a> OperationAttempt<'a> {
    /// Transfers producer custody, or preserves the occupied generation's event.
    pub fn into_owner(self) -> Result<OperationClaim<'a>, OperationObservation> {
        match self {
            Self::Owned(owner) => Ok(owner),
            Self::Observed(observation) => Err(observation),
        }
    }
}

// Original admission is borrowed for this traversal. Only its owned allocation
// tokens enter the index; no copied allowance or replacement ledger is created.
struct Accounting<V, A> {
    visit: V,
    admit: A,
}

impl<H, E, V, A> IndexWork for Accounting<V, A>
where
    H: Send + Sync + 'static,
    V: FnMut() -> Result<(), E>,
    A: FnMut(u64, u64) -> Result<H, E>,
{
    type Error = E;
    type Allocation = Storage;

    fn check(&mut self) -> Result<(), E> {
        (self.visit)()
    }

    fn charge_items(&mut self, items: u64) -> Result<(), E> {
        for _ in 0..items {
            (self.visit)()?;
        }
        Ok(())
    }

    fn claim(&mut self, bytes: u64) -> Result<Storage, E> {
        // Fund both the index node and the erased original token before either
        // allocation. A zero-sized token has no physical Box allocation.
        let token_bytes = std::mem::size_of::<H>() as u64;
        let token = (self.admit)(bytes + token_bytes, 1 + u64::from(token_bytes != 0))?;
        Ok(Box::new(token))
    }
}

impl OperationClaims {
    /// Returns the sole resource of this type retained by this provider owner.
    /// Forwarding adapters therefore share it even when constructed separately.
    ///
    /// `admit` funds the exact registry node before allocation; `create` returns
    /// an already funded resource. Initialization runs under the owner lock and
    /// must not recursively access this registry. The resource must not retain
    /// its provider owner; use a weak reference when it needs such a handle.
    ///
    /// # Errors
    /// Returns a visit, admission or initialization error without changing the
    /// registry or discarding any previously initialized resource.
    pub fn shared_resource<T, H, E, C>(
        &self,
        context: &mut C,
        visit: impl FnMut(&mut C) -> Result<(), E>,
        admit: impl FnOnce(u64, &mut C) -> Result<H, E>,
        create: impl FnOnce(&mut C) -> Result<std::sync::Arc<T>, E>,
    ) -> Result<std::sync::Arc<T>, E>
    where
        T: Send + Sync + 'static,
        H: Send + Sync + 'static,
    {
        self.resources.get(context, visit, admit, create)
    }

    /// Stable process-local identity shared by adapters of this provider owner.
    /// This value identifies live exclusion state; it conveys no authority.
    #[must_use]
    pub const fn owner_identity(&self) -> [u8; 16] {
        self.owner
    }

    /// Claims a full-width operation key, with work bounded by key width.
    /// A duplicate visits only its fixed-depth path and allocates nothing.
    /// `admit(bytes, allocations)` retains the original owned funding for the
    /// exact index node and its erased token. It can run twice for insertion.
    /// Refusal or unwinding publishes no partial registration.
    pub fn claim<H, E>(
        &self,
        key: [u8; 48],
        visit: impl FnMut() -> Result<(), E>,
        admit: impl FnMut(u64, u64) -> Result<H, E>,
    ) -> Result<OperationAttempt<'_>, E>
    where
        H: Send + Sync + 'static,
    {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut inserted = false;
        let registration =
            active.get_or_try_insert_with(key, &mut Accounting { visit, admit }, |work| {
                inserted = true;
                Ok(Registration {
                    observation: OperationObservation::create(&mut work.admit)?,
                    armed: true,
                })
            })?;
        let observation = registration.observation.clone();
        Ok(if inserted {
            OperationAttempt::Owned(OperationClaim {
                claims: self,
                key,
                observation: Some(observation),
            })
        } else {
            OperationAttempt::Observed(observation)
        })
    }
}

impl OperationClaim<'_> {
    /// Pins this producer's exact-generation termination event before handoff.
    pub fn observe(&self) -> OperationObservation {
        self.observation
            .as_ref()
            .expect("live operation owner")
            .clone()
    }

    /// Explicitly retires producer custody. This does not invent a durable result.
    pub fn release(mut self) {
        self.retire(OperationTermination::Released);
    }

    fn retire(&mut self, terminal: OperationTermination) {
        let Some(observation) = self.observation.take() else {
            return;
        };
        let removed = self
            .claims
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove_if(self.key, |registration| {
                registration.observation.same_generation(&observation)
            });
        // Destroy the detached node before releasing its original funding.
        // The guard's borrow excludes destruction of the provider owner.
        let (_, mut registration, storage) = removed
            .expect("live operation claim disappeared")
            .into_parts();
        registration.armed = false;
        drop(registration);
        drop(storage);
        observation.publish(terminal);
    }
}

impl Drop for OperationClaim<'_> {
    fn drop(&mut self) {
        self.retire(OperationTermination::ProducerDropped);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn duplicate_claim_work_is_bounded_by_key_width_and_never_allocates() {
        for population in [1_u32, 4096] {
            let claims = OperationClaims::default();
            let owners: Vec<_> = (0..population)
                .map(|value| {
                    let mut key = [0; 48];
                    key[44..].copy_from_slice(&value.to_be_bytes());
                    claims
                        .claim(key, || Ok::<_, ()>(()), |_, _| Ok(()))
                        .unwrap()
                        .into_owner()
                        .unwrap()
                })
                .collect();
            let visits = Cell::new(0_u64);
            assert!(
                claims
                    .claim(
                        [0; 48],
                        || {
                            visits.set(visits.get() + 1);
                            Ok::<_, ()>(())
                        },
                        |_, _| -> Result<(), ()> { panic!("duplicate allocated") }
                    )
                    .unwrap()
                    .into_owner()
                    .is_err()
            );
            assert!(visits.get() <= 1 + 3 * (48 * 8 + 1) + 2 * 48);
            drop(owners);
            assert!(
                claims
                    .claim([0; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                    .unwrap()
                    .into_owner()
                    .is_ok()
            );
        }
    }

    #[test]
    fn panicking_or_refused_admission_preserves_original_ownership() {
        let claims = OperationClaims::default();
        let original = claims
            .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
            .unwrap()
            .into_owner()
            .unwrap();
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = claims.claim(
                [2; 48],
                || Ok::<_, ()>(()),
                |_, _| -> Result<(), ()> { panic!("admission failed") },
            );
        }));
        assert!(failed.is_err());
        assert!(
            claims
                .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                .unwrap()
                .into_owner()
                .is_err()
        );
        assert!(
            claims
                .claim([2; 48], || Ok::<_, ()>(()), |_, _| Err::<(), _>(()))
                .is_err()
        );
        assert!(
            claims
                .claim([2; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                .unwrap()
                .into_owner()
                .is_ok()
        );
        drop(original);
        assert!(
            claims
                .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                .unwrap()
                .into_owner()
                .is_ok()
        );
    }
}
