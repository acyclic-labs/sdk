//! Payload ownership shared by cache entries and rendered views.

use acyclic_stream::{OwnedBytes, OwnedBytesBacking};
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

#[derive(Default)]
struct State {
    owners: BTreeMap<OwnedBytesBacking, Weak<Charge>>,
    bytes: usize,
}

fn locked(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct Charge {
    // Keep the allocation alive through index removal, including when the
    // last view and an admission on another thread race.
    backing: OwnedBytesBacking,
    state: Arc<Mutex<State>>,
}

impl Drop for Charge {
    fn drop(&mut self) {
        let mut state = locked(&self.state);
        if state
            .owners
            .get(&self.backing)
            .is_some_and(|owner| std::ptr::eq(owner.as_ptr(), self))
        {
            state.owners.remove(&self.backing);
            state.bytes -= self.backing.capacity();
        }
    }
}

/// Immutable record payload retaining one shared allocation charge.
///
/// Cloning a row keeps the charge alive even after its cache entry is evicted.
/// Borrowing the payload does not create an uncharged owning byte view.
#[derive(Clone)]
pub struct RecordBytes {
    value: OwnedBytes,
    charge: Arc<Charge>,
}

impl RecordBytes {
    pub(super) fn backing(&self) -> &OwnedBytesBacking {
        &self.charge.backing
    }
}

impl Deref for RecordBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.value.as_slice()
    }
}

impl AsRef<[u8]> for RecordBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for RecordBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl PartialEq for RecordBytes {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl Eq for RecordBytes {}

#[derive(Clone, Default)]
pub(super) struct BackingPool(Arc<Mutex<State>>);

impl BackingPool {
    pub(super) fn bytes(&self) -> usize {
        locked(&self.0).bytes
    }

    pub(super) fn retain(&self, mut value: OwnedBytes, budget: usize) -> RecordBytes {
        if let Some(backing) = value.backing()
            && let Some(charge) = locked(&self.0).owners.get(&backing).and_then(Weak::upgrade)
        {
            return RecordBytes { value, charge };
        }
        // Unknown transport capacity cannot be charged from its visible length.
        // A small slice must also not pin an allocation larger than the entire
        // cache budget. Already shared owners above keep their existing charge.
        if value
            .retained_capacity()
            .is_none_or(|capacity| capacity > budget && capacity > value.len())
        {
            value = OwnedBytes::copy_from_slice(value.as_slice());
        }
        let backing = value
            .backing()
            .expect("copied payloads retain their original capacity");
        let mut state = locked(&self.0);
        let charge = match state.owners.get(&backing).and_then(Weak::upgrade) {
            Some(charge) => charge,
            None => {
                // A last-owner destructor may be waiting for this lock. Its
                // replacement takes over the existing charge; that destructor
                // must neither remove the replacement nor subtract its bytes.
                if !state.owners.contains_key(&backing) {
                    state.bytes += backing.capacity();
                }
                let charge = Arc::new(Charge {
                    backing: backing.clone(),
                    state: Arc::clone(&self.0),
                });
                state.owners.insert(backing, Arc::downgrade(&charge));
                charge
            }
        };
        RecordBytes { value, charge }
    }
}
