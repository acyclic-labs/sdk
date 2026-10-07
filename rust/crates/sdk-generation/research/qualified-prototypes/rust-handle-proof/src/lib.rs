#![forbid(unsafe_code)]

//! A bounded, pure model of a Rust-owned opaque-handle registry.
//!
//! This is an external proof prototype. It deliberately stores an integer
//! object identifier instead of a pointer and has no ABI or allocator code.
//! `SLOTS` and `LEASES` are model capacities, not a proposed production limit.

const DEFAULT_REGISTRY_DOMAIN: u64 = 0x_8e0d_42a7_51c9_6bf3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Handle {
    domain: u64,
    slot: u64,
    generation: u64,
    lease_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidHandle,
    AlreadyReleased,
    SlotCapacityExhausted,
    LeaseCapacityExhausted,
    LeaseIdExhausted,
    GenerationExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotState {
    Vacant,
    Live,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeaseState {
    Empty,
    Active,
    Released,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Slot {
    state: SlotState,
    generation: u64,
    object: u64,
    active_leases: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Lease {
    state: LeaseState,
    domain: u64,
    slot: u64,
    generation: u64,
    id: u64,
}

/// Test-only evidence that the final release ran a destructor-like action.
///
/// This deliberately records an event in the model-owned boolean instead of
/// using a process-global counter or a raw pointer.
struct DropWitness<'a> {
    dropped: &'a mut bool,
}

impl Drop for DropWitness<'_> {
    fn drop(&mut self) {
        *self.dropped = true;
    }
}

impl Slot {
    const VACANT: Self = Self {
        state: SlotState::Vacant,
        generation: 1,
        object: 0,
        active_leases: 0,
    };
}

impl Lease {
    const EMPTY: Self = Self {
        state: LeaseState::Empty,
        domain: 0,
        slot: 0,
        generation: 0,
        id: 0,
    };
}

pub struct Registry<const SLOTS: usize, const LEASES: usize> {
    domain: u64,
    slots: [Slot; SLOTS],
    leases: [Lease; LEASES],
    next_lease_id: u64,
    drops: u64,
    drop_witness: [bool; SLOTS],
}

impl<const SLOTS: usize, const LEASES: usize> Registry<SLOTS, LEASES> {
    pub const fn new() -> Self {
        Self::with_domain(DEFAULT_REGISTRY_DOMAIN)
    }

    /// Construct a model registry with an explicit issuer/domain tag.
    ///
    /// Production code must allocate distinct tags from a Rust-owned issuer;
    /// this pure model accepts the tag as input so cross-registry rejection is
    /// directly testable without pretending the tag is cryptographically
    /// unforgeable.
    pub const fn with_domain(domain: u64) -> Self {
        Self {
            domain,
            slots: [Slot::VACANT; SLOTS],
            leases: [Lease::EMPTY; LEASES],
            next_lease_id: 1,
            drops: 0,
            drop_witness: [false; SLOTS],
        }
    }

    pub const fn drops(&self) -> u64 {
        self.drops
    }

    #[cfg(kani)]
    fn drop_witnessed(&self, slot: usize) -> bool {
        self.drop_witness[slot]
    }

    pub fn create(&mut self, object: u64) -> Result<Handle, Error> {
        let mut saw_generation_exhaustion = false;
        let mut index = 0;
        while index < SLOTS {
            if self.slots[index].state == SlotState::Vacant {
                let generation = self.slots[index].generation;
                if generation == u64::MAX {
                    self.slots[index].state = SlotState::Retired;
                    saw_generation_exhaustion = true;
                    index += 1;
                    continue;
                }
                let handle = self.issue_lease(index as u64, generation)?;
                self.slots[index].state = SlotState::Live;
                self.slots[index].object = object;
                self.slots[index].active_leases = 1;
                return Ok(handle);
            }
            index += 1;
        }
        if saw_generation_exhaustion {
            Err(Error::GenerationExhausted)
        } else {
            Err(Error::SlotCapacityExhausted)
        }
    }

    pub fn lookup(&self, handle: Handle) -> Result<u64, Error> {
        let lease_index = self.find_lease(handle)?;
        let lease = self.leases[lease_index];
        let slot = self
            .slots
            .get(lease.slot as usize)
            .ok_or(Error::InvalidHandle)?;
        if slot.state == SlotState::Live && slot.generation == lease.generation {
            Ok(slot.object)
        } else {
            Err(Error::InvalidHandle)
        }
    }

    pub fn clone_lease(&mut self, handle: Handle) -> Result<Handle, Error> {
        let lease_index = self.find_lease(handle)?;
        let lease = self.leases[lease_index];
        self.live_slot(lease)?;
        let next_active_leases = self
            .slots
            .get(lease.slot as usize)
            .ok_or(Error::InvalidHandle)?
            .active_leases
            .checked_add(1)
            .ok_or(Error::LeaseCapacityExhausted)?;
        let new_handle = self.issue_lease(lease.slot, lease.generation)?;
        let slot = self
            .slots
            .get_mut(lease.slot as usize)
            .ok_or(Error::InvalidHandle)?;
        slot.active_leases = next_active_leases;
        Ok(new_handle)
    }

    /// Transfer one active lease to a fresh, distinct lease ID.
    pub fn move_lease(&mut self, handle: Handle) -> Result<Handle, Error> {
        let lease_index = self.find_lease(handle)?;
        let lease = self.leases[lease_index];
        self.live_slot(lease)?;
        let new_handle = self.issue_lease(lease.slot, lease.generation)?;
        self.leases[lease_index].state = LeaseState::Released;
        Ok(new_handle)
    }

    pub fn release(&mut self, handle: Handle) -> Result<(), Error> {
        let lease_index = self.find_lease(handle)?;
        let lease = self.leases[lease_index];
        let slot_index = lease.slot as usize;
        let slot = self.slots.get(slot_index).ok_or(Error::InvalidHandle)?;
        if slot.state != SlotState::Live
            || slot.generation != lease.generation
            || slot.active_leases == 0
        {
            return Err(Error::InvalidHandle);
        }
        if slot.active_leases == 1 && self.drops == u64::MAX {
            return Err(Error::LeaseCapacityExhausted);
        }

        // Consume the token before any destructor-like transition. The same
        // copied token therefore can never perform a second release.
        self.leases[lease_index].state = LeaseState::Released;
        let active_leases = slot.active_leases - 1;
        self.slots[slot_index].active_leases = active_leases;
        if active_leases == 0 {
            self.drops += 1;
            {
                let witness = DropWitness {
                    dropped: &mut self.drop_witness[slot_index],
                };
                drop(witness);
            }
            if self.slots[slot_index].generation == u64::MAX - 1 {
                self.slots[slot_index].state = SlotState::Retired;
            } else {
                self.slots[slot_index].generation = self.slots[slot_index]
                    .generation
                    .checked_add(1)
                    .ok_or(Error::GenerationExhausted)?;
                self.slots[slot_index].state = SlotState::Vacant;
            }
        }
        Ok(())
    }

    fn issue_lease(&mut self, slot: u64, generation: u64) -> Result<Handle, Error> {
        if self.next_lease_id == 0 || self.next_lease_id == u64::MAX {
            return Err(Error::LeaseIdExhausted);
        }
        let index = self
            .leases
            .iter()
            .position(|lease| lease.state == LeaseState::Empty)
            .ok_or(Error::LeaseCapacityExhausted)?;
        let id = self.next_lease_id;
        self.next_lease_id = self
            .next_lease_id
            .checked_add(1)
            .ok_or(Error::LeaseIdExhausted)?;
        self.leases[index] = Lease {
            state: LeaseState::Active,
            domain: self.domain,
            slot,
            generation,
            id,
        };
        Ok(Handle {
            domain: self.domain,
            slot,
            generation,
            lease_id: id,
        })
    }

    fn find_lease(&self, handle: Handle) -> Result<usize, Error> {
        if handle.domain != self.domain {
            return Err(Error::InvalidHandle);
        }
        for (index, lease) in self.leases.iter().enumerate() {
            if lease.domain == handle.domain
                && lease.slot == handle.slot
                && lease.generation == handle.generation
                && lease.id == handle.lease_id
            {
                return match lease.state {
                    LeaseState::Active => Ok(index),
                    LeaseState::Released => Err(Error::AlreadyReleased),
                    LeaseState::Empty => Err(Error::InvalidHandle),
                };
            }
        }
        Err(Error::InvalidHandle)
    }

    fn live_slot(&self, lease: Lease) -> Result<(), Error> {
        let slot = self
            .slots
            .get(lease.slot as usize)
            .ok_or(Error::InvalidHandle)?;
        if slot.state == SlotState::Live && slot.generation == lease.generation {
            Ok(())
        } else {
            Err(Error::InvalidHandle)
        }
    }

    #[cfg(kani)]
    fn force_vacant_generation(&mut self, slot: usize, generation: u64) {
        assert!(self.slots[slot].state == SlotState::Vacant);
        self.slots[slot].generation = generation;
    }

    #[cfg(kani)]
    fn force_next_lease_id(&mut self, id: u64) {
        self.next_lease_id = id;
    }

    #[cfg(kani)]
    fn force_active_leases(&mut self, slot: usize, active_leases: u64) {
        self.slots[slot].active_leases = active_leases;
    }
}

#[cfg(kani)]
mod proofs {
    use super::{Error, Registry};

    #[kani::proof]
    fn stale_and_unissued_handles_reject() {
        let mut registry = Registry::<2, 4>::new();
        let original = registry.create(10).unwrap();
        registry.release(original).unwrap();
        let replacement = registry.create(20).unwrap();
        assert_ne!(original.generation, replacement.generation);
        assert_eq!(registry.lookup(original), Err(Error::AlreadyReleased));

        let mut unissued_generation = replacement;
        unissued_generation.generation = unissued_generation.generation.wrapping_add(1);
        assert_eq!(
            registry.lookup(unissued_generation),
            Err(Error::InvalidHandle)
        );

        let mut unissued_domain = replacement;
        unissued_domain.domain ^= 1;
        assert_eq!(registry.lookup(unissued_domain), Err(Error::InvalidHandle));

        // Quantify over every bit pattern. A token is valid only when it is
        // exactly the currently issued token; no other unissued tuple may
        // resolve to the replacement object. A bitwise copy of the live
        // token is intentionally the same token and is covered separately.
        let arbitrary = super::Handle {
            domain: kani::any(),
            slot: kani::any(),
            generation: kani::any(),
            lease_id: kani::any(),
        };
        let result = registry.lookup(arbitrary);
        if arbitrary == replacement {
            assert_eq!(result, Ok(20));
        } else {
            assert_ne!(result, Ok(20));
        }
    }

    #[kani::proof]
    fn distinct_registry_domains_reject_cross_registry_handles() {
        let mut left = Registry::<1, 2>::with_domain(0x11);
        let right = Registry::<1, 2>::with_domain(0x22);
        let handle = left.create(41).unwrap();
        assert_eq!(left.lookup(handle), Ok(41));
        assert_eq!(right.lookup(handle), Err(Error::InvalidHandle));
    }

    #[kani::proof]
    fn copied_token_is_idempotent_and_drops_once() {
        let mut registry = Registry::<1, 4>::new();
        let handle = registry.create(7).unwrap();
        let copied = handle;
        assert!(!registry.drop_witnessed(0));
        assert_eq!(registry.release(handle), Ok(()));
        assert_eq!(registry.drops(), 1);
        assert!(registry.drop_witnessed(0));
        assert_eq!(registry.release(copied), Err(Error::AlreadyReleased));
        assert_eq!(registry.drops(), 1);
        assert!(registry.drop_witnessed(0));
    }

    #[kani::proof]
    fn moves_and_clones_have_distinct_release_tokens() {
        let mut registry = Registry::<1, 4>::new();
        let original = registry.create(99).unwrap();
        let clone = registry.clone_lease(original).unwrap();
        let moved = registry.move_lease(original).unwrap();
        assert_ne!(clone.lease_id, moved.lease_id);
        assert_eq!(registry.release(original), Err(Error::AlreadyReleased));
        assert_eq!(registry.release(clone), Ok(()));
        assert_eq!(registry.drops(), 0);
        assert_eq!(registry.lookup(moved), Ok(99));
        assert_eq!(registry.release(moved), Ok(()));
        assert_eq!(registry.drops(), 1);
    }

    #[kani::proof]
    fn generation_exhaustion_retires_without_reuse() {
        let mut registry = Registry::<1, 4>::new();
        let first = registry.create(1).unwrap();
        registry.release(first).unwrap();
        registry.force_vacant_generation(0, u64::MAX);
        assert_eq!(registry.create(2), Err(Error::GenerationExhausted));
        assert_eq!(registry.create(3), Err(Error::SlotCapacityExhausted));
        assert_eq!(registry.drops(), 1);
    }

    #[kani::proof]
    fn generation_neighbor_boundaries_retire_without_reuse() {
        let mut registry = Registry::<1, 4>::new();
        registry.force_vacant_generation(0, u64::MAX - 1);
        let final_generation = registry.create(2).unwrap();
        assert_eq!(final_generation.generation, u64::MAX - 1);
        assert_eq!(registry.release(final_generation), Ok(()));
        assert_eq!(registry.create(3), Err(Error::SlotCapacityExhausted));

        let mut max_generation = Registry::<1, 4>::new();
        max_generation.force_vacant_generation(0, u64::MAX);
        assert_eq!(max_generation.create(4), Err(Error::GenerationExhausted));
    }

    #[kani::proof]
    fn lease_id_exhaustion_does_not_mutate_live_state() {
        let mut registry = Registry::<1, 4>::new();
        let handle = registry.create(5).unwrap();
        registry.force_next_lease_id(u64::MAX - 1);
        let final_id = registry.clone_lease(handle).unwrap();
        assert_eq!(final_id.lease_id, u64::MAX - 1);
        assert_eq!(registry.clone_lease(handle), Err(Error::LeaseIdExhausted));
        assert_eq!(registry.lookup(handle), Ok(5));
        assert_eq!(registry.lookup(final_id), Ok(5));
        assert_eq!(registry.release(final_id), Ok(()));
        assert_eq!(registry.release(handle), Ok(()));
        assert_eq!(registry.drops(), 1);
    }

    #[kani::proof]
    fn clone_capacity_failure_is_atomic() {
        let mut registry = Registry::<1, 1>::new();
        let handle = registry.create(6).unwrap();
        assert_eq!(
            registry.clone_lease(handle),
            Err(Error::LeaseCapacityExhausted)
        );
        assert_eq!(registry.lookup(handle), Ok(6));
        assert_eq!(registry.release(handle), Ok(()));
        assert_eq!(registry.drops(), 1);
    }

    #[kani::proof]
    fn clone_active_count_overflow_is_atomic() {
        let mut registry = Registry::<1, 4>::new();
        let handle = registry.create(8).unwrap();
        registry.force_active_leases(0, u64::MAX);
        let next_lease_id = registry.next_lease_id;
        assert_eq!(
            registry.clone_lease(handle),
            Err(Error::LeaseCapacityExhausted)
        );
        assert_eq!(registry.next_lease_id, next_lease_id);
        assert_eq!(registry.lookup(handle), Ok(8));
        assert_eq!(registry.release(handle), Ok(()));
        assert_eq!(registry.drops(), 0);
        assert!(!registry.drop_witnessed(0));
    }

    #[kani::proof]
    fn drop_witness_emits_once_at_final_release() {
        let mut registry = Registry::<1, 4>::new();
        let first = registry.create(9).unwrap();
        let second = registry.clone_lease(first).unwrap();
        assert!(!registry.drop_witnessed(0));
        assert_eq!(registry.release(first), Ok(()));
        assert_eq!(registry.drops(), 0);
        assert!(!registry.drop_witnessed(0));
        assert_eq!(registry.release(second), Ok(()));
        assert_eq!(registry.drops(), 1);
        assert!(registry.drop_witnessed(0));
        assert_eq!(registry.release(second), Err(Error::AlreadyReleased));
        assert_eq!(registry.drops(), 1);
        assert!(registry.drop_witnessed(0));
    }
}
