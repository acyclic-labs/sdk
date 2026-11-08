use super::*;
use crate::kernel::allocation::{
    fixed_index::PreparedIndexWork,
    retained::{RetainedAllocation, RetainedArc},
};
use std::sync::atomic::AtomicUsize;

type Owner = RetainedArc<ReadOwner>;
pub(super) type ReaderIndex = acyclic_collections::FixedIndex<Owner, RetainedAllocation, 8>;

pub(super) struct ReadOwner {
    pub(super) readers: AtomicUsize,
}

impl CallbackOwner {
    pub(super) fn key(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

// Reentrancy detection is part of an already admitted callback. The shared
// index bounds this exact lookup by the fixed key width, never reader count.
pub(super) struct OwnerLookup;
impl acyclic_collections::IndexReadWork for OwnerLookup {
    type Error = std::convert::Infallible;
    fn validate(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn examine(&mut self, _items: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl ViewState {
    fn read_generation(
        &self,
        gate: &ViewGate,
        expected: Option<u64>,
    ) -> Result<Option<u64>, MountSourceError> {
        if self.writer {
            return Ok(None);
        }
        let generation = gate.generation.load(Ordering::Acquire);
        if !generation.is_multiple_of(2) || expected.is_some_and(|expected| expected != generation)
        {
            return Err(MountSourceError::Stale);
        }
        Ok(Some(generation))
    }

    fn accept_reader(
        &mut self,
        gate: &Arc<ViewGate>,
        key: [u8; 8],
        generation: u64,
        owner: Owner,
        completion: &mut Option<crate::demand::ViewReadCompletion>,
    ) -> Result<leases::ViewReadLease, MountSourceError> {
        let readers = self
            .readers
            .checked_add(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let owned = owner
            .readers
            .load(Ordering::Relaxed)
            .checked_add(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let completion = completion
            .take()
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        owner.readers.store(owned, Ordering::Relaxed);
        self.readers = readers;
        Ok(leases::ViewReadLease {
            generation,
            gate: gate.clone(),
            key,
            owner,
            payload: None,
            completion,
        })
    }
}

enum ReadProbe {
    Blocked,
    Missing,
    Accepted(leases::ViewReadLease),
}

impl ViewGate {
    pub(super) fn try_read_as(
        self: &Arc<Self>,
        reader: CallbackOwner,
        expected: Option<u64>,
        carrier: &mut super::super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<leases::ViewReadLease>, MountSourceError> {
        let mut completion = Some(
            carrier
                .with_work(crate::demand::DemandWork::reserve_view_read)
                .map_err(MountSourceError::from)?,
        );
        let key = reader.key();
        let existing = carrier.with_accounting(|accounting| {
            PreparedIndexWork::read::<8, _>(
                accounting,
                |work, _| -> Result<ReadProbe, MountSourceError> {
                    let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                    let Some(generation) = state.read_generation(self, expected)? else {
                        return Ok(ReadProbe::Blocked);
                    };
                    let owner = state
                        .owners
                        .get(key, work)
                        .map_err(super::super::lazy::mount_engine_error)?
                        .cloned();
                    match owner {
                        Some(owner) => state
                            .accept_reader(self, key, generation, owner, &mut completion)
                            .map(ReadProbe::Accepted),
                        None => Ok(ReadProbe::Missing),
                    }
                },
            )
        })??;
        match existing {
            ReadProbe::Blocked => Ok(None),
            ReadProbe::Accepted(lease) => Ok(Some(lease)),
            ReadProbe::Missing => carrier.with_accounting(|accounting| {
                let mut admission = Some(RetainedArc::<ReadOwner>::admit(accounting)?);
                PreparedIndexWork::insert::<Owner, 8, _>(
                    accounting,
                    |work, _| -> Result<Option<leases::ViewReadLease>, MountSourceError> {
                        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                        let Some(generation) = state.read_generation(self, expected)? else {
                            return Ok(None);
                        };
                        if state.waiting_writers != 0 && !state.mutation_phase {
                            return Ok(None);
                        }
                        // Inserting a new owner cannot leave an empty retained entry
                        // if the global reader count is exhausted.
                        state
                            .readers
                            .checked_add(1)
                            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                        let owner = state
                            .owners
                            .get_or_try_insert_with(key, work, |_| {
                                let admitted =
                                    admission.take().ok_or(crate::WorkError::Overflow)?;
                                Ok(admitted.into_value(ReadOwner {
                                    readers: AtomicUsize::new(0),
                                }))
                            })
                            .map_err(super::super::lazy::mount_engine_error)?
                            .clone();
                        // The lease owns cleanup before prepared work settles, so
                        // settlement failure retires the same admitted reader.
                        state
                            .accept_reader(self, key, generation, owner, &mut completion)
                            .map(Some)
                    },
                )
            })?,
        }
    }
}
