//! Path lookup over the watch's existing records, without duplicate path storage.
use super::*;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

const EMPTY: usize = usize::MAX;
const DELETED: usize = usize::MAX - 1;

#[derive(Default)]
pub(super) struct Registrations {
    pub(super) records: Vec<(PathBuf, i32)>,
    slots: Vec<usize>,
    used: usize,
    hash: RandomState,
    descriptors: super::descriptors::DescriptorIndex,
    pub(super) revision: u64,
}

impl std::ops::Deref for Registrations {
    type Target = [(PathBuf, i32)];
    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl Registrations {
    fn hash_path<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<usize, DemandError> {
        work.with_original_leaf(|work| {
            work.started_io(WorkCounters {
                items_examined: path.as_os_str().len() as u64,
                ..WorkCounters::UNCHARGED
            })
        })?;
        Ok(self.hash.hash_one(path) as usize)
    }

    fn locate<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<usize, DemandError> {
        let mut at = self.hash_path(path, work)? & (self.slots.len() - 1);
        let mut deleted = None;
        loop {
            work.with_original_leaf(|work| work.started_io(ITEM))?;
            let record = self.slots[at];
            if record == EMPTY {
                return Ok(deleted.unwrap_or(at));
            }
            if record == DELETED {
                deleted.get_or_insert(at);
            } else if self.records[record].0 == path {
                return Ok(at);
            }
            at = (at + 1) & (self.slots.len() - 1);
        }
    }

    pub(super) fn contains<W: SourceDemandReadWork>(
        &self,
        path: &Path,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        if self.slots.is_empty() {
            // Startup owns only the root, before any directory is admitted.
            work.with_original_leaf(|work| work.started_io(ITEM))?;
            return Ok(self.records.first().is_some_and(|(root, _)| root == path));
        }
        let at = self.locate(path, work)?;
        Ok(self.slots[at] < DELETED)
    }

    pub(super) fn prepare<W: SourceDemandReadWork>(
        &mut self,
        path: &Path,
        work: &mut W,
        retained: &AtomicU64,
        ownership: &SourcePayloadOwnership,
    ) -> Result<usize, DemandError> {
        if self.used.saturating_add(1) > self.slots.len() / 2 {
            let count = self
                .records
                .len()
                .checked_add(1)
                .and_then(|count| count.checked_mul(2))
                .and_then(usize::checked_next_power_of_two)
                .ok_or(WorkError::Overflow)?;
            self.rebuild(count.max(self.slots.len()), work, retained, ownership)?;
        }
        self.locate(path, work)
    }

    fn rebuild<W: SourceDemandReadWork>(
        &mut self,
        count: usize,
        work: &mut W,
        retained: &AtomicU64,
        ownership: &SourcePayloadOwnership,
    ) -> Result<(), DemandError> {
        let bytes = (count as u64)
            .checked_mul(size_of::<usize>() as u64)
            .ok_or(WorkError::Overflow)?;
        ownership.claim_retained(
            bytes,
            WorkCounters {
                items_examined: count as u64,
                ..WorkCounters::UNCHARGED
            },
            work,
        )?;
        let mut replacement = vec![EMPTY; count];
        let rehashed = (|| {
            for (record, (path, _)) in self.records.iter().enumerate() {
                let mut at = self.hash_path(path, work)? & (count - 1);
                loop {
                    work.with_original_leaf(|work| work.started_io(ITEM))?;
                    if replacement[at] == EMPTY {
                        replacement[at] = record;
                        break;
                    }
                    at = (at + 1) & (count - 1);
                }
            }
            Ok::<(), DemandError>(())
        })();
        if let Err(error) = rehashed {
            drop(replacement);
            ownership.discard_retained(bytes, work)?;
            return Err(error);
        }
        let old_bytes = (self.slots.capacity() * size_of::<usize>()) as u64;
        self.slots = replacement;
        self.used = self.records.len();
        retained.fetch_add(bytes - old_bytes, Ordering::AcqRel);
        ownership.retire(old_bytes)?;
        if !ownership.has_native_custody() {
            work.release(bytes)?;
        }
        Ok(())
    }

    pub(super) fn insert(&mut self, slot: usize, path: PathBuf, descriptor: i32) {
        if self.slots[slot] == EMPTY {
            self.used += 1;
        }
        self.slots[slot] = self.records.len();
        self.records.push((path, descriptor));
    }

    /// Do every fallible lookup before withdrawing the kernel registration.
    pub(super) fn prepare_remove<W: SourceDemandReadWork>(
        &self,
        record: usize,
        work: &mut W,
    ) -> Result<(usize, usize), DemandError> {
        if self.slots.is_empty() {
            work.with_original_leaf(|work| work.started_io(ITEM))?;
            return Ok((EMPTY, EMPTY));
        }
        let removed = self.locate(&self.records[record].0, work)?;
        let moved = self.locate(
            &self.records.last().ok_or(DemandError::InvalidRequest)?.0,
            work,
        )?;
        Ok((removed, moved))
    }

    pub(super) fn remove(&mut self, record: usize, slots: (usize, usize)) -> (PathBuf, i32) {
        self.descriptors.invalidate();
        self.revision = self.revision.wrapping_add(1);
        let (removed, moved) = slots;
        if removed == EMPTY {
            return self.records.swap_remove(record);
        }
        if moved != removed {
            self.slots[moved] = record;
        }
        self.slots[removed] = DELETED;
        self.records.swap_remove(record)
    }

    pub(super) fn clear(&mut self) -> WorkCounters {
        self.descriptors = Default::default();
        self.slots = Vec::new();
        self.used = 0;
        clear_records(&mut self.records)
    }

    pub(super) fn descriptor_alias<W: SourceDemandReadWork>(
        &mut self,
        descriptor: i32,
        cursor: usize,
        work: &mut W,
        retained: &AtomicU64,
        ownership: &SourcePayloadOwnership,
    ) -> Result<Option<(usize, usize)>, DemandError> {
        self.descriptors
            .alias(&self.records, descriptor, cursor, work, retained, ownership)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert(
        registrations: &mut Registrations,
        path: &Path,
        work: &mut DemandWork,
        retained: &AtomicU64,
        ownership: &SourcePayloadOwnership,
    ) {
        let slot = registrations
            .prepare(path, work, retained, ownership)
            .unwrap();
        let owned = registration::prepare_insert_with_work(
            &mut registrations.records,
            path,
            work,
            retained,
            ownership,
        )
        .unwrap();
        retained.fetch_add(owned.capacity() as u64, Ordering::AcqRel);
        work.release(owned.as_os_str().len() as u64).unwrap();
        registrations.insert(slot, owned, 1);
    }

    #[test]
    fn name_search_watch_membership_work_and_storage_are_linear() {
        let count = 50_000;
        let mut work = crate::native_host::ordinary_work();
        let ownership = SourcePayloadOwnership::new(&mut work).unwrap();
        let retained = AtomicU64::new(0);
        let mut registrations = Registrations::default();
        for ordinal in 0..count {
            insert(
                &mut registrations,
                Path::new(&format!("directory-{ordinal:08}")),
                &mut work,
                &retained,
                &ownership,
            );
        }
        let before = work.receipt().items_examined;
        for ordinal in 0..count {
            assert!(
                registrations
                    .contains(Path::new(&format!("directory-{ordinal:08}")), &mut work)
                    .unwrap()
            );
            assert!(
                !registrations
                    .contains(Path::new(&format!("unwatched-{ordinal:08}")), &mut work)
                    .unwrap()
            );
        }
        let examined = work.receipt().items_examined - before;
        assert!(examined < 100 * count as u64);
        // Negative control: the former vector visits every earlier record
        // for hits and the whole registered tree for each absent directory.
        let linear_scan = count as u64 * (count as u64 + 1) / 2 + count as u64 * count as u64;
        assert!(linear_scan > 100 * examined);
        assert!(registrations.slots.len() < 4 * registrations.len());
        let physical_bytes = registrations.records.capacity() * size_of::<(PathBuf, i32)>()
            + registrations.slots.capacity() * size_of::<usize>()
            + registrations
                .iter()
                .map(|(path, _)| path.capacity())
                .sum::<usize>();
        assert_eq!(retained.load(Ordering::Acquire), physical_bytes as u64);

        for ordinal in (0..count).step_by(2) {
            let path = format!("directory-{ordinal:08}");
            let slot = registrations.locate(Path::new(&path), &mut work).unwrap();
            let record = registrations.slots[slot];
            let removed = registrations.prepare_remove(record, &mut work).unwrap();
            let (path, _) = registrations.remove(record, removed);
            retained.fetch_sub(path.capacity() as u64, Ordering::AcqRel);
        }
        for ordinal in 0..count {
            assert_eq!(
                registrations
                    .contains(Path::new(&format!("directory-{ordinal:08}")), &mut work)
                    .unwrap(),
                ordinal % 2 != 0,
                "swap removal must update the moved record's slot"
            );
        }
        let table_length = registrations.slots.len();
        for ordinal in 0..count / 2 {
            insert(
                &mut registrations,
                Path::new(&format!("replacement-{ordinal:08}")),
                &mut work,
                &retained,
                &ownership,
            );
        }
        assert_eq!(registrations.len(), count);
        assert_eq!(registrations.slots.len(), table_length);
        assert_eq!(work.live_allocation_bytes(), 0);
    }

    #[test]
    fn name_search_denied_watch_rehash_preserves_every_existing_registration() {
        let mut work = crate::native_host::ordinary_work();
        let ownership = SourcePayloadOwnership::new(&mut work).unwrap();
        let retained = AtomicU64::new(0);
        let mut registrations = Registrations::default();
        insert(
            &mut registrations,
            Path::new(""),
            &mut work,
            &retained,
            &ownership,
        );
        insert(
            &mut registrations,
            Path::new("directory"),
            &mut work,
            &retained,
            &ownership,
        );
        assert_eq!(registrations.used, registrations.slots.len() / 2);
        let before = registrations.slots.clone();
        let retained_before = retained.load(Ordering::Acquire);
        let cancellation = crate::CancellationToken::new();
        // Enough credit to allocate and initialize the replacement, then
        // denial while hashing its second record exercises rollback.
        let next_count = (2 * (registrations.len() + 1)).next_power_of_two();
        let mut denied = DemandWork::new(
            WorkBudget {
                items_examined: next_count as u64 + 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        assert!(matches!(
            registrations.prepare(Path::new("later"), &mut denied, &retained, &ownership),
            Err(DemandError::Work(WorkError::BudgetExceeded { .. }))
        ));
        assert_eq!(denied.live_allocation_bytes(), 0);
        assert_eq!(registrations.slots, before);
        assert_eq!(retained.load(Ordering::Acquire), retained_before);
        assert!(registrations.contains(Path::new(""), &mut work).unwrap());
        assert!(
            registrations
                .contains(Path::new("directory"), &mut work)
                .unwrap()
        );
        assert!(
            !registrations
                .contains(Path::new("later"), &mut work)
                .unwrap()
        );
    }

    #[test]
    fn name_search_colliding_watch_paths_remain_distinct_after_removal() {
        let mut work = crate::native_host::ordinary_work();
        let ownership = SourcePayloadOwnership::new(&mut work).unwrap();
        let retained = AtomicU64::new(0);
        let mut registrations = Registrations::default();
        let paths: Vec<_> = (0..)
            .map(|ordinal| PathBuf::from(format!("collision-{ordinal}")))
            .filter(|path| registrations.hash_path(path, &mut work).unwrap() & 7 == 0)
            .take(4)
            .collect();
        for path in paths.iter().take(3) {
            insert(&mut registrations, path, &mut work, &retained, &ownership);
        }
        assert_eq!(registrations.slots.len(), 8);
        // A fingerprint alone would incorrectly claim this fourth path exists.
        assert!(!registrations.contains(&paths[3], &mut work).unwrap());
        let slots = registrations.prepare_remove(0, &mut work).unwrap();
        let (removed, _) = registrations.remove(0, slots);
        retained.fetch_sub(removed.capacity() as u64, Ordering::AcqRel);
        assert!(!registrations.contains(&paths[0], &mut work).unwrap());
        assert!(registrations.contains(&paths[1], &mut work).unwrap());
        assert!(registrations.contains(&paths[2], &mut work).unwrap());
        let used = registrations.used;
        insert(
            &mut registrations,
            &paths[3],
            &mut work,
            &retained,
            &ownership,
        );
        assert_eq!(registrations.used, used, "reuse the deleted collision slot");
        assert!(registrations.contains(&paths[3], &mut work).unwrap());
        assert_eq!(work.live_allocation_bytes(), 0);
    }
}
