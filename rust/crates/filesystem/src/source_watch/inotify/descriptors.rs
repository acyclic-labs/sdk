//! A disposable fold over registration records. Kernel registration remains
//! infallible after admission; the first callback indexes only its new suffix.
use super::*;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

const EMPTY: usize = usize::MAX;

#[derive(Default)]
pub(super) struct DescriptorIndex {
    heads: Vec<usize>,
    next: Vec<usize>,
    hash: RandomState,
    dirty: bool,
}

impl DescriptorIndex {
    pub(super) fn invalidate(&mut self) {
        self.dirty = true;
    }

    fn slot<W: SourceDemandReadWork>(
        &self,
        records: &[(PathBuf, i32)],
        descriptor: i32,
        work: &mut W,
    ) -> Result<usize, DemandError> {
        let mut at = self.hash.hash_one(descriptor) as usize & (self.heads.len() - 1);
        loop {
            work.with_original_leaf(|work| work.started_io(ITEM))?;
            if self.heads[at] == EMPTY || records[self.heads[at]].1 == descriptor {
                return Ok(at);
            }
            at = (at + 1) & (self.heads.len() - 1);
        }
    }

    fn prepare<W: SourceDemandReadWork>(
        &mut self,
        records: &[(PathBuf, i32)],
        work: &mut W,
        retained: &AtomicU64,
        ownership: &SourcePayloadOwnership,
    ) -> Result<(), DemandError> {
        if records.len() > self.heads.len() / 2 {
            let capacity = records
                .len()
                .checked_next_power_of_two()
                .ok_or(WorkError::Overflow)?;
            let heads = capacity.checked_mul(2).ok_or(WorkError::Overflow)?;
            let bytes = heads
                .checked_add(capacity)
                .and_then(|count| count.checked_mul(size_of::<usize>()))
                .ok_or(WorkError::Overflow)? as u64;
            ownership.claim_retained(
                bytes,
                WorkCounters {
                    items_examined: heads as u64,
                    ..WorkCounters::UNCHARGED
                },
                work,
            )?;
            let old = (self.heads.capacity() + self.next.capacity()) * size_of::<usize>();
            self.heads = vec![EMPTY; heads];
            self.next = Vec::with_capacity(capacity);
            self.dirty = false;
            retained.fetch_add(bytes - old as u64, Ordering::AcqRel);
            ownership.retire(old as u64)?;
            if !ownership.has_native_custody() {
                work.release(bytes)?;
            }
        } else if self.dirty {
            // A swap removal changes record positions. Preserve allocation,
            // but rebuild this derived fold before trusting any saved link.
            work.with_original_leaf(|work| {
                work.started_io(WorkCounters {
                    items_examined: self.heads.len() as u64,
                    ..WorkCounters::UNCHARGED
                })
            })?;
            self.heads.fill(EMPTY);
            self.next.clear();
            self.dirty = false;
        }
        while self.next.len() < records.len() {
            let record = self.next.len();
            // All fallible work precedes mutation, so a denied callback can
            // resume from this exact suffix without dropping earlier aliases.
            let slot = self.slot(records, records[record].1, work)?;
            self.next.push(self.heads[slot]);
            self.heads[slot] = record;
        }
        Ok(())
    }

    pub(super) fn alias<W: SourceDemandReadWork>(
        &mut self,
        records: &[(PathBuf, i32)],
        descriptor: i32,
        cursor: usize,
        work: &mut W,
        retained: &AtomicU64,
        ownership: &SourcePayloadOwnership,
    ) -> Result<Option<(usize, usize)>, DemandError> {
        if cursor == EMPTY || records.is_empty() {
            return Ok(None);
        }
        self.prepare(records, work, retained, ownership)?;
        let record = if cursor == 0 {
            self.heads[self.slot(records, descriptor, work)?]
        } else {
            cursor - 1
        };
        if record == EMPTY {
            return Ok(None);
        }
        work.with_original_leaf(|work| work.started_io(ITEM))?;
        let next = self.next[record];
        Ok(Some((record, if next == EMPTY { EMPTY } else { next + 1 })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_search_descriptor_callbacks_visit_new_records_and_only_their_aliases() {
        let count = 50_000;
        let mut records: Vec<_> = (0..count)
            .map(|record| (PathBuf::from(format!("directory-{record}")), record as i32))
            .collect();
        let mut index = DescriptorIndex::default();
        let retained = AtomicU64::new(0);
        let mut work = crate::native_host::ordinary_work();
        let ownership = SourcePayloadOwnership::new(&mut work).unwrap();
        assert_eq!(
            index
                .alias(&records, 0, 0, &mut work, &retained, &ownership)
                .unwrap(),
            Some((0, EMPTY))
        );
        let before = work.receipt().items_examined;
        for descriptor in 0..count {
            assert_eq!(
                index
                    .alias(
                        &records,
                        descriptor as i32,
                        0,
                        &mut work,
                        &retained,
                        &ownership
                    )
                    .unwrap(),
                Some((descriptor, EMPTY))
            );
        }
        let examined = work.receipt().items_examined - before;
        assert!(examined < 20 * count as u64);
        // Negative control: the old callback inspected every registration for
        // each raw event, even when precisely one alias matched its descriptor.
        assert!(count as u64 * count as u64 > 100 * examined);
        assert_eq!(
            retained.load(Ordering::Acquire),
            ((index.heads.capacity() + index.next.capacity()) * size_of::<usize>()) as u64
        );
        assert!(retained.load(Ordering::Acquire) < 6 * count as u64 * size_of::<usize>() as u64);

        // Multiple bindings of one host inode share a descriptor. An appended
        // alias joins its own chain without replaying the existing namespace.
        records.push((PathBuf::from("same-inode-alias"), 7));
        let before = work.receipt().items_examined;
        let first = index
            .alias(&records, 7, 0, &mut work, &retained, &ownership)
            .unwrap()
            .unwrap();
        assert_eq!(first, (count, 8));
        assert_eq!(
            index
                .alias(&records, 7, first.1, &mut work, &retained, &ownership)
                .unwrap(),
            Some((7, EMPTY))
        );
        assert!(work.receipt().items_examined - before < 20);
        records.swap_remove(7);
        index.invalidate();
        assert_eq!(
            index
                .alias(&records, 7, 0, &mut work, &retained, &ownership)
                .unwrap(),
            Some((7, EMPTY))
        );
        assert_eq!(work.live_allocation_bytes(), 0);
    }

    #[test]
    fn name_search_descriptor_aliases_share_one_bucket_and_resume_denied_folds() {
        let records: Vec<_> = (0..1024)
            .map(|record| (PathBuf::from(format!("alias-{record}")), 9))
            .collect();
        let retained = AtomicU64::new(0);
        let mut ordinary = crate::native_host::ordinary_work();
        let ownership = SourcePayloadOwnership::new(&mut ordinary).unwrap();
        let mut index = DescriptorIndex::default();
        let cancellation = crate::CancellationToken::new();
        let mut denied = DemandWork::new(
            WorkBudget {
                items_examined: 2048 + 8,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        assert!(
            index
                .alias(&records, 9, 0, &mut denied, &retained, &ownership)
                .is_err()
        );
        assert_eq!(denied.live_allocation_bytes(), 0);
        assert!(index.next.len() < records.len());
        let before = ordinary.receipt().items_examined;
        let mut cursor = 0;
        let mut aliases = 0;
        while let Some((record, next)) = index
            .alias(&records, 9, cursor, &mut ordinary, &retained, &ownership)
            .unwrap()
        {
            assert_eq!(record, records.len() - aliases - 1);
            aliases += 1;
            cursor = next;
        }
        assert_eq!(aliases, records.len());
        assert!(ordinary.receipt().items_examined - before < 3 * records.len() as u64);
        assert!(
            index
                .alias(&records, 10, 0, &mut ordinary, &retained, &ownership)
                .unwrap()
                .is_none()
        );
        assert_eq!(ordinary.live_allocation_bytes(), 0);
    }
}
