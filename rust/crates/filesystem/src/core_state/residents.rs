//! Fixed-width record identities bound every lookup independently of residency.
use super::*;

const KEY_BYTES: usize = 17;
const KEY_BITS: usize = KEY_BYTES * 8;

impl RecordKey {
    fn index_key(self) -> [u8; KEY_BYTES] {
        let (tag, id) = match self {
            Self::Context(id) => (0, id.into_bytes()),
            Self::Children(id) => (1, id.into_bytes()),
            Self::ChildCount(id) => (2, id.into_bytes()),
            Self::ChildIndex => (3, [0; 16]),
            Self::Lineage(id) => (4, id.into_bytes()),
            Self::LazyWorkspace(id) => (5, id.into_bytes()),
        };
        let mut bytes = [0; KEY_BYTES];
        bytes[0] = tag;
        bytes[1..].copy_from_slice(&id);
        bytes
    }
}

#[derive(Clone, Copy, Debug)]
enum IndexNode {
    Leaf {
        key: [u8; KEY_BYTES],
        resident: usize,
    },
    Branch {
        bit: u8,
        children: [usize; 2],
    },
}

#[derive(Debug, Default)]
struct RecordIndex {
    nodes: Vec<IndexNode>,
    root: Option<usize>,
    // Backing always retires before its original ledger custody.
    allocation: Option<crate::NativeAllocation>,
}

fn side(key: &[u8; KEY_BYTES], bit: u8) -> Result<usize, LocalCoreStateStoreError> {
    let byte = key
        .get(usize::from(bit) / 8)
        .ok_or(LocalCoreStateStoreError::Integrity)?;
    Ok(usize::from((byte >> (7 - bit % 8)) & 1))
}

impl RecordIndex {
    fn clear(&mut self) {
        self.nodes.clear();
        self.root = None;
    }

    fn reserve(
        &mut self,
        additions: usize,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let needed = additions
            .checked_mul(2)
            .and_then(|count| self.nodes.len().checked_add(count))
            .ok_or(crate::NativeTreeMaterializationError::Work(
                crate::WorkError::Overflow,
            ))?;
        if let Some(work) = work {
            reserve_retained_slots(&mut self.nodes, needed, work, &mut self.allocation)?;
        } else {
            // Ordinary callers cannot silently outgrow native backing custody.
            if let Some(allocation) = &self.allocation {
                let original = allocation.work().clone();
                reserve_retained_slots(&mut self.nodes, needed, &original, &mut self.allocation)?;
            } else {
                self.nodes
                    .try_reserve(needed.saturating_sub(self.nodes.len()))
                    .map_err(std::io::Error::other)?;
            }
        }
        Ok(())
    }

    fn leaf(
        &self,
        key: &[u8; KEY_BYTES],
        examined: &mut u64,
    ) -> Result<Option<([u8; KEY_BYTES], usize)>, LocalCoreStateStoreError> {
        self.leaf_with(key, &mut || {
            *examined += 1;
            Ok(())
        })
    }

    fn leaf_with(
        &self,
        key: &[u8; KEY_BYTES],
        visit: &mut impl FnMut() -> Result<(), LocalCoreStateStoreError>,
    ) -> Result<Option<([u8; KEY_BYTES], usize)>, LocalCoreStateStoreError> {
        let Some(mut next) = self.root else {
            return Ok(None);
        };
        // Branch bit positions strictly increase. There are only KEY_BITS.
        for _ in 0..=KEY_BITS {
            visit()?;
            match *self
                .nodes
                .get(next)
                .ok_or(LocalCoreStateStoreError::Integrity)?
            {
                IndexNode::Leaf { key, resident } => return Ok(Some((key, resident))),
                IndexNode::Branch { bit, children } => {
                    next = *children
                        .get(side(key, bit)?)
                        .ok_or(LocalCoreStateStoreError::Integrity)?;
                }
            }
        }
        Err(LocalCoreStateStoreError::Integrity)
    }

    fn position(
        &self,
        key: RecordKey,
        examined: &mut u64,
    ) -> Result<Option<usize>, LocalCoreStateStoreError> {
        let key = key.index_key();
        Ok(self
            .leaf(&key, examined)?
            .and_then(|(found, index)| (found == key).then_some(index)))
    }

    fn position_with(
        &self,
        key: RecordKey,
        visit: &mut impl FnMut() -> Result<(), LocalCoreStateStoreError>,
    ) -> Result<Option<usize>, LocalCoreStateStoreError> {
        let key = key.index_key();
        Ok(self
            .leaf_with(&key, visit)?
            .and_then(|(found, index)| (found == key).then_some(index)))
    }

    /// Uses only pre-admitted nodes; every inserted branch has an increasing bit.
    fn insert(
        &mut self,
        key: RecordKey,
        resident: usize,
        examined: &mut u64,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.insert_key(key.index_key(), resident, examined)
    }

    fn insert_key(
        &mut self,
        key: [u8; KEY_BYTES],
        resident: usize,
        examined: &mut u64,
    ) -> Result<(), LocalCoreStateStoreError> {
        let Some((prior, _)) = self.leaf(&key, examined)? else {
            if self.nodes.len() == self.nodes.capacity() {
                return Err(LocalCoreStateStoreError::Integrity);
            }
            self.root = Some(self.nodes.len());
            self.nodes.push(IndexNode::Leaf { key, resident });
            return Ok(());
        };
        let Some((differing, xor)) = prior
            .iter()
            .zip(key)
            .enumerate()
            .find_map(|(offset, (a, b))| (*a != b).then_some((offset, a ^ b)))
        else {
            return Err(LocalCoreStateStoreError::Integrity);
        };
        let bit = u8::try_from(differing * 8 + xor.leading_zeros() as usize)
            .map_err(|_| LocalCoreStateStoreError::Integrity)?;
        let mut parent = None;
        let mut next = self.root.ok_or(LocalCoreStateStoreError::Integrity)?;
        for _ in 0..=KEY_BITS {
            *examined += 1;
            match *self
                .nodes
                .get(next)
                .ok_or(LocalCoreStateStoreError::Integrity)?
            {
                IndexNode::Branch {
                    bit: existing,
                    children,
                } if existing < bit => {
                    let direction = side(&key, existing)?;
                    parent = Some((next, direction));
                    next = *children
                        .get(direction)
                        .ok_or(LocalCoreStateStoreError::Integrity)?;
                }
                _ => {
                    if self.nodes.capacity().saturating_sub(self.nodes.len()) < 2 {
                        return Err(LocalCoreStateStoreError::Integrity);
                    }
                    let leaf = self.nodes.len();
                    let branch = leaf + 1;
                    let direction = side(&key, bit)?;
                    let mut children = [next; 2];
                    *children
                        .get_mut(direction)
                        .ok_or(LocalCoreStateStoreError::Integrity)? = leaf;
                    self.nodes.push(IndexNode::Leaf { key, resident });
                    self.nodes.push(IndexNode::Branch { bit, children });
                    if let Some((parent, direction)) = parent {
                        let Some(IndexNode::Branch { children, .. }) = self.nodes.get_mut(parent)
                        else {
                            return Err(LocalCoreStateStoreError::Integrity);
                        };
                        *children
                            .get_mut(direction)
                            .ok_or(LocalCoreStateStoreError::Integrity)? = branch;
                    } else {
                        self.root = Some(branch);
                    }
                    return Ok(());
                }
            }
        }
        Err(LocalCoreStateStoreError::Integrity)
    }
}

/// Cached backing keeps its original ledger while a new invocation borrows it.
/// Context changes require a new allocation only when the backing actually grows.
pub(super) fn reserve_retained_slots<T>(
    values: &mut Vec<T>,
    needed: usize,
    work: &crate::NativeMaterializationWork,
    allocation: &mut Option<crate::NativeAllocation>,
) -> Result<(), LocalCoreStateStoreError> {
    work.charge(crate::WorkCounters::UNCHARGED)?;
    let backing = values
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::NativeTreeMaterializationError::Work(
            crate::WorkError::Overflow,
        ))? as u64;
    if needed <= values.capacity()
        && allocation
            .as_ref()
            .is_some_and(|held| held.bytes() >= backing)
    {
        return Ok(());
    }
    reserve_slots(values, needed, work, allocation)
}

#[derive(Debug)]
struct Entry {
    record: Resident,
    dirty_epoch: Option<Uuid>,
    pending: bool,
    next_pending: Option<usize>,
}

#[derive(Debug, Default)]
pub(super) struct Residents {
    entries: Vec<Entry>,
    index: RecordIndex,
    pending: Option<usize>,
    allocation: Option<crate::NativeAllocation>,
}

/// Construction proves full payload funding; the record is immutable thereafter.
pub(super) struct AdoptedResident(Resident);

fn fund(
    record: &mut Resident,
    work: &crate::NativeMaterializationWork,
) -> Result<(), LocalCoreStateStoreError> {
    work.charge(crate::WorkCounters::UNCHARGED)?;
    if record
        .value
        .as_ref()
        .is_some_and(|value| !value.matches(record.key))
    {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    let bytes = record
        .value
        .as_ref()
        .map(|value| native_records::native_record_owned_bytes(value, work))
        .transpose()?
        .unwrap_or(0);
    if record.allocation.is_none() {
        record.allocation = Some(work.claim(bytes, 0, crate::WorkCounters::UNCHARGED)?);
    } else {
        record
            .allocation
            .as_mut()
            .ok_or(crate::NativeTreeMaterializationError::MissingCustody)?
            .resize(bytes)?;
    }
    Ok(())
}

impl AdoptedResident {
    pub(super) fn key(&self) -> RecordKey {
        self.0.key
    }

    pub(super) fn new(
        mut record: Resident,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, LocalCoreStateStoreError> {
        fund(&mut record, work)?;
        Ok(Self(record))
    }
}

pub(super) enum PreparedResident {
    Ordinary(Resident),
    Native(AdoptedResident),
}

/// The only constructor funds every native after-image before the WAL append.
pub(super) struct PreparedChange {
    change: Change,
    native: bool,
}

pub(super) struct PreparedRecords {
    records: std::vec::IntoIter<Resident>,
    native: bool,
    // Last: the vector backing retires before its original ledger custody.
    _allocation: Option<crate::NativeAllocation>,
}

impl Iterator for PreparedRecords {
    type Item = PreparedResident;

    fn next(&mut self) -> Option<Self::Item> {
        self.records.next().map(|record| {
            if self.native {
                PreparedResident::Native(AdoptedResident(record))
            } else {
                PreparedResident::Ordinary(record)
            }
        })
    }
}

impl PreparedChange {
    pub(super) fn new(
        mut change: Change,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, LocalCoreStateStoreError> {
        if let Some(work) = work {
            for record in &mut change.0 {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                fund(record, work)?;
            }
        }
        Ok(Self {
            change,
            native: work.is_some(),
        })
    }

    pub(super) fn change(&self) -> &Change {
        &self.change
    }

    pub(super) fn into_records(self) -> PreparedRecords {
        let Change(records, allocation) = self.change;
        PreparedRecords {
            records: records.into_iter(),
            native: self.native,
            _allocation: allocation,
        }
    }
}

impl Residents {
    fn requirements(
        &self,
        key: RecordKey,
        epoch: Uuid,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(usize, bool), LocalCoreStateStoreError> {
        let index = self.position(key, work)?;
        let dirty = index
            .map(|index| {
                self.entries
                    .get(index)
                    .ok_or(LocalCoreStateStoreError::Integrity)
                    .map(|entry| entry.dirty_epoch != Some(epoch))
            })
            .transpose()?
            .unwrap_or(true);
        Ok((usize::from(index.is_none()), dirty))
    }

    pub(super) fn admit_record(
        &mut self,
        key: RecordKey,
        epoch: Uuid,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, LocalCoreStateStoreError> {
        let (added, dirty) = self.requirements(key, epoch, Some(work))?;
        self.reserve(added, Some(work))?;
        Ok(dirty)
    }

    pub(super) fn admit_change(
        &mut self,
        change: &Change,
        epoch: Uuid,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<usize, LocalCoreStateStoreError> {
        let mut added = 0_usize;
        let mut dirty = 0_usize;
        for record in &change.0 {
            let (slots, newly_dirty) = self.requirements(record.key, epoch, work)?;
            added = added
                .checked_add(slots)
                .ok_or(crate::NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                ))?;
            dirty = dirty.checked_add(usize::from(newly_dirty)).ok_or(
                crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
            )?;
        }
        self.reserve(added, work)?;
        Ok(dirty)
    }

    pub(super) fn installation_work(
        count: usize,
    ) -> Result<crate::WorkCounters, LocalCoreStateStoreError> {
        let visits = count.checked_mul(3 * (KEY_BITS + 1)).ok_or(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        Ok(crate::WorkCounters {
            items_examined: visits as u64,
            ..crate::WorkCounters::UNCHARGED
        })
    }

    pub(super) fn install_loaded(
        &mut self,
        record: Resident,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let prepared = match work {
            Some(work) => PreparedResident::Native(AdoptedResident::new(record, work)?),
            None => PreparedResident::Ordinary(record),
        };
        self.reserve(1, work)?;
        let reservation = work
            .map(|work| {
                work.reserve_work(Self::installation_work(1)?)
                    .map_err(LocalCoreStateStoreError::from)
            })
            .transpose()?;
        let mut examined = 0;
        let result = self.install(prepared, None, &mut examined);
        if let Some(reservation) = reservation {
            reservation.record_completed(crate::WorkCounters {
                items_examined: examined,
                ..crate::WorkCounters::UNCHARGED
            })?;
        }
        result.map(|_| ())
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
    pub(super) fn get(&self, index: usize) -> Option<&Resident> {
        self.entries.get(index).map(|entry| &entry.record)
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &Resident> {
        self.entries.iter().map(|entry| &entry.record)
    }
    pub(super) fn keys(&self) -> impl Iterator<Item = &RecordKey> {
        self.iter().map(|record| &record.key)
    }
    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.index.clear();
        self.pending = None;
    }

    pub(super) fn reserve(
        &mut self,
        additions: usize,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let needed = self.entries.len().checked_add(additions).ok_or(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        if let Some(work) = work {
            reserve_retained_slots(&mut self.entries, needed, work, &mut self.allocation)?;
        } else if let Some(allocation) = &self.allocation {
            let original = allocation.work().clone();
            reserve_retained_slots(&mut self.entries, needed, &original, &mut self.allocation)?;
        } else {
            self.entries
                .try_reserve(additions)
                .map_err(std::io::Error::other)?;
        }
        self.index.reserve(additions, work)
    }

    pub(super) fn position(
        &self,
        key: RecordKey,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Option<usize>, LocalCoreStateStoreError> {
        self.index.position_with(key, &mut || {
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
            }
            Ok(())
        })
    }

    /// No allocation or budget admission is permitted at this installation point.
    pub(super) fn install(
        &mut self,
        prepared: PreparedResident,
        dirty_epoch: Option<Uuid>,
        examined: &mut u64,
    ) -> Result<bool, LocalCoreStateStoreError> {
        let (record, funded) = match prepared {
            PreparedResident::Ordinary(record) => (record, false),
            PreparedResident::Native(AdoptedResident(record)) => (record, true),
        };
        let index = self.index.position(record.key, examined)?;
        let newly_dirty;
        let index = if let Some(index) = index {
            let entry = self
                .entries
                .get_mut(index)
                .ok_or(LocalCoreStateStoreError::Integrity)?;
            newly_dirty = dirty_epoch.is_some_and(|epoch| entry.dirty_epoch != Some(epoch));
            entry.record = record;
            if let Some(epoch) = dirty_epoch {
                entry.dirty_epoch = Some(epoch);
            }
            index
        } else {
            if self.entries.len() == self.entries.capacity() {
                return Err(LocalCoreStateStoreError::Integrity);
            }
            let index = self.entries.len();
            self.index.insert(record.key, index, examined)?;
            newly_dirty = dirty_epoch.is_some();
            self.entries.push(Entry {
                record,
                dirty_epoch,
                pending: false,
                next_pending: None,
            });
            index
        };
        let entry = self
            .entries
            .get_mut(index)
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        if !funded && !entry.pending {
            entry.next_pending = self.pending;
            entry.pending = true;
            self.pending = Some(index);
        }
        Ok(newly_dirty)
    }

    pub(super) fn adopt_native(
        &mut self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.reserve(0, Some(work))?;
        while let Some(index) = self.pending {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let entry = self
                .entries
                .get_mut(index)
                .ok_or(LocalCoreStateStoreError::Integrity)?;
            // On any admission failure, both the record and queue link survive.
            fund(&mut entry.record, work)?;
            self.pending = entry.next_pending.take();
            entry.pending = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod index_integrity_tests {
    use super::*;

    #[test]
    fn every_invalid_branch_bit_returns_integrity_without_indexing_outside_the_key() {
        for bit in u8::try_from(KEY_BITS).unwrap()..=u8::MAX {
            let index = RecordIndex {
                nodes: vec![
                    IndexNode::Branch {
                        bit,
                        children: [1, 1],
                    },
                    IndexNode::Leaf {
                        key: [0; KEY_BYTES],
                        resident: 0,
                    },
                ],
                root: Some(0),
                allocation: None,
            };
            assert!(matches!(
                index.leaf(&[0; KEY_BYTES], &mut 0),
                Err(LocalCoreStateStoreError::Integrity)
            ));
        }
    }

    #[test]
    fn every_valid_branch_bit_keeps_invalid_child_indices_as_integrity_errors() {
        for bit in 0..u8::try_from(KEY_BITS).unwrap() {
            let index = RecordIndex {
                nodes: vec![IndexNode::Branch {
                    bit,
                    children: [usize::MAX, usize::MAX],
                }],
                root: Some(0),
                allocation: None,
            };
            for key in [[0; KEY_BYTES], [u8::MAX; KEY_BYTES]] {
                assert!(matches!(
                    index.leaf(&key, &mut 0),
                    Err(LocalCoreStateStoreError::Integrity)
                ));
            }
        }
    }
}
