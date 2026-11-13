use super::*;

pub(super) enum Staged {
    /// Held in memory. `referenced` records a read since the window last
    /// considered spilling it; `order` is its key in `Index::order`;
    /// `decoded` holds its first decoded representation, which lives and
    /// leaves with the resident bytes.
    Resident {
        object: HashedObject,
        referenced: AtomicBool,
        order: u64,
        incarnation: u64,
        decoded: OnceLock<(DecodedCacheKey, DecodedCacheValue)>,
    },
    Spilled {
        offset: u64,
        length: u64,
        stored_length: u64,
    },
}

impl Staged {
    pub(super) fn stored_length(&self) -> u64 {
        match self {
            Self::Resident { object, .. } => byte_length(object.stored_bytes()),
            Self::Spilled { stored_length, .. } => *stored_length,
        }
    }
    pub(super) fn length(&self) -> u64 {
        match self {
            Self::Resident { object, .. } => object.length(),
            Self::Spilled { length, .. } => *length,
        }
    }
}

pub(super) fn resident_length(object: &HashedObject) -> u64 {
    object
        .length()
        .saturating_add(if object.object_id().storage.is_some() {
            byte_length(object.stored_bytes())
        } else {
            0
        })
}

/// Where one object is staged, as one read finds it.
pub(super) enum Lookup {
    Absent,
    Resident {
        bytes: OwnedBytes,
        length: u64,
    },
    Spilled {
        offset: u64,
        length: u64,
        stored_length: u64,
    },
}

/// Hashes an [`ObjectId`] by folding the words it writes: its digest is
/// already a cryptographic hash, so mixing it again, as the default
/// `SipHash` does, only adds work to every staged read and admission.
#[derive(Default)]
pub(super) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn write(&mut self, bytes: &[u8]) {
        let (words, rest) = bytes.as_chunks::<8>();
        for &word in words {
            self.write_u64(u64::from_le_bytes(word));
        }
        for &byte in rest {
            self.write_u64(u64::from(byte));
        }
    }

    fn write_u64(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[derive(Default)]
pub(super) struct Index {
    pub(super) objects: HashMap<ObjectId, Staged, BuildHasherDefault<IdentityHasher>>,
    // Resident identities in admission order, each re-queued once when read
    // since it was last considered: the window spills from the front, so
    // pages the current candidate keeps reading stay in memory while
    // superseded ones leave it.
    pub(super) order: BTreeMap<u64, ObjectId>,
    pub(super) next_order: u64,
    next_incarnation: u64,
    pub(super) resident_bytes: u64,
    /// Live physical extents in file order, for bounded collection pages.
    /// The map length is the spilled count; there is no separate counter.
    /// Identity breaks ties for empty objects sharing a zero-length range.
    pub(super) spilled: BTreeMap<(u64, ObjectId), u64>,
    /// Coalesced holes, ordered by length for logarithmic best-fit reuse.
    free: std::collections::BTreeSet<(u64, u64)>,
    // Highest admitted spill byte. Holes can be reused before this edge.
    pub(super) spill_end: u64,
    // Objects the durable provider acknowledged while the collection's sweep
    // count was `drained_at`. Only a sweep removes a durable object, so while
    // the count is unchanged each is still durable, and staging one again
    // (as every new file restages the shared default metadata) needs no
    // second write.
    pub(super) drained: HashSet<ObjectId, BuildHasherDefault<IdentityHasher>>,
    pub(super) drained_at: u64,
}

impl Index {
    /// A bounded page in the existing best-fit order. The caller holds the
    /// spill lease through deallocation so these extents cannot be reused.
    pub(super) fn free_page(&self, after: Option<(u64, u64)>, limit: usize) -> Vec<(u64, u64)> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.free
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .take(limit)
            .copied()
            .collect()
    }

    pub(super) fn lookup(&self, object_id: ObjectId) -> Lookup {
        self.lookup_representation(object_id, false)
    }

    pub(super) fn lookup_representation(&self, object_id: ObjectId, stored: bool) -> Lookup {
        match self.objects.get(&object_id) {
            None => Lookup::Absent,
            Some(Staged::Resident {
                object, referenced, ..
            }) => {
                referenced.store(true, Ordering::Relaxed);
                Lookup::Resident {
                    bytes: if stored {
                        object.stored_bytes()
                    } else {
                        object.bytes()
                    }
                    .clone(),
                    length: object.length(),
                }
            }
            Some(&Staged::Spilled {
                offset,
                length,
                stored_length,
            }) => Lookup::Spilled {
                offset,
                length,
                stored_length,
            },
        }
    }

    /// Holds the original seal in memory unless the identity is
    /// already staged. Returns whether the object was admitted.
    pub(super) fn admit_resident(
        &mut self,
        object: HashedObject,
    ) -> Result<bool, ObjectStoreError> {
        let object_id = object.object_id();
        if self.objects.contains_key(&object_id) {
            return Ok(false);
        }
        self.insert_resident(object)?;
        Ok(true)
    }

    pub(super) fn insert_resident(&mut self, object: HashedObject) -> Result<(), ObjectStoreError> {
        let object_id = object.object_id();
        let incarnation = self.next_incarnation;
        let next = incarnation
            .checked_add(1)
            .ok_or(ObjectStoreError::Work(crate::WorkError::Overflow))?;
        self.next_incarnation = next;
        let order = self.next_order;
        self.next_order += 1;
        self.resident_bytes = self.resident_bytes.saturating_add(resident_length(&object));
        self.order.insert(order, object_id);
        if let Some(Staged::Spilled {
            offset,
            stored_length,
            ..
        }) = self.objects.insert(
            object_id,
            Staged::Resident {
                object,
                referenced: AtomicBool::new(false),
                order,
                incarnation,
                decoded: OnceLock::new(),
            },
        ) {
            self.retire_spilled(offset, object_id, stored_length);
        }
        Ok(())
    }

    /// Takes the resident objects to spill, oldest first, until the window
    /// would be half full. An object read since it was last considered is
    /// re-queued once instead.
    pub(super) fn spill_victims(&mut self, victims: &mut Vec<(ObjectId, OwnedBytes)>) {
        let mut spilled = 0_u64;
        while self.resident_bytes.saturating_sub(spilled) > MAXIMUM_RESIDENT_BYTES / 2 {
            let Some((_, object_id)) = self.order.pop_first() else {
                break;
            };
            let Some(Staged::Resident {
                object,
                referenced,
                order,
                ..
            }) = self.objects.get_mut(&object_id)
            else {
                continue;
            };
            if referenced.swap(false, Ordering::Relaxed) {
                *order = self.next_order;
                self.next_order += 1;
                self.order.insert(*order, object_id);
                continue;
            }
            spilled = spilled.saturating_add(resident_length(object));
            victims.push((object_id, object.stored_bytes().clone()));
        }
        // The caller owns the preallocated vector and its native custody.
    }

    /// The existing exclusive file lease keeps this choice stable until the
    /// physical write finishes. Failed/withdrawn writes consume no free span.
    pub(super) fn spill_offset(&self, length: u64) -> Result<u64, ObjectStoreError> {
        let offset = if length == 0 {
            self.spill_end
        } else {
            self.free
                .range((length, 0)..)
                .next()
                .map_or(self.spill_end, |&(_, offset)| offset)
        };
        offset
            .checked_add(length)
            .ok_or(crate::WorkError::Overflow)?;
        Ok(offset)
    }

    fn occupy_spill(&mut self, offset: u64, length: u64) {
        if length == 0 {
            return;
        }
        let end = offset
            .checked_add(length)
            .expect("spill range checked before physical write");
        if offset >= self.spill_end {
            if offset > self.spill_end {
                self.free.insert((offset - self.spill_end, self.spill_end));
            }
            self.spill_end = end;
            return;
        }
        let &(available, start) = self
            .free
            .range((length, 0)..)
            .next()
            .expect("exclusive spill lease retains the selected free span");
        assert_eq!(
            start, offset,
            "selected spill span changed before completion"
        );
        self.free.remove(&(available, start));
        if available > length {
            self.free.insert((available - length, end));
        }
    }

    /// Neighboring live extents identify both bordering holes, so a second
    /// offset-ordered free index is unnecessary. Empty objects occupy no range.
    fn retire_spilled(&mut self, offset: u64, object_id: ObjectId, length: u64) {
        self.spilled.remove(&(offset, object_id));
        if length == 0 {
            return;
        }
        let key = (offset, object_id);
        let previous = self
            .spilled
            .range(..key)
            .rev()
            .find(|(_, length)| **length != 0)
            .map_or(0, |(&(start, _), &length)| start + length);
        let next = self
            .spilled
            .range(key..)
            .find(|(_, length)| **length != 0)
            .map_or(self.spill_end, |(&(start, _), _)| start);
        if previous < offset {
            self.free.remove(&(offset - previous, previous));
        }
        let end = offset + length;
        if end < next {
            self.free.remove(&(next - end, end));
        }
        self.free.insert((next - previous, previous));
    }

    /// The file is closed/truncated under its exclusive lease before reset.
    pub(super) fn reset_spill(&mut self) {
        assert!(self.spilled.is_empty());
        self.free.clear();
        self.spill_end = 0;
    }

    /// Records victims written at `offset` onward as spilled. Each is still
    /// resident: only spills and drains move objects, and both hold the
    /// spill file exclusively.
    pub(super) fn mark_spilled(
        &mut self,
        victims: &[(ObjectId, OwnedBytes)],
        mut offset: u64,
        length: u64,
    ) {
        self.occupy_spill(offset, length);
        for (object_id, bytes) in victims {
            let stored_length = byte_length(bytes);
            if let Some(staged) = self.objects.get_mut(object_id) {
                let length = staged.length();
                let resident = match staged {
                    Staged::Resident { object, .. } => resident_length(object),
                    Staged::Spilled { .. } => {
                        unreachable!("selected spill victim remains resident")
                    }
                };
                *staged = Staged::Spilled {
                    offset,
                    length,
                    stored_length,
                };
                self.spilled.insert((offset, *object_id), stored_length);
                self.resident_bytes = self.resident_bytes.saturating_sub(resident);
            }
            offset = offset.saturating_add(stored_length);
        }
    }

    /// Forgets one object the durable provider has acknowledged, and
    /// remembers it as drained.
    pub(super) fn acknowledge(&mut self, object_id: ObjectId) {
        self.remove(object_id);
        if self.drained.len() >= MAXIMUM_DRAINED_IDENTITIES {
            self.drained.clear();
        }
        self.drained.insert(object_id);
    }

    /// Forgets one staged object.
    pub(super) fn remove(&mut self, object_id: ObjectId) {
        match self.objects.remove(&object_id) {
            Some(Staged::Resident { object, order, .. }) => {
                self.order.remove(&order);
                self.resident_bytes = self.resident_bytes.saturating_sub(resident_length(&object));
            }
            Some(Staged::Spilled {
                offset,
                stored_length,
                ..
            }) => {
                self.retire_spilled(offset, object_id, stored_length);
            }
            None => {}
        }
    }
}

#[cfg(test)]
#[test]
fn exhausted_resident_incarnation_refuses_without_reusing_a_retired_slot() {
    let bytes = OwnedBytes::from_static(b"resident incarnation");
    let object = HashedObject::new(crate::ObjectKind::BlobChunk, bytes);
    let id = object.object_id();
    let mut index = Index::default();
    assert!(index.admit_resident(object.clone()).unwrap());
    let next_order = index.next_order;
    index.remove(id);
    index.next_incarnation = u64::MAX;
    assert!(matches!(
        index.admit_resident(object),
        Err(ObjectStoreError::Work(crate::WorkError::Overflow))
    ));
    assert!(index.objects.is_empty());
    assert!(index.order.is_empty());
    assert_eq!(index.next_order, next_order);
    assert_eq!(index.resident_bytes, 0);
}

#[cfg(test)]
#[test]
fn spill_range_simulation_preserves_bytes_across_reuse_and_promotion() {
    // Exercise the real staging index against a separate byte-addressed medium.
    // The medium oracle detects reuse of any live interval, not just map shape.
    let mut index = Index::default();
    let mut medium = Vec::new();
    let mut live = Vec::<HashedObject>::new();
    let mut seed = 0x86a9_71c3_u64;
    for step in 0_u64..768 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        if !live.is_empty() && seed % 3 == 0 {
            let position = (seed as usize) % live.len();
            let object = live.swap_remove(position);
            index.remove(object.object_id());
        } else {
            let mut bytes = if step == 0 {
                Vec::new()
            } else {
                step.to_le_bytes().to_vec()
            };
            if step != 0 {
                bytes.resize(8 + (seed as usize % 113), (seed >> 32) as u8);
            }
            let object = HashedObject::new(crate::ObjectKind::BlobChunk, bytes.into());
            index.admit_resident(object.clone()).unwrap();
            live.push(object);
        }
        // Promote a physical object through the same index transition as read_private.
        if !live.is_empty() && seed % 5 == 0 {
            let object = live[(seed as usize) % live.len()].clone();
            if matches!(
                index.objects.get(&object.object_id()),
                Some(Staged::Spilled { .. })
            ) {
                index.insert_resident(object).unwrap();
            }
        }
        let residents = index.order.values().copied().collect::<Vec<_>>();
        for id in residents {
            let Some(Staged::Resident { object, order, .. }) = index.objects.get(&id) else {
                panic!("resident order points at a nonresident");
            };
            let bytes = object.stored_bytes().clone();
            let order = *order;
            index.order.remove(&order);
            let offset = index.spill_offset(byte_length(&bytes)).unwrap();
            let end = offset as usize + bytes.len();
            medium.resize(medium.len().max(end), 0);
            medium[offset as usize..end].copy_from_slice(&bytes);
            let length = byte_length(&bytes);
            index.mark_spilled(&[(id, bytes)], offset, length);
        }
        for object in &live {
            let id = object.object_id();
            let Lookup::Spilled {
                offset,
                stored_length,
                ..
            } = index.lookup(id)
            else {
                panic!("simulation did not spill a surviving object");
            };
            assert_eq!(
                &medium[offset as usize..(offset + stored_length) as usize],
                object.stored_bytes().as_slice(),
                "live bytes were overwritten at step {step}",
            );
        }
        let mut intervals = index
            .spilled
            .iter()
            .map(|(&(offset, _), &length)| (offset, length))
            .chain(index.free.iter().map(|&(length, offset)| (offset, length)))
            .collect::<Vec<_>>();
        intervals.retain(|(_, length)| *length != 0);
        intervals.sort_unstable();
        let mut cursor = 0;
        for (offset, length) in intervals {
            assert_eq!(offset, cursor, "overlap or untracked range at step {step}");
            cursor += length;
        }
        assert_eq!(cursor, index.spill_end);
        // With no surviving extents, the real exclusive lease closes the file.
        if live.is_empty() {
            index.reset_spill();
            medium.clear();
        }
    }
    for object in live {
        index.remove(object.object_id());
    }
    index.reset_spill();
    assert!(index.spilled.is_empty() && index.free.is_empty());
}
