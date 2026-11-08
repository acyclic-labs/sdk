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
    },
}

impl Staged {
    pub(super) fn length(&self) -> u64 {
        match self {
            Self::Resident { object, .. } => object.length(),
            Self::Spilled { length, .. } => *length,
        }
    }
}

/// Where one object is staged, as one read finds it.
pub(super) enum Lookup {
    Absent,
    Resident(OwnedBytes),
    Spilled { offset: u64, length: u64 },
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
    pub(super) spilled_objects: usize,
    // Append offset of the spill file; space is reused only once no staged
    // object remains spilled.
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
    pub(super) fn lookup(&self, object_id: ObjectId) -> Lookup {
        match self.objects.get(&object_id) {
            None => Lookup::Absent,
            Some(Staged::Resident {
                object, referenced, ..
            }) => {
                referenced.store(true, Ordering::Relaxed);
                Lookup::Resident(object.bytes().clone())
            }
            Some(&Staged::Spilled { offset, length }) => Lookup::Spilled { offset, length },
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
        self.resident_bytes = self.resident_bytes.saturating_add(object.length());
        self.order.insert(order, object_id);
        if let Some(Staged::Spilled { .. }) = self.objects.insert(
            object_id,
            Staged::Resident {
                object,
                referenced: AtomicBool::new(false),
                order,
                incarnation,
                decoded: OnceLock::new(),
            },
        ) {
            self.spilled_objects -= 1;
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
            spilled = spilled.saturating_add(object.length());
            victims.push((object_id, object.bytes().clone()));
        }
        // The caller owns the preallocated vector and its native custody.
    }

    /// Records victims written at `offset` onward as spilled. Each is still
    /// resident: only spills and drains move objects, and both hold the
    /// spill file exclusively.
    pub(super) fn mark_spilled(&mut self, victims: &[(ObjectId, OwnedBytes)], mut offset: u64) {
        for (object_id, bytes) in victims {
            let length = byte_length(bytes);
            if let Some(staged) = self.objects.get_mut(object_id) {
                *staged = Staged::Spilled { offset, length };
                self.spilled_objects += 1;
                self.resident_bytes = self.resident_bytes.saturating_sub(length);
            }
            offset = offset.saturating_add(length);
        }
        self.spill_end = offset;
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
                self.resident_bytes = self.resident_bytes.saturating_sub(object.length());
            }
            Some(Staged::Spilled { .. }) => self.spilled_objects -= 1,
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
