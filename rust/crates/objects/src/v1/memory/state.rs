use super::*;

#[derive(Clone, Default)]
pub(in crate::v1) struct State {
    pub(super) buckets: OrdMap<SharedInfo<String>, Bucket>,
    pub(super) uploads: OrdMap<SharedInfo<String>, Upload>,
    pub(super) receipts: OrdMap<SharedInfo<String>, Receipt>,
    pub(super) sequence: u64,
    /// Body bytes of every stored object, maintained by [`stored_bytes`].
    pub(super) object_bytes: usize,
    pub(super) upload_bytes: usize,
    pub(super) nested_entries: usize,
}
#[derive(Clone)]
pub(super) struct Bucket {
    pub(super) info: SharedInfo<wire::Bucket>,
    pub(super) objects: OrdMap<SharedInfo<String>, Stored>,
}
#[derive(Clone, PartialEq)]
pub(super) struct Stored {
    pub(super) info: SharedInfo,
    pub(super) body: StoredBody,
}
#[derive(Clone)]
pub(super) struct Upload {
    pub(super) bucket: SharedInfo<String>,
    pub(super) key: SharedInfo<String>,
    pub(super) metadata: SharedInfo<Option<wire::ObjectMetadata>>,
    pub(super) parts: OrdMap<u32, (SharedInfo<wire::UploadedPart>, StoredBody)>,
}

// Persistent-map diff must compare subtree identity, never walk its contents
// just to decide whether that subtree changed. Unequal identities deliberately
// retain equal-value updates for the inner diff to resolve.
impl PartialEq for Bucket {
    fn eq(&self, other: &Self) -> bool {
        self.info == other.info && self.objects.ptr_eq(&other.objects)
    }
}
impl PartialEq for Upload {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.key == other.key
            && self.metadata == other.metadata
            && self.parts.ptr_eq(&other.parts)
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(super) fn changed<'a, K: Clone + Ord, V: Clone + PartialEq>(
    before: Option<&'a OrdMap<K, V>>,
    next: &'a OrdMap<K, V>,
) -> impl Iterator<Item = (&'a K, &'a V)> {
    before
        .into_iter()
        .flat_map(move |before| {
            before
                .diff(next)
                .inspect(|_| examined())
                .filter_map(|item| match item {
                    imbl::ordmap::DiffItem::Add(key, value)
                    | imbl::ordmap::DiffItem::Update {
                        new: (key, value), ..
                    } => Some((key, value)),
                    imbl::ordmap::DiffItem::Remove(..) => None,
                })
        })
        .chain(
            before
                .is_none()
                .then_some(next)
                .into_iter()
                .flat_map(|next| next.iter().inspect(|_| examined())),
        )
}
#[derive(Clone, PartialEq)]
pub(super) struct Receipt {
    pub(super) digest: [u8; 32],
    pub(super) response: OwnedBytes,
    pub(super) put_info: Option<SharedInfo>,
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(super) kind: u32,
}

/// Updates stored bytes from changed persistent subtrees.
pub(super) fn stored_bytes(before: &State, next: &mut State) -> Option<usize> {
    #[cfg(all(test, not(objects_loom)))]
    if tests::restore_scan() {
        let count = next
            .buckets
            .values()
            .map(|bucket| {
                examined();
                bucket.objects.len()
            })
            .sum::<usize>()
            + next
                .uploads
                .values()
                .map(|upload| {
                    examined();
                    upload
                        .parts
                        .values()
                        .map(|(_, body)| {
                            examined();
                            std::hint::black_box(body.len());
                            1
                        })
                        .sum::<usize>()
                })
                .sum::<usize>();
        std::hint::black_box(count);
    }
    let sum = |bucket: &Bucket| {
        bucket.objects.values().try_fold(0_usize, |total, object| {
            examined();
            total.checked_add(object.body.len())
        })
    };
    let (mut added, mut removed) = (0_usize, 0_usize);
    let (mut entries_added, mut entries_removed) = (0_usize, 0_usize);
    for item in before.buckets.diff(&next.buckets) {
        examined();
        let (old, bucket) = match item {
            imbl::ordmap::DiffItem::Add(_, bucket) => {
                added = added.checked_add(sum(bucket)?)?;
                entries_added = entries_added.checked_add(bucket.objects.len())?;
                continue;
            }
            imbl::ordmap::DiffItem::Remove(_, bucket) => {
                removed = removed.checked_add(sum(bucket)?)?;
                entries_removed = entries_removed.checked_add(bucket.objects.len())?;
                continue;
            }
            imbl::ordmap::DiffItem::Update {
                old: (_, old),
                new: (_, bucket),
            } => (old, bucket),
        };
        entries_removed = entries_removed.checked_add(old.objects.len())?;
        entries_added = entries_added.checked_add(bucket.objects.len())?;
        for item in old.objects.diff(&bucket.objects) {
            examined();
            let (old, new) = match item {
                imbl::ordmap::DiffItem::Add(_, new) => (0, new.body.len()),
                imbl::ordmap::DiffItem::Update {
                    old: (_, old),
                    new: (_, new),
                } => (old.body.len(), new.body.len()),
                imbl::ordmap::DiffItem::Remove(_, old) => (old.body.len(), 0),
            };
            added = added.checked_add(new)?;
            removed = removed.checked_add(old)?;
        }
    }
    let object_bytes = adjusted_total(before.object_bytes, removed, added)?;
    let sum_parts = |upload: &Upload| {
        upload.parts.values().try_fold(0_usize, |total, (_, body)| {
            examined();
            total.checked_add(body.len())
        })
    };
    let (mut added, mut removed) = (0_usize, 0_usize);
    for item in before.uploads.diff(&next.uploads) {
        examined();
        let (old, upload) = match item {
            imbl::ordmap::DiffItem::Add(_, upload) => {
                added = added.checked_add(sum_parts(upload)?)?;
                entries_added = entries_added.checked_add(upload.parts.len())?;
                continue;
            }
            imbl::ordmap::DiffItem::Remove(_, upload) => {
                removed = removed.checked_add(sum_parts(upload)?)?;
                entries_removed = entries_removed.checked_add(upload.parts.len())?;
                continue;
            }
            imbl::ordmap::DiffItem::Update {
                old: (_, old),
                new: (_, upload),
            } => (old, upload),
        };
        entries_removed = entries_removed.checked_add(old.parts.len())?;
        entries_added = entries_added.checked_add(upload.parts.len())?;
        for item in old.parts.diff(&upload.parts) {
            examined();
            let (old, new) = match item {
                imbl::ordmap::DiffItem::Add(_, (_, body)) => (0, body.len()),
                imbl::ordmap::DiffItem::Remove(_, (_, body)) => (body.len(), 0),
                imbl::ordmap::DiffItem::Update {
                    old: (_, (_, old)),
                    new: (_, (_, new)),
                } => (old.len(), new.len()),
            };
            added = added.checked_add(new)?;
            removed = removed.checked_add(old)?;
        }
    }
    let upload_bytes = adjusted_total(before.upload_bytes, removed, added)?;
    let entries = adjusted_total(before.nested_entries, entries_removed, entries_added)?;
    let total = object_bytes.checked_add(upload_bytes)?;
    next.object_bytes = object_bytes;
    next.upload_bytes = upload_bytes;
    next.nested_entries = entries;
    #[cfg(all(test, not(objects_loom)))]
    if tests::omit_update() {
        next.nested_entries = before.nested_entries;
    }
    Some(total)
}

pub(super) fn adjusted_total(total: usize, removed: usize, added: usize) -> Option<usize> {
    #[cfg(all(test, not(objects_loom)))]
    if tests::restore_add_first() {
        return total.checked_add(added)?.checked_sub(removed);
    }
    total.checked_sub(removed)?.checked_add(added)
}

#[inline]
pub(super) fn examined() {
    #[cfg(all(test, not(objects_loom)))]
    tests::examined();
}

#[cfg(all(test, not(objects_loom)))]
mod tests;

#[derive(Clone)]
pub(super) struct SharedInfo<T = wire::ObjectInfo>(pub(super) Arc<super::Guarded<T>>);
impl<T> std::ops::Deref for SharedInfo<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0.value
    }
}
impl<T: PartialEq> PartialEq for SharedInfo<T> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0.value == other.0.value
    }
}
impl<T> From<T> for SharedInfo<T> {
    fn from(info: T) -> Self {
        Self(Arc::new(super::Guarded {
            value: info,
            _allocation: Default::default(),
        }))
    }
}

impl std::borrow::Borrow<str> for SharedInfo<String> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
impl std::fmt::Display for SharedInfo<String> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.value.fmt(f)
    }
}
impl<T: Eq> Eq for SharedInfo<T> {}
impl<T: Ord> PartialOrd for SharedInfo<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Ord> Ord for SharedInfo<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.value.cmp(&other.0.value)
    }
}
