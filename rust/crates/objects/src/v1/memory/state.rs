use super::*;

#[derive(Clone, Default)]
pub(in crate::v1) struct State {
    pub(super) buckets: OrdMap<SharedInfo<String>, Bucket>,
    pub(super) uploads: OrdMap<SharedInfo<String>, Upload>,
    pub(super) receipts: OrdMap<SharedInfo<String>, Receipt>,
    pub(super) sequence: u64,
    /// A durable command's sampled time; ordinary providers sample the clock.
    pub(super) publication_time: Option<prost_types::Timestamp>,
    /// Body bytes of current objects and staged parts, maintained by [`stored_bytes`].
    pub(super) body_bytes: usize,
}
impl State {
    pub(super) fn entry_count(&self) -> usize {
        self.buckets.len()
            + self.uploads.len()
            + self.receipts.len()
            + self
                .buckets
                .values()
                .map(|bucket| bucket.objects.len())
                .sum::<usize>()
            + self
                .uploads
                .values()
                .map(|upload| upload.parts.len())
                .sum::<usize>()
    }
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
// Nested persistent maps must compare through their changed subtrees; ordinary
// ordered iteration would revisit every preceding object/part on each update.
impl PartialEq for Bucket {
    fn eq(&self, other: &Self) -> bool {
        self.info == other.info && self.objects.diff(&other.objects).next().is_none()
    }
}
impl PartialEq for Upload {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.key == other.key
            && self.metadata == other.metadata
            && self.parts.diff(&other.parts).next().is_none()
    }
}

#[derive(Clone, PartialEq)]
pub(super) struct Receipt {
    pub(super) digest: [u8; 32],
    pub(super) response: OwnedBytes,
    pub(super) put_info: Option<SharedInfo>,
    #[cfg(all(any(feature = "local", feature = "s3"), not(target_arch = "wasm32")))]
    pub(super) kind: u32,
}

/// Updates stored bytes from changed persistent subtrees.
pub(super) fn stored_bytes(before: &State, next: &mut State) -> Option<usize> {
    let (mut added, mut removed) = (0_usize, 0_usize);
    for change in before.buckets.diff(&next.buckets) {
        let empty = OrdMap::new();
        let (old, new) = match change {
            imbl::ordmap::DiffItem::Add(_, bucket) => (&empty, &bucket.objects),
            imbl::ordmap::DiffItem::Update {
                old: (_, old),
                new: (_, new),
            } => (&old.objects, &new.objects),
            imbl::ordmap::DiffItem::Remove(_, bucket) => (&bucket.objects, &empty),
        };
        for item in old.diff(new) {
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
    for change in before.uploads.diff(&next.uploads) {
        let empty = OrdMap::new();
        let (old, new) = match change {
            imbl::ordmap::DiffItem::Add(_, upload) => (&empty, &upload.parts),
            imbl::ordmap::DiffItem::Update {
                old: (_, old),
                new: (_, new),
            } => (&old.parts, &new.parts),
            imbl::ordmap::DiffItem::Remove(_, upload) => (&upload.parts, &empty),
        };
        for change in old.diff(new) {
            let (old, new) = match change {
                imbl::ordmap::DiffItem::Add(_, (_, body)) => (0, body.len()),
                imbl::ordmap::DiffItem::Update {
                    old: (_, (_, old)),
                    new: (_, (_, new)),
                } => (old.len(), new.len()),
                imbl::ordmap::DiffItem::Remove(_, (_, body)) => (body.len(), 0),
            };
            added = added.checked_add(new)?;
            removed = removed.checked_add(old)?;
        }
    }
    next.body_bytes = before.body_bytes.checked_add(added)?.checked_sub(removed)?;
    Some(next.body_bytes)
}

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
