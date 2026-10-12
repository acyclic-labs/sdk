use super::*;

pub(super) struct StampedCursor<T> {
    pub(super) generation: (u64, u64),
    pub(super) cursor: T,
}

pub(super) struct CursorTable<T> {
    pub(super) entries: Mutex<BTreeMap<u64, StampedCursor<T>>>,
    pub(super) next: AtomicU64,
    pub(super) maximum: usize,
}

impl<T> CursorTable<T> {
pub(super) fn new(maximum: usize) -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            next: AtomicU64::new(1),
            maximum,
        }
    }

pub(super) fn remember(&self, generation: (u64, u64), cursor: T) -> Result<Vec<u8>, MountSourceError> {
        let mut token = self.next.fetch_add(1, Ordering::Relaxed);
        if token == 0 {
            token = self.next.fetch_add(1, Ordering::Relaxed);
        }
        let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
        while entries.len() >= self.maximum {
            let Some(oldest) = entries.first_key_value().map(|(&key, _)| key) else {
                break;
            };
            entries.remove(&oldest);
        }
        entries.insert(token, StampedCursor { generation, cursor });
        Ok(token.to_le_bytes().to_vec())
    }

pub(super) fn take(
        &self,
        generation: (u64, u64),
        cursor: Option<&[u8]>,
    ) -> Result<Option<T>, MountSourceError> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
        let stamped = self
            .entries
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .remove(&u64::from_le_bytes(bytes))
            .ok_or(MountSourceError::Stale)?;
        if stamped.generation != generation {
            return Err(MountSourceError::Stale);
        }
        Ok(Some(stamped.cursor))
    }

pub(super) fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}

/// Source identities last proven absent from the authored checkout, each
pub(super) type UnstagedIdentities = Arc<UnstagedTable>;

#[derive(Default)]
pub(super) struct UnstagedTable(pub(super) Mutex<HashMap<FileId, u64>>);

impl UnstagedTable {
pub(super) fn checked_at(&self, file_id: FileId, revision: u64) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&file_id)
            == Some(&revision)
    }

pub(super) fn remember(&self, file_id: FileId, revision: u64) {
        let mut checked = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if checked.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
            checked.clear();
        }
        checked.insert(file_id, revision);
    }
}

/// How the view answers a name the authored checkout lacks: the source node
pub(super) enum Resolution {
    /// The authored checkout answers the name.
    Authored(MountLookup),
    /// Nothing answers the name.
    Absent,
    /// The lazy workspace answers the name the authored checkout lacks.
    Unauthored(LazyLookup, Option<SourceReference>),
}

/// How a remembered deferral proves the source still names its node. The
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum SourceProof {
    /// A fresh source lookup must name exactly the remembered node.
    Lookup,
    /// The caller opens the remembered node at its exact version, which
    Open,
}

/// A name the view deferred to the source: the authored checkout and the
#[derive(Clone, Copy)]
pub(super) struct Deferral {
    /// Sampled before the name was resolved.
    pub(super) stamp: ViewStamp,
    pub(super) source: SourceReference,
    pub(super) node: Option<SourceNode>,
}

impl Deferral {
pub(super) fn resolution(self) -> Resolution {
        self.node.map_or(Resolution::Absent, |node| {
            Resolution::Unauthored(LazyLookup::Source(node), Some(self.source))
        })
    }
}

/// Names the view deferred to the source. A deferral holds only while the
#[derive(Default)]
pub(super) struct Resolutions(pub(super) Mutex<HashMap<MountPath, Deferral>>);

impl Resolutions {
pub(super) fn get(&self, path: &MountPath) -> Option<Deferral> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .copied()
    }

pub(super) fn remember(&self, path: &MountPath, deferral: Deferral) {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
            entries.clear();
        }
        entries.insert(path.clone(), deferral);
    }

pub(super) fn forget(&self, path: &MountPath) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(path);
    }
}

/// Directories a source lacks; see [`LazyMountSource::source_lacks`].
#[derive(Default)]
pub(super) struct SourceAbsentDirectories(pub(super) Mutex<HashMap<MountPath, (ViewStamp, SourceReference)>>);

impl SourceAbsentDirectories {
pub(super) fn get(&self, path: &MountPath) -> Option<(ViewStamp, SourceReference)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .copied()
    }

pub(super) fn remember(&self, path: &MountPath, stamp: ViewStamp, source: SourceReference) {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
            entries.clear();
        }
        entries.insert(path.clone(), (stamp, source));
    }
}

#[derive(Default)]
pub(super) struct MountedRemovals {
    pub(super) paths: BTreeMap<String, FileId>,
    pub(super) records: BTreeMap<FileId, (FileRecord, FileMetadata)>,
    pub(super) epoch: u64,
}

impl MountedRemovals {
pub(super) fn rebound(&mut self, path: &str) -> bool {
        let Some(file_id) = self.paths.remove(path) else {
            return false;
        };
        if !self.paths.values().any(|pending| *pending == file_id) {
            self.records.remove(&file_id);
        }
        self.epoch = self.epoch.wrapping_add(1);
        true
    }
}

pub(super) struct DetachedIdentity<A, O> {
    pub(super) file: Arc<CheckoutDetachedFile<A, O>>,
    pub(super) ready: bool,
    pub(super) published_epoch: u64,
    pub(super) pending_epoch: Option<u64>,
}

pub(super) type DetachedIdentities<A, O> = Arc<Mutex<BTreeMap<FileId, DetachedIdentity<A, O>>>>;

pub(super) type OpenIdentityHandles = Arc<Mutex<BTreeMap<FileId, Vec<Weak<dyn MountOpenFile>>>>>;

pub(super) type DirtyDetachedRecord = (FileId, FileRecord, FileMetadata, u64);
