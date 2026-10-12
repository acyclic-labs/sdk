use super::*;

pub(super) struct ChangeLabels {
    pub(super) next: AtomicU64,
    pub(super) labels: Mutex<DarwinOwned<HashMap<FileId, IssuedLabel>>>,
    pub(super) cleared: AtomicU64,
}

pub(super) struct IssuedLabel {
    pub(super) label: u64,
    pub(super) stamp: CacheStamp,
    pub(super) binding: Option<u64>,
    pub(super) path: DarwinOwned<MountPath>,
    pub(super) lookup: MountLookup,
}

impl ChangeLabels {
    pub(super) fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
            labels: Mutex::new(DarwinOwned::empty(HashMap::new())),
            cleared: AtomicU64::new(0),
        }
    }

    pub(super) fn fresh(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn issued(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<DarwinOwned<Vec<(DarwinOwned<MountPath>, MountLookup)>>>, NativeMountError>
    {
        let labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        work.charge(scan_work(labels.capacity() as u64))?;
        if labels.len() > MAXIMUM_VERIFIED_LABELS {
            return Ok(None);
        }
        let bytes = labels
            .len()
            .checked_mul(std::mem::size_of::<(DarwinOwned<MountPath>, MountLookup)>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        let mut rows = owned(work, bytes, || {
            let mut rows = Vec::new();
            rows.try_reserve_exact(labels.len())?;
            Ok(rows)
        })?;
        if let Some(allocation) = &mut rows.allocation {
            allocation.reconcile_capacity(
                rows.value
                    .capacity()
                    .checked_mul(std::mem::size_of::<(DarwinOwned<MountPath>, MountLookup)>())
                    .ok_or(crate::WorkError::Overflow)? as u64,
            )?;
        }
        for issued in labels.values() {
            let path = clone_path(&issued.path, work)?;
            copying(
                work,
                copy_work(std::mem::size_of::<(DarwinOwned<MountPath>, MountLookup)>() as u64),
                || rows.push((path, issued.lookup)),
            )?;
        }
        Ok(Some(rows))
    }
}

pub(super) fn same_attributes(mut current: MountLookup, previous: MountLookup) -> bool {
    current.metadata.accessed_ns = previous.metadata.accessed_ns;
    current == previous
        && (current.node.kind != MountNodeKind::Directory
            || matches!(current.metadata.changed_ns, MetadataField::Value(_)))
}

impl DarwinMountContext {
    pub(super) fn change_label(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        stamp: Option<CacheStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        let Some(stamp) = stamp else {
            return Ok(self.changes.fresh());
        };
        let binding = self.source.binding_epoch_with_meter(work)?;
        let mut labels = self
            .changes
            .labels
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        work.charge(scan_work(labels.capacity() as u64))?;
        if let Some(issued) = labels.get_mut(&lookup.node.file_id)
            && issued.binding == binding
        {
            let since = issued.stamp.min(stamp);
            if self.unchanged_since(path, Some(lookup.node.file_id), since, work)? {
                return Ok(issued.label);
            }
            if same_attributes(lookup, issued.lookup)
                && self.reported_unchanged_since(path, Some(lookup.node.file_id), since, work)?
            {
                issued.stamp = issued.stamp.max(stamp);
                return Ok(issued.label);
            }
        }
        if labels.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            labels.clear();
            self.changes.cleared.fetch_add(1, Ordering::AcqRel);
        }
        if !labels.contains_key(&lookup.node.file_id) {
            grow_map(&mut labels, work)?;
        }
        let path = clone_path(path, work)?;
        let label = self.changes.fresh();
        copying(
            work,
            copy_work(std::mem::size_of::<IssuedLabel>() as u64),
            || {
                labels.insert(
                    lookup.node.file_id,
                    IssuedLabel {
                        label,
                        stamp,
                        binding,
                        path,
                        lookup,
                    },
                )
            },
        )?;
        Ok(label)
    }
}
