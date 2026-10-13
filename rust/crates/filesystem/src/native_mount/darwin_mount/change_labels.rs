use super::*;

pub(super) struct IssuedLabel {
    pub(super) label: u64,
    pub(super) stamp: CacheStamp,
    pub(super) binding: Option<u64>,
    pub(super) path: DarwinOwned<MountPath>,
    pub(super) lookup: MountLookup,
}

impl DarwinMountContext {
    pub(super) fn issued_labels(
        &self,
        work: &Work,
    ) -> Result<Option<DarwinOwned<Vec<(DarwinOwned<MountPath>, MountLookup)>>>, NativeMountError>
    {
        let labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);

        if labels.len() > MAXIMUM_VERIFIED_LABELS {
            return Ok(None);
        }
        let mut rows = vector(work, labels.len())?;
        for issued in labels.values() {
            let path = clone_path(&issued.path, work)?;
            rows.push((path, issued.lookup));
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
        work: &Work,
    ) -> Result<u64, NativeMountError> {
        let Some(stamp) = stamp else {
            return Ok(self.next_label.fetch_add(1, Ordering::Relaxed));
        };
        let binding = self.source.binding_epoch_with_meter(work)?;
        let mut labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);

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
            self.labels_cleared.fetch_add(1, Ordering::AcqRel);
        }
        if !labels.contains_key(&lookup.node.file_id) {
            grow_map(&mut labels, work)?;
        }
        let path = clone_path(path, work)?;
        let label = self.next_label.fetch_add(1, Ordering::Relaxed);
        labels.insert(
            lookup.node.file_id,
            IssuedLabel {
                label,
                stamp,
                binding,
                path,
                lookup,
            },
        );
        Ok(label)
    }
}
