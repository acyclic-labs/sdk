use super::*;

/// Per-object NFS change attributes (RFC 7530 s5.4).
///
/// A node keeps its label while the source reports nothing that node's
/// attributes, listing, or name depend on changed since the label was
/// issued, or, across changes the source could not confirm it reported,
/// while the attributes observed again match those it was issued for;
/// otherwise it gets a fresh one. Labels come from one counter and are
/// never reissued, so a stale cached value can never match again, and a
/// write to one file leaves every other object's cached state valid.
pub(super) struct ChangeLabels {
    pub(super) next: AtomicU64,
    pub(super) labels: Mutex<HashMap<FileId, IssuedLabel>>,
    /// Times the labels were dropped at capacity, after which a revalidation
    /// cannot tell what the client holds.
    pub(super) cleared: AtomicU64,
}

#[derive(Clone)]
pub(super) struct IssuedLabel {
    pub(super) label: u64,
    /// Sampled before the attributes the label was issued for.
    pub(super) stamp: CacheStamp,
    pub(super) binding: Option<u64>,
    /// The attributes it was issued for, and the path they were observed at.
    pub(super) observed: Observed,
}

/// Attributes a label was issued for, as observed at a path.
#[derive(Clone)]
pub(super) struct Observed {
    pub(super) path: MountPath,
    pub(super) lookup: MountLookup,
}

impl Observed {
    /// Whether `lookup` shows the client what these attributes did. Access
    /// times are left out: no source reports reads, so no label follows them.
    ///
    /// A directory's label also covers its listing, which its attributes
    /// witness only through its change time: every entry created, removed,
    /// or renamed in it sets that time, and no caller can set it back. A
    /// directory whose change time is unknown is never taken as unchanged.
    pub(in crate::native_mount) fn matches(&self, lookup: &MountLookup) -> bool {
        let mut seen = *lookup;
        seen.metadata.accessed_ns = self.lookup.metadata.accessed_ns;
        seen == self.lookup
            && (lookup.node.kind != MountNodeKind::Directory
                || matches!(lookup.metadata.changed_ns, MetadataField::Value(_)))
    }
}

impl ChangeLabels {
    pub(in crate::native_mount) fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
            labels: Mutex::new(HashMap::new()),
            cleared: AtomicU64::new(0),
        }
    }

    /// The label for `file_id`'s attributes, `observed` after `stamp` was
    /// sampled under source binding `binding`; `unchanged` tells whether
    /// nothing they depend on changed since a stamp, and `reported_unchanged`
    /// whether nothing reported did. A source without stamps gets a fresh
    /// label every time, so its cached state is never trusted past a
    /// revalidation.
    pub(in crate::native_mount) fn label(
        &self,
        file_id: FileId,
        stamp: Option<CacheStamp>,
        binding: Option<u64>,
        observed: Observed,
        unchanged: impl FnOnce(CacheStamp) -> bool,
        reported_unchanged: impl FnOnce(CacheStamp) -> bool,
    ) -> u64 {
        let Some(stamp) = stamp else {
            return self.fresh();
        };
        // Labels only ever move to fresh values, so a panicked holder
        // cannot leave one that repeats.
        let mut labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        // The issued label names these attributes only if nothing changed
        // since the older of the two observations, so attributes observed
        // before a change never take a label issued after it.
        if let Some(issued) = labels.get_mut(&file_id)
            && issued.binding == binding
        {
            let since = issued.stamp.min(stamp);
            if unchanged(since) {
                return issued.label;
            }
            // Read again across changes the source could not confirm, with
            // the same result: the client's copy still holds.
            if issued.observed.matches(&observed.lookup) && reported_unchanged(since) {
                issued.stamp = issued.stamp.max(stamp);
                return issued.label;
            }
        }
        if labels.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            labels.clear();
            self.cleared.fetch_add(1, Ordering::AcqRel);
        }
        let label = self.fresh();
        labels.insert(
            file_id,
            IssuedLabel {
                label,
                stamp,
                binding,
                observed,
            },
        );
        label
    }

    /// What the client may hold labels for: each labelled node with the
    /// attributes and path it was labelled at; `None` past
    /// [`MAXIMUM_VERIFIED_LABELS`].
    pub(in crate::native_mount) fn issued(&self) -> Option<Vec<(FileId, Observed)>> {
        let labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        (labels.len() <= MAXIMUM_VERIFIED_LABELS).then(|| {
            labels
                .iter()
                .map(|(file_id, issued)| (*file_id, issued.observed.clone()))
                .collect()
        })
    }

    pub(in crate::native_mount) fn fresh(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

