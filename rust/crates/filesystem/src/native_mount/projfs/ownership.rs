use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReadBasis {
    pub(super) stamp: ViewStamp,
    pub(super) binding: u64,
}

impl ReadBasis {
    pub(super) fn sample(source: &dyn MountFilesystem) -> Option<Self> {
        if !source.view_is_stable() {
            return None;
        }
        Some(Self {
            stamp: source.view_stamp()?,
            binding: source.binding_epoch()?,
        })
    }

    pub(super) fn still_describes(
        self,
        source: &dyn MountFilesystem,
        path: &MountPath,
        file_id: Option<FileId>,
    ) -> bool {
        source.binding_epoch() == Some(self.binding)
            && source.unchanged_since(path, file_id, self.stamp)
    }

    pub(super) fn reported_still_describes(
        self,
        source: &dyn MountFilesystem,
        path: &MountPath,
        file_id: Option<FileId>,
    ) -> bool {
        source.binding_epoch() == Some(self.binding)
            && source.reported_unchanged_since(path, file_id, self.stamp)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Release {
    Done,
    Busy,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Owned {
    Link(u64),
    File(HostWindowsMetadata),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Placed {
    Nothing,
    Unowned,
    Owned(Owned),
    Retry,
}

impl Placed {
    pub(super) const fn owned(self) -> Option<Owned> {
        match self {
            Self::Owned(owned) => Some(owned),
            Self::Nothing | Self::Unowned | Self::Retry => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct EntryFacts {
    pub(super) lookup: MountLookup,
    pub(super) pin: Option<MountContentPin>,
}

impl EntryFacts {
    pub(super) fn describe(&self, current: &Self) -> bool {
        let unaccessed = |facts: &Self| {
            let mut lookup = facts.lookup;
            lookup.metadata.accessed_ns = MetadataField::Unavailable;
            (lookup, facts.pin)
        };
        unaccessed(self) == unaccessed(current)
    }
}

#[derive(Default)]
pub(super) struct ProjectionCache {
    pub(super) bindings: HashMap<MountPath, ReadOnlyBinding>,
}

impl ProjectionCache {
    pub(super) fn clear(&mut self) {
        self.bindings.clear();
    }

    pub(super) fn remember_binding(&mut self, path: MountPath, binding: ReadOnlyBinding) {
        if self.bindings.len() >= MAXIMUM_CACHED_BINDINGS {
            self.bindings.clear();
        }
        self.bindings.insert(path, binding);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct WrittenEntry {
    pub(super) file_id: FileId,
    pub(super) owned: Option<Owned>,
    pub(super) facts: Option<EntryFacts>,
    pub(super) basis: Option<ReadBasis>,
}

impl WrittenEntry {
    pub(super) fn superseded(&self, source: &dyn MountFilesystem, path: &MountPath) -> bool {
        self.basis
            .is_none_or(|basis| !basis.still_describes(source, path, Some(self.file_id)))
    }

    pub(super) fn unconfirmed(&self, source: &dyn MountFilesystem, path: &MountPath) -> bool {
        self.facts.is_some()
            && self.basis.is_some_and(|basis| {
                basis.reported_still_describes(source, path, Some(self.file_id))
            })
    }
}

#[derive(Default)]
pub(super) struct Carried {
    pub(super) basis: Option<ReadBasis>,
    pub(super) files: Vec<(MountPath, EntryFacts)>,
}

impl Carried {
    pub(super) fn read(
        source: &dyn MountFilesystem,
        from: &MountPath,
        to: &MountPath,
        current: &[MountPath],
    ) -> Self {
        let basis = ReadBasis::sample(source);
        let files = current
            .iter()
            .filter_map(|path| {
                let moved = rebased(path, from, to)?;
                let (lookup, pin) = source.lookup_pinned(&moved).ok().flatten()?;
                (lookup.node.kind != MountNodeKind::Directory)
                    .then_some((moved, EntryFacts { lookup, pin }))
            })
            .collect();
        Self { basis, files }
    }
}
