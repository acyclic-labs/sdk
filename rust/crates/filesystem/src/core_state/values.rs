use super::*;

/// Identity of one record served through the core-state log.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(super) enum RecordKey {
    Context(WorkspaceContextId),
    Children(WorkspaceContextId),
    ChildCount(WorkspaceContextId),
    ChildIndex,
    Lineage(WorkspaceId),
    LazyWorkspace(WorkspaceId),
}

impl RecordKey {
    pub(super) fn paths(
        self,
        root: &Namespace,
    ) -> Result<RecordPaths<'_>, LocalCoreStateStoreError> {
        match self {
            Self::Context(id) => root.record(WORKSPACE_CONTEXT_FAMILY, &id.into_bytes()),
            Self::Children(id) => root.record(CONTEXT_CHILDREN_FAMILY, &id.into_bytes()),
            Self::ChildCount(id) => root.record(CONTEXT_CHILD_COUNT_FAMILY, &id.into_bytes()),
            Self::ChildIndex => root.record(CONTEXT_CHILDREN_INDEX_FAMILY, &[0; 16]),
            Self::Lineage(id) => root.record(LINEAGE_FAMILY, &id.into_bytes()),
            Self::LazyWorkspace(id) => root.record(LAZY_WORKSPACE_FAMILY, &id.into_bytes()),
        }
    }
}

/// One logged record's value; its variant always matches its key's.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum RecordValue {
    Context(WorkspaceContext),
    Children(ContextChildren),
    ChildCount(ContextChildCount),
    ChildIndex(ContextChildIndex),
    Lineage(WorkspaceLineageRecord),
    LazyWorkspace(LazyWorkspaceState),
}

/// A record type served through the core-state log, bound to its key.
pub(super) trait Logged: Clone + Serialize + DeserializeOwned + Send + 'static {
    type Id: Copy + Send + 'static;

    fn key(id: Self::Id) -> RecordKey;
    fn into_value(self) -> RecordValue;
    fn from_value(value: RecordValue) -> Option<Self>;
}

macro_rules! logged {
    ($type:ty, $id:ty, $variant:ident, |$key:ident| $record:expr) => {
        impl Logged for $type {
            type Id = $id;

            fn key($key: $id) -> RecordKey {
                $record
            }

            fn into_value(self) -> RecordValue {
                RecordValue::$variant(self)
            }

            fn from_value(value: RecordValue) -> Option<Self> {
                match value {
                    RecordValue::$variant(value) => Some(value),
                    _ => None,
                }
            }
        }
    };
}

/// Direct children of one context.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(transparent)]
pub(super) struct ContextChildren(pub(super) BTreeSet<WorkspaceContextId>);

impl<'de> Deserialize<'de> for ContextChildren {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_newtype_struct("CoreStateChildSet", Self::default())
    }
}
impl<'de> serde::de::Visitor<'de> for ContextChildren {
    type Value = Self;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("context children")
    }
    fn visit_newtype_struct<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        BTreeSet::deserialize(deserializer).map(Self)
    }
}

/// Number of direct children of one context, read before its bucket.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct ContextChildCount(pub(super) u64);

/// Marks the child index as complete for every context.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct ContextChildIndex(pub(super) u16);

logged!(WorkspaceContext, WorkspaceContextId, Context, |id| {
    RecordKey::Context(id)
});
logged!(ContextChildren, WorkspaceContextId, Children, |id| {
    RecordKey::Children(id)
});
logged!(ContextChildCount, WorkspaceContextId, ChildCount, |id| {
    RecordKey::ChildCount(id)
});
logged!(ContextChildIndex, (), ChildIndex, |_id| {
    RecordKey::ChildIndex
});
logged!(WorkspaceLineageRecord, WorkspaceId, Lineage, |id| {
    RecordKey::Lineage(id)
});
logged!(LazyWorkspaceState, WorkspaceId, LazyWorkspace, |id| {
    RecordKey::LazyWorkspace(id)
});

/// Complete after-images of the records one commit changes; `None` records an absence.
#[derive(Debug, Default, Deserialize)]
#[serde(try_from = "Vec<Resident>")]
pub(super) struct Change(
    pub(super) Vec<Resident>,
    #[serde(skip)] pub(super) Option<crate::materializer::NativeAllocation>,
);

#[derive(Debug, Deserialize)]
#[serde(from = "(RecordKey, Option<RecordValue>)")]
pub(super) struct Resident {
    pub(super) key: RecordKey,
    pub(super) value: Option<RecordValue>,
    pub(super) allocation: Option<crate::materializer::NativeAllocation>,
}

impl From<(RecordKey, Option<RecordValue>)> for Resident {
    fn from((key, value): (RecordKey, Option<RecordValue>)) -> Self {
        Self {
            key,
            value,
            allocation: None,
        }
    }
}

impl Serialize for Change {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for record in &self.0 {
            sequence.serialize_element(&(record.key, &record.value))?;
        }
        sequence.end()
    }
}

impl TryFrom<Vec<Resident>> for Change {
    type Error = &'static str;

    fn try_from(records: Vec<Resident>) -> Result<Self, Self::Error> {
        for record in &records {
            let Some(value) = &record.value else { continue };
            if !value.matches(record.key) {
                return Err("core-state record value does not match its key");
            }
        }
        Ok(Self(records, None))
    }
}

impl Change {
    /// Replaces a child bucket together with the count read before it.
    pub(super) fn put_children(
        &mut self,
        mut record: Resident,
    ) -> Result<(), LocalCoreStateStoreError> {
        let RecordKey::Children(parent) = record.key else {
            return Err(LocalCoreStateStoreError::Integrity);
        };
        let count = match &record.value {
            Some(RecordValue::Children(children)) => children.0.len() as u64,
            None => 0,
            _ => return Err(LocalCoreStateStoreError::Integrity),
        };
        if count == 0 {
            record.value = None;
        }
        self.put(record)?;
        self.put(
            (
                RecordKey::ChildCount(parent),
                (count != 0).then_some(RecordValue::ChildCount(ContextChildCount(count))),
            )
                .into(),
        )
    }
}

/// Direct children of every context in `records`.
pub(super) fn context_children(
    records: impl IntoIterator<Item = WorkspaceContext>,
) -> WorkspaceContextChildren {
    let mut children = WorkspaceContextChildren::new();
    for record in records {
        update_context_children(&mut children, None, Some(&record));
    }
    children
}

/// Installs the child index derived from every context.
pub(super) fn index_change(
    records: impl IntoIterator<Item = WorkspaceContext>,
) -> Result<Change, LocalCoreStateStoreError> {
    let mut change = Change::default();
    for (parent, descendants) in context_children(records) {
        change.put_children(
            (
                RecordKey::Children(parent),
                Some(RecordValue::Children(ContextChildren(descendants))),
            )
                .into(),
        )?;
    }
    change.put(
        (
            RecordKey::ChildIndex,
            Some(RecordValue::ChildIndex(ContextChildIndex(1))),
        )
            .into(),
    )?;
    Ok(change)
}

/// One log line: `<blake3 hex> <frame json>`.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LogFrame<C> {
    pub(super) epoch: Uuid,
    pub(super) sequence: u64,
    pub(super) change: C,
}

#[derive(Debug, Default)]
pub(super) struct Residents(
    pub(super) Vec<Resident>,
    pub(super) Option<crate::materializer::NativeAllocation>,
);

impl Residents {
    pub(super) fn position(
        &self,
        key: RecordKey,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Option<usize>, LocalCoreStateStoreError> {
        for (index, record) in self.0.iter().enumerate() {
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
            }
            if record.key == key {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }
    pub(super) fn insert(&mut self, key: RecordKey, value: Option<RecordValue>) {
        let record = Resident {
            key,
            value,
            allocation: None,
        };
        if let Some(index) = self.position(key, None).expect("unmetered lookup") {
            self.0[index] = record;
        } else {
            self.0.push(record);
        }
    }
    pub(super) fn clear(&mut self) {
        self.0.clear();
    }
    pub(super) fn keys(&self) -> impl Iterator<Item = &RecordKey> {
        self.0.iter().map(|record| &record.key)
    }
    pub(super) fn adopt_native(
        &mut self,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let needed = self.0.len();
        reserve_slots(&mut self.0, needed, work, &mut self.1)?;
        for record in &mut self.0 {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            let bytes = record
                .value
                .as_ref()
                .map(|value| native_records::native_record_owned_bytes(value, work))
                .transpose()?
                .unwrap_or(0);
            if record.allocation.is_none() {
                record.allocation = Some(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?);
            }
            let allocation = record.allocation.as_mut().expect("adopted resident");
            if allocation.bytes() < bytes {
                let mut prefix = work.claim(
                    bytes - allocation.bytes(),
                    0,
                    crate::WorkCounters::UNCHARGED,
                )?;
                allocation.absorb(&mut prefix)?;
            }
        }
        Ok(())
    }
}

impl RecordValue {
    pub(super) fn matches(&self, key: RecordKey) -> bool {
        matches!(
            (key, self),
            (RecordKey::Context(_), Self::Context(_))
                | (RecordKey::Children(_), Self::Children(_))
                | (RecordKey::ChildCount(_), Self::ChildCount(_))
                | (RecordKey::ChildIndex, Self::ChildIndex(_))
                | (RecordKey::Lineage(_), Self::Lineage(_))
                | (RecordKey::LazyWorkspace(_), Self::LazyWorkspace(_))
        )
    }
}

impl Change {
    pub(super) fn new(
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Self, LocalCoreStateStoreError> {
        work.charge(crate::WorkCounters::UNCHARGED)?;
        Ok(Self(
            Vec::new(),
            Some(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?),
        ))
    }

    pub(super) fn put(&mut self, record: Resident) -> Result<(), LocalCoreStateStoreError> {
        if let Some(allocation) = &self.1 {
            let work = allocation.work().clone();
            let needed = self.0.len() + 1;
            reserve_slots(&mut self.0, needed, &work, &mut self.1)?;
        }
        self.0.push(record);
        Ok(())
    }
}
