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
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub(super) enum RecordValue {
    #[serde(skip_deserializing)]
    Context(WorkspaceContext),
    #[serde(skip_deserializing)]
    Children(ContextChildren),
    ChildCount(ContextChildCount),
    ChildIndex(ContextChildIndex),
    Lineage(WorkspaceLineageRecord),
    LazyWorkspace(LazyWorkspaceState),
    /// Flat replay DTOs stay flat until an ordinary context consumer requests them.
    #[serde(rename = "Context")]
    FlatContext(FlatWorkspaceContext),
    #[serde(rename = "Children")]
    FlatChildren(Vec<WorkspaceContextId>),
}

impl Serialize for RecordValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Context(value) => {
                serializer.serialize_newtype_variant("RecordValue", 0, "Context", value)
            }
            Self::Children(value) => {
                serializer.serialize_newtype_variant("RecordValue", 1, "Children", value)
            }
            Self::ChildCount(value) => {
                serializer.serialize_newtype_variant("RecordValue", 2, "ChildCount", value)
            }
            Self::ChildIndex(value) => {
                serializer.serialize_newtype_variant("RecordValue", 3, "ChildIndex", value)
            }
            Self::Lineage(value) => {
                serializer.serialize_newtype_variant("RecordValue", 4, "Lineage", value)
            }
            Self::LazyWorkspace(value) => {
                serializer.serialize_newtype_variant("RecordValue", 5, "LazyWorkspace", value)
            }
            Self::FlatContext(value) => {
                serializer.serialize_newtype_variant("RecordValue", 0, "Context", value)
            }
            Self::FlatChildren(value) => {
                serializer.serialize_newtype_variant("RecordValue", 1, "Children", value)
            }
        }
    }
}

impl RecordValue {
    /// Encodes a record file: the bare value, or `null` for an absence.
    pub(super) fn encode(value: Option<&Self>) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = Vec::new();
        Self::write_bare(value, &mut bytes)?;
        Ok(bytes)
    }
}

/// A record type served through the core-state log, bound to its key.
pub(super) trait Logged: Clone + Serialize + DeserializeOwned + Send + 'static {
    type Id: Copy + Send + 'static;

    fn key(id: Self::Id) -> RecordKey;
    fn into_value(self) -> RecordValue;
    fn from_value(value: &RecordValue) -> Option<&Self>;
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

            fn from_value(value: &RecordValue) -> Option<&Self> {
                match value {
                    RecordValue::$variant(value) => Some(value),
                    _ => None,
                }
            }
        }
    };
}

/// Direct children of one context.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct ContextChildren(pub(super) BTreeSet<WorkspaceContextId>);

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
#[serde(try_from = "Vec<(RecordKey, Option<RecordValue>)>")]
pub(super) struct Change(
    pub(super) Vec<Resident>,
    #[serde(skip)] pub(super) Option<crate::materializer::NativeAllocation>,
);

#[derive(Debug)]
pub(super) struct Resident {
    pub(super) key: RecordKey,
    pub(super) value: Option<RecordValue>,
    pub(super) allocation: Option<crate::materializer::NativeAllocation>,
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

impl TryFrom<Vec<(RecordKey, Option<RecordValue>)>> for Change {
    type Error = &'static str;

    fn try_from(records: Vec<(RecordKey, Option<RecordValue>)>) -> Result<Self, Self::Error> {
        for (key, value) in &records {
            let Some(value) = value else { continue };
            if !value.matches(*key) {
                return Err("core-state record value does not match its key");
            }
        }
        Ok(Self(
            records
                .into_iter()
                .map(|(key, value)| Resident {
                    key,
                    value,
                    allocation: None,
                })
                .collect(),
            None,
        ))
    }
}

impl Change {
    pub(super) fn put<T: Logged>(&mut self, id: T::Id, value: Option<T>) {
        self.0.push(Resident {
            key: T::key(id),
            value: value.map(T::into_value),
            allocation: None,
        });
    }

    /// Replaces a child bucket together with the count read before it.
    pub(super) fn put_children(
        &mut self,
        parent: WorkspaceContextId,
        descendants: Option<BTreeSet<WorkspaceContextId>>,
    ) {
        let count = descendants
            .as_ref()
            .map(|children| ContextChildCount(children.len() as u64));
        self.put(parent, descendants.map(ContextChildren));
        self.put(parent, count);
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
pub(super) fn index_change(records: impl IntoIterator<Item = WorkspaceContext>) -> Change {
    let mut change = Change::default();
    for (parent, descendants) in context_children(records) {
        change.put_children(parent, Some(descendants));
    }
    change.put((), Some(ContextChildIndex(1)));
    change
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
    pub(super) fn get(&self, key: &RecordKey) -> Option<&Option<RecordValue>> {
        self.0
            .iter()
            .find(|record| record.key == *key)
            .map(|record| &record.value)
    }
    pub(super) fn insert(&mut self, key: RecordKey, value: Option<RecordValue>) {
        let record = Resident {
            key,
            value,
            allocation: None,
        };
        if let Some(index) = self.0.iter().position(|record| record.key == key) {
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

/// Public context records use trees; log replay retains their exact flat JSON shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FlatWorkspaceContext {
    pub(super) version: u32,
    pub(super) revision: u64,
    pub(super) context_id: WorkspaceContextId,
    pub(super) parent_context_id: Option<WorkspaceContextId>,
    pub(super) roots: FlatMap<crate::WorkspaceRootId, FlatWorkspaceContextRoot>,
    pub(super) state: crate::workspace_context::WorkspaceContextState,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FlatWorkspaceContextRoot {
    pub(super) root_id: crate::WorkspaceRootId,
    pub(super) source_path: String,
    pub(super) workspace_id: WorkspaceId,
    pub(super) workspace_name: String,
    pub(super) parent_workspace_id: Option<WorkspaceId>,
    pub(super) mount_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct FlatMap<K, V>(pub(super) Vec<(K, V)>);

impl<K: Serialize, V: Serialize> Serialize for FlatMap<K, V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for FlatMap<K, V> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FlatMapVisitor<K, V>(PhantomData<(K, V)>);
        impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> serde::de::Visitor<'de>
            for FlatMapVisitor<K, V>
        {
            type Value = FlatMap<K, V>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a core-state flat map")
            }
            fn visit_newtype_struct<D: serde::Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Self::Value, D::Error> {
                deserializer.deserialize_map(self)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    entries.push(entry);
                }
                Ok(FlatMap(entries))
            }
        }
        deserializer.deserialize_newtype_struct("CoreStateFlatMap", FlatMapVisitor(PhantomData))
    }
}

impl RecordValue {
    pub(super) fn matches(&self, key: RecordKey) -> bool {
        matches!(
            (key, self),
            (
                RecordKey::Context(_),
                Self::Context(_) | Self::FlatContext(_)
            ) | (
                RecordKey::Children(_),
                Self::Children(_) | Self::FlatChildren(_)
            ) | (RecordKey::ChildCount(_), Self::ChildCount(_))
                | (RecordKey::ChildIndex, Self::ChildIndex(_))
                | (RecordKey::Lineage(_), Self::Lineage(_))
                | (RecordKey::LazyWorkspace(_), Self::LazyWorkspace(_))
        )
    }

    pub(super) fn normalize(
        &mut self,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        match (self, work) {
            (Self::FlatContext(context), Some(work)) => measured_order(&mut context.roots.0, work)?,
            (Self::FlatChildren(children), Some(work)) => {
                native_records::canonical_ids(children, work)?
            }
            (Self::FlatContext(context), None) => {
                context.roots.0.sort_by(|left, right| left.0.cmp(&right.0));
                context.roots.0.dedup_by(|later, earlier| {
                    if later.0 != earlier.0 {
                        return false;
                    }
                    std::mem::swap(later, earlier);
                    true
                });
            }
            (Self::FlatChildren(children), None) => {
                children.sort_unstable();
                children.dedup();
            }
            _ => {}
        }
        Ok(())
    }
}

impl Change {
    pub(super) fn new(
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Self, LocalCoreStateStoreError> {
        work.charge(crate::WorkCounters::UNCHARGED)?;
        Ok(Self::default())
    }

    pub(super) fn put_owned(
        &mut self,
        key: RecordKey,
        value: NativeOwnedCoreState<RecordValue>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.push(key, Some(value), work)
    }

    pub(super) fn put_scalar(
        &mut self,
        key: RecordKey,
        value: Option<RecordValue>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        if let Some(value) = &value {
            if native_records::native_record_owned_bytes(value, work)? != 0 {
                return Err(LocalCoreStateStoreError::Integrity);
            }
        }
        let value = match value {
            Some(value) => Some(NativeOwnedCoreState::from_parts(
                value,
                work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            )),
            None => None,
        };
        self.push(key, value, work)
    }

    pub(super) fn push(
        &mut self,
        key: RecordKey,
        value: Option<NativeOwnedCoreState<RecordValue>>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let mut empty = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let needed = self.0.len() + 1;
        reserve_slots(&mut self.0, needed, work, &mut self.1)?;
        let value = if let Some(value) = value {
            let (value, mut allocation) = value.into_parts();
            if !value.matches(key) {
                return Err(LocalCoreStateStoreError::Integrity);
            }
            if !allocation.work().shares_allocation_context(work) {
                drop(value);
                return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
            }
            empty.absorb(&mut allocation)?;
            Some(value)
        } else {
            None
        };
        self.0.push(Resident {
            key,
            value,
            allocation: Some(empty),
        });
        Ok(())
    }
}
