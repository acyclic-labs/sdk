use super::*;

/// Entries retained per cache generation before the older generation is dropped.
pub(super) const NODE_CACHE_GENERATION: usize = 16_384;

/// Two-generation bounded memo: an entry read since the last rotation survives the next one, so hot treap spines stay resident.
#[derive(Debug)]
pub(super) struct Generations<K, V> {
    pub(super) current: Vec<(K, (V, crate::NativeAllocation))>,
    pub(super) previous: Vec<(K, (V, crate::NativeAllocation))>,
    /// When each node was last put; see [`LAZY_NODE_GRACE`].
    pub(super) put: Vec<(K, Instant)>,
    /// The size at which expired put times are next pruned: twice what the last pruning kept, so pruning stays amortized constant per put.
    pub(super) prune_at: usize,
    /// Counts nodes collected; a load memoizes what it read only if none was collected since it began, so the memo never outlives a node's file.
    pub(super) collected: u64,
    pub(super) slots: [Option<crate::materializer::NativeAllocation>; 3],
}

/// How long a node that was put stays safe from collection: long enough for the mutation that put it to swap it into its workspace's state.
pub(super) const LAZY_NODE_GRACE: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(10 * 60)
};

impl<K: Copy + Ord, V> Generations<K, V> {
    pub(super) fn new() -> Self {
        Self {
            current: Vec::new(),
            previous: Vec::new(),
            put: Vec::new(),
            prune_at: NODE_CACHE_GENERATION,
            collected: 0,
            slots: [None, None, None],
        }
    }

    pub(super) fn position<T>(
        entries: &[(K, T)],
        key: &K,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Result<usize, usize>, LocalCoreStateStoreError> {
        let mut start = 0;
        let mut end = entries.len();
        while start < end {
            let index = start + (end - start) / 2;
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            match entries[index].0.cmp(key) {
                std::cmp::Ordering::Less => start = index + 1,
                std::cmp::Ordering::Greater => end = index,
                std::cmp::Ordering::Equal => return Ok(Ok(index)),
            }
        }
        Ok(Err(start))
    }

    /// Records puts before the durable boundary under collection's own lock.
    pub(super) fn touch(
        &mut self,
        key: K,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        if self.put.len() >= self.prune_at {
            let width = std::mem::size_of::<(K, Instant)>() as u64;
            work.admit(crate::WorkCounters {
                items_examined: self.put.len() as u64,
                bytes_copied: self.put.len() as u64 * width,
                ..Default::default()
            })?;
            let mut hole = false;
            let mut moved = 0_u64;
            let visited = self.put.len();
            self.put.retain(|(_, instant)| {
                if instant.elapsed() >= LAZY_NODE_GRACE {
                    hole = true;
                    false
                } else {
                    moved += u64::from(hole);
                    true
                }
            });
            work.spent(crate::WorkCounters {
                items_examined: visited as u64,
                bytes_copied: moved * width,
                ..Default::default()
            })?;
            self.prune_at = NODE_CACHE_GENERATION.max(self.put.len().saturating_mul(2));
        }
        match Self::position(&self.put, &key, work)? {
            Ok(index) => self.put[index].1 = Instant::now(),
            Err(index) => {
                let needed = self.put.len() + 1;
                reserve_slots(&mut self.put, needed, work, &mut self.slots[2])?;
                work.charge(crate::WorkCounters {
                    bytes_copied: ((self.put.len() - index) * std::mem::size_of::<(K, Instant)>())
                        as u64,
                    ..Default::default()
                })?;
                self.put.insert(index, (key, Instant::now()));
            }
        }
        Ok(())
    }

    /// Forgets a collected node, retaining the original put grace custody.
    pub(super) fn release(&mut self, key: &K) -> bool {
        if self
            .put
            .iter()
            .any(|(candidate, put)| candidate == key && put.elapsed() < LAZY_NODE_GRACE)
        {
            return false;
        }
        self.put.retain(|(candidate, _)| candidate != key);
        self.current.retain(|(candidate, _)| candidate != key);
        self.previous.retain(|(candidate, _)| candidate != key);
        self.collected += 1;
        true
    }

    pub(super) fn get(
        &mut self,
        key: &K,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<&V>, LocalCoreStateStoreError> {
        if let Ok(index) = Self::position(&self.current, key, work)? {
            return Ok(Some((&self.current[index].1.0)));
        }
        let Ok(index) = Self::position(&self.previous, key, work)? else {
            return Ok(None);
        };
        // Promote the owned node without cloning it twice.
        work.charge(crate::WorkCounters {
            bytes_copied: ((self.previous.len() - index - 1)
                * std::mem::size_of::<(K, (V, crate::NativeAllocation))>())
                as u64,
            ..Default::default()
        })?;
        let (key, value) = self.previous.remove(index);
        let index = self.insert(key, value, work)?;
        Ok(Some((&self.current[index].1.0)))
    }

    pub(super) fn insert(
        &mut self,
        key: K,
        value: (V, crate::NativeAllocation),
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<usize, LocalCoreStateStoreError> {
        let mut index = match Self::position(&self.current, &key, work)? {
            Ok(index) => {
                self.current[index].1 = value;
                return Ok(index);
            }
            Err(index) => index,
        };
        if self.current.len() >= NODE_CACHE_GENERATION {
            work.charge(crate::WorkCounters {
                items_examined: self.previous.len() as u64,
                ..Default::default()
            })?;
            self.previous.clear();
            std::mem::swap(&mut self.previous, &mut self.current);
            self.slots.swap(0, 1);
            index = 0;
        }
        let needed = self.current.len() + 1;
        reserve_slots(&mut self.current, needed, work, &mut self.slots[0])?;
        work.charge(crate::WorkCounters {
            bytes_copied: ((self.current.len() - index)
                * std::mem::size_of::<(K, (V, crate::NativeAllocation))>())
                as u64,
            ..Default::default()
        })?;
        self.current.insert(index, (key, value));
        Ok(index)
    }
}

/// Immutable overlay and shadow nodes this store and its clones have read or written.
#[derive(Debug)]
pub(super) struct NodeMemo {
    pub(super) overlays: Mutex<Generations<LazyOverlayId, LazyOverlay>>,
    pub(super) shadows: Mutex<Generations<crate::LazyShadowId, crate::LazyShadow>>,
}

impl Default for NodeMemo {
    fn default() -> Self {
        Self {
            overlays: Mutex::new(Generations::new()),
            shadows: Mutex::new(Generations::new()),
        }
    }
}

/// An immutable content-addressed lazy node.
pub(super) trait LazyNode:
    crate::lazy_workspace::MemoryLazyNode + Default + Send + 'static
{
    type Id: Copy + Ord + Send + 'static;
    type Wire: DeserializeOwned;
    const FAMILY: &'static str;

    fn address(id: Self::Id) -> [u8; 32];
    fn memo(nodes: &NodeMemo) -> MutexGuard<'_, Generations<Self::Id, Self>>;
    fn from_wire(
        wire: Self::Wire,
        work: &crate::materializer::NativeMaterializationWork,
        allocation: &mut crate::materializer::NativeAllocation,
    ) -> Result<Self, LocalCoreStateStoreError>;
}

impl LazyNode for LazyOverlay {
    type Id = LazyOverlayId;
    type Wire = Self;
    const FAMILY: &'static str = "lazy-overlays";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn memo(nodes: &NodeMemo) -> MutexGuard<'_, Generations<Self::Id, Self>> {
        nodes
            .overlays
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn from_wire(
        wire: Self::Wire,
        _work: &crate::materializer::NativeMaterializationWork,
        _allocation: &mut crate::materializer::NativeAllocation,
    ) -> Result<Self, LocalCoreStateStoreError> {
        Ok(wire)
    }
}

impl LazyNode for crate::LazyShadow {
    type Id = crate::LazyShadowId;
    type Wire = LazyShadowWire;
    const FAMILY: &'static str = "lazy-shadows-v1";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn memo(nodes: &NodeMemo) -> MutexGuard<'_, Generations<Self::Id, Self>> {
        nodes.shadows.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn from_wire(
        wire: Self::Wire,
        work: &crate::materializer::NativeMaterializationWork,
        allocation: &mut crate::materializer::NativeAllocation,
    ) -> Result<Self, LocalCoreStateStoreError> {
        Ok(match wire {
            LazyShadowWire::Empty => Self::Empty,
            LazyShadowWire::Node {
                file_id,
                priority,
                record,
                metadata,
                left,
                right,
            } => {
                let bytes = std::mem::size_of::<crate::WorkspaceMetadata>() as u64;
                work.admit(crate::WorkCounters {
                    bytes_copied: bytes,
                    ..Default::default()
                })?;
                allocation.grow(bytes)?;
                let metadata = Box::new(metadata);
                work.spent(crate::WorkCounters {
                    bytes_copied: bytes,
                    ..Default::default()
                })?;
                Self::Node {
                    file_id,
                    priority,
                    record,
                    metadata,
                    left,
                    right,
                }
            }
        })
    }
}

/// The same durable shadow JSON, with its metadata inline during bounded decode.
#[derive(Deserialize)]
pub(super) enum LazyShadowWire {
    Empty,
    Node {
        file_id: crate::FileId,
        priority: u64,
        record: Vec<u8>,
        metadata: crate::WorkspaceMetadata,
        left: crate::LazyShadowId,
        right: crate::LazyShadowId,
    },
}

pub(super) fn lazy_digest<T: Serialize>(
    value: &T,
    meter: &crate::materializer::NativeMaterializationWork,
) -> Result<[u8; 32], LocalCoreStateStoreError> {
    meter.allocations(|allocations, work, budget, cancellation| {
        crate::lazy_workspace::hash_lazy_value(value,
            &mut crate::kernel::KernelAccounting::borrowed_standalone(
                allocations, work, budget, cancellation))
            .map_err(LocalCoreStateStoreError::LazyWork)
    }).map(|(value, _allocation)| value)
}

pub(super) fn native_core_error(error: LocalCoreStateStoreError) -> crate::LazyWorkspaceError {
    use crate::LazyWorkspaceError as Lazy;
    match error {
        LocalCoreStateStoreError::Io(error) => Lazy::NativeRecordIo(error, None),
        LocalCoreStateStoreError::LockTimeout(path) => Lazy::NativeRecordLockTimeout(path, None),
        LocalCoreStateStoreError::OwnershipConflict(path) => {
            Lazy::NativeRecordOwnershipConflict(path, None)
        }
        // serde_json's private error message/formatter storage is opaque codec
        // storage; decoded SDK payloads retain their separate original guards.
        LocalCoreStateStoreError::Json(error) => Lazy::NativeRecordJson(error, None),
        LocalCoreStateStoreError::MaterializationInProgress => {
            Lazy::NativeRecordMaterializationInProgress
        }
        LocalCoreStateStoreError::MaterializationWork(error) => match error {
            crate::materializer::NativeTreeMaterializationError::Cancelled => Lazy::Cancelled,
            crate::materializer::NativeTreeMaterializationError::Work(error) => {
                Lazy::Demand(crate::DemandError::Work(error))
            }
            error => Lazy::NativeWork(error),
        },
        LocalCoreStateStoreError::LazyWork(error) => error,
        LocalCoreStateStoreError::Integrity => Lazy::NativeRecordIntegrity,
        LocalCoreStateStoreError::NativeLedgerMismatch => Lazy::NativeLedgerMismatch,
        LocalCoreStateStoreError::ContextTransaction(message) => {
            Lazy::NativeRecordContextTransaction(message, None)
        }
        LocalCoreStateStoreError::Corrupt(path) => Lazy::NativeRecordCorrupt(path, None),
    }
}

pub(super) fn lazy_native_failure(
    (cause, work): (
        crate::materializer::NativeTreeMaterializationError,
        crate::WorkCounters,
    ),
) -> crate::LazyWorkspaceError {
    crate::LazyWorkspaceError::NativeConstruction { cause, work }
}

fn core_lazy_work(
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<crate::NativeMaterializationWork, crate::LazyWorkspaceError> {
    if let Some(original) = accounting.native_work() {
        return Ok(original.clone());
    }
    let cancellation = match &*accounting {
        crate::kernel::KernelAccounting::Standalone { cancellation, .. }
        | crate::kernel::KernelAccounting::BorrowedStandalone { cancellation, .. } => *cancellation,
        _ => unreachable!("native context has its original work"),
    };
    crate::NativeMaterializationWork::new(accounting.remaining()?, cancellation).map_err(
        |(cause, work)| {
            if let Some((_, prefix, _, _)) = accounting.standalone_parts() {
                *prefix = prefix
                    .checked_add(work)
                    .expect("constructor used its remaining allowance");
            }
            crate::LazyWorkspaceError::NativeConstruction { cause, work }
        },
    )
}

fn finish_core_lazy<T>(
    result: Result<T, LocalCoreStateStoreError>,
    work: &crate::NativeMaterializationWork,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<T, crate::LazyWorkspaceError> {
    if let Some((_, prefix, _, _)) = accounting.standalone_parts() {
        *prefix = prefix
            .checked_add(work.receipt())
            .map_err(crate::LazyWorkspaceError::from)?;
    }
    result.map_err(native_core_error)
}

fn core_lazy_node<T: crate::lazy_workspace::MemoryLazyNode>(
    result: Result<Option<(T, crate::NativeAllocation)>, LocalCoreStateStoreError>,
    work: &crate::NativeMaterializationWork,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<Option<(T, crate::kernel::allocation::KernelOwnedAllocation)>, crate::LazyWorkspaceError>
{
    let result = finish_core_lazy(result, work, accounting)?;
    result
        .map(|node| {
            if accounting.native_work().is_some() {
                return Ok((
                    node.0,
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(node.1),
                ));
            }
            let bytes = node.0.owned_bytes();
            let (mut allocation, admitted) = accounting.record_actual_owned(bytes)?;
            if let Err(error) = admitted {
                drop(node);
                accounting.release_owned(&mut allocation, bytes)?;
                return Err(error.into());
            }
            Ok((node.0, allocation))
        })
        .transpose()
}

pub(super) fn clone_node<T: crate::lazy_workspace::MemoryLazyNode>(
    value: &T,
    meter: &crate::materializer::NativeMaterializationWork,
) -> Result<(T, crate::NativeAllocation), LocalCoreStateStoreError> {
    meter
        .allocations(|allocations, work, budget, cancellation| {
            Ok(crate::lazy_workspace::clone_lazy_node(
                value,
                allocations,
                work,
                budget,
                cancellation,
            )?)
        })
        .map(|(value, allocation)| (value, allocation))
}

impl LocalCoreStateStore {
    pub(super) async fn load_node<T: LazyNode>(
        &self,
        id: T::Id,
        meter: crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<(T, crate::NativeAllocation)>, LocalCoreStateStoreError> {
        if T::address(id) == lazy_digest(&T::default(), &meter)? {
            return Ok(Some((
                T::default(),
                meter.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            )));
        }
        let collected = {
            let mut memo = T::memo(&self.nodes);
            if let Some(value) = memo.get(&id, &meter)? {
                return clone_node(value, &meter).map(Some);
            }
            memo.collected
        };
        let value = self
            .materialization(meter.clone())
            .transaction(move |namespace| {
                let work = namespace
                    ._allocation
                    .as_ref()
                    .expect("bound native work")
                    .work();
                let paths = namespace.record(T::FAMILY, &T::address(id))?;
                materialization_lock(paths, work, |paths| {
                    let Some((wire, mut allocation)) =
                        measured_read_record::<T::Wire>(&paths, work)?
                    else {
                        return Ok(None);
                    };
                    let value = T::from_wire(wire, work, &mut allocation)?;
                    allocation.substitute(value.owned_bytes())?;
                    let owned = (value, allocation);
                    if lazy_digest((&owned.0), work)? != T::address(id) {
                        return Err(LocalCoreStateStoreError::Integrity);
                    }
                    Ok(Some(owned))
                })
            })
            .await?;
        if let Some(owned) = value {
            let mut memo = T::memo(&self.nodes);
            // Collection's generation guards against a late read resurrecting a memo.
            if memo.collected == collected {
                memo.insert(id, clone_node((&owned.0), &meter)?, &meter)?;
            }
            Ok(Some(owned))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn put_node<T: LazyNode>(
        &self,
        id: T::Id,
        owned: (T, crate::NativeAllocation),
        meter: crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let (value, allocation) = owned;
        let same_ledger = meter.shares_ledger(allocation.work());
        let owned = (value, allocation);
        if !same_ledger {
            return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
        }
        let empty = lazy_digest(&T::default(), &meter)?;
        if lazy_digest((&owned.0), &meter)? != T::address(id) {
            return Err(LocalCoreStateStoreError::Integrity);
        }
        if T::address(id) == empty {
            return if (&owned.0) == &T::default() {
                Ok(())
            } else {
                Err(LocalCoreStateStoreError::Integrity)
            };
        }
        {
            let mut memo = T::memo(&self.nodes);
            memo.touch(id, &meter)?;
            if let Some(existing) = memo.get(&id, &meter)? {
                meter.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
                return if existing == (&owned.0) {
                    Ok(())
                } else {
                    Err(LocalCoreStateStoreError::Integrity)
                };
            }
        }
        let stored = self
            .materialization(meter.clone())
            .transaction(move |namespace| {
                let work = namespace
                    ._allocation
                    .as_ref()
                    .expect("bound native work")
                    .work();
                let paths = namespace.record(T::FAMILY, &T::address(id))?;
                materialization_lock(paths, work, |paths| {
                    if let Some((wire, mut allocation)) =
                        measured_read_record::<T::Wire>(&paths, work)?
                    {
                        let existing = T::from_wire(wire, work, &mut allocation)?;
                        work.charge(crate::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        })?;
                        if &existing != (&owned.0) {
                            return Err(LocalCoreStateStoreError::Integrity);
                        }
                    } else {
                        measured_write_record(&paths, (&owned.0), work)?;
                    }
                    Ok(owned)
                })
            })
            .await?;
        T::memo(&self.nodes).insert(id, stored, &meter)?;
        Ok(())
    }
}

impl LazyWorkspaceStore for LocalCoreStateStore {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, crate::LazyWorkspaceError> {
        let work = core_lazy_work(accounting)?;
        let state = finish_core_lazy(
            self.load_lazy_logged(id, work.clone()).await,
            &work,
            accounting,
        )?;
        if state.as_ref().is_some_and(|state| {
            state.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA
                || state.workspace_id != id
        }) {
            return Err(crate::LazyWorkspaceError::NativeRecordIntegrity);
        }
        Ok(state)
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        state: LazyWorkspaceState,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<bool, crate::LazyWorkspaceError> {
        let work = core_lazy_work(accounting)?;
        if state.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA
            || state.workspace_id != id
        {
            return Err(crate::LazyWorkspaceError::NativeRecordIntegrity);
        }
        finish_core_lazy(
            self.swap_lazy_logged(id, revision, state, work.clone())
                .await,
            &work,
            accounting,
        )
    }

    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<
        Option<(
            LazyOverlay,
            crate::kernel::allocation::KernelOwnedAllocation,
        )>,
        crate::LazyWorkspaceError,
    > {
        let work = core_lazy_work(accounting)?;
        core_lazy_node(
            self.load_node::<LazyOverlay>(id, work.clone()).await,
            &work,
            accounting,
        )
    }

    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (
            LazyOverlay,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::LazyWorkspaceError> {
        let bytes = value.0.owned_bytes();
        let work = core_lazy_work(accounting)?;
        let mut value = value;
        let allocation = match &mut value.1 {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
                if !allocation.work().shares_allocation_context(&work) {
                    return Err(crate::LazyWorkspaceError::NativeLedgerMismatch);
                }
                allocation.split_off(allocation.bytes())?
            }
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                work.claim(bytes, 0, crate::WorkCounters::UNCHARGED)?
            }
        };
        let result = self.put_node(id, (value.0, allocation), work.clone()).await;
        if matches!(
            value.1,
            crate::kernel::allocation::KernelOwnedAllocation::Standalone
        ) {
            accounting.release_owned(&mut value.1, bytes)?;
        }
        finish_core_lazy(result, &work, accounting)
    }

    async fn load_lazy_shadow(
        &self,
        id: crate::LazyShadowId,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<
        Option<(
            crate::LazyShadow,
            crate::kernel::allocation::KernelOwnedAllocation,
        )>,
        crate::LazyWorkspaceError,
    > {
        let work = core_lazy_work(accounting)?;
        core_lazy_node(
            self.load_node::<crate::LazyShadow>(id, work.clone()).await,
            &work,
            accounting,
        )
    }

    async fn put_lazy_shadow(
        &self,
        id: crate::LazyShadowId,
        value: (
            crate::LazyShadow,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::LazyWorkspaceError> {
        let bytes = value.0.owned_bytes();
        let work = core_lazy_work(accounting)?;
        let mut value = value;
        let allocation = match &mut value.1 {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
                if !allocation.work().shares_allocation_context(&work) {
                    return Err(crate::LazyWorkspaceError::NativeLedgerMismatch);
                }
                allocation.split_off(allocation.bytes())?
            }
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                work.claim(bytes, 0, crate::WorkCounters::UNCHARGED)?
            }
        };
        let result = self.put_node(id, (value.0, allocation), work.clone()).await;
        if matches!(
            value.1,
            crate::kernel::allocation::KernelOwnedAllocation::Standalone
        ) {
            accounting.release_owned(&mut value.1, bytes)?;
        }
        finish_core_lazy(result, &work, accounting)
    }
}
