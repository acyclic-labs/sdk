use super::*;

/// Number of recent puts that triggers the first grace-period sweep.
const PUT_PRUNE_THRESHOLD: usize = 16_384;

/// Time allowed for a put node to become reachable through a workspace CAS.
pub(super) const LAZY_NODE_GRACE: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(10 * 60)
};

#[derive(Debug)]
pub(super) struct RecentPuts<K> {
    put: Vec<(K, Instant)>,
    prune_at: usize,
    allocation: Option<crate::NativeAllocation>,
}

impl<K> Default for RecentPuts<K> {
    fn default() -> Self {
        Self {
            put: Vec::new(),
            prune_at: PUT_PRUNE_THRESHOLD,
            allocation: None,
        }
    }
}

impl<K: Copy + Ord> RecentPuts<K> {
    /// Records a put before persistence under collection's same lock.
    pub(super) fn touch(
        &mut self,
        key: K,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let width = std::mem::size_of::<(K, Instant)>();
        if self.put.len() >= self.prune_at {
            work.admit(crate::WorkCounters {
                items_examined: self.put.len() as u64,
                bytes_copied: (self.put.len() * width) as u64,
                ..Default::default()
            })?;
            let mut hole = false;
            let mut moved = 0;
            let visited = self.put.len();
            self.put.retain(|(_, instant)| {
                if instant.elapsed() >= LAZY_NODE_GRACE {
                    hole = true;
                    false
                } else {
                    moved += usize::from(hole);
                    true
                }
            });
            work.spent(crate::WorkCounters {
                items_examined: visited as u64,
                bytes_copied: (moved * width) as u64,
                ..Default::default()
            })?;
            self.prune_at = PUT_PRUNE_THRESHOLD.max(self.put.len().saturating_mul(2));
        }
        let mut start = 0;
        let mut end = self.put.len();
        while start < end {
            let index = start + (end - start) / 2;
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            match self.put[index].0.cmp(&key) {
                std::cmp::Ordering::Less => start = index + 1,
                std::cmp::Ordering::Greater => end = index,
                std::cmp::Ordering::Equal => {
                    self.put[index].1 = Instant::now();
                    return Ok(());
                }
            }
        }
        if self.put.len() == self.put.capacity() {
            let needed = self
                .put
                .capacity()
                .checked_mul(2)
                .and_then(|capacity| {
                    self.put
                        .len()
                        .checked_add(1)
                        .map(|length| capacity.max(length))
                })
                .ok_or(crate::NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                ))?;
            let mut replacement = (
                Vec::new(),
                work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            );
            reserve(&mut replacement.0, needed, work, &mut replacement.1)?;
            work.charge(crate::WorkCounters {
                bytes_copied: (self.put.len() * width) as u64,
                ..Default::default()
            })?;
            replacement.0.extend(self.put.drain(..));
            drop(std::mem::replace(&mut self.put, replacement.0));
            // Both complete buffers remain funded until the old one is retired.
            self.allocation = Some(replacement.1);
        }
        work.charge(crate::WorkCounters {
            bytes_copied: ((self.put.len() - start) * width) as u64,
            ..Default::default()
        })?;
        self.put.insert(start, (key, Instant::now()));
        Ok(())
    }

    /// Allows collection only after a node's last put grace has elapsed.
    pub(super) fn release(&mut self, key: &K) -> bool {
        if self
            .put
            .iter()
            .any(|(candidate, put)| candidate == key && put.elapsed() < LAZY_NODE_GRACE)
        {
            return false;
        }
        self.put.retain(|(candidate, _)| candidate != key);
        true
    }
}

/// Recent overlay and shadow puts retained across this store's clones.
#[derive(Debug, Default)]
pub(super) struct NodeRetention {
    pub(super) overlays: Mutex<RecentPuts<LazyOverlayId>>,
    pub(super) shadows: Mutex<RecentPuts<crate::LazyShadowId>>,
}

/// An immutable content-addressed lazy node.
pub(super) trait LazyNode:
    crate::lazy_workspace::MemoryLazyNode + DeserializeOwned + Default + Send + 'static
{
    type Id: Copy + Ord + Send + 'static;
    const FAMILY: &'static str;

    fn address(id: Self::Id) -> [u8; 32];
    fn recent_puts(nodes: &NodeRetention) -> MutexGuard<'_, RecentPuts<Self::Id>>;
}

impl LazyNode for LazyOverlay {
    type Id = LazyOverlayId;
    const FAMILY: &'static str = "lazy-overlays";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn recent_puts(nodes: &NodeRetention) -> MutexGuard<'_, RecentPuts<Self::Id>> {
        nodes
            .overlays
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl LazyNode for crate::LazyShadow {
    type Id = crate::LazyShadowId;
    const FAMILY: &'static str = "lazy-shadows-v1";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn recent_puts(nodes: &NodeRetention) -> MutexGuard<'_, RecentPuts<Self::Id>> {
        nodes.shadows.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub(super) fn lazy_digest<T: Serialize>(
    value: &T,
    meter: &crate::materializer::NativeMaterializationWork,
) -> Result<[u8; 32], LocalCoreStateStoreError> {
    meter
        .allocations(|allocations, work, budget, cancellation| {
            crate::lazy_workspace::hash_lazy_value(
                value,
                &mut crate::kernel::KernelAccounting::borrowed_standalone(
                    allocations,
                    work,
                    budget,
                    cancellation,
                ),
            )
            .map_err(LocalCoreStateStoreError::LazyWork)
        })
        .map(|(value, _allocation)| value)
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
        LocalCoreStateStoreError::LazyWork(error) => error,
        LocalCoreStateStoreError::Integrity => Lazy::NativeRecordIntegrity,
        LocalCoreStateStoreError::Corrupt(path) => Lazy::NativeRecordCorrupt(path, None),
    }
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
            crate::LazyWorkspaceError::NativeWork(cause)
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

impl LocalCoreStateStore {
    pub(super) async fn load_node<T: LazyNode>(
        &self,
        id: T::Id,
        work: crate::NativeMaterializationWork,
    ) -> Result<Option<(T, crate::NativeAllocation)>, LocalCoreStateStoreError> {
        if T::address(id) == lazy_digest(&T::default(), &work)? {
            return Ok(Some((
                T::default(),
                work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            )));
        }
        self.materialization(work)
            .transaction(move |namespace| {
                let work = namespace
                    ._allocation
                    .as_ref()
                    .expect("bound native work")
                    .work();
                let mut paths = namespace.record(T::FAMILY, &T::address(id))?;
                with_lock(&mut paths, |paths| {
                    let Some(mut owned) = read_recoverable::<T>(paths)? else {
                        return Ok(None);
                    };
                    owned
                        .1
                        .as_mut()
                        .expect("native record allocation")
                        .resize(owned.0.owned_bytes())?;
                    if lazy_digest(&owned.0, work)? != T::address(id) {
                        return Err(LocalCoreStateStoreError::Integrity);
                    }
                    Ok(Some((owned.0, owned.1.expect("native record allocation"))))
                })
            })
            .await
    }

    pub(super) async fn put_node<T: LazyNode>(
        &self,
        id: T::Id,
        value: (T, crate::kernel::allocation::KernelOwnedAllocation),
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
        let owned = (value.0, allocation);
        let meter = work.clone();
        let result = async {
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
            T::recent_puts(&self.recent_puts).touch(id, &meter)?;
            self.materialization(meter.clone())
                .transaction(move |namespace| {
                    let work = namespace
                        ._allocation
                        .as_ref()
                        .expect("bound native work")
                        .work();
                    let mut paths = namespace.record(T::FAMILY, &T::address(id))?;
                    with_lock(&mut paths, |paths| {
                        if let Some(existing) = read_recoverable::<T>(paths)? {
                            work.charge(crate::WorkCounters {
                                items_examined: 1,
                                ..Default::default()
                            })?;
                            if &existing.0 != &owned.0 {
                                return Err(LocalCoreStateStoreError::Integrity);
                            }
                        } else {
                            write_journaled(paths, (&owned.0))?;
                        }
                        Ok(())
                    })
                })
                .await?;
            Ok(())
        }
        .await;
        if matches!(
            value.1,
            crate::kernel::allocation::KernelOwnedAllocation::Standalone
        ) {
            accounting.release_owned(&mut value.1, bytes)?;
        }
        finish_core_lazy(result, &work, accounting)
    }
}

mod store;
