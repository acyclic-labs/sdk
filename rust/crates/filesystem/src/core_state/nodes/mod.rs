use super::*;

/// An immutable content-addressed lazy node.
pub(super) trait LazyNode:
    crate::lazy_workspace::MemoryLazyNode + DeserializeOwned + Default + Send + 'static
{
    type Id: Copy + Ord + Send + 'static;
    const FAMILY: &'static str;

    fn address(id: Self::Id) -> [u8; 32];
}

impl LazyNode for LazyOverlay {
    type Id = LazyOverlayId;
    const FAMILY: &'static str = "lazy-overlays";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }
}

impl LazyNode for crate::LazyShadow {
    type Id = crate::LazyShadowId;
    const FAMILY: &'static str = "lazy-shadows-v1";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }
}

pub(super) fn lazy_digest<T: LazyNode>(
    value: &T,
    meter: &crate::materializer::NativeMaterializationWork,
) -> Result<[u8; 32], LocalCoreStateStoreError> {
    crate::lazy_workspace::hash_lazy_value(
        value,
        &mut crate::kernel::KernelAccounting::native(meter, meter.cancellation()),
    )
    .map_err(LocalCoreStateStoreError::LazyWork)
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
                    .checked_add(*work)
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
            if lazy_digest(&owned.0, &meter)? != T::address(id) {
                return Err(LocalCoreStateStoreError::Integrity);
            }
            if T::address(id) == empty {
                return if owned.0 == T::default() {
                    Ok(())
                } else {
                    Err(LocalCoreStateStoreError::Integrity)
                };
            }
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
                            if existing.0 != owned.0 {
                                return Err(LocalCoreStateStoreError::Integrity);
                            }
                        } else {
                            write_journaled(paths, &owned.0)?;
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
