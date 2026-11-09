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
        LocalCoreStateStoreError::Integrity | LocalCoreStateStoreError::DurabilityPoisoned => {
            Lazy::NativeRecordIntegrity
        }
        LocalCoreStateStoreError::Corrupt(path) => Lazy::NativeRecordCorrupt(path, None),
    }
}

fn finish_core_lazy<T>(
    result: Result<T, LocalCoreStateStoreError>,
    owner: crate::kernel::allocation::retained::NativeAccounting,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<T, crate::LazyWorkspaceError> {
    owner.restore(accounting).map_err(lazy_accounting_error)?;
    result.map_err(native_core_error)
}

fn lazy_accounting_error(error: crate::FsError) -> crate::LazyWorkspaceError {
    match error {
        crate::FsError::NativeKernel(error) => native_core_error(error.into()),
        error => error.into(),
    }
}

fn core_lazy_node<T: crate::lazy_workspace::MemoryLazyNode>(
    result: Result<Option<(T, crate::NativeAllocation)>, LocalCoreStateStoreError>,
    owner: crate::kernel::allocation::retained::NativeAccounting,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<Option<(T, crate::kernel::allocation::KernelOwnedAllocation)>, crate::LazyWorkspaceError>
{
    let result = result.and_then(|node| {
        node.map(|(value, allocation)| {
            let allocation = owner
                .return_allocation(allocation, value.owned_bytes())
                .map_err(|error| LocalCoreStateStoreError::LazyWork(error.into()))?;
            Ok((value, allocation))
        })
        .transpose()
    });
    finish_core_lazy(result, owner, accounting)
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
            .read_transaction(move |namespace| {
                let work = namespace
                    ._allocation
                    .as_ref()
                    .expect("bound native work")
                    .work();
                let paths = namespace.content_record(T::FAMILY, &T::address(id))?;
                {
                    let paths = &paths;
                    let Some(mut owned) = read_published::<T>(paths)? else {
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
                }
            })
            .await
    }

    pub(super) async fn put_node<T: LazyNode>(
        &self,
        id: T::Id,
        value: (T, crate::kernel::allocation::KernelOwnedAllocation),
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::LazyWorkspaceError> {
        accounting.check().map_err(lazy_accounting_error)?;
        let bytes = value.0.owned_bytes();
        if let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
            &value.1
            && !accounting
                .native_work()
                .is_some_and(|original| allocation.work().shares_allocation_context(original))
        {
            return Err(crate::LazyWorkspaceError::NativeLedgerMismatch);
        }
        let owned = crate::kernel::allocation::retained::Retained::from_owned(
            crate::kernel::Owned {
                value: value.0,
                allocation: value.1,
                bytes,
            },
            accounting,
        )?;
        let owner = crate::kernel::allocation::retained::NativeAccounting::take(accounting)
            .map_err(lazy_accounting_error)?;
        let meter = owner.work.clone();
        let result = async {
            let empty = lazy_digest(&T::default(), &meter)?;
            if lazy_digest(&owned.value, &meter)? != T::address(id) {
                return Err(LocalCoreStateStoreError::Integrity);
            }
            if T::address(id) == empty {
                return if owned.value == T::default() {
                    Ok(())
                } else {
                    Err(LocalCoreStateStoreError::Integrity)
                };
            }
            self.materialization(meter.clone())
                .transaction(move |namespace| {
                    let owned = owned;
                    let work = namespace
                        ._allocation
                        .as_ref()
                        .expect("bound native work")
                        .work();
                    let mut paths = namespace.content_record(T::FAMILY, &T::address(id))?;
                    with_lock(&mut paths, |paths| {
                        if let Some(existing) = read_published::<T>(paths)? {
                            work.charge(crate::WorkCounters {
                                items_examined: 1,
                                ..Default::default()
                            })?;
                            if existing.0 != owned.value {
                                return Err(LocalCoreStateStoreError::Integrity);
                            }
                        } else {
                            write_journaled(paths, &owned.value)?;
                        }
                        Ok(())
                    })
                })
                .await?;
            Ok(())
        }
        .await;
        finish_core_lazy(result, owner, accounting)
    }
}

mod store;
