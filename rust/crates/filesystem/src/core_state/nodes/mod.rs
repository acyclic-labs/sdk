use super::*;

/// An immutable content-addressed lazy node.
pub(super) trait LazyNode:
    crate::lazy_workspace::MemoryLazyNode + DeserializeOwned + Default + Send + 'static
{
    type Id: Copy + Ord + Send + 'static;
    const FAMILY: &'static str;

    fn address(id: Self::Id) -> [u8; 32];

    fn retain_decoded(
        &mut self,
        allocation: &mut crate::kernel::KernelOwnedAllocation,
        ctx: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::FsError>;
}

impl LazyNode for LazyOverlay {
    type Id = LazyOverlayId;
    const FAMILY: &'static str = "lazy-overlays";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn retain_decoded(
        &mut self,
        allocation: &mut crate::kernel::KernelOwnedAllocation,
        ctx: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if let Self::Node {
            change: crate::lazy_workspace::LazyOverlayChange::Observe { source, .. },
            ..
        } = self
        {
            source.retain_decoded(allocation, ctx)?;
        }
        Ok(())
    }
}

impl LazyNode for crate::LazyShadow {
    type Id = crate::LazyShadowId;
    const FAMILY: &'static str = "lazy-shadows-v2";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn retain_decoded(
        &mut self,
        _allocation: &mut crate::kernel::KernelOwnedAllocation,
        _ctx: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        Ok(())
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

pub(crate) fn native_core_error(error: LocalCoreStateStoreError) -> crate::LazyWorkspaceError {
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
                let paths = namespace.record(T::FAMILY, &T::address(id))?;
                {
                    let paths = &paths;
                    let Some(mut owned) = read_published::<T>(paths)? else {
                        return Ok(None);
                    };
                    let mut allocation = crate::kernel::KernelOwnedAllocation::NativeOriginal(
                        owned.1.take().expect("native record allocation"),
                    );
                    owned
                        .0
                        .retain_decoded(
                            &mut allocation,
                            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                        )
                        .map_err(crate::LazyWorkspaceError::from)
                        .map_err(LocalCoreStateStoreError::LazyWork)?;
                    let crate::kernel::KernelOwnedAllocation::NativeOriginal(mut allocation) =
                        allocation
                    else {
                        return Err(LocalCoreStateStoreError::Integrity);
                    };
                    allocation.resize(owned.0.owned_bytes())?;
                    if lazy_digest(&owned.0, work)? != T::address(id) {
                        return Err(LocalCoreStateStoreError::Integrity);
                    }
                    Ok(Some((owned.0, allocation)))
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
                    // Immutable publication needs only its final leaf, never
                    // the mutable journal's previous, temporary or lock paths.
                    let mut leaf = [0_u8; 69];
                    let encoding = crate::WorkCounters {
                        bytes_encoded: 64,
                        ..crate::WorkCounters::UNCHARGED
                    };
                    work.admit(encoding)?;
                    hex::encode_to_slice(T::address(id), &mut leaf[..64])
                        .map_err(std::io::Error::other)?;
                    work.spent(encoding)?;
                    leaf[64..].copy_from_slice(b".json");
                    let leaf = std::str::from_utf8(&leaf).map_err(std::io::Error::other)?;
                    let mut encoded = LogBuffer {
                        bytes: Vec::new(),
                        allocation: Some(work.claim(0, 0, crate::WorkCounters::UNCHARGED)?),
                    };
                    serde_json::to_writer(&mut encoded, &owned.value)?;
                    // Immutable records need atomic no-replace publication,
                    // not exclusion between unrelated content addresses.
                    #[cfg(all(test, target_os = "linux"))]
                    publication_pause::install(&namespace.root, T::address(id));
                    let mut original = work.clone();
                    let mut publish = || -> Result<(), crate::DemandError> {
                        let root =
                            crate::native_host::HostRoot::open(&namespace.root, &mut original)?
                                .create_directories(
                                    Path::new(T::FAMILY),
                                    crate::NativeDurability::Durable,
                                    &mut original,
                                )?;
                        root.publish_immutable(
                            Path::new(leaf),
                            &encoded.bytes,
                            crate::NativeDurability::Durable,
                            &mut original,
                        )
                    };
                    publish().map_err(|error| match error {
                        crate::DemandError::Io(error)
                            if error.kind() == std::io::ErrorKind::InvalidData =>
                        {
                            LocalCoreStateStoreError::Integrity
                        }
                        error => LocalCoreStateStoreError::from(
                            crate::NativeTreeMaterializationError::from(error),
                        ),
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

#[cfg(all(test, target_os = "linux"))]
pub(super) mod publication_pause {
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, PoisonError, mpsc};
    struct Pause {
        root: PathBuf,
        address: [u8; 32],
        entered: tokio::sync::oneshot::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    static PAUSE: Mutex<Option<Pause>> = Mutex::new(None);

    pub(in crate::core_state) struct Held {
        root: PathBuf,
        address: [u8; 32],
        entered: tokio::sync::oneshot::Receiver<()>,
        release: mpsc::Sender<()>,
    }
    impl Held {
        pub(in crate::core_state) async fn entered(&mut self) {
            (&mut self.entered)
                .await
                .expect("original publisher reached its physical cut");
        }
    }
    impl Drop for Held {
        fn drop(&mut self) {
            let mut slot = PAUSE.lock().unwrap_or_else(PoisonError::into_inner);
            if slot
                .as_ref()
                .is_some_and(|pause| pause.root == self.root && pause.address == self.address)
            {
                slot.take();
            }
            let _ = self.release.send(());
        }
    }
    pub(in crate::core_state) fn hold(root: &Path, address: [u8; 32]) -> Held {
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, released) = mpsc::channel();
        let mut slot = PAUSE.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(slot.is_none(), "only one publication fixture owns this cut");
        *slot = Some(Pause {
            root: root.to_owned(),
            address,
            entered,
            release: released,
        });
        Held {
            root: root.to_owned(),
            address,
            entered: observed,
            release,
        }
    }
    pub(super) fn install(root: &Path, address: [u8; 32]) {
        let pause = {
            let mut slot = PAUSE.lock().unwrap_or_else(PoisonError::into_inner);
            if slot
                .as_ref()
                .is_some_and(|pause| pause.root == root && pause.address == address)
            {
                slot.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            crate::native_host::publish_probe::before_directory_sync(move || {
                let _ = pause.entered.send(());
                let _ = pause.release.recv();
            });
        }
    }
}
