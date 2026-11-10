use super::*;
use crate::lock_order::{Mutex as OrderedMutex, OPERATION};
use std::sync::{OnceLock, Weak};

type Owners<T> = OrderedMutex<BTreeMap<PathBuf, ([u8; 16], Weak<T>)>, OPERATION>;

/// Shared joins retain this entire value atomically, including its native
/// handle. Retire the handle before the owner's Arc and physical teardown.
#[derive(Debug)]
pub(super) struct OwnerRef {
    _handle: acyclic_native_runtime::StoreHandle,
    owner: Arc<Owner>,
}

impl OwnerRef {
    pub(super) fn new(owner: Arc<Owner>) -> Self {
        Self {
            _handle: owner._claim.handle(),
            owner,
        }
    }
}

impl std::ops::Deref for OwnerRef {
    type Target = Owner;

    fn deref(&self) -> &Owner {
        &self.owner
    }
}

pub(super) fn create(
    root: &Path,
    cache_limit: Option<usize>,
) -> Result<Arc<Owner>, LocalCoreStateStoreError> {
    let claim =
        acyclic_native_runtime::StoreClaim::acquire(&root.join(OWNER_LOCK)).map_err(|error| {
            if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
                LocalCoreStateStoreError::OwnershipConflict(root.to_owned())
            } else {
                error.into()
            }
        })?;
    Ok(Arc::new(Owner {
        residents: Arc::default(),
        log: Mutex::default(),
        flush_gate: Mutex::default(),
        pending: AtomicBool::new(false),
        failed: AtomicBool::new(false),
        cache_limit,
        _claim: claim,
    }))
}

// Registry -> OS namespace lock. Neither the record log nor provider/view locks
// are acquired while holding this registry. The constructor uses try-lock only.
fn join<T, const FENCE: bool>(
    owners: &Owners<T>,
    root: PathBuf,
    identity: [u8; 16],
    create: impl FnOnce() -> Result<Arc<T>, LocalCoreStateStoreError>,
) -> Result<Arc<T>, LocalCoreStateStoreError> {
    let mut guard = Some(owners.lock().unwrap_or_else(PoisonError::into_inner));
    if let Some((original, owner)) = guard.as_ref().unwrap().get(&root)
        && let Some(owner) = owner.upgrade()
    {
        return if *original == identity {
            Ok(owner)
        } else {
            Err(LocalCoreStateStoreError::OwnershipConflict(root))
        };
    }
    // FENCE=false is the deliberately broken initialization race.
    if !FENCE {
        guard.take();
    }
    let owner = create()?;
    let mut guard = guard.unwrap_or_else(|| owners.lock().unwrap_or_else(PoisonError::into_inner));
    guard.insert(root, (identity, Arc::downgrade(&owner)));
    // Weak entries never keep an owner, its file lock, or its WAL alive.
    guard.retain(|_, (_, owner)| owner.strong_count() != 0);
    Ok(owner)
}

impl LocalCoreStateStore {
    /// Joins this process's existing owner of a canonical private namespace.
    /// Other processes remain excluded until the last owned handle drops.
    pub fn open_shared_owner(root: impl Into<PathBuf>) -> Result<Self, LocalCoreStateStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let root = std::fs::canonicalize(root)?;
        let held =
            crate::native_host::HostRoot::open(&root, &mut crate::native_host::ordinary_work())
                .map_err(|error| LocalCoreStateStoreError::LazyWork(error.into()))?;
        static OWNERS: OnceLock<Owners<OwnerRef>> = OnceLock::new();
        let owner = join::<_, true>(
            OWNERS.get_or_init(|| OrderedMutex::new(BTreeMap::new())),
            root.clone(),
            held.identity().to_bytes(),
            || {
                create(&root, Some(super::contexts::OWNED_CHECKPOINT as usize))
                    .map(OwnerRef::new)
                    .map(Arc::new)
            },
        )?;
        Ok(Self {
            root: Arc::new(root),
            ownership: Ownership::Owned(owner),
            deferral: None,
            inspection: false,
            journal_work: None,
            ownership_anchor: None,
        })
    }
}

#[cfg(test)]
#[cfg(loom)]
mod models;
#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn bounded_cache(peak: usize) -> proptest::test_runner::TestCaseResult {
        prop_assert!(peak <= super::super::contexts::OWNED_CHECKPOINT as usize);
        Ok(())
    }

    async fn query_misses(store: &LocalCoreStateStore, queries: usize) -> usize {
        let Ownership::Owned(owner) = &store.ownership else {
            panic!("not owned")
        };
        let value = super::super::tests::crash_lineage();
        assert!(
            WorkspaceLineageStore::compare_and_swap(store, value.workspace_id, 0, value.clone())
                .await
                .unwrap()
        );
        let mut peak = 0;
        for index in 0..queries {
            assert!(
                WorkspaceLineageStore::load(
                    store,
                    WorkspaceId::from_bytes([128 + index as u8; 16])
                )
                .await
                .unwrap()
                .is_none()
            );
            peak = peak.max(owner.residents.read().unwrap().len());
        }
        assert_eq!(
            WorkspaceLineageStore::load(store, value.workspace_id)
                .await
                .unwrap(),
            Some(value)
        );
        peak
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4))]
        #[test]
        fn joinable_cache_work_is_bounded_across_distinct_queries(queries in 4usize..16) {
            let (bounded, old) = tokio::runtime::Builder::new_current_thread().enable_all()
                .build().unwrap().block_on(async {
                    let root = tempfile::tempdir().unwrap();
                    let bounded = LocalCoreStateStore::open_shared_owner(root.path().join("bounded")).unwrap();
                    let old = LocalCoreStateStore::open_owned(root.path().join("old")).unwrap();
                    let peaks = (query_misses(&bounded, queries).await, query_misses(&old, queries).await);
                    drop((bounded, old));
                    let reopened = LocalCoreStateStore::open_shared_owner(root.path().join("bounded")).unwrap();
                    let value = super::super::tests::crash_lineage();
                    assert_eq!(WorkspaceLineageStore::load(&reopened, value.workspace_id).await.unwrap(), Some(value));
                    peaks
                });
            bounded_cache(bounded)?;
            prop_assert!(bounded_cache(old).is_err(), "unbounded-cache control escaped the same assertion");
        }
    }

    #[test]
    fn joins_canonical_aliases_without_extending_the_owner_lifetime() {
        let directory = tempfile::tempdir().unwrap();
        let exclusive = LocalCoreStateStore::open_owned(directory.path()).unwrap();
        assert!(LocalCoreStateStore::open_shared_owner(directory.path()).is_err());
        drop(exclusive);
        let first = LocalCoreStateStore::open_shared_owner(directory.path()).unwrap();
        let second = LocalCoreStateStore::open_shared_owner(directory.path().join(".")).unwrap();
        let (Ownership::Owned(a), Ownership::Owned(b)) = (&first.ownership, &second.ownership)
        else {
            panic!("joined store is not owned")
        };
        assert!(Arc::ptr_eq(&a.owner, &b.owner));
        assert!(LocalCoreStateStore::open_owned(directory.path()).is_err());
        drop(first);
        assert!(LocalCoreStateStore::open_owned(directory.path()).is_err());
        drop(second);
        LocalCoreStateStore::open_owned(directory.path()).unwrap();
    }
    #[test]
    fn joining_a_replaced_identity_never_returns_the_live_owner() {
        let owners = OrderedMutex::new(BTreeMap::new());
        let path = PathBuf::from("namespace");
        let first = join::<_, true>(&owners, path.clone(), [1; 16], || Ok(Arc::new(1))).unwrap();
        let alias = join::<_, true>(&owners, path.clone(), [1; 16], || {
            panic!("an existing identity must be joined")
        })
        .unwrap();
        assert!(Arc::ptr_eq(&first, &alias));
        assert!(matches!(
            join::<_, true>(&owners, path.clone(), [2; 16], || {
                panic!("replacement cannot initialize while the owner is live")
            }),
            Err(LocalCoreStateStoreError::OwnershipConflict(_))
        ));
        drop(first);
        drop(alias);
        assert_eq!(
            *join::<_, true>(&owners, path, [2; 16], || Ok(Arc::new(2))).unwrap(),
            2
        );
    }

    #[test]
    fn a_live_owner_cannot_be_joined_after_its_directory_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("namespace");
        let first = LocalCoreStateStore::open_shared_owner(&root).unwrap();
        let retired = directory.path().join("retired");
        // An idle owner holds no descriptor in its namespace, so no platform
        // pins the directory: replacing it is refused at the join instead.
        std::fs::rename(&root, &retired).unwrap();
        std::fs::create_dir(&root).unwrap();
        assert!(matches!(
            LocalCoreStateStore::open_shared_owner(&root),
            Err(LocalCoreStateStoreError::OwnershipConflict(_))
        ));
        drop(first);
        LocalCoreStateStore::open_shared_owner(&root).unwrap();
    }

    #[test]
    fn shared_join_cannot_turn_an_exclusive_open_into_a_retirement_wait() {
        crate::initialize_test_native_io();

        fn retain<T>(owner: &Arc<T>) -> Arc<T> {
            let registry = Owners::new(BTreeMap::from([(
                PathBuf::from("namespace"),
                ([1; 16], Arc::downgrade(owner)),
            )]));
            join::<_, true>(&registry, "namespace".into(), [1; 16], || {
                panic!("the shared join must retain the existing owner")
            })
            .unwrap()
        }

        fn conflicts(retain_handle: bool) -> bool {
            let directory = tempfile::tempdir().unwrap();
            let original = Arc::new(OwnerRef::new(create(directory.path(), None).unwrap()));
            let release = original._claim.release_observer();
            // Stop the shared opener immediately after its registry upgrade,
            // before it constructs a store. The negative control upgrades only
            // the bare owner, reproducing the original Arc/handle gap.
            let (joined, bare) = if retain_handle {
                (Some(retain(&original)), None)
            } else {
                (None, Some(retain(&original.owner)))
            };
            drop(original);

            let (events, received) = std::sync::mpsc::channel();
            let registered = events.clone();
            let observer = std::thread::spawn(move || {
                if release.wait_registered().is_ok() {
                    registered.send(true).unwrap();
                }
            });
            let path = directory.path().to_owned();
            let opener = std::thread::spawn(move || {
                let result = LocalCoreStateStore::open_owned(path);
                events.send(false).unwrap();
                result
            });
            // The actual claim registration event races the exclusive open's
            // completion. No timeout, sleep or readiness probe is involved.
            let waited = received.recv().unwrap();
            drop((joined, bare));
            observer.join().unwrap();
            let result = opener.join().unwrap();
            if waited {
                assert!(result.is_ok(), "{result:?}");
                false
            } else {
                matches!(result, Err(LocalCoreStateStoreError::OwnershipConflict(_)))
            }
        }

        assert!(
            conflicts(true),
            "a shared join waited on its own live store"
        );
        assert!(!conflicts(false), "the bare-owner negative control escaped");
    }

    #[test]
    fn reopening_waits_for_the_retiring_native_owner() {
        crate::initialize_test_native_io();
        for shared in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = if shared {
                LocalCoreStateStore::open_shared_owner(directory.path())
            } else {
                LocalCoreStateStore::open_owned(directory.path())
            }
            .unwrap();
            let Ownership::Owned(owner) = &store.ownership else {
                panic!("not owned")
            };
            // Retain physical teardown after the last public handle retires.
            let retiring = owner.owner.clone();
            let release = owner._claim.release_observer();
            drop(store);
            let (events, received) = std::sync::mpsc::channel();
            let registered = events.clone();
            let observer = std::thread::spawn(move || {
                registered.send(release.wait_registered().is_ok()).unwrap();
            });
            let path = directory.path().to_owned();
            let opener = std::thread::spawn(move || {
                let result = LocalCoreStateStore::open_owned(path);
                events.send(false).unwrap();
                result
            });
            let waited = received.recv().unwrap();
            drop(retiring);
            observer.join().unwrap();
            let reopened = opener.join().unwrap();
            assert!(
                waited,
                "reopen refused instead of waiting for native retirement"
            );
            assert!(reopened.is_ok(), "{reopened:?}");
        }
    }
}
