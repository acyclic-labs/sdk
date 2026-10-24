use super::*;
use crate::{
    CachedObjectStore, ObjectCacheOptions, kernel::DecodeLimits, memory::MemoryObjectStore,
};

thread_local! {
    static OMIT_TRANSFER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(super) fn omit_transfer() -> bool {
    OMIT_TRANSFER.get()
}
pub(crate) fn set_omit_transfer(omit: bool) {
    OMIT_TRANSFER.set(omit);
}
struct Restore;
impl Drop for Restore {
    fn drop(&mut self) {
        OMIT_TRANSFER.set(false);
    }
}
type Store = StagedObjects<CachedObjectStore<MemoryObjectStore>>;
fn cached() -> CachedObjectStore<MemoryObjectStore> {
    CachedObjectStore::new(MemoryObjectStore::default(), ObjectCacheOptions::default()).unwrap()
}
fn fixture() -> (HashedObject, DecodedCacheKey, DecodedCacheValue) {
    let object = HashedObject::new(crate::ObjectKind::Blob, OwnedBytes::from_static(b"page"));
    let key = DecodedCacheKey::new::<u64>(object.object_id(), DecodeLimits::default());
    let value = DecodedCacheValue {
        value: Arc::new(42_u64),
        logical_bytes: 8,
        native_custody: None,
    };
    (object, key, value)
}
fn assert_warm(store: &Store, key: DecodedCacheKey, value: &DecodedCacheValue) {
    let cached = store
        .decoded_cache_get(key)
        .unwrap()
        .expect("published decoding was lost");
    assert!(Arc::ptr_eq(&cached.value, &value.value));
}

#[cfg(not(acyclic_fs_loom))]
mod ordinary {
    use super::*;

    async fn publication(omit: bool, native: bool, closure: bool) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(cached(), directory.path().into())
            .await
            .unwrap();
        let (object, key, value) = fixture();
        let token = CancellationToken::new();
        let mut unmetered = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        store
            .put_hashed(object.clone(), &mut unmetered)
            .await
            .unwrap();
        store.decoded_cache_admit(key, value.clone()).unwrap();
        // Private staged decoding cannot leak through the durable store.
        assert!(store.inner.decoded_cache_get(key).unwrap().is_none());
        OMIT_TRANSFER.set(omit);
        let _restore = Restore;
        let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token).unwrap();
        let mut work = if native {
            AuthorityWork::native(&original, &token)
        } else {
            AuthorityWork::new(WorkBudget::UNBOUNDED, &token)
        };
        let ids = [object.object_id()];
        let scope = if closure {
            PublicationScope::Closure {
                objects: &ids,
                proven_at: store.collection.sweeps(),
            }
        } else {
            PublicationScope::Everything
        };
        let hold = store.flush_before_publish(scope, &mut work).await.unwrap();
        assert!(store.index().objects.is_empty());
        let read = store
            .read(object.object_id(), object.length(), &mut unmetered)
            .await
            .unwrap();
        assert_eq!(&read.bytes[..], &object.bytes()[..]);
        assert_warm(&store, key, &value);
        drop(hold);
        // Re-staging an acknowledged object takes the drained shortcut. It
        // must transfer a newly admitted decoding even after cache eviction.
        store.inner.clear().unwrap();
        store.put_hashed(object, &mut unmetered).await.unwrap();
        store.decoded_cache_admit(key, value.clone()).unwrap();
        drop(
            store
                .flush_before_publish(PublicationScope::Everything, &mut work)
                .await
                .unwrap(),
        );
        assert_warm(&store, key, &value);
    }

    #[tokio::test]
    async fn scalar_and_batch_publication_retain_decoded_identity() {
        for native in [false, true] {
            for closure in [false, true] {
                publication(false, native, closure).await;
            }
        }
    }

    #[tokio::test]
    #[should_panic(expected = "published decoding was lost")]
    async fn lost_transfer_fails_the_same_publication_assertion() {
        publication(true, false, false).await;
    }

    #[tokio::test]
    async fn rejected_cache_handoff_retains_retryable_staging_and_exact_error() {
        let directory = tempfile::tempdir().unwrap();
        let store = StagedObjects::open(
            crate::test_support::OwnedReadObjectStore::default(),
            directory.path().into(),
        )
        .await
        .unwrap();
        let (object, key, value) = fixture();
        let id = object.object_id();
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        store.put_hashed(object.clone(), &mut work).await.unwrap();
        store.decoded_cache_admit(key, value.clone()).unwrap();
        store
            .inner
            .reject_decoded_admission
            .store(true, Ordering::Relaxed);
        assert!(matches!(
            store
                .flush_before_publish(PublicationScope::Everything, &mut work)
                .await,
            Err(ObjectStoreError::Corrupt)
        ));
        assert!(store.index().objects.contains_key(&id));
        assert!(!store.index().drained.contains(&id));
        assert!(Arc::ptr_eq(
            &store.decoded_cache_get(key).unwrap().unwrap().value,
            &value.value
        ));
        assert!(store.inner.contains(id, &mut work).await.unwrap());
        store
            .inner
            .reject_decoded_admission
            .store(false, Ordering::Relaxed);
        // Explicit Uncached admission is a successful handoff: cache capacity
        // never becomes a requirement for durable publication.
        drop(
            store
                .flush_before_publish(PublicationScope::Everything, &mut work)
                .await
                .unwrap(),
        );
        assert!(store.index().objects.is_empty());
        let read = store.read(id, object.length(), &mut work).await.unwrap();
        assert_eq!(&read.bytes[..], &object.bytes()[..]);
    }
}

#[cfg(acyclic_fs_loom)]
mod models {
    use super::*;

    const _: fn(IndexLock<Index>) -> loom::sync::RwLock<Index> = |lock| lock;

    fn scenario(omit: bool, racing_admission: bool) {
        let store = loom::sync::Arc::new(Store {
            inner: cached(),
            spill: NativeFile::from_file(tempfile::tempfile().unwrap(), None).unwrap(),
            index: IndexLock::new(Index::default()),
            spill_io: RwLock::new(()),
            collection: Arc::default(),
        });
        let (object, key, value) = fixture();
        // The publication acknowledgement is the modeled cut. Native I/O is
        // already complete; exclusion and both cache paths are production code.
        store
            .index_mut()
            .admit_resident(object.object_id(), object.bytes().clone());
        if !racing_admission {
            store.decoded_cache_admit(key, value.clone()).unwrap();
        }
        let writer = {
            let store = store.clone();
            loom::thread::spawn(move || {
                OMIT_TRANSFER.set(omit);
                let _restore = Restore;
                store
                    .index_mut()
                    .acknowledge(object.object_id(), &store.inner)
                    .unwrap();
            })
        };
        let reader = {
            let store = store.clone();
            let value = value.clone();
            loom::thread::spawn(move || {
                if racing_admission {
                    store.decoded_cache_admit(key, value.clone()).unwrap();
                }
                store
                    .decoded_cache_get(key)
                    .unwrap()
                    .is_some_and(|cached| Arc::ptr_eq(&cached.value, &value.value))
            })
        };
        writer.join().unwrap();
        let observed = reader.join().unwrap();
        assert!(observed, "published decoding was lost");
        assert_warm(&store, key, &value);
    }

    #[test]
    fn publication_and_decoded_read_share_the_index_lock() {
        loom::model(|| scenario(false, false));
    }

    #[test]
    fn first_decoded_admission_and_publication_share_the_index_lock() {
        loom::model(|| scenario(false, true));
    }

    #[test]
    #[should_panic(expected = "published decoding was lost")]
    fn missing_handoff_fails_the_same_interleaving_proof() {
        loom::model(|| scenario(true, false));
    }
}
