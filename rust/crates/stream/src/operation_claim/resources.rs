//! Funded, typed state retained by the provider owner rather than its wrappers.

use std::any::Any;
use std::sync::Arc;

trait Resource: Send + Sync {
    fn value(&self) -> &(dyn Any + Send + Sync);
    fn next(&self) -> Option<&dyn Resource>;
    fn next_mut(&mut self) -> &mut Option<Box<dyn Resource>>;
}

struct Entry<T, H> {
    value: Arc<T>,
    next: Option<Box<dyn Resource>>,
    _storage: H,
}

impl<T: Send + Sync + 'static, H: Send + Sync> Resource for Entry<T, H> {
    fn value(&self) -> &(dyn Any + Send + Sync) {
        &self.value
    }

    fn next(&self) -> Option<&dyn Resource> {
        self.next.as_deref()
    }

    fn next_mut(&mut self) -> &mut Option<Box<dyn Resource>> {
        &mut self.next
    }
}

#[derive(Default)]
pub(super) struct Resources(crate::query_sync::SyncMutex<Option<Box<dyn Resource>>>);

impl Resources {
    pub(super) fn get<T, H, E, C>(
        &self,
        context: &mut C,
        mut visit: impl FnMut(&mut C) -> Result<(), E>,
        admit: impl FnOnce(u64, &mut C) -> Result<H, E>,
        create: impl FnOnce(&mut C) -> Result<Arc<T>, E>,
    ) -> Result<Arc<T>, E>
    where
        T: Send + Sync + 'static,
        H: Send + Sync + 'static,
    {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut cursor = entries.as_deref();
        while let Some(entry) = cursor {
            visit(context)?;
            if let Some(value) = entry.value().downcast_ref::<Arc<T>>() {
                return Ok(Arc::clone(value));
            }
            cursor = entry.next();
        }
        // Both callbacks precede mutation. Failed admission, initialization or
        // unwinding leaves every existing resource and its custody intact.
        let storage = admit(std::mem::size_of::<Entry<T, H>>() as u64, context)?;
        let value = create(context)?;
        let entry = Box::new(Entry {
            value: Arc::clone(&value),
            next: entries.take(),
            _storage: storage,
        });
        *entries = Some(entry);
        Ok(value)
    }
}

impl Drop for Resources {
    fn drop(&mut self) {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some(mut entry) = entries.take() {
            *entries = entry.next_mut().take();
        }
    }
}

#[cfg(all(test, not(acyclic_loom)))]
mod tests {
    use super::*;
    use std::sync::{
        Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn simultaneous_wrappers_initialize_one_owner_resource() {
        let claims = crate::OperationClaims::default();
        let initialized = AtomicUsize::new(0);
        let admitted = AtomicUsize::new(0);
        let barrier = Barrier::new(4);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        claims
                            .shared_resource(
                                &mut (),
                                |()| Ok::<_, ()>(()),
                                |bytes, ()| {
                                    assert_eq!(bytes, std::mem::size_of::<Entry<u64, ()>>() as u64);
                                    admitted.fetch_add(1, Ordering::SeqCst);
                                    Ok(())
                                },
                                |()| {
                                    initialized.fetch_add(1, Ordering::SeqCst);
                                    Ok(Arc::new(47_u64))
                                },
                            )
                            .unwrap()
                    })
                })
                .collect();
            let values: Vec<_> = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect();
            let first = values
                .first()
                .expect("resource callers returned no handles");
            assert!(values.iter().all(|value| Arc::ptr_eq(value, first)));
        });
        assert_eq!(initialized.load(Ordering::SeqCst), 1);
        assert_eq!(admitted.load(Ordering::SeqCst), 1);
        // Dropping all handles does not discard the provider's actor state.
        let retained = claims
            .shared_resource(
                &mut (),
                |()| Ok::<_, ()>(()),
                |_, ()| -> Result<(), ()> { panic!("existing resource allocated another node") },
                |()| -> Result<Arc<u64>, ()> { panic!("existing resource initialized twice") },
            )
            .unwrap();
        assert_eq!(*retained, 47);
        let independent = crate::OperationClaims::default()
            .shared_resource(
                &mut (),
                |()| Ok::<_, ()>(()),
                |_, ()| Ok(()),
                |()| Ok(Arc::new(47_u64)),
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&retained, &independent));
    }

    struct NodeStorage(Arc<Mutex<Vec<&'static str>>>);
    impl Drop for NodeStorage {
        fn drop(&mut self) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("node");
        }
    }
    struct Value(Arc<Mutex<Vec<&'static str>>>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("value");
        }
    }

    #[test]
    fn failed_initialization_preserves_resources_and_node_custody() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let claims = crate::OperationClaims::default();
        let value = claims
            .shared_resource(
                &mut (),
                |()| Ok::<_, ()>(()),
                |_, ()| Ok(NodeStorage(Arc::clone(&events))),
                |()| Ok(Arc::new(Value(Arc::clone(&events)))),
            )
            .unwrap();
        let denied = claims.shared_resource(
            &mut (),
            |()| Ok::<_, ()>(()),
            |_, ()| Err::<(), _>(()),
            |()| -> Result<Arc<u64>, ()> { panic!("allocation denial initialized state") },
        );
        assert!(denied.is_err());
        let failed = claims.shared_resource(
            &mut (),
            |()| Ok::<_, ()>(()),
            |_, ()| Ok(NodeStorage(Arc::clone(&events))),
            |()| Err::<Arc<u64>, _>(()),
        );
        assert!(failed.is_err());
        assert_eq!(*events.lock().unwrap(), ["node"]);
        drop(value);
        assert_eq!(*events.lock().unwrap(), ["node"]);
        drop(claims);
        assert_eq!(*events.lock().unwrap(), ["node", "value", "node"]);
    }
}
