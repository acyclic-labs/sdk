use super::*;

struct ActiveGuard {
    id: OperationId,
    group: Arc<GroupState>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        locked(&self.group.admission).active.remove(&self.id);
    }
}

struct GroupAdmission {
    closed: bool,
    active: BTreeMap<OperationId, AbortHandle>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct GroupChildCustody {
    _previous: Option<Box<GroupChildCustody>>,
    allocation: acyclic_fs::NativeAllocation,
}

struct GroupState {
    semaphore: Arc<Semaphore>,
    admission: Mutex<GroupAdmission>,
    children: Mutex<Vec<Weak<GroupState>>>,
    _parent: Option<Arc<GroupState>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    children_custody: Mutex<Option<Box<GroupChildCustody>>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Controls admission and lifetime for related live tasks.
#[derive(Clone)]
pub struct TaskGroup {
    state: Arc<GroupState>,
}

impl TaskGroup {
    /// Creates a task group with bounded concurrency.
    pub fn new(max_concurrency: usize, work: NativeWork<'_>) -> crate::Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = if let Some(work) = work {
            let bytes =
                (std::mem::size_of::<GroupState>() + std::mem::size_of::<Semaphore>()) as u64;
            let custody = work
                .claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)
                .map_err(|cause| crate::Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            work.charge(acyclic_fs::WorkCounters {
                allocation_operations: 2,
                ..Default::default()
            })
            .map_err(|cause| crate::Error::NativeWork {
                cause,
                custody: None,
            })?;
            Some(custody)
        } else {
            None
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = work;
        Ok(Self {
            state: Arc::new(GroupState {
                semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
                admission: Mutex::new(GroupAdmission {
                    closed: false,
                    active: BTreeMap::new(),
                }),
                children: Mutex::new(Vec::new()),
                _parent: None,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                children_custody: Mutex::new(None),
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                _native_custody: custody,
            }),
        })
    }

    /// Creates an independently bounded child retained in this cancellation tree.
    pub fn child(&self, max_concurrency: usize, work: NativeWork<'_>) -> crate::Result<Self> {
        let mut child = Self::new(max_concurrency, work)?;
        Arc::get_mut(&mut child.state)
            .expect("new unattached task group")
            ._parent = Some(Arc::clone(&self.state));
        let admission = locked(&self.state.admission);
        let mut children = locked(&self.state.children);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            if children.len() == children.capacity() {
                let old = children.capacity();
                let target = old.checked_mul(2).unwrap_or(usize::MAX).max(4);
                let bytes = target
                    .checked_sub(old)
                    .and_then(|slots| slots.checked_mul(std::mem::size_of::<Weak<GroupState>>()))
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<GroupChildCustody>()))
                    .ok_or(crate::Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(
                            acyclic_fs::WorkError::Overflow,
                        ),
                        custody: None,
                    })? as u64;
                let custody = work
                    .claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)
                    .map_err(|cause| crate::Error::NativeWork {
                        cause,
                        custody: None,
                    })?;
                work.charge(acyclic_fs::WorkCounters {
                    allocation_operations: 1,
                    ..Default::default()
                })
                .map_err(|cause| crate::Error::NativeWork {
                    cause,
                    custody: None,
                })?;
                let mut retained = locked(&self.state.children_custody);
                let previous = retained.take();
                *retained = Some(Box::new(GroupChildCustody {
                    _previous: previous,
                    allocation: custody,
                }));
                let growth = work
                    .charge(acyclic_fs::WorkCounters {
                        allocation_operations: 1,
                        ..Default::default()
                    })
                    .map_err(|cause| crate::Error::NativeWork {
                        cause,
                        custody: None,
                    })
                    .and_then(|()| {
                        let extra = target - children.len();
                        children.try_reserve_exact(extra).map_err(|cause| {
                            crate::Error::NativeWork {
                                cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(
                                    cause,
                                ),
                                custody: None,
                            }
                        })
                    });
                if let Err(error) = growth {
                    let mut unused = retained.take().expect("installed child-vector custody");
                    *retained = unused._previous.take();
                    drop(unused);
                    return Err(error);
                }
                let actual = ((children.capacity() - old) * std::mem::size_of::<Weak<GroupState>>()
                    + std::mem::size_of::<GroupChildCustody>()) as u64;
                retained
                    .as_mut()
                    .expect("installed child-vector custody")
                    .allocation
                    .reconcile_capacity(actual)
                    .map_err(|cause| crate::Error::NativeWork {
                        cause,
                        custody: None,
                    })?;
            }
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = work;
        children.push(Arc::downgrade(&child.state));
        drop(children);
        let closed = admission.closed;
        drop(admission);
        if closed {
            locked(&child.state.admission).closed = true;
        }
        Ok(child)
    }

    /// Admits one live task.
    pub async fn spawn<T, F>(&self, id: OperationId, future: F) -> Admission<TaskHandle<T>>
    where
        T: Send + 'static,
        F: Future<Output = T> + acyclic_stream::ProviderTask + 'static,
    {
        let mut admission = locked(&self.state.admission);
        if admission.closed || admission.active.contains_key(&id) {
            return Admission::Rejected {
                reason: if admission.closed {
                    "task group is closed"
                } else {
                    "operation is already active"
                }
                .into(),
            };
        }
        let semaphore = Arc::clone(&self.state.semaphore);
        let guard = ActiveGuard {
            id,
            group: Arc::clone(&self.state),
        };
        let (start, admitted) = oneshot::channel();
        let task = async move {
            let _guard = guard;
            let _ = admitted.await;
            match semaphore.acquire_owned().await {
                Ok(_permit) => Outcome::Succeeded(future.await),
                Err(_) => Outcome::Failed {
                    message: "task group closed".into(),
                },
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        let handle = TaskHandle {
            id,
            join: tokio::spawn(task),
        };
        #[cfg(target_arch = "wasm32")]
        let handle = {
            let (abort, registration) = futures::future::AbortHandle::new_pair();
            let (sender, outcome) = oneshot::channel();
            wasm_bindgen_futures::spawn_local(async move {
                let result = futures::future::Abortable::new(task, registration)
                    .await
                    .unwrap_or(Outcome::Cancelled);
                let _ = sender.send(result);
            });
            TaskHandle { id, outcome, abort }
        };
        admission.active.insert(id, handle.abort_handle());
        drop(admission);
        let _ = start.send(());
        Admission::Accepted(handle)
    }

    /// Closes admission and requests cancellation of this group and descendants.
    pub fn cancel(&self) {
        let mut pending = vec![Arc::clone(&self.state)];
        while let Some(group) = pending.pop() {
            let active = {
                let mut admission = locked(&group.admission);
                admission.closed = true;
                std::mem::take(&mut admission.active)
            };
            for handle in active.values() {
                handle.abort();
            }
            let children = {
                let mut retained = locked(&group.children);
                let children = retained
                    .iter()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>();
                retained.retain(|child| child.strong_count() > 0);
                children
            };
            pending.extend(children);
        }
    }

    /// Closes admission throughout the group tree while accepted work drains.
    pub fn close(&self) {
        let mut pending = vec![Arc::clone(&self.state)];
        while let Some(group) = pending.pop() {
            locked(&group.admission).closed = true;
            let mut retained = locked(&group.children);
            pending.extend(retained.iter().filter_map(Weak::upgrade));
            retained.retain(|child| child.strong_count() > 0);
        }
    }
}

fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
