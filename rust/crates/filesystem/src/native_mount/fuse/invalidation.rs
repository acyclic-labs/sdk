use super::*;

#[derive(Debug, Eq, PartialEq)]
pub(super) enum KernelCacheItem {
    Inode(u64),
    Entry {
        parent: u64,
        name: FuseOwned<Vec<u8>>,
    },
}

#[derive(Default)]
pub(super) struct InvalidationQueue {
    pub(super) notified: Option<ViewStamp>,
    pub(super) scanned: Option<ViewStamp>,
    pub(super) processed: Option<ViewStamp>,
    pub(super) deferred: FuseOwned<Vec<KernelCacheItem>>,
    pub(super) pending_tail: FuseOwned<std::vec::IntoIter<KernelCacheItem>>,
    pub(super) in_flight: bool,
    pub(super) failure: Option<(NativeMountError, crate::WorkCounters)>,
}

impl InvalidationQueue {
    pub(super) fn is_idle(&self) -> bool {
        !self.is_pending(self.notified)
    }

    pub(super) fn is_pending(&self, target: Option<ViewStamp>) -> bool {
        self.processed < target
            || !self.deferred.is_empty()
            || !self.pending_tail.as_slice().is_empty()
            || self.in_flight
    }

    pub(super) fn take_deferred(&mut self) -> FuseOwned<std::vec::IntoIter<KernelCacheItem>> {
        self.in_flight = true;
        if self.pending_tail.as_slice().is_empty() {
            std::mem::take(&mut self.deferred).map(Vec::into_iter)
        } else {
            std::mem::take(&mut self.pending_tail)
        }
    }

    pub(super) fn retain_pending_tail(
        &mut self,
        items: FuseOwned<std::vec::IntoIter<KernelCacheItem>>,
    ) {
        assert!(
            self.pending_tail.as_slice().is_empty(),
            "one original invalidator pass owns its tail"
        );
        if !items.as_slice().is_empty() {
            self.pending_tail = items;
        }
    }

    pub(super) fn finish(
        &mut self,
        through: Option<ViewStamp>,
        dropped: Result<(), (NativeMountError, crate::WorkCounters)>,
    ) {
        if self.pending_tail.as_slice().is_empty() {
            self.processed = self.processed.max(through);
        }
        self.in_flight = false;
        if let Err(error) = dropped {
            self.failure.get_or_insert(error);
        }
    }
}

pub(super) fn stale_kernel_items(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &HashMap<FuseOwned<MountPath>, u64>,
    unchanged_since: impl Fn(
        &MountPath,
        Option<crate::FileId>,
        ViewStamp,
    ) -> Result<bool, NativeMountError>,
    through: ViewStamp,
    work: &crate::NativeMaterializationWork,
) -> Result<FuseOwned<Vec<KernelCacheItem>>, NativeMountError> {
    enum Action {
        Inode(u64),
        Negative { inode: u64, item: usize },
        Binding { inode: u64, index: usize },
    }
    let mut items = fuse_vec(0, work)?;
    let mut actions = fuse_vec(0, work)?;
    for (inode, entry) in by_inode.iter() {
        fuse_item(work)?;
        let file_id = entry.lookup.node.file_id;
        let anchor = entry
            .bindings
            .first()
            .map_or(&ROOT_PATH, |binding| &*binding.path);
        if let Some(held) = entry.kernel {
            if !unchanged_since(anchor, Some(file_id), held)? {
                fuse_reserve(&mut items, 1, work)?;
                fuse_reserve(&mut actions, 1, work)?;
                items.push(KernelCacheItem::Inode(*inode));
                actions.push(Action::Inode(*inode));
            }
        }
        for (name, held) in &entry.negative_children {
            fuse_item(work)?;
            let path = fuse_child_path(anchor, name, work)?;
            if !unchanged_since(&path, None, *held)? {
                let name = fuse_copy_bytes(name, work)?;
                fuse_reserve(&mut items, 1, work)?;
                fuse_reserve(&mut actions, 1, work)?;
                actions.push(Action::Negative {
                    inode: *inode,
                    item: items.len(),
                });
                items.push(KernelCacheItem::Entry {
                    parent: *inode,
                    name,
                });
            }
        }
        for (index, binding) in entry.bindings.iter().enumerate() {
            fuse_item(work)?;
            if let Some(held) = binding.kernel {
                if !unchanged_since(&binding.path, Some(file_id), held)? {
                    if let Some(parent) = fuse_parent(&binding.path, work)? {
                        if let Some(parent_inode) = inode_by_path.get(&*parent).copied() {
                            let name = fuse_copy_bytes(
                                binding.path.components().last().ok_or(libc::ESTALE)?,
                                work,
                            )?;
                            fuse_reserve(&mut items, 1, work)?;
                            items.push(KernelCacheItem::Entry {
                                parent: parent_inode,
                                name,
                            });
                        }
                    }
                    fuse_reserve(&mut actions, 1, work)?;
                    actions.push(Action::Binding {
                        inode: *inode,
                        index,
                    });
                }
            }
        }
    }
    for action in actions.iter() {
        match action {
            Action::Inode(inode) => {
                by_inode
                    .get_mut(inode)
                    .expect("planned inode remains locked")
                    .kernel = Some(through)
            }
            Action::Binding { inode, index } => {
                by_inode
                    .get_mut(inode)
                    .expect("planned binding remains locked")
                    .bindings[*index]
                    .kernel = None
            }
            Action::Negative { inode, item } => {
                let KernelCacheItem::Entry { name, .. } = &items[*item] else {
                    unreachable!("planned negative entry")
                };
                drop(
                    by_inode
                        .get_mut(inode)
                        .expect("planned directory remains locked")
                        .negative_children
                        .remove(name.as_slice()),
                );
            }
        }
    }
    Ok(items)
}

pub(super) struct FuseInvalidationBatch<'a> {
    pub(super) core: &'a ProjectionCore,
    pub(super) items: FuseOwned<std::vec::IntoIter<KernelCacheItem>>,
}

impl Drop for FuseInvalidationBatch<'_> {
    fn drop(&mut self) {
        let items = std::mem::replace(&mut self.items, FuseOwned::empty(Vec::new().into_iter()));
        self.core
            .state_to_finish()
            .invalidation
            .retain_pending_tail(items);
    }
}
pub(super) fn run_invalidator(core: &ProjectionCore, notifier: &Notifier) {
    let mut blocked: Option<(
        Option<ViewStamp>,
        usize,
        (
            Weak<crate::demand::NativeDemandReadMeter>,
            Option<Weak<crate::materializer::NativeResourceAllowance>>,
        ),
    )> = None;
    loop {
        let mut state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        let scope_identity = loop {
            if core.stopping.load(Ordering::Acquire) {
                return;
            }
            let identity = core
                .startup_meter
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .upgrade()
                .as_ref()
                .map_or_else(
                    || (Weak::new(), None),
                    |work| {
                        (
                            Arc::downgrade(work.native_read_meter()),
                            work.resource_allowance().map(Arc::downgrade),
                        )
                    },
                );
            let denied = blocked.as_ref().is_some_and(|(stamp, deferred, previous)| {
                *stamp == state.invalidation.notified
                    && *deferred == state.invalidation.deferred.len()
                    && Weak::ptr_eq(&previous.0, &identity.0)
                    && match (&previous.1, &identity.1) {
                        (None, None) => true,
                        (Some(previous), Some(current)) => Weak::ptr_eq(previous, current),
                        _ => false,
                    }
            });
            if !state.invalidation.is_idle() && !denied {
                break identity;
            }
            state = core
                .invalidation
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        };
        let mut wait_key = (
            state.invalidation.notified,
            state.invalidation.deferred.len(),
            scope_identity,
        );
        drop(state);
        let mut completed_through = None;
        let mut sdk_failure = None;
        let result = core.request(|work| {
            let items = {
                let mut state = core.state_to_finish();
                let items = state.invalidation.take_deferred();
                wait_key.0 = state.invalidation.notified;
                wait_key.1 = state.invalidation.deferred.len();
                items
            };
            let mut items = FuseInvalidationBatch { core, items };
            let dropped = drop_kernel_caches(notifier, &mut items.items, work, &mut sdk_failure);
            drop(items);
            dropped?;
            let items = {
                let mut state = core.state_to_finish();
                let through = state.invalidation.notified;
                if let Some(through) = through
                    && state.invalidation.scanned < Some(through)
                {
                    let state = &mut *state;
                    let items = stale_kernel_items(
                        &mut state.by_inode,
                        &state.inode_by_path,
                        |path, file_id, stamp| {
                            core.source
                                .unchanged_since_with_meter(path, file_id, stamp, work)
                                .map_err(NativeMountError::from)
                        },
                        through,
                        work,
                    )?;
                    state.invalidation.scanned = Some(through);
                    completed_through = Some(through);
                    items.map(Vec::into_iter)
                } else {
                    completed_through = through;
                    FuseOwned::empty(Vec::new().into_iter())
                }
            };
            let mut items = FuseInvalidationBatch { core, items };
            let dropped = drop_kernel_caches(notifier, &mut items.items, work, &mut sdk_failure);
            drop(items);
            dropped?;
            sdk_failure.take().map_or(Ok(()), Err)
        });
        let denied = result.is_err();
        let mut state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        if denied {
            core.handler_failed.store(true, Ordering::Release);
        }
        if let Some(cause) = sdk_failure {
            if let Err(failure) = &result {
                state.invalidation.failure.get_or_insert((cause, failure.1));
            }
        }
        state.invalidation.finish(completed_through, result);
        core.invalidation.notify_all();
        blocked = if denied { Some(wait_key) } else { None };
    }
}

pub(super) fn drop_kernel_caches(
    notifier: &Notifier,
    items: &mut FuseOwned<std::vec::IntoIter<KernelCacheItem>>,
    work: &crate::NativeMaterializationWork,
    first_sdk_failure: &mut Option<NativeMountError>,
) -> Result<(), NativeMountError> {
    while let Some(item) = items.as_slice().first() {
        work.charge(crate::WorkCounters {
            backend_write_operations: 1,
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let dropped = match item {
            KernelCacheItem::Inode(inode) => notifier.inval_inode(INodeNo(*inode), 0, 0),
            KernelCacheItem::Entry { parent, name } => {
                notifier.inval_entry(INodeNo(*parent), OsStr::from_bytes(name))
            }
        };
        // The real SDK attempted this item, including a genuine failed call.
        drop(items.next());
        if let Err(error) = dropped
            && error.kind() != std::io::ErrorKind::NotFound
        {
            first_sdk_failure.get_or_insert(NativeMountError::Io(error));
        }
    }
    Ok(())
}
