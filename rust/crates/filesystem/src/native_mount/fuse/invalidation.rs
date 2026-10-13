use super::*;

impl FuseSession {
    pub(in crate::native_mount) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let result = self.core.request(|work| {
            let _active = NativeMountWorkScope::new(&self.core, work)?;
            let mut components = path
                .strip_prefix(b"/")
                .unwrap_or(path)
                .split(|byte| *byte == b'/');
            let name = components.next_back().expect("split has a leaf");
            for component in components.clone().chain(std::iter::once(name)) {
                work.charge(Items.delta(1))?;
                if component.is_empty()
                    || component.contains(&0)
                    || component == b"."
                    || component == b".."
                {
                    return Err(NativeMountError::Lifecycle(
                        "FUSE invalidation requires a canonical non-root path",
                    ));
                }
            }
            let parent = fuse_path(components.clone().count(), components, work)?;
            let child = ChildName::new(
                self.core.source.as_ref(),
                fuse_child_path(&parent, name, work)?,
                work,
            )?;
            let parent = match self.core.source.folded_path_with_meter(&parent, work)? {
                Some((path, bytes)) => FuseOwned::adopt(path, bytes, work)?,
                None => parent,
            };
            let mut state = self.core.state()?;
            work.charge(Items.delta(1))?;
            let file_inode = state.inode_by_path.get(child.key()).copied();
            work.charge(Items.delta(1))?;
            let parent_inode = state.inode_by_path.get(&*parent).copied();
            let count = usize::from(file_inode.is_some()) + 2 * usize::from(parent_inode.is_some());
            let mut items = fuse_vec(count, work)?;
            if let Some(inode) = file_inode {
                items.push(KernelCacheItem::Inode(inode));
            }
            if let Some(parent) = parent_inode {
                items.push(KernelCacheItem::Entry {
                    parent,
                    name: fuse_copy_bytes(name, work)?,
                });
                items.push(KernelCacheItem::Inode(parent));
            }
            let copied = items
                .len()
                .checked_mul(std::mem::size_of::<KernelCacheItem>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            work.admit(Copy.delta(copied))?;
            fuse_reserve(&mut state.invalidation.deferred, items.len(), work)?;
            state.invalidation.deferred.append(&mut items);
            work.record_admitted(Copy.delta(copied))?;
            let target = state.invalidation.notified;
            self.core.invalidation.notify_all();
            self.core.wait_for_invalidation(state, target, work)
        });
        result.map_err(|failure| failure.0)
    }

    pub(in crate::native_mount) fn revalidate(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let _active = NativeMountWorkScope::new(&self.core, work)?;
        self.core.source.fence_changes_with_meter(work)?;
        let state = self
            .core
            .state
            .lock()
            .map_err(|_| NativeMountError::Lifecycle("FUSE projection state is poisoned"))?;
        let target = state.invalidation.notified;
        self.core.wait_for_invalidation(state, target, work)
    }
}

impl ProjectionCore {
    fn wait_for_invalidation(
        &self,
        mut state: MutexGuard<'_, ProjectionState>,
        target: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        loop {
            work.charge(Items.delta(1))?;
            if self.stopping.load(Ordering::Acquire) {
                return Err(NativeMountError::Lifecycle("session is stopped"));
            }
            if self.handler_failed.load(Ordering::Acquire) || state.invalidation.failure.is_some() {
                return Err(NativeMountError::Lifecycle(
                    "FUSE invalidation previously failed",
                ));
            }
            let condition = if state.invalidation.is_pending(target) {
                &self.invalidation
            } else if target.is_some_and(|target| state.page_stores.precede(target)) {
                &self.page_stored
            } else {
                return Ok(());
            };
            state = condition
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| NativeMountError::Lifecycle("FUSE projection state is poisoned"))?
                .0;
        }
    }
}

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
        work.charge(Items.delta(1))?;
        let file_id = entry.lookup.node.file_id;
        let anchor = entry.anchor();
        if let Some(held) = entry.kernel
            && !unchanged_since(anchor, Some(file_id), held)?
        {
            fuse_reserve(&mut items, 1, work)?;
            fuse_reserve(&mut actions, 1, work)?;
            items.push(KernelCacheItem::Inode(*inode));
            actions.push(Action::Inode(*inode));
        }
        for (name, held) in &entry.negative_children {
            work.charge(Items.delta(1))?;
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
            work.charge(Items.delta(1))?;
            if let Some(held) = binding.kernel
                && !unchanged_since(&binding.path, Some(file_id), held)?
            {
                if let Some(parent) = fuse_parent(&binding.path, work)?
                    && let Some(parent_inode) = inode_by_path.get(&*parent).copied()
                {
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
                fuse_reserve(&mut actions, 1, work)?;
                actions.push(Action::Binding {
                    inode: *inode,
                    index,
                });
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
                    unreachable!("planned negative entry");
                };
                by_inode
                    .get_mut(inode)
                    .expect("planned directory remains locked")
                    .negative_children
                    .remove(name.as_slice());
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
        let items = std::mem::take(&mut self.items);
        self.core
            .state_to_finish()
            .invalidation
            .retain_pending_tail(items);
    }
}

pub(super) fn run_invalidator(core: &ProjectionCore, notifier: &Notifier) {
    let mut blocked = None;
    loop {
        let mut state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut wait_key = loop {
            if core.stopping.load(Ordering::Acquire) {
                return;
            }
            let activation = core
                .startup_meter
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let key = (
                state.invalidation.notified,
                state.invalidation.deferred.len(),
                activation.generation,
                activation.depth != 0,
            );
            drop(activation);
            if state.invalidation.is_pending(state.invalidation.notified) && blocked != Some(key) {
                break key;
            }
            state = core
                .invalidation
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        };
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
                            Ok(core
                                .source
                                .unchanged_since_with_meter(path, file_id, stamp, work)?)
                        },
                        through,
                        work,
                    )?;
                    state.invalidation.scanned = Some(through);
                    completed_through = Some(through);
                    items.map(Vec::into_iter)
                } else {
                    completed_through = through;
                    FuseOwned::default()
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
        if let Some(cause) = sdk_failure
            && let Err(failure) = &result
        {
            state.invalidation.failure.get_or_insert((cause, failure.1));
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
        // Retire only items actually attempted, including genuine SDK failures.
        drop(items.next());
        if let Err(error) = dropped
            && error.kind() != std::io::ErrorKind::NotFound
        {
            first_sdk_failure.get_or_insert(NativeMountError::Io(error));
        }
    }
    Ok(())
}
