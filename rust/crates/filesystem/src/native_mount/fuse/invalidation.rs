use super::*;
use crate::native_mount::run_native_mount_handler_on_meter;
use crate::{NativeMaterializationWork as Work, WorkCounters};

#[cfg(test)]
#[cfg(acyclic_fs_loom)]
pub(super) mod model_checking;

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
    /// The last pass failed, so the kernel may hold stale entries.
    pub(super) denied: bool,
    /// Failed passes so far, so each waiter learns of a failure after it began.
    pub(super) denials: u64,
    /// A waiter arrived after a failed pass, so the invalidator must try again
    /// even if its inputs are unchanged. Otherwise the waiter could wait for a
    /// pass that never comes.
    pub(super) retry: bool,
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

    pub(super) fn finish(&mut self, through: Option<ViewStamp>) {
        if self.pending_tail.as_slice().is_empty() {
            self.processed = self.processed.max(through);
        }
        self.in_flight = false;
    }
}

impl FuseSession {
    pub(in crate::native_mount) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        self.core
            .request(|work| {
                let _active = NativeMountWorkScope::new(&self.core, work)?;
                let mut components = path
                    .strip_prefix(b"/")
                    .unwrap_or(path)
                    .split(|byte| *byte == b'/');
                let name = components.next_back().ok_or(libc::EINVAL)?;
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
                let source = self.core.source.as_ref();
                let parent = fuse_path(components.clone().count(), components, work)?;
                let child = ChildName::new(source, fuse_child_path(&parent, name, work)?, work)?;
                let parent = match source.folded_path_with_meter(&parent, work)? {
                    Some((path, bytes)) => FuseOwned::adopt(path, bytes, work)?,
                    None => parent,
                };
                let mut state = self.core.state()?;
                work.charge(Items.delta(2))?;
                let file_inode = state.inode_by_path.get(child.key()).copied();
                let parent_inode = state.inode_by_path.get(&*parent).copied();
                let entry_name = parent_inode
                    .map(|_| fuse_copy_bytes(name, work))
                    .transpose()?;
                let count =
                    usize::from(file_inode.is_some()) + 2 * usize::from(parent_inode.is_some());
                fuse_reserve(&mut state.invalidation.deferred, count, work)?;
                if let Some(inode) = file_inode {
                    state
                        .invalidation
                        .deferred
                        .push(KernelCacheItem::Inode(inode));
                }
                if let Some(parent) = parent_inode {
                    state.invalidation.deferred.push(KernelCacheItem::Entry {
                        parent,
                        name: entry_name.ok_or(libc::EIO)?,
                    });
                    state
                        .invalidation
                        .deferred
                        .push(KernelCacheItem::Inode(parent));
                }
                let target = state.invalidation.notified;
                self.core.wait_for_invalidation(state, target, work)
            })
            .map_err(|failure| failure.0)
    }

    pub(in crate::native_mount) fn revalidate(&self, work: &Work) -> Result<(), NativeMountError> {
        work.admit(WorkCounters::UNCHARGED)?;
        let _active = NativeMountWorkScope::new(&self.core, work)?;
        self.core.source.fence_changes_with_meter(work)?;
        let state = self.core.state()?;
        let target = state.invalidation.notified;
        self.core.wait_for_invalidation(state, target, work)
    }
}

impl ProjectionCore {
    pub(super) fn wait_for_invalidation(
        &self,
        mut state: StateGuard<'_>,
        target: Option<ViewStamp>,
        work: &Work,
    ) -> Result<(), NativeMountError> {
        let denials = state.invalidation.denials;
        if state.invalidation.denied {
            state.invalidation.retry = true;
            self.invalidation.notify_all();
        }
        loop {
            work.charge(Items.delta(1))?;
            if self.stopping.load(Ordering::Acquire) {
                return Err(NativeMountError::Lifecycle("session is stopped"));
            }
            if self.handler_failed.load(Ordering::Acquire) {
                return Err(NativeMountError::Lifecycle(
                    "FUSE invalidation previously failed",
                ));
            }
            // The invalidator retries on the next change; this caller learns
            // that its changes may not be visible yet.
            if state.invalidation.denials != denials {
                return Err(NativeMountError::Lifecycle("FUSE invalidation failed"));
            }
            if !state.invalidation.is_pending(target) {
                return Ok(());
            }
            state = state.wait_timeout(&self.invalidation, Duration::from_millis(10))?;
        }
    }
}

impl ProjectionState {
    fn stale_kernel_items(
        &mut self,
        source: &dyn MountFilesystem,
        through: ViewStamp,
        work: &Work,
    ) -> Result<FuseOwned<Vec<KernelCacheItem>>, NativeMountError> {
        let mut items = fuse_vec(0, work)?;
        let mut bindings = fuse_vec(0, work)?;
        for (inode, entry) in &self.by_inode {
            work.charge(Items.delta(1))?;
            let file_id = entry.lookup.node.file_id;
            let anchor = entry.anchor();
            if let Some(held) = entry.kernel
                && !source.unchanged_since_with_meter(anchor, Some(file_id), held, work)?
            {
                fuse_reserve(&mut items, 1, work)?;
                items.push(KernelCacheItem::Inode(*inode));
            }
            for (name, held) in &entry.negative_children {
                work.charge(Items.delta(1))?;
                let path = fuse_child_path(anchor, name, work)?;
                if !source.unchanged_since_with_meter(&path, None, *held, work)? {
                    let name = fuse_copy_bytes(name, work)?;
                    fuse_reserve(&mut items, 1, work)?;
                    items.push(KernelCacheItem::Entry {
                        parent: *inode,
                        name,
                    });
                }
            }
            for (index, binding) in entry.bindings.iter().enumerate() {
                work.charge(Items.delta(1))?;
                if let Some(held) = binding.kernel
                    && !source.unchanged_since_with_meter(
                        binding.native_path(),
                        Some(file_id),
                        held,
                        work,
                    )?
                {
                    if let Some(parent) = fuse_parent(&binding.path, work)?
                        && let Some(parent_inode) = self.inode_by_path.get(&*parent).copied()
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
                    fuse_reserve(&mut bindings, 1, work)?;
                    bindings.push((*inode, index));
                }
            }
        }
        for item in items.iter() {
            match item {
                KernelCacheItem::Inode(inode) => {
                    self.by_inode.get_mut(inode).ok_or(libc::EIO)?.kernel = Some(through);
                }
                KernelCacheItem::Entry { parent, name } => {
                    if let Some(entry) = self.by_inode.get_mut(parent) {
                        entry.negative_children.remove(name.as_slice());
                    }
                }
            }
        }
        for (inode, index) in bindings.iter() {
            self.by_inode
                .get_mut(inode)
                .ok_or(libc::EIO)?
                .bindings
                .get_mut(*index)
                .ok_or(libc::EIO)?
                .kernel = None;
        }
        Ok(items)
    }
}

/// The kernel cache notifications the invalidator sends.
pub(super) trait KernelCacheNotifier {
    fn inval_inode(&self, inode: u64) -> std::io::Result<()>;
    fn inval_entry(&self, parent: u64, name: &[u8]) -> std::io::Result<()>;
}

impl KernelCacheNotifier for Notifier {
    fn inval_inode(&self, inode: u64) -> std::io::Result<()> {
        Notifier::inval_inode(self, INodeNo(inode), 0, 0)
    }

    fn inval_entry(&self, parent: u64, name: &[u8]) -> std::io::Result<()> {
        Notifier::inval_entry(self, INodeNo(parent), OsStr::from_bytes(name))
    }
}

pub(super) fn run_invalidator(core: &ProjectionCore, notifier: &impl KernelCacheNotifier) {
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
            if state.invalidation.is_pending(state.invalidation.notified)
                && (blocked != Some(key) || state.invalidation.retry)
            {
                state.invalidation.retry = false;
                break key;
            }
            #[cfg(all(test, acyclic_fs_loom))]
            model_checking::before_invalidator_wait();
            state = core
                .invalidation
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        };
        drop(state);
        let mut through = None;
        let mut sdk_failure = None;
        let result = core.request(|work| {
            let mut items = {
                let mut state = core.state_to_finish();
                let items = state.invalidation.take_deferred();
                wait_key.0 = state.invalidation.notified;
                wait_key.1 = state.invalidation.deferred.len();
                items
            };
            loop {
                let dropped = drop_kernel_caches(notifier, &mut items, work, &mut sdk_failure);
                core.state_to_finish()
                    .invalidation
                    .retain_pending_tail(items);
                dropped?;
                // A refused notification stays queued, so this pass settles
                // nothing past it and the next pass starts with it.
                if through.is_some() || sdk_failure.is_some() {
                    break;
                }
                let mut state = core.state_to_finish();
                let Some(target) = state
                    .invalidation
                    .notified
                    .filter(|target| state.invalidation.scanned < Some(*target))
                else {
                    through = state.invalidation.notified;
                    break;
                };
                items = state
                    .stale_kernel_items(core.source.as_ref(), target, work)?
                    .map(Vec::into_iter);
                state.invalidation.scanned = Some(target);
                through = Some(target);
            }
            sdk_failure.take().map_or(Ok(()), Err)
        });
        let denied = result.is_err();
        // A failed pass does not fail the mount. It is logged, nothing new is
        // cached until a pass succeeds, and the pass is retried once its
        // inputs change.
        if let Err((cause, _)) = &result {
            tracing::warn!(
                error = %sdk_failure.as_ref().unwrap_or(cause),
                "FUSE invalidation failed; caching stops until a later pass succeeds"
            );
        }
        {
            let mut state = core.state_to_finish();
            state.invalidation.denied = denied;
            if denied {
                state.invalidation.denials = state.invalidation.denials.wrapping_add(1);
            }
            state.invalidation.finish(through);
        }
        if denied {
            core.invalidation.notify_all();
        }
        blocked = if denied { Some(wait_key) } else { None };
    }
}

fn drop_kernel_caches(
    notifier: &impl KernelCacheNotifier,
    items: &mut FuseOwned<std::vec::IntoIter<KernelCacheItem>>,
    work: &Work,
    first_sdk_failure: &mut Option<NativeMountError>,
) -> Result<(), NativeMountError> {
    run_native_mount_handler_on_meter(work, |work| {
        while let Some(item) = items.as_slice().first() {
            work.charge(WorkCounters {
                backend_write_operations: 1,
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let dropped = match item {
                KernelCacheItem::Inode(inode) => notifier.inval_inode(*inode),
                KernelCacheItem::Entry { parent, name } => notifier.inval_entry(*parent, name),
            };
            // Retire only notifications the kernel accepted, or for which it
            // holds nothing. A refused one and all after it stay queued.
            if let Err(error) = dropped
                && error.kind() != std::io::ErrorKind::NotFound
            {
                first_sdk_failure.get_or_insert(NativeMountError::Io(error));
                break;
            }
            drop(items.next());
        }
        Ok(())
    })
}
