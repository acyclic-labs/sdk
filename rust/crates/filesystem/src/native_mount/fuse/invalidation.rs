use super::*;
use crate::native_mount::run_native_mount_handler_on_meter;
use crate::{NativeMaterializationWork as Work, WorkCounters};

#[cfg(test)]
#[cfg(loom)]
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
    pub(super) in_flight: bool,
    /// A notification failed; the view's terminal receipt owns its cause.
    pub(super) denied: bool,
}

impl InvalidationQueue {
    pub(super) fn is_pending(&self, target: Option<ViewStamp>) -> bool {
        self.processed < target || !self.deferred.is_empty() || self.in_flight
    }

    pub(super) fn take_deferred(&mut self) -> FuseOwned<std::vec::IntoIter<KernelCacheItem>> {
        self.in_flight = true;
        std::mem::take(&mut self.deferred).map(Vec::into_iter)
    }

    pub(super) fn finish(&mut self, through: Option<ViewStamp>) {
        self.processed = self.processed.max(through);
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
                if state.invalidation.denied {
                    return Err(NativeMountError::Lifecycle(
                        "FUSE invalidation previously failed",
                    ));
                }
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
    pub(super) fn wait_for_invalidation<'a>(
        self: &'a Arc<Self>,
        mut state: StateGuard<'a>,
        target: Option<ViewStamp>,
        work: &Work,
    ) -> Result<(), NativeMountError> {
        let mut cancellation = None;
        loop {
            work.charge(Items.delta(1))?;
            if self.stopping.load(Ordering::Acquire) {
                return Err(NativeMountError::Lifecycle("session is stopped"));
            }
            if state.invalidation.denied || self.handler_failed.load(Ordering::Acquire) {
                return Err(NativeMountError::Lifecycle(
                    "FUSE invalidation previously failed",
                ));
            }
            if !state.invalidation.is_pending(target) {
                return Ok(());
            }
            if cancellation.is_none() {
                // Register outside state: a cancelled token invokes its
                // notification synchronously. The callback takes state so
                // cancellation cannot race releasing state into the OS wait.
                drop(state);
                let core = Arc::clone(self);
                cancellation = Some(work.cancellation().on_cancel(Some(work), move || {
                    let _state = core.state_to_finish();
                    core.invalidation.notify_all();
                })?);
                state = self.state()?;
                continue;
            }
            state = state.wait(&self.invalidation)?;
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

/// Delivers each admitted notification once. Failure closes the view instead
/// of repeating an operation whose effect could not be certified.
pub(super) fn run_invalidator(core: &ProjectionCore, notifier: &impl KernelCacheNotifier) {
    loop {
        let mut state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if core.stopping.load(Ordering::Acquire)
                || core.handler_failed.load(Ordering::Acquire)
                || state.invalidation.denied
            {
                return;
            }
            if state.invalidation.is_pending(state.invalidation.notified) {
                break;
            }
            #[cfg(all(test, loom))]
            model_checking::before_invalidator_wait();
            state = core
                .invalidation
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        drop(state);
        invalidation_pass(core, notifier);
    }
}

/// One pass for a view of a shared session. The session's one invalidator
/// serializes delivery, and failed views never enqueue another pass.
pub(super) fn shared_invalidation_pass(core: &ProjectionCore, notifier: &impl KernelCacheNotifier) {
    let state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
    let ready = !core.stopping.load(Ordering::Acquire)
        && !core.handler_failed.load(Ordering::Acquire)
        && !state.invalidation.denied
        && state.invalidation.is_pending(state.invalidation.notified);
    drop(state);
    if ready {
        invalidation_pass(core, notifier);
    }
}

fn invalidation_pass(core: &ProjectionCore, notifier: &impl KernelCacheNotifier) {
    let mut through = None;
    let result = core.request(|work| {
        let mut items = core.state_to_finish().invalidation.take_deferred();
        loop {
            drop_kernel_caches(notifier, &mut items, work)?;
            if through.is_some() {
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
        Ok(())
    });
    {
        let mut state = core.state_to_finish();
        state.invalidation.denied = result.is_err();
        state
            .invalidation
            .finish(if result.is_ok() { through } else { None });
    }
    if let Err(failure) = result {
        core.retain_handler_failure(failure);
    }
}

fn drop_kernel_caches(
    notifier: &impl KernelCacheNotifier,
    items: &mut FuseOwned<std::vec::IntoIter<KernelCacheItem>>,
    work: &Work,
) -> Result<(), NativeMountError> {
    run_native_mount_handler_on_meter(work, |work| {
        while !items.as_slice().is_empty() {
            work.charge(WorkCounters {
                backend_write_operations: 1,
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let item = items.next().ok_or(libc::EPROTO)?;
            let result = match item {
                KernelCacheItem::Inode(inode) => notifier.inval_inode(inode),
                KernelCacheItem::Entry { parent, name } => notifier.inval_entry(parent, &name),
            };
            if let Err(error) = result
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(NativeMountError::Io(error));
            }
        }
        Ok(())
    })
}
