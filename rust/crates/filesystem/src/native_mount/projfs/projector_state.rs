use super::*;

pub(super) struct Materialized {
    pub(super) basis: Option<ReadBasis>,
    pub(super) names: HashMap<Vec<u8>, bool>,
}

pub(super) const MATERIALIZE_WORKERS: usize = 8;

pub(super) struct SharedProjection<'a>(pub(super) Projection<'a>);

// SAFETY: the source is `Sync`, the root a shared path, and the context an
unsafe impl Sync for SharedProjection<'_> {}

pub(super) struct MaterializeQueue {
    pub(super) state: Mutex<MaterializeProgress>,
    pub(super) changed: Condvar,
}

pub(super) struct MaterializeProgress {
    pub(super) pending: Vec<MountPath>,
    pub(super) writing: usize,
    pub(super) failure: Option<String>,
}

impl MaterializeQueue {
    pub(super) fn new(pending: Vec<MountPath>) -> Self {
        Self {
            state: Mutex::new(MaterializeProgress {
                pending,
                writing: 0,
                failure: None,
            }),
            changed: Condvar::new(),
        }
    }

    pub(super) fn next(&self) -> Option<MountPath> {
        let mut state = lock_recover(&self.state);
        loop {
            if state.failure.is_some() {
                return None;
            }
            if let Some(directory) = state.pending.pop() {
                state.writing += 1;
                return Some(directory);
            }
            if state.writing == 0 {
                return None;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(super) fn done(&self, written: Result<Vec<MountPath>, String>) {
        let mut state = lock_recover(&self.state);
        state.writing -= 1;
        match written {
            Ok(found) => state.pending.extend(found),
            Err(error) => {
                state.failure.get_or_insert(error);
            }
        }
        self.changed.notify_all();
    }

    pub(super) fn finish(self) -> Result<(), String> {
        self.state
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .map_or(Ok(()), Err)
    }
}

#[derive(Clone, Copy)]
pub(super) struct Projection<'a> {
    pub(super) runtime: &'a Runtime,
    pub(super) work: &'a crate::NativeMaterializationWork,
    pub(super) context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    pub(super) starting: bool,
}

pub(super) struct Projector {
    pub(super) state: Mutex<ProjectorState>,
    pub(super) tree: Mutex<()>,
    pub(super) paths: KeyedLocks<MountPath>,
    pub(super) changed: Condvar,
    pub(super) worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(super) lifecycle: Mutex<ProjectorLifecycle>,
    pub(super) lifecycle_changed: Condvar,
}

#[derive(Default)]
pub(super) struct ProjectorLifecycle {
    pub(super) context: Option<SendContext>,
    pub(super) owner: Option<ProjFsSession>,
    pub(super) custody: (bool, bool),
    pub(super) failure: Option<NativeMountError>,
    pub(super) requested: bool,
    pub(super) complete: bool,
    pub(super) abandoned: bool,
}

pub(super) const HELD_RETRY: std::time::Duration = std::time::Duration::from_millis(20);
pub(super) const HELD_SETTLE_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Default)]
pub(super) struct ProjectorState {
    pub(super) written: HashMap<MountPath, WrittenEntry>,
    pub(super) pending: HashMap<MountPath, WrittenEntry>,
    pub(super) directories: HashMap<MountPath, Materialized>,
    pub(super) stale: HashSet<MountPath>,
    pub(super) relist: HashSet<MountPath>,
    pub(super) vacated: HashSet<MountPath>,
    pub(super) relisting: bool,
    pub(super) retry: bool,
    pub(super) notified: Option<ViewStamp>,
    pub(super) processed: Option<ViewStamp>,
    pub(super) failure: Option<String>,
    pub(super) stopping: bool,
}

impl ProjectorState {
    pub(super) fn record_attempt(
        &mut self,
        path: &MountPath,
        attempted: &WrittenEntry,
        current: Option<WrittenEntry>,
    ) {
        if self.pending.get(path) == Some(attempted) {
            self.pending.remove(path);
            if self.relist.remove(path)
                && let Some(parent) = path.parent()
            {
                self.stale.insert(parent);
            }
        }
        if let Some(current) = current {
            let entry = self.written.entry(path.clone()).or_insert(current);
            if entry.basis.map(|basis| basis.stamp) < current.basis.map(|basis| basis.stamp) {
                *entry = current;
            }
        }
    }
}

impl ViewObserver for Projector {
    fn view_changed(&self, position: ViewStamp, _origin: ViewOrigin) {
        let mut state = lock_recover(&self.state);
        state.notified = state.notified.max(Some(position));
        self.changed.notify_all();
    }
}

pub(super) enum Replaced {
    Rewritten(Box<WrittenEntry>),
    Released,
    Busy,
}

pub(super) fn rebased(path: &MountPath, from: &MountPath, to: &MountPath) -> Option<MountPath> {
    projfs_path_suffix(path, from).map(|suffix| {
        suffix
            .iter()
            .fold(to.clone(), |path, component| path.child(component.clone()))
    })
}
