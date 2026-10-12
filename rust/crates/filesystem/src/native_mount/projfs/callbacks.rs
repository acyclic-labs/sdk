use super::*;

pub(super) struct Runtime {
    pub(super) source: Arc<dyn MountFilesystem>,
    pub(super) work: Mutex<crate::NativeMaterializationWork>,
    pub(super) active_work: Mutex<super::super::NativeMountMeterActivation>,
    pub(super) original_work: Mutex<Option<crate::NativeMaterializationWork>>,
    pub(super) callback_guard: Option<Arc<dyn super::super::NativeMountCallbackGuard>>,
    pub(super) root: PathBuf,
    pub(super) metadata_root: Arc<HostRoot>,
    pub(super) staging: Mutex<Option<(cap_std::fs::Dir, crate::NativeWorkReservation)>>,
    pub(super) writable: bool,
    pub(super) metadata_baselines: Arc<Mutex<HashMap<u128, OpenMetadataState>>>,
    pub(super) projection: Arc<Mutex<ProjectionCache>>,
    pub(super) projector: Arc<Projector>,
    pub(super) metadata_probes: Arc<Mutex<HashMap<MountPath, usize>>>,
    pub(super) metadata_edits: Arc<FileLocks>,
    pub(super) post_operation_failure: Arc<Mutex<PostOperationFailures>>,
    pub(super) callbacks: CallbackGate,
    pub(super) _allocation: crate::NativeAllocation,
}

pub(super) struct CallbackGate {
    pub(super) shards: Box<[Mutex<()>]>,
    pub(super) active: Mutex<(usize, bool)>,
    pub(super) idle: Condvar,
}

pub(super) struct ActiveCallback<'a>(pub(super) &'a CallbackGate);

impl Drop for ActiveCallback<'_> {
    fn drop(&mut self) {
        let mut active = lock_recover(&self.0.active);
        active.0 = active.0.saturating_sub(1);
        if active.0 == 0 {
            self.0.idle.notify_all();
        }
    }
}

#[derive(Default)]
pub(super) struct PostOperationFailures {
    pub(super) pending_captures: HashMap<MountPath, PendingCapture>,
    pub(super) next_capture_serial: u64,
    pub(super) unreplayable: Option<String>,
    pub(super) callback_failure: Option<(NativeMountError, crate::WorkCounters)>,
}

#[derive(Clone)]
pub(super) struct PendingCapture {
    pub(super) serial: u64,
    pub(super) error: String,
    pub(super) subtree: bool,
}

impl PostOperationFailures {
    pub(super) fn queue_capture(&mut self, path: MountPath, error: String) {
        self.next_capture_serial = self.next_capture_serial.wrapping_add(1);
        let subtree = self
            .pending_captures
            .get(&path)
            .is_some_and(|capture| capture.subtree);
        self.pending_captures.insert(
            path,
            PendingCapture {
                serial: self.next_capture_serial,
                error,
                subtree,
            },
        );
    }

    pub(super) fn queue_subtree(&mut self, path: &MountPath, error: String) {
        self.queue_capture(path.clone(), error);
        if let Some(capture) = self.pending_captures.get_mut(path) {
            capture.subtree = true;
        }
    }

    pub(super) fn rename_pending_captures(&mut self, source: &MountPath, destination: &MountPath) {
        let moved = self
            .pending_captures
            .iter()
            .filter_map(|(path, capture)| {
                projfs_path_suffix(path, source).map(|suffix| {
                    let destination = suffix.iter().fold(destination.clone(), |path, component| {
                        path.child(component.clone())
                    });
                    (path.clone(), destination, capture.clone())
                })
            })
            .collect::<Vec<_>>();
        for (source, destination, capture) in moved {
            self.pending_captures.remove(&source);
            if let Some(existing) = self.pending_captures.get_mut(&destination) {
                existing.subtree |= capture.subtree;
                existing.serial = existing.serial.max(capture.serial);
            } else {
                self.pending_captures.insert(destination, capture);
            }
        }
    }
}

pub(super) fn projfs_path_suffix<'a>(
    path: &'a MountPath,
    prefix: &MountPath,
) -> Option<&'a [Vec<u8>]> {
    let suffix = path.components().get(prefix.components().len()..)?;
    path.components()
        .iter()
        .zip(prefix.components())
        .all(|(left, right)| same_projfs_name(left, right))
        .then_some(suffix)
}

pub(super) fn same_projfs_name(left: &[u8], right: &[u8]) -> bool {
    if left == right {
        return true;
    }
    matches!(compare_projfs_name(left, right), Some(0))
}

pub(super) fn compare_projfs_name(left: &[u8], right: &[u8]) -> Option<i32> {
    let (Some(mut left), Some(mut right)) = (decode_utf16_name(left), decode_utf16_name(right))
    else {
        return None;
    };
    left.push(0);
    right.push(0);
    // SAFETY: both owned buffers are live, NUL-terminated UTF-16 names.
    Some(unsafe {
        PrjFileNameCompare(
            PCWSTR::from_raw(left.as_ptr()),
            PCWSTR::from_raw(right.as_ptr()),
        )
    })
}

impl CallbackGate {
    pub(super) fn enter(&self) -> Option<ActiveCallback<'_>> {
        let mut active = lock_recover(&self.active);
        if !active.1 {
            return None;
        }
        active.0 = active.0.checked_add(1)?;
        Some(ActiveCallback(self))
    }
    pub(super) fn start() -> Result<Self, NativeMountError> {
        const SHARDS: usize = 64;
        let mut shards = Vec::new();
        shards
            .try_reserve_exact(SHARDS)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        for _ in 0..SHARDS {
            shards.push(Mutex::new(()));
        }
        Ok(Self {
            shards: shards.into_boxed_slice(),
            active: Mutex::new((0, true)),
            idle: Condvar::new(),
        })
    }

    pub(super) fn drain(&self) {
        let mut active = lock_recover(&self.active);
        while active.0 != 0 {
            active = self
                .idle
                .wait(active)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(super) fn call_observed<T>(
        &self,
        key: u128,
        operation: impl FnOnce() -> T,
        observe: impl FnOnce(&T),
    ) -> T {
        let shard = (key % self.shards.len() as u128) as usize;
        // The SDK entry already owns admission through operation and observation.
        let _serial = lock_recover(&self.shards[shard]);
        let result = operation();
        observe(&result);
        result
    }

    pub(super) fn close_admission(&self) {
        lock_recover(&self.active).1 = false;
    }
}

pub(super) fn flush_callback_gate(
    callbacks: &CallbackGate,
    failure: Arc<Mutex<PostOperationFailures>>,
    capture: impl Fn(&[(MountPath, bool)]) -> Result<(), MountSourceError>,
) -> Result<(), NativeMountError> {
    callbacks.drain();
    let mut pending = lock_recover(failure.as_ref())
        .pending_captures
        .iter()
        .map(|(path, capture)| (path.clone(), capture.serial, capture.subtree))
        .collect::<Vec<_>>();
    pending.sort_by(|(left, ..), (right, ..)| {
        left.components()
            .len()
            .cmp(&right.components().len())
            .then_with(|| left.components().cmp(right.components()))
    });
    let paths = pending
        .iter()
        .map(|(path, _, subtree)| (path.clone(), *subtree))
        .collect::<Vec<_>>();
    let result = capture(&paths);
    {
        let mut failure = lock_recover(failure.as_ref());
        for (path, serial, _) in pending {
            if failure
                .pending_captures
                .get(&path)
                .map(|capture| capture.serial)
                != Some(serial)
            {
                continue;
            }
            match &result {
                Ok(()) => {
                    failure.pending_captures.remove(&path);
                }
                Err(error) => {
                    if let Some(capture) = failure.pending_captures.get_mut(&path) {
                        capture.error = error.to_string();
                    }
                }
            }
        }
    }
    callbacks.drain();
    let failure = lock_recover(failure.as_ref());
    let failure = failure.unreplayable.clone().or_else(|| {
        failure
            .pending_captures
            .iter()
            .next()
            .map(|(path, capture)| format!("{path:?}: {}", capture.error))
    });
    if let Some(failure) = failure {
        Err(NativeMountError::Driver(format!(
            "ProjFS post-operation capture failed; mount cannot publish: {failure}"
        )))
    } else {
        Ok(())
    }
}

pub(super) fn defer_host_capture(failure: &Mutex<PostOperationFailures>, path: MountPath) {
    lock_recover(failure).queue_capture(path, "host capture pending".to_owned());
}
