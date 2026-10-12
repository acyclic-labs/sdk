use super::*;

pub(super) struct DarwinMountContext {
    pub(super) source: Arc<dyn MountFilesystem>,
    pub(super) handler_failure: Arc<DarwinOwned<Mutex<DarwinCallbackState>>>,
    pub(super) handler_budget: crate::WorkBudget,
    pub(super) handler_cancellation: crate::CancellationToken,
    pub(super) startup_meter: Mutex<crate::native_mount::NativeMountMeterActivation>,
    pub(super) writable: bool,
    pub(super) mount_uid: u32,
    pub(super) mount_gid: u32,
    pub(super) next_handle: AtomicU64,
    pub(super) next_inode: AtomicU64,
    pub(super) inodes: Mutex<DarwinOwned<HashMap<FileId, u64>>>,
    pub(super) forgotten: AtomicU64,
    pub(super) files: RwLock<DarwinOwned<HashMap<u64, FileHandle>>>,
    pub(super) directories:
        Mutex<DarwinOwned<HashMap<u64, Arc<DarwinOwned<Mutex<DirectoryHandle>>>>>>,
    pub(super) ledger: FlushLedger,
    pub(super) changes: ChangeLabels,
    pub(super) origin: ViewOrigin,
    pub(super) callbacks: CallbackGate,
    pub(super) around: AroundChanges,
    pub(super) callback_guard: Option<Arc<dyn crate::native_mount::NativeMountCallbackGuard>>,
    pub(super) source_original_work: Option<crate::NativeMaterializationWork>,
    pub(super) native_allocation: crate::NativeAllocation,
}

#[derive(Default)]
pub(super) struct DarwinCallbackState {
    pub(super) completed_future_request_work: crate::WorkCounters,
    pub(super) first_fatal: Option<(Option<NativeMountError>, crate::WorkCounters)>,
}

impl DarwinCallbackState {
    pub(super) fn take_failure(&mut self) -> Option<(NativeMountError, crate::WorkCounters)> {
        let (cause, work) = self.first_fatal.as_mut()?;
        cause.take().map(|cause| (cause, *work))
    }
}

pub(super) struct AroundChanges {
    pub(super) made: AtomicU64,
    pub(super) settled: AtomicU64,
    pub(super) unconfirmed: AtomicU64,
    pub(super) verified: AtomicU64,
}

impl ViewObserver for DarwinMountContext {
    fn view_changed(&self, _position: ViewStamp, origin: ViewOrigin) {
        // The client learns of this mount's own changes from their replies.
        if origin != self.origin {
            self.around.made.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn view_unconfirmed(&self, _position: ViewStamp) {
        self.around.unconfirmed.fetch_add(1, Ordering::AcqRel);
    }
}

pub(super) fn uptime(
    work: &crate::NativeMaterializationWork,
) -> Result<Duration, NativeMountError> {
    work.charge(READ_WORK)?;
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, writable timespec; the clock always exists.
    if unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, &raw mut now) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(Duration::new(
        u64::try_from(now.tv_sec).unwrap_or(0),
        u32::try_from(now.tv_nsec).unwrap_or(0),
    ))
}
