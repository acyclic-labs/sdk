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
    /// Changes the source did not record that this mount was told of.
    pub(super) forgotten: AtomicU64,
    pub(super) files: RwLock<DarwinOwned<HashMap<u64, FileHandle>>>,
    pub(super) directories:
        Mutex<DarwinOwned<HashMap<u64, Arc<DarwinOwned<Mutex<DirectoryHandle>>>>>>,
    pub(super) directory_checkpoints:
        Mutex<DarwinOwned<HashMap<MountPath, DarwinOwned<(CacheEpochs, i64)>>>>,
    pub(super) ledger: FlushLedger,
    pub(super) changes: ChangeLabels,
    /// Attributes every change this mount's callbacks make to the mount.
    pub(super) origin: ViewOrigin,
    pub(super) callbacks: CallbackGate,
    pub(super) around: AroundChanges,
    pub(super) callback_guard: Option<Arc<dyn crate::native_mount::NativeMountCallbackGuard>>,
    pub(super) source_original_work: Option<crate::NativeMaterializationWork>,
    pub(super) native_allocation: crate::NativeAllocation,
    pub(super) allocation_custody: Option<crate::demand::NativeMountAllocationCustody>,
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

/// Changes made around the mount, which the NFS client learns of only when its cached attributes expire.
pub(super) struct AroundChanges {
    /// Changes another origin reported, plus views forgotten.
    pub(super) made: AtomicU64,
    /// `made` as of the latest barrier that waited them out.
    pub(super) settled: AtomicU64,
    /// Fences that could not confirm every change was reported.
    pub(super) unconfirmed: AtomicU64,
    /// `unconfirmed` as of the latest barrier that verified or waited them out.
    pub(super) verified: AtomicU64,
}

impl ViewObserver for DarwinMountContext {
    fn allocation_custody(&self) -> Option<crate::demand::NativeMountAllocationCustody> {
        self.allocation_custody.clone()
    }

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

/// Uptime excluding sleep: the clock the NFS client stamps cached attributes with (`microuptime`).
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
