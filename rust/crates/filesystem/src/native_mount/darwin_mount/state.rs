use super::*;

pub(super) struct DarwinMountContext {
    pub(super) source: Arc<dyn MountFilesystem>,
    pub(super) writable: bool,
    pub(super) mount_uid: u32,
    pub(super) mount_gid: u32,
    pub(super) next_handle: AtomicU64,
    pub(super) next_inode: AtomicU64,
    pub(super) inodes: Mutex<HashMap<FileId, u64>>,
    /// Changes the source did not record that this mount was told of.
    pub(super) forgotten: AtomicU64,
    pub(super) lookups: Mutex<LookupCache>,
    pub(super) files: RwLock<HashMap<u64, FileHandle>>,
    pub(super) directories: Mutex<HashMap<u64, Arc<Mutex<DirectoryHandle>>>>,
    pub(super) directory_checkpoints: Mutex<HashMap<MountPath, DirectoryCheckpoint>>,
    pub(super) namespace_revision: AtomicU64,
    pub(super) ledger: FlushLedger,
    pub(super) changes: ChangeLabels,
    /// Attributes every change this mount's callbacks make to the mount.
    pub(super) origin: ViewOrigin,
    pub(super) callbacks: CallbackGate,
    pub(super) around: AroundChanges,
}


/// Changes made around the mount, which the NFS client learns of only when
/// its cached attributes expire.
pub(super) struct AroundChanges {
    /// Changes another origin reported, plus views forgotten.
    pub(super) made: AtomicU64,
    /// `made` as of the latest barrier that waited them out.
    pub(super) settled: AtomicU64,
    /// Fences that could not confirm every change was reported.
    pub(super) unconfirmed: AtomicU64,
    /// `unconfirmed` as of the latest barrier that verified or waited them
    /// out.
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

/// Uptime excluding sleep: the clock the NFS client stamps cached
/// attributes with (`microuptime`).
pub(super) fn uptime() -> Duration {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, writable timespec; the clock always exists.
    unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, &raw mut now) };
    Duration::new(
        u64::try_from(now.tv_sec).unwrap_or(0),
        u32::try_from(now.tv_nsec).unwrap_or(0),
    )
}

/// Lookups, each with the stamp it was resolved after.
pub(super) struct LookupCache {
    pub(super) entries: HashMap<MountPath, (Option<MountLookup>, CacheStamp)>,
}

