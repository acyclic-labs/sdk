use super::*;

/// One allocated data extent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostDataRange {
    pub offset: u64,
    pub length: u64,
}

/// Native file kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostFileType(u32);
impl HostFileType {
    pub fn is_dir(self) -> bool {
        self.0 == 0o040000
    }
    pub fn is_file(self) -> bool {
        self.0 == 0o100000
    }
    pub fn is_symlink(self) -> bool {
        self.0 == 0o120000
    }
    pub fn is_fifo(self) -> bool {
        self.0 == 0o010000
    }
    pub fn is_socket(self) -> bool {
        self.0 == 0o140000
    }
    pub fn is_char_device(self) -> bool {
        self.0 == 0o020000
    }
    pub fn is_block_device(self) -> bool {
        self.0 == 0o060000
    }
}

/// One object's native metadata, without following its final link.
#[cfg(unix)]
#[derive(Clone)]
pub struct HostStat {
    #[cfg(target_os = "linux")]
    raw: rustix::fs::Stat,
    #[cfg(target_os = "macos")]
    raw: libc::stat,
    len: u64,
    created: Option<cap_std::time::SystemTime>,
}
#[cfg(unix)]
impl std::fmt::Debug for HostStat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostStat")
            .field("identity", &self.identity())
            .field("length", &self.len())
            .finish_non_exhaustive()
    }
}
#[cfg(unix)]
impl HostStat {
    pub fn file_type(&self) -> HostFileType {
        HostFileType(self.mode() & 0o170000)
    }
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }
    pub fn len(&self) -> u64 {
        self.len
    }
    pub fn mode(&self) -> u32 {
        self.raw.st_mode as u32
    }
    pub fn dev(&self) -> u64 {
        self.raw.st_dev as u64
    }
    pub fn ino(&self) -> u64 {
        self.raw.st_ino as u64
    }
    pub fn nlink(&self) -> u64 {
        self.raw.st_nlink as u64
    }
    pub fn uid(&self) -> u32 {
        self.raw.st_uid as u32
    }
    pub fn gid(&self) -> u32 {
        self.raw.st_gid as u32
    }
    pub fn rdev(&self) -> u64 {
        self.raw.st_rdev as u64
    }
    pub fn atime(&self) -> i64 {
        self.raw.st_atime as i64
    }
    pub fn atime_nsec(&self) -> i64 {
        self.raw.st_atime_nsec as i64
    }
    pub fn mtime(&self) -> i64 {
        self.raw.st_mtime as i64
    }
    pub fn mtime_nsec(&self) -> i64 {
        self.raw.st_mtime_nsec as i64
    }
    pub fn ctime(&self) -> i64 {
        self.raw.st_ctime as i64
    }
    pub fn ctime_nsec(&self) -> i64 {
        self.raw.st_ctime_nsec as i64
    }

    pub fn created(&self) -> io::Result<cap_std::time::SystemTime> {
        self.created
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "birth time unavailable"))
    }
    pub fn modified(&self) -> io::Result<cap_std::time::SystemTime> {
        unix_time(self.mtime(), self.mtime_nsec())
    }
    pub fn accessed(&self) -> io::Result<cap_std::time::SystemTime> {
        unix_time(self.atime(), self.atime_nsec())
    }
    pub(super) fn from_stat(
        #[cfg(target_os = "linux")] raw: rustix::fs::Stat,
        #[cfg(target_os = "macos")] raw: libc::stat,
    ) -> io::Result<Self> {
        let len =
            u64::try_from(raw.st_size).map_err(|_| io::Error::other("negative file length"))?;
        #[cfg(target_os = "linux")]
        let created = None;
        #[cfg(target_os = "macos")]
        let created = unix_time(raw.st_birthtime, raw.st_birthtime_nsec).ok();
        Ok(Self { raw, len, created })
    }
    #[cfg(target_os = "linux")]
    pub(super) fn from_statx(stat: rustix::fs::Statx) -> io::Result<Self> {
        // SAFETY: the native stat record contains only integers and integer arrays.
        let mut raw: rustix::fs::Stat = unsafe { std::mem::zeroed() };
        raw.st_mode = stat.stx_mode as _;
        raw.st_size = stat.stx_size as _;
        raw.st_dev = rustix::fs::makedev(stat.stx_dev_major, stat.stx_dev_minor) as _;
        raw.st_ino = stat.stx_ino as _;
        raw.st_nlink = stat.stx_nlink as _;
        raw.st_uid = stat.stx_uid as _;
        raw.st_gid = stat.stx_gid as _;
        raw.st_rdev = rustix::fs::makedev(stat.stx_rdev_major, stat.stx_rdev_minor) as _;
        raw.st_atime = stat.stx_atime.tv_sec as _;
        raw.st_atime_nsec = stat.stx_atime.tv_nsec as _;
        raw.st_mtime = stat.stx_mtime.tv_sec as _;
        raw.st_mtime_nsec = stat.stx_mtime.tv_nsec as _;
        raw.st_ctime = stat.stx_ctime.tv_sec as _;
        raw.st_ctime_nsec = stat.stx_ctime.tv_nsec as _;

        let created = if stat.stx_mask & rustix::fs::StatxFlags::BTIME.bits() != 0 {
            unix_time(stat.stx_btime.tv_sec, stat.stx_btime.tv_nsec as i64).ok()
        } else {
            None
        };
        Ok(Self {
            raw,
            len: stat.stx_size,
            created,
        })
    }
}
#[cfg(unix)]
pub(super) fn unix_time(seconds: i64, nanos: i64) -> io::Result<cap_std::time::SystemTime> {
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    let magnitude = nanos.unsigned_abs();
    let duration = std::time::Duration::new(
        u64::try_from(magnitude / 1_000_000_000)
            .map_err(|_| io::Error::other("timestamp overflow"))?,
        (magnitude % 1_000_000_000) as u32,
    );
    let value = if nanos < 0 {
        std::time::UNIX_EPOCH.checked_sub(duration)
    } else {
        std::time::UNIX_EPOCH.checked_add(duration)
    };
    value
        .map(cap_std::time::SystemTime::from_std)
        .ok_or_else(|| io::Error::other("timestamp overflow"))
}

/// One object's Windows file-record metadata.
#[cfg(windows)]
#[derive(Clone, Copy, Debug)]
pub struct HostStat {
    raw: windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION,
    volume_serial_number: u32,
}
#[cfg(windows)]
impl HostStat {
    pub(super) fn from_stat_information(
        raw: &windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION,
        volume_serial_number: u32,
    ) -> io::Result<Self> {
        for field in [
            raw.EndOfFile,
            raw.CreationTime,
            raw.LastAccessTime,
            raw.LastWriteTime,
        ] {
            u64::try_from(field).map_err(|_| io::Error::other("negative stat field"))?;
        }
        Ok(Self {
            raw: *raw,
            volume_serial_number,
        })
    }
    pub fn file_type(&self) -> HostFileType {
        HostFileType(
            if matches!(self.raw.ReparseTag, 0xA000_0003 | 0xA000_000C) {
                0o120000
            } else if self.raw.FileAttributes
                & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY.0
                != 0
            {
                0o040000
            } else {
                0o100000
            },
        )
    }
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }
    pub const fn len(&self) -> u64 {
        self.raw.EndOfFile as u64
    }
    pub const fn file_attributes(&self) -> u32 {
        self.raw.FileAttributes
    }
    pub const fn creation_time(&self) -> u64 {
        self.raw.CreationTime as u64
    }
    pub const fn last_write_time(&self) -> u64 {
        self.raw.LastWriteTime as u64
    }
    pub const fn volume_serial_number(&self) -> u32 {
        self.volume_serial_number
    }
    pub const fn file_index(&self) -> u64 {
        self.raw.FileId as u64
    }
    pub const fn nlink(&self) -> u64 {
        self.raw.NumberOfLinks as u64
    }
    pub fn created(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.creation_time()))
    }
    pub fn modified(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.last_write_time()))
    }
    pub fn accessed(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.raw.LastAccessTime as u64))
    }
}

/// FILETIME ticks at the Unix epoch.
#[cfg(windows)]
pub(crate) const UNIX_EPOCH_FILETIME: i64 = 116_444_736_000_000_000;

#[cfg(all(windows, any(feature = "native-mount", test)))]
pub(crate) fn filetime_to_unix_nanoseconds(ticks: i64) -> Option<i64> {
    ticks.checked_sub(UNIX_EPOCH_FILETIME)?.checked_mul(100)
}

#[cfg(all(windows, any(feature = "native-mount", test)))]
pub(crate) fn unix_nanoseconds_to_filetime(nanoseconds: i64) -> i64 {
    nanoseconds.div_euclid(100) + UNIX_EPOCH_FILETIME
}

#[cfg(windows)]
pub(super) fn windows_time(ticks: u64) -> cap_std::time::SystemTime {
    const UNIX_EPOCH_TICKS: u64 = UNIX_EPOCH_FILETIME.unsigned_abs();
    let since = |ticks: u64| std::time::Duration::from_nanos(ticks.saturating_mul(100));
    cap_std::time::SystemTime::from_std(if ticks >= UNIX_EPOCH_TICKS {
        std::time::UNIX_EPOCH + since(ticks - UNIX_EPOCH_TICKS)
    } else {
        std::time::UNIX_EPOCH - since(UNIX_EPOCH_TICKS - ticks)
    })
}

/// One enumerated native name and its optional facts.
pub struct HostListedEntry {
    #[cfg(unix)]
    pub name: OsString,
    #[cfg(windows)]
    pub name: Vec<u16>,
    pub stat: Option<HostStat>,
}
impl HostListedEntry {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        #[cfg(unix)]
        {
            self.name.as_encoded_bytes().len() as u64
        }
        #[cfg(windows)]
        {
            self.name.len() as u64 * 2
        }
    }
}
pub(crate) fn stat_file<W: NativeHostWork>(
    file: &File,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    #[cfg(unix)]
    {
        stat_unix_fd_at(file.as_raw_fd(), OsStr::new(""), true, work)
    }
    #[cfg(windows)]
    {
        let (identity, _) = file_identity_and_length(file, work)?;
        let volume = u32::try_from(identity.device)
            .map_err(|_| io::Error::other("native volume serial exceeds u32"))?;
        stat_windows_file(file, volume, work)
    }
}
impl HostStat {
    pub(crate) fn identity(&self) -> NativeRootIdentity {
        #[cfg(unix)]
        {
            NativeRootIdentity {
                device: self.dev(),
                object: self.ino(),
            }
        }
        #[cfg(windows)]
        {
            NativeRootIdentity {
                device: self.volume_serial_number() as u64,
                object: self.file_index(),
            }
        }
    }
}
