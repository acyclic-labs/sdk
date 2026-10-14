use super::*;

/// One allocated data extent.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostDataRange {
    pub offset: u64,
    pub length: u64,
}

/// No-follow native metadata with the host's identity bits preserved.
#[derive(Clone)]
#[cfg_attr(windows, derive(Copy, Debug))]
pub struct HostStat {
    /// Native Unix fields.
    #[cfg(unix)]
    pub raw: libc::stat,
    /// Native Windows file-record fields.
    #[cfg(windows)]
    pub raw: nt::FILE_STAT_INFORMATION,
    /// Volume containing the file record.
    #[cfg(windows)]
    pub volume_serial_number: u32,
    #[cfg(unix)]
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
impl HostStat {
    pub fn file_type(&self) -> crate::demand::SourceNodeKind {
        use crate::demand::SourceNodeKind as Kind;
        #[cfg(unix)]
        {
            match self.raw.st_mode as u32 & 0o170000 {
                0o040000 => Kind::Directory,
                0o100000 => Kind::RegularFile,
                0o120000 => Kind::SymbolicLink,
                0o010000 => Kind::Fifo,
                0o140000 => Kind::Socket,
                0o020000 => Kind::CharacterDevice,
                0o060000 => Kind::BlockDevice,
                _ => Kind::Unsupported,
            }
        }
        #[cfg(windows)]
        {
            if matches!(self.raw.ReparseTag, 0xA000_0003 | 0xA000_000C) {
                Kind::SymbolicLink
            } else if self.raw.FileAttributes & fs::FILE_ATTRIBUTE_DIRECTORY.0 != 0 {
                Kind::Directory
            } else {
                Kind::RegularFile
            }
        }
    }
    pub fn is_dir(&self) -> bool {
        self.file_type() == crate::demand::SourceNodeKind::Directory
    }
    /// Whether the host object has zero bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u64 {
        #[cfg(unix)]
        {
            self.raw.st_size as u64
        }
        #[cfg(windows)]
        {
            self.raw.EndOfFile.cast_unsigned()
        }
    }
    pub fn nlink(&self) -> u64 {
        #[cfg(unix)]
        {
            self.raw.st_nlink as u64
        }
        #[cfg(windows)]
        {
            u64::from(self.raw.NumberOfLinks)
        }
    }
    pub(crate) fn identity(&self) -> NativeRootIdentity {
        #[cfg(unix)]
        {
            NativeRootIdentity {
                device: self.raw.st_dev as u64,
                object: self.raw.st_ino as u64,
            }
        }
        #[cfg(windows)]
        {
            NativeRootIdentity {
                device: u64::from(self.volume_serial_number),
                object: self.raw.FileId.cast_unsigned(),
            }
        }
    }
    pub fn created(&self) -> io::Result<cap_std::time::SystemTime> {
        #[cfg(unix)]
        {
            self.created
                .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "birth time unavailable"))
        }
        #[cfg(windows)]
        {
            Ok(windows_time(self.raw.CreationTime.cast_unsigned()))
        }
    }
    pub fn modified(&self) -> io::Result<cap_std::time::SystemTime> {
        #[cfg(unix)]
        {
            unix_time(self.raw.st_mtime as i64, self.raw.st_mtime_nsec as i64)
        }
        #[cfg(windows)]
        {
            Ok(windows_time(self.raw.LastWriteTime.cast_unsigned()))
        }
    }
    pub fn accessed(&self) -> io::Result<cap_std::time::SystemTime> {
        #[cfg(unix)]
        {
            unix_time(self.raw.st_atime as i64, self.raw.st_atime_nsec as i64)
        }
        #[cfg(windows)]
        {
            Ok(windows_time(self.raw.LastAccessTime.cast_unsigned()))
        }
    }
    #[cfg(unix)]
    pub(super) fn from_stat(raw: libc::stat) -> io::Result<Self> {
        u64::try_from(raw.st_size).map_err(|_| io::Error::other("negative file length"))?;
        #[cfg(target_os = "linux")]
        let created = None;
        #[cfg(target_os = "macos")]
        let created = unix_time(raw.st_birthtime, raw.st_birthtime_nsec).ok();
        Ok(Self { raw, created })
    }
    #[cfg(target_os = "linux")]
    pub(super) fn from_statx(stat: rustix::fs::Statx) -> Self {
        // SAFETY: native stat consists only of integers and integer arrays.
        let mut raw: libc::stat = unsafe { std::mem::zeroed() };
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
        Self { raw, created }
    }
    #[cfg(windows)]
    pub(super) fn from_stat_information(
        raw: &nt::FILE_STAT_INFORMATION,
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
}
#[cfg(unix)]
pub(super) fn unix_time(seconds: i64, nanos: i64) -> io::Result<cap_std::time::SystemTime> {
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    let magnitude = nanos.unsigned_abs();
    let duration = std::time::Duration::new(
        (magnitude / 1_000_000_000) as u64,
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
    let since = |ticks: u64| {
        std::time::Duration::from_secs(ticks / 10_000_000)
            + std::time::Duration::from_nanos((ticks % 10_000_000) * 100)
    };
    cap_std::time::SystemTime::from_std(if ticks >= UNIX_EPOCH_TICKS {
        std::time::UNIX_EPOCH + since(ticks - UNIX_EPOCH_TICKS)
    } else {
        std::time::UNIX_EPOCH - since(UNIX_EPOCH_TICKS - ticks)
    })
}

/// One enumerated native name and its optional facts.
pub struct HostListedEntry {
    /// Original directory-entry name.
    #[cfg(unix)]
    pub name: OsString,
    /// Original UTF-16 directory-entry name.
    #[cfg(windows)]
    pub name: Vec<u16>,
    /// Facts observed without following a link.
    pub stat: Option<HostStat>,
}
impl HostListedEntry {
    pub(crate) fn is_named(&self, name: &str) -> bool {
        #[cfg(unix)]
        {
            self.name == OsStr::new(name)
        }
        #[cfg(windows)]
        {
            self.name.iter().copied().eq(name.encode_utf16())
        }
    }
}
pub(crate) fn stat_file<W: NativeHostWork>(
    file: &File,
    work: &mut W,
) -> Result<HostStat, W::Error> {
    #[cfg(unix)]
    {
        stat_at(file.as_raw_fd(), c"", work)
    }
    #[cfg(windows)]
    {
        let (identity, _) = file_identity_and_length(file, work)?;
        let volume = u32::try_from(identity.device).map_err(|_| W::invalid_request())?;
        stat_windows_file(file, volume, work)
    }
}
