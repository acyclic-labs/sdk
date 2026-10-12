use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostDataRange {
    pub offset: u64,
    pub length: u64,
}

/// One object's kind, size, times, and identity, read without following its name's final link.
#[cfg(unix)]
#[derive(Clone, Debug)]
pub struct HostStat {
    pub(super) mode: u32,
    pub(super) len: u64,
    pub(super) dev: u64,
    pub(super) ino: u64,
    pub(super) nlink: u64,
    pub(super) uid: u32,
    pub(super) gid: u32,
    pub(super) rdev: u64,
    pub(super) atime: i64,
    pub(super) atime_nsec: i64,
    pub(super) mtime: i64,
    pub(super) mtime_nsec: i64,
    pub(super) ctime: i64,
    pub(super) ctime_nsec: i64,
    pub(super) created: Option<cap_std::time::SystemTime>,
}

/// One native object's file kind.
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

#[cfg(unix)]
pub(super) fn unix_time(seconds: i64, nanos: i64) -> io::Result<cap_std::time::SystemTime> {
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    let magnitude = nanos.unsigned_abs();
    let duration = std::time::Duration::new(
        u64::try_from(magnitude / 1_000_000_000)
            .map_err(|_| io::Error::other("host time overflow"))?,
        u32::try_from(magnitude % 1_000_000_000)
            .map_err(|_| io::Error::other("host time overflow"))?,
    );
    let time = if nanos < 0 {
        std::time::UNIX_EPOCH.checked_sub(duration)
    } else {
        std::time::UNIX_EPOCH.checked_add(duration)
    };
    time.map(cap_std::time::SystemTime::from_std)
        .ok_or_else(|| io::Error::other("host time overflow"))
}

#[cfg(unix)]
impl HostStat {
    pub fn file_type(&self) -> HostFileType {
        HostFileType(self.mode & libc::S_IFMT as u32)
    }
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }
    pub fn len(&self) -> u64 {
        self.len
    }
    pub fn mode(&self) -> u32 {
        self.mode
    }
    pub fn dev(&self) -> u64 {
        self.dev
    }
    pub fn ino(&self) -> u64 {
        self.ino
    }
    pub fn nlink(&self) -> u64 {
        self.nlink
    }
    pub fn uid(&self) -> u32 {
        self.uid
    }
    pub fn gid(&self) -> u32 {
        self.gid
    }
    pub fn rdev(&self) -> u64 {
        self.rdev
    }
    pub fn atime(&self) -> i64 {
        self.atime
    }
    pub fn atime_nsec(&self) -> i64 {
        self.atime_nsec
    }
    pub fn mtime(&self) -> i64 {
        self.mtime
    }
    pub fn mtime_nsec(&self) -> i64 {
        self.mtime_nsec
    }
    pub fn ctime(&self) -> i64 {
        self.ctime
    }
    pub fn ctime_nsec(&self) -> i64 {
        self.ctime_nsec
    }
    pub fn created(&self) -> io::Result<cap_std::time::SystemTime> {
        self.created
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "birth time unavailable"))
    }
    pub fn modified(&self) -> io::Result<cap_std::time::SystemTime> {
        unix_time(self.mtime, self.mtime_nsec)
    }
    pub fn accessed(&self) -> io::Result<cap_std::time::SystemTime> {
        unix_time(self.atime, self.atime_nsec)
    }
    #[cfg(target_os = "linux")]
    pub(super) fn from_stat(stat: rustix::fs::Stat) -> io::Result<Self> {
        Ok(Self {
            mode: stat.st_mode as u32,
            len: u64::try_from(stat.st_size)
                .map_err(|_| io::Error::other("negative file length"))?,
            dev: stat.st_dev as u64,
            ino: stat.st_ino as u64,
            nlink: stat.st_nlink as u64,
            uid: stat.st_uid,
            gid: stat.st_gid,
            rdev: stat.st_rdev as u64,
            atime: stat.st_atime as i64,
            atime_nsec: stat.st_atime_nsec as i64,
            mtime: stat.st_mtime as i64,
            mtime_nsec: stat.st_mtime_nsec as i64,
            ctime: stat.st_ctime as i64,
            ctime_nsec: stat.st_ctime_nsec as i64,
            created: None,
        })
    }
    #[cfg(target_os = "macos")]
    pub(super) fn from_stat(stat: libc::stat) -> io::Result<Self> {
        Ok(Self {
            mode: u32::from(stat.st_mode),
            len: u64::try_from(stat.st_size)
                .map_err(|_| io::Error::other("negative file length"))?,
            dev: stat.st_dev as u64,
            ino: stat.st_ino,
            nlink: u64::from(stat.st_nlink),
            uid: stat.st_uid,
            gid: stat.st_gid,
            rdev: stat.st_rdev as u64,
            atime: stat.st_atime,
            atime_nsec: stat.st_atime_nsec,
            mtime: stat.st_mtime,
            mtime_nsec: stat.st_mtime_nsec,
            ctime: stat.st_ctime,
            ctime_nsec: stat.st_ctime_nsec,
            created: unix_time(stat.st_birthtime, stat.st_birthtime_nsec).ok(),
        })
    }
}

/// One object's kind, size, times, attributes, and identity, read without following its name's final link.
#[cfg(windows)]
#[derive(Clone, Copy, Debug)]
pub struct HostStat {
    pub(super) symlink: bool,
    pub(super) len: u64,
    pub(super) attributes: u32,
    pub(super) creation_time: u64,
    pub(super) last_access_time: u64,
    pub(super) last_write_time: u64,
    pub(super) volume_serial_number: u32,
    pub(super) file_index: u64,
    pub(super) number_of_links: u32,
}

#[cfg(windows)]
impl HostStat {
    pub(super) fn from_stat_information(
        information: &windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION,
        volume_serial_number: u32,
    ) -> io::Result<Self> {
        let unsigned =
            |value: i64| u64::try_from(value).map_err(|_| io::Error::other("negative stat field"));
        Ok(Self {
            symlink: matches!(information.ReparseTag, 0xA000_0003 | 0xA000_000C),
            len: unsigned(information.EndOfFile)?,
            attributes: information.FileAttributes,
            creation_time: unsigned(information.CreationTime)?,
            last_access_time: unsigned(information.LastAccessTime)?,
            last_write_time: unsigned(information.LastWriteTime)?,
            volume_serial_number,
            file_index: information.FileId as u64,
            number_of_links: information.NumberOfLinks,
        })
    }
    #[must_use]
    pub const fn file_type(&self) -> HostFileType {
        HostFileType(if self.symlink {
            0o120000
        } else if self.attributes & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY.0
            != 0
        {
            0o040000
        } else {
            0o100000
        })
    }
    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }
    #[must_use]
    #[allow(
        clippy::len_without_is_empty,
        reason = "mirrors Metadata::len: the byte length of the named object"
    )]
    pub const fn len(&self) -> u64 {
        self.len
    }
    #[must_use]
    pub const fn file_attributes(&self) -> u32 {
        self.attributes
    }
    #[must_use]
    pub const fn creation_time(&self) -> u64 {
        self.creation_time
    }
    #[must_use]
    pub const fn last_write_time(&self) -> u64 {
        self.last_write_time
    }
    #[must_use]
    pub const fn volume_serial_number(&self) -> u32 {
        self.volume_serial_number
    }
    #[must_use]
    pub const fn file_index(&self) -> u64 {
        self.file_index
    }
    /// How many names the file record has, which the file record keeps exact for every one of them.
    #[must_use]
    pub const fn number_of_links(&self) -> Option<u32> {
        Some(self.number_of_links)
    }
    pub fn created(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.creation_time))
    }
    pub fn modified(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.last_write_time))
    }
    pub fn accessed(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.last_access_time))
    }
}

/// `FILETIME` ticks (100 ns since 1601-01-01 UTC) at the Unix epoch.
#[cfg(windows)]
pub(crate) const UNIX_EPOCH_FILETIME: i64 = 116_444_736_000_000_000;

/// Unix nanoseconds one `FILETIME` names. Every tick is a whole number of
/// nanoseconds, so this is exact or `None` beyond the `i64` nanosecond range.
#[cfg(all(windows, any(feature = "native-mount", test)))]
pub(crate) fn filetime_to_unix_nanoseconds(ticks: i64) -> Option<i64> {
    ticks.checked_sub(UNIX_EPOCH_FILETIME)?.checked_mul(100)
}

/// The `FILETIME` tick containing one Unix-nanosecond instant: the instant
/// rounded toward the past, so ordering is preserved, including before 1970.
/// It never overflows, because `i64` nanoseconds span far fewer ticks than
/// `i64`. A writer that must reproduce an instant exactly rejects
/// `nanoseconds % 100 != 0` first.
#[cfg(all(windows, any(feature = "native-mount", test)))]
pub(crate) fn unix_nanoseconds_to_filetime(nanoseconds: i64) -> i64 {
    nanoseconds.div_euclid(100) + UNIX_EPOCH_FILETIME
}

/// The instant one `FILETIME` names, as the standard library reads it.
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

/// One enumerated name with its facts. `stat` is `None` when enumeration
/// cannot report the facts exactly as [`HostRoot::stat`] would, and the name
/// must be stat'ed on its own.
pub struct HostListedEntry {
    pub name: OsString,
    pub stat: Option<HostStat>,
}

impl HostListedEntry {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.name.as_encoded_bytes().len() as u64
    }
}

#[cfg(windows)]
pub(crate) struct HostListedWideEntry {
    pub(crate) name: Vec<u16>,
    pub(crate) stat: Option<HostStat>,
}

#[cfg(windows)]
impl HostListedWideEntry {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.name.len() as u64 * 2
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
    pub(crate) fn identity(&self) -> Result<crate::NativeRootIdentity, DemandError> {
        #[cfg(unix)]
        {
            Ok(crate::NativeRootIdentity {
                device: self.dev(),
                object: self.ino(),
            })
        }
        #[cfg(windows)]
        {
            Ok(crate::NativeRootIdentity {
                device: u64::from(self.volume_serial_number()),
                object: self.file_index(),
            })
        }
    }
}
