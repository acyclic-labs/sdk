use super::*;

#[allow(clippy::struct_field_names, reason = "SETATTR names")]
pub(super) struct AttributeChanges {
    pub(super) mode: Option<u32>,
    pub(super) uid: Option<u32>,
    pub(super) gid: Option<u32>,
    pub(super) size: Option<u64>,
    pub(super) atime: Option<TimeOrNow>,
    pub(super) mtime: Option<TimeOrNow>,
    pub(super) changed: Option<SystemTime>,
    pub(super) crtime: Option<SystemTime>,
    pub(super) flags: Option<u32>,
}

impl AttributeChanges {
    pub(super) fn is_empty(&self) -> bool {
        self.mode.is_none() && self.uid.is_none() && self.gid.is_none() && self.size.is_none()
            && self.atime.is_none() && self.mtime.is_none() && self.changed.is_none()
            && self.crtime.is_none() && self.flags.is_none()
    }

    pub(super) fn applied_to(&self, mut metadata: FileMetadata) -> Result<FileMetadata, i32> {
        if let Some(mode) = self.mode { metadata.posix_mode = MetadataField::Value(mode); }
        if let Some(uid) = self.uid { metadata.posix_uid = MetadataField::Value(uid); }
        if let Some(gid) = self.gid { metadata.posix_gid = MetadataField::Value(gid); }
        if let Some(atime) = self.atime { metadata.accessed_ns = MetadataField::Value(time_or_now_ns(atime)?); }
        if let Some(mtime) = self.mtime { metadata.modified_ns = MetadataField::Value(time_or_now_ns(mtime)?); }
        if let Some(changed) = self.changed { metadata.changed_ns = MetadataField::Value(system_time_ns(changed)?); }
        if let Some(created) = self.crtime { metadata.created_ns = MetadataField::Value(system_time_ns(created)?); }
        if let Some(flags) = self.flags { metadata.posix_flags = MetadataField::Value(u64::from(flags)); }
        Ok(metadata)
    }
}

pub(super) fn file_type(kind: MountNodeKind) -> Result<FileType, i32> {
    Ok(match kind {
        MountNodeKind::Regular => FileType::RegularFile,
        MountNodeKind::Directory => FileType::Directory,
        MountNodeKind::SymbolicLink => FileType::Symlink,
        MountNodeKind::Fifo => FileType::NamedPipe,
        MountNodeKind::Socket => FileType::Socket,
        MountNodeKind::CharacterDevice => FileType::CharDevice,
        MountNodeKind::BlockDevice => FileType::BlockDevice,
        MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
    })
}

pub(super) fn metadata_time(field: MetadataField<i64>) -> SystemTime {
    match field {
        MetadataField::Unavailable => SystemTime::UNIX_EPOCH,
        MetadataField::Value(value) if value >= 0 => SystemTime::UNIX_EPOCH + Duration::from_nanos(value.unsigned_abs()),
        MetadataField::Value(value) => SystemTime::UNIX_EPOCH - Duration::from_nanos(value.unsigned_abs()),
    }
}

pub(super) fn time_or_now_ns(value: TimeOrNow) -> Result<i64, i32> {
    system_time_ns(match value {
        TimeOrNow::SpecificTime(value) => value,
        TimeOrNow::Now => SystemTime::now(),
    })
}

pub(super) fn native_device_number(major: u32, minor: u32) -> Result<u32, i32> {
    crate::native_mount::device::join_device(major, minor)
        .and_then(|device| u32::try_from(device).map_err(|_| libc::EOVERFLOW))
}

pub(super) fn admit_open(writable: bool, flags: i32) -> Result<(), i32> {
    if flags & libc::O_TRUNC != 0 && !writable { Err(libc::EROFS) } else { Ok(()) }
}
