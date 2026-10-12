use super::*;

pub(in crate::demand) const fn version_hash_bytes() -> u64 {
        b"acyclic-fs-native-source-version-v1\0".len() as u64 + 8
            + if cfg!(unix) { 48 } else { 32 }
    }

pub(in crate::demand) fn identity_hash_bytes(metadata: &HostStat) -> u64 {
        #[cfg(unix)]
        let facts = 16 + if metadata.created().ok()
            .and_then(|born| born.into_std().duration_since(std::time::UNIX_EPOCH).ok()).is_some() { 16 } else { 0 };
        #[cfg(windows)]
        let facts = { let _ = metadata; 12 };
        b"acyclic-fs-native-source-file-v1\0".len() as u64 + facts
    }

pub(in crate::demand) fn version<W: SourceDemandReadWork>(metadata: &HostStat, work: &mut W) -> Result<SourceVersion, DemandError> {
        work.charge(WorkCounters { bytes_hashed: version_hash_bytes(), ..WorkCounters::UNCHARGED })?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-version-v1\0");
        hash.update(&metadata.len().to_le_bytes());
        #[cfg(unix)]
        {
            hash.update(&metadata.dev().to_le_bytes());
            hash.update(&metadata.ino().to_le_bytes());
            hash.update(&metadata.mtime().to_le_bytes());
            hash.update(&metadata.mtime_nsec().to_le_bytes());
            hash.update(&metadata.ctime().to_le_bytes());
            hash.update(&metadata.ctime_nsec().to_le_bytes());
        }
        #[cfg(windows)]
        {
            hash.update(&metadata.file_attributes().to_le_bytes());
            hash.update(&metadata.last_write_time().to_le_bytes());
            hash.update(&metadata.creation_time().to_le_bytes());
            hash.update(&metadata.volume_serial_number().to_le_bytes());
            hash.update(&metadata.file_index().to_le_bytes());
        }
        Ok(SourceVersion(*hash.finalize().as_bytes()))
    }

#[cfg(unix)]
    pub(in crate::demand) fn file_identity(metadata: &HostStat) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-file-v1\0");
        hash.update(&metadata.dev().to_le_bytes());
        hash.update(&metadata.ino().to_le_bytes());
        if let Some(born) = metadata
            .created()
            .ok()
            .and_then(|born| born.into_std().duration_since(std::time::UNIX_EPOCH).ok())
        {
            hash.update(&born.as_nanos().to_le_bytes());
        }
        *hash.finalize().as_bytes()
    }

#[cfg(windows)]
    pub(in crate::demand) fn file_identity(metadata: &HostStat) -> [u8; 32] {
        windows_file_identity(
            u64::from(metadata.volume_serial_number()),
            metadata.file_index(),
        )
    }

#[cfg(windows)]
    pub(in crate::demand) fn windows_file_identity(volume: u64, index: u64) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-native-source-file-v1\0");
        hash.update(&u32::try_from(volume).unwrap_or_default().to_le_bytes());
        hash.update(&index.to_le_bytes());
        *hash.finalize().as_bytes()
    }

pub(in crate::demand) fn link_count(metadata: &HostStat) -> Option<u64> {
        #[cfg(unix)]
        {
            Some(metadata.nlink())
        }
        #[cfg(not(unix))]
        {
            metadata.number_of_links().map(u64::from)
        }
    }

#[allow(
        clippy::useless_conversion,
        reason = "dev_t and major/minor result widths differ across Unix targets"
    )]
    pub(in crate::demand) fn device_identity(metadata: &HostStat, kind: SourceNodeKind) -> Option<(u32, u32)> {
        #[cfg(unix)]
        {
            if !matches!(
                kind,
                SourceNodeKind::CharacterDevice | SourceNodeKind::BlockDevice
            ) {
                return None;
            }
            let raw: libc::dev_t = metadata.rdev().try_into().ok()?;
            Some((
                libc::major(raw).try_into().ok()?,
                libc::minor(raw).try_into().ok()?,
            ))
        }
        #[cfg(not(unix))]
        {
            let _ = (metadata, kind);
            None
        }
    }

pub(in crate::demand) fn source_metadata(metadata: &HostStat) -> SourceMetadata {
        let mut result = SourceMetadata {
            created_ns: metadata.created().ok().and_then(system_time_nanos),
            modified_ns: metadata.modified().ok().and_then(system_time_nanos),
            accessed_ns: metadata.accessed().ok().and_then(system_time_nanos),
            ..SourceMetadata::default()
        };
        #[cfg(unix)]
        {
            result.posix_mode = Some(metadata.mode());
            result.posix_uid = Some(metadata.uid());
            result.posix_gid = Some(metadata.gid());
            result.changed_ns = Some(
                i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec()),
            );
        }
        #[cfg(windows)]
        {
            result.windows_attributes = Some(metadata.file_attributes());
        }
        result
    }

pub(in crate::demand) fn system_time_nanos(time: cap_std::time::SystemTime) -> Option<i128> {
        match time.into_std().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => Some(
                i128::from(duration.as_secs()) * 1_000_000_000
                    + i128::from(duration.subsec_nanos()),
            ),
            Err(error) => Some(
                -(i128::from(error.duration().as_secs()) * 1_000_000_000
                    + i128::from(error.duration().subsec_nanos())),
            ),
        }
    }
