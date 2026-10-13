use super::*;

pub(super) const STAGING_PREFIX: &str = ".acyclic-copy-";

pub(super) async fn copy_windows_file_worker(
    source_root: HostRoot,
    source_path: std::path::PathBuf,
    staged: &std::sync::Arc<StagedWindowsFile>,
    receiver: &tokio::sync::oneshot::Sender<io::Result<()>>,
) -> io::Result<()> {
    use acyclic_native_runtime::{NativeFile, OwnedRead, OwnedWrite};
    use fs::FILE_ATTRIBUTE_REPARSE_POINT;
    const CHUNK: usize = 1024 * 1024;
    if receiver.is_closed() {
        return Err(io::ErrorKind::Interrupted.into());
    }
    let target = std::sync::Arc::clone(staged);
    let (source, length, cloned) = acyclic_native_runtime::run_blocking_io(
        move || {
            let mut work = ordinary_work();
            let source = with_wide_path(&source_path, &mut work, |path, work| {
                source_root.open_file(path, nt::FILE_SYNCHRONOUS_IO_NONALERT, work)
            })?
            .into_std();
            let stat = source_root.stat_file(&source, &mut work)?;
            if !stat.file_type().is_file()
                || stat.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
            {
                return Err(DemandError::InvalidRequest);
            }
            let cloned = clonable_windows_length(stat.len())
                && clone_windows_file(&source, &target.guard, stat.len(), &mut work)?;
            Ok::<_, DemandError>((source, stat.len(), cloned))
        },
        None,
    )
    .expect("unmetered task construction cannot fail")
    .await?
    .map_err(ordinary_error)?;
    if receiver.is_closed() {
        return Err(io::ErrorKind::Interrupted.into());
    }
    if cloned {
        return Ok(());
    }
    let source = NativeFile::from_file(source, None).map_err(|(error, _)| error)?;
    let destination =
        NativeFile::from_file(staged.guard.try_clone()?, None).map_err(|(error, _)| error)?;
    let mut offset = 0;
    while offset < length {
        if receiver.is_closed() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let requested = (length - offset).min(CHUNK as u64) as usize;
        let bytes = source
            .read_batch_async(vec![OwnedRead {
                offset,
                length: requested,
            }])
            .await?
            .pop()
            .ok_or_else(|| io::Error::other("copy read missing"))?;
        if bytes.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let copied = bytes.len() as u64;
        destination
            .write_all_batch_async(vec![OwnedWrite { offset, bytes }])
            .await?;
        offset += copied;
    }
    Ok(())
}

pub(crate) struct StagedWindowsFile {
    pub(super) parent: Dir,
    pub(super) guard: File,
}
impl StagedWindowsFile {
    pub(crate) fn create(parent: Dir) -> io::Result<Self> {
        use cap_std::fs::OpenOptionsExt as _;
        use fs::*;
        let name = format!("{STAGING_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .access_mode(DELETE.0 | FILE_GENERIC_WRITE.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0)
            .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
            .custom_flags(FILE_FLAG_DELETE_ON_CLOSE.0);
        let guard = parent.open_with(name, &options)?.into_std();
        Ok(Self { parent, guard })
    }
    pub(crate) fn publish(&self, name: &OsStr) -> io::Result<()> {
        with_wide_path(Path::new(name), &mut ordinary_work(), |name, work| {
            set_windows_name(
                &self.guard,
                &self.parent,
                name,
                false,
                nt::FileLinkInformation,
                work,
            )
        })
        .map_err(ordinary_error)
    }
}
impl Drop for StagedWindowsFile {
    fn drop(&mut self) {
        use windows::{
            Wdk::Storage::FileSystem::FILE_INFORMATION_CLASS, Win32::Storage::FileSystem::*,
        };
        let set = |flags| windows_set_information(&self.guard, &flags, FILE_INFORMATION_CLASS(64));
        if set(FILE_DISPOSITION_FLAG_ON_CLOSE.0).is_ok()
            && set(FILE_DISPOSITION_FLAG_DELETE.0
                | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
                | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0)
            .is_err()
        {
            let _ = set(FILE_DISPOSITION_FLAG_ON_CLOSE.0 | FILE_DISPOSITION_FLAG_DELETE.0);
        }
    }
}

pub(super) fn clonable_windows_length(length: u64) -> bool {
    length >= 4 * 1024 && i64::try_from(length).is_ok()
}
