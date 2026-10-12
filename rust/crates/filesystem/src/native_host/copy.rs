use super::*;

pub(super) const STAGING_PREFIX: &str = ".acyclic-copy-";

pub(super) async fn copy_windows_file_worker(
    source_root: HostRoot,
    source_path: std::path::PathBuf,
    staged: &std::sync::Arc<StagedWindowsFile>,
    receiver: &tokio::sync::oneshot::Sender<io::Result<()>>,
) -> io::Result<()> {
    use acyclic_native_runtime::{NativeFile, OwnedRead, OwnedWrite};
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_FLAG_OVERLAPPED, FILE_GENERIC_READ, FILE_GENERIC_WRITE,
    };
    const CHUNK: usize = 1024 * 1024;
    let (mut source, length) = acyclic_native_runtime::run_blocking_io(
        move || {
            let mut work = ordinary_work();
            let source = source_root
                .open_file_positional(&source_path, &mut work)?
                .into_std();
            let stat = source_root.stat_file(&source, &mut work)?;
            if !stat.file_type().is_file()
                || stat.file_attributes()
                    & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0
                    != 0
            {
                return Err(DemandError::InvalidRequest);
            }
            Ok::<_, DemandError>((source, stat.len()))
        },
        None,
    )
    .expect("unmetered task construction cannot fail")
    .await?
    .map_err(ordinary_error)?;
    if receiver.is_closed() {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
    }
    if clonable_windows_length(length) {
        let clone_target = std::sync::Arc::clone(staged);
        let (returned, cloned) = acyclic_native_runtime::run_blocking_io(
            move || {
                let mut work = ordinary_work();
                let clone_source = reopen_windows_file(
                    &source,
                    FILE_GENERIC_READ,
                    FILE_FLAG_OPEN_REPARSE_POINT,
                    &mut work,
                )?;
                let target = reopen_windows_file(
                    &clone_target.guard,
                    FILE_GENERIC_WRITE,
                    Default::default(),
                    &mut work,
                )?;
                let cloned = clone_windows_file(&clone_source, &target, length, &mut work)?;
                Ok::<_, DemandError>((source, cloned))
            },
            None,
        )
        .expect("unmetered task construction cannot fail")
        .await?
        .map_err(ordinary_error)?;
        source = returned;
        if cloned {
            return Ok(());
        }
        if receiver.is_closed() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
        }
    }
    let writer = std::sync::Arc::clone(staged);
    let destination = acyclic_native_runtime::run_blocking_io(
        move || {
            reopen_windows_file(
                &writer.guard,
                FILE_GENERIC_WRITE,
                FILE_FLAG_OVERLAPPED,
                &mut ordinary_work(),
            )
            .map_err(ordinary_error)
        },
        None,
    )
    .expect("unmetered task construction cannot fail")
    .await??;
    // SAFETY: both handles were opened with FILE_FLAG_OVERLAPPED and are transferred once; no independent I/O is performed after this point.
    let source = unsafe {
        NativeFile::from_overlapped_file_unchecked(source, None).map_err(|(error, _)| error)?
    };
    // SAFETY: as above.
    let destination = unsafe {
        NativeFile::from_overlapped_file_unchecked(destination, None).map_err(|(error, _)| error)?
    };
    let mut offset = 0_u64;
    while offset < length {
        if receiver.is_closed() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
        }
        let requested = usize::try_from((length - offset).min(CHUNK as u64))
            .map_err(|_| io::Error::other("copy chunk overflow"))?;
        let mut result = source
            .read_batch_async(vec![OwnedRead {
                offset,
                length: requested,
            }])
            .await?;
        let bytes = result
            .pop()
            .ok_or_else(|| io::Error::other("copy read missing"))?;
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "copy source became shorter",
            ));
        }
        let copied = bytes.len() as u64;
        destination
            .write_all_batch_async(vec![OwnedWrite { offset, bytes }])
            .await?;
        offset += copied;
    }
    Ok(())
}
