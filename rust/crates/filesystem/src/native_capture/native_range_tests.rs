use super::*;

#[cfg(test)]
mod native_range_tests {
    use super::*;
    use crate::kernel::AsyncBlobSource as _;

    fn budget() -> WorkBudget {
        WorkCounters {
            backend_read_operations: 16,
            source_bytes_read: 32,
            allocation_operations: 128,
            peak_allocation_bytes: 1024 * 1024,
            items_examined: 32,
            bytes_copied: 32,
            ..WorkCounters::UNCHARGED
        }
    }

    fn fixture_file(path: &Path) -> std::io::Result<std::fs::File> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            options.custom_flags(windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED.0);
        }
        options.open(path)
    }

    #[cfg(unix)]
    #[test]
    #[allow(
        unsafe_code,
        reason = "advisory locks observe the real imported descriptor's physical retirement"
    )]
    fn cancelled_native_import_retires_std_file_before_preserving_original_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::fd::AsRawFd as _;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("cancelled-import");
        std::fs::write(&path, b"actual")?;
        let file = fixture_file(&path)?;
        let contender = fixture_file(&path)?;
        assert_eq!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        assert_ne!(
            unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        cancellation.cancel();
        assert!(matches!(
            native_positional_file(file, &scope.native),
            Err(CaptureError::Kernel(crate::FsError::Cancelled(_)))
        ));
        assert_eq!(
            unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        scope.release()?;
        Ok(())
    }

    #[tokio::test]
    async fn captured_source_stale_and_drop_retire_only_terminal_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("captured-stale");
        std::fs::write(&path, b"abcdefgh")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        let native =
            std::sync::Arc::new(native_positional_file(fixture_file(&path)?, &scope.native)?);
        let reader =
            acyclic_native_runtime::AsyncRangeReader::new(std::sync::Arc::clone(&native), 0, 8);
        let mut source = NativeRangeSource { reader };
        std::fs::write(&path, b"abc")?;
        let mut destination = [0xa5; 8];
        assert!(matches!(
            source
                .read_with_native_work(&mut destination, &original, &cancellation,)
                .await,
            Err(crate::FsError::SourceSnapshotStale)
        ));
        assert_eq!(destination, [0xa5; 8]);
        drop(source);
        scope.release()?;
        drop(native);
        Ok(())
    }

    #[tokio::test]
    async fn original_native_byte_and_copy_refusals_precede_io()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("original-denied");
        std::fs::write(&path, b"abc")?;
        for counter in ["source_bytes_read", "bytes_copied"] {
            let cancellation = CancellationToken::new();
            let mut limit = budget();
            if counter == "source_bytes_read" {
                limit.source_bytes_read = 2;
            } else {
                limit.bytes_copied = 2;
            }
            let original = crate::NativeMaterializationWork::new(limit, &cancellation)
                .expect("fund original accounting state");
            let scope = CaptureWorkScope::new(&original)?;
            let native =
                std::sync::Arc::new(native_positional_file(fixture_file(&path)?, &scope.native)?);
            let mut source = NativeRangeSource {
                reader: acyclic_native_runtime::AsyncRangeReader::new(native, 0, 3),
            };
            let baseline = original.receipt();
            let live = original.live_allocation_bytes();
            let mut destination = [0xa5; 8];
            let error = source
                .read_with_native_work(&mut destination, &original, &cancellation)
                .await
                .expect_err("a denied full range must not become truncated success");
            assert!(matches!(error,
                crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
                    crate::WorkError::BudgetExceeded { counter: refused, .. }
                )) if refused == counter));
            assert_eq!(destination, [0xa5; 8]);
            assert_eq!(source.reader.remaining(), 3);
            assert_eq!(original.receipt(), baseline);
            assert_eq!(original.live_allocation_bytes(), live);
        }
        Ok(())
    }
}
