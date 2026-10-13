use super::*;

#[cfg(all(test, windows))]
mod windows_sparse_tests {
    use super::*;

    /// Settles `reported` for the file at `name` under `directory`.
    async fn settle(
        directory: &Path,
        name: &str,
        reported: Vec<HostDataRange>,
        maximum_ranges: u32,
    ) -> std::io::Result<Vec<(u64, u64)>> {
        let root = HostRoot::open(directory, &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let file = root
            .open_file_positional(Path::new(name), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let metadata = test_host_stat(&root, Path::new(name)).map_err(std::io::Error::other)?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| std::io::Error::other(error))?;
        let native = std::sync::Arc::new(
            native_positional_file(file.into_std(), &original).map_err(std::io::Error::other)?,
        );
        let mut scope = CaptureWorkScope::new(&original)
            .map_err(|failure| std::io::Error::other(failure.error))?;
        scope
            .native
            .with_original_leaf(|work| {
                work.claim_allocation(
                    reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64,
                    0,
                    WorkCounters::UNCHARGED,
                )
            })
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let ranges = settled_sparse_ranges(
            &native,
            &metadata,
            reported,
            maximum_ranges,
            &cancellation,
            &scope,
        )
        .await
        .map_err(std::io::Error::other)?;
        let converted = ranges
            .into_iter()
            .map(|range| (range.offset, range.length))
            .collect();
        scope.release().map_err(std::io::Error::other)?;
        Ok(converted)
    }

    #[tokio::test]
    async fn written_zeros_in_a_sparse_file_capture_as_holes() -> std::io::Result<()> {
        use std::io::{Seek as _, SeekFrom, Write as _};
        use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

        let directory = tempfile::tempdir()?;
        let length = 64 * 1024 + 100;
        let whole = || vec![HostDataRange { offset: 0, length }];
        let plain = std::fs::File::create(directory.path().join("plain.bin"))?;
        plain.set_len(length)?;
        drop(plain);
        // Not marked sparse: its report stands.
        assert_eq!(
            settle(directory.path(), "plain.bin", whole(), 16).await?,
            [(0, length)]
        );

        let mut file = std::fs::File::create(directory.path().join("sparse.bin"))?;
        acyclic_native_runtime::device_control_in_place(
            &file,
            FSCTL_SET_SPARSE,
            &[],
            &mut [],
            None,
        )?;
        file.set_len(length)?;
        file.write_all(&[1; 10])?;
        // Zeros written, so allocated, around the data block at 40 KiB.
        file.seek(SeekFrom::Start(4096))?;
        file.write_all(&[0; 36 * 1024])?;
        file.write_all(&[2; 5])?;
        file.seek(SeekFrom::Start(length - 3))?;
        file.write_all(&[3; 3])?;
        drop(file);
        assert_eq!(
            settle(directory.path(), "sparse.bin", whole(), 16).await?,
            [(0, 4096), (40 * 1024, 4096), (64 * 1024, 100)]
        );
        // Settling it would take more ranges than admitted: the report stands.
        assert_eq!(
            settle(directory.path(), "sparse.bin", whole(), 2).await?,
            [(0, length)]
        );

        // Every byte read is charged, and the scan stops at the budget or once cancelled.
        let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let file = root
            .open_file_positional(
                Path::new("sparse.bin"),
                &mut crate::native_host::ordinary_work(),
            )
            .map_err(crate::native_host::ordinary_error)?;
        let metadata =
            test_host_stat(&root, Path::new("sparse.bin")).map_err(std::io::Error::other)?;
        let file_work =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| std::io::Error::other(error))?;
        let native = std::sync::Arc::new(
            native_positional_file(file.into_std(), &file_work).map_err(std::io::Error::other)?,
        );
        let scan = |budget: WorkBudget, cancellation: CancellationToken| {
            let (native, metadata) = (std::sync::Arc::clone(&native), metadata.clone());
            async move {
                let reported = whole();
                let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(error, work)| {
                        OperationFailure::new(CaptureError::from(error), work)
                    })?;
                let mut scope = CaptureWorkScope::new(&original)?;
                {
                    let mut work = &scope;
                    work.native
                        .with_original_leaf(|work| {
                            work.claim_allocation(
                                reported.capacity() as u64
                                    * std::mem::size_of::<HostDataRange>() as u64,
                                0,
                                WorkCounters::UNCHARGED,
                            )
                        })
                        .map_err(|error| {
                            OperationFailure::new(
                                capture_demand_error(error),
                                work.native.receipt(),
                            )
                        })?;
                }
                let result =
                    settled_sparse_ranges(&native, &metadata, reported, 16, &cancellation, &scope)
                        .await
                        .map(drop);
                scope
                    .release()
                    .map_err(|error| OperationFailure::new(error, original.receipt()))?;
                let work = original.receipt();
                result
                    .map(|()| ((), work))
                    .map_err(|error| OperationFailure::new(error, work))
            }
        };
        let short = WorkBudget {
            source_bytes_read: 4096,
            ..WorkBudget::UNBOUNDED
        };
        assert!(matches!(
            scan(short, CancellationToken::new()).await,
            Err(failure) if matches!(failure.error, CaptureError::Kernel(crate::FsError::Work(_) | crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_))))
        ));
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(scan(WorkBudget::UNBOUNDED, cancelled).await.is_err());
        Ok(())
    }
}
