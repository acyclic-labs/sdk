#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn original_blob_custom_source_rejects_before_io_without_a_fresh_meter()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let mut source = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: None,
    };
    let error = (build_blob_with_accounting(
        &MemoryObjectStore::default(),
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 4,
            page_items: 2,
            page_bytes: 256,
            maximum_blob_bytes: 6,
        },
        &mut accounting,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("custom source used ordinary I/O")?;
    assert!(matches!(
        error,
        FsError::BlobBuild(BlobBuildError::NativeSourceUnavailable)
    ));
    assert_eq!(source.position, 0);
    assert_eq!(original.receipt().source_bytes_read, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn original_blob_partial_reads_retain_source_failure_and_cancellation_prefixes()
-> Result<(), Box<dyn std::error::Error>> {
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut source = NativePartialBlobSource {
            source: std::io::Cursor::new(b"abcdef"),
            fail_at: (!cancelled).then_some(3),
            cancel_at: cancelled.then_some(3),
        };
        let error = (build_blob_with_accounting(
            &MemoryObjectStore::default(),
            &mut source,
            BlobBuildOptions {
                chunk_bytes: 4,
                page_items: 2,
                page_bytes: 256,
                maximum_blob_bytes: 6,
            },
            &mut accounting,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("source failure was erased")?;
        if cancelled {
            assert!(matches!(
                error,
                FsError::NativeKernel(crate::NativeTreeMaterializationError::Cancelled)
                    | FsError::Cancelled(_)
            ));
        } else {
            assert!(
                matches!(error, FsError::BlobBuild(BlobBuildError::Source(error))
                if error.kind() == std::io::ErrorKind::PermissionDenied)
            );
        }
        assert_eq!(source.source.position(), 3);
        assert_eq!(original.receipt().source_bytes_read, 3);
        assert_eq!(original.receipt().backend_write_operations, 0);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[test]
#[cfg(target_pointer_width = "64")]
fn impossible_blob_buffers_fail_before_allocation_and_preserve_accounting()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut chunk = empty_blob_vec::<u8>();
    let error = reserve_blob_vec(&mut chunk, usize::MAX, &mut accounting)
        .err()
        .ok_or("impossible chunk allocation succeeded")?;
    assert!(matches!(
        error,
        FsError::BlobBuild(BlobBuildError::ReserveFailed(_))
    ));
    release_blob_vec(chunk, &mut accounting)?;
    let mut items = empty_blob_vec::<u8>();
    let error = reserve_blob_vec(&mut items, usize::MAX, &mut accounting)
        .err()
        .ok_or("impossible page item allocation succeeded")?;
    assert!(matches!(
        error,
        FsError::BlobBuild(BlobBuildError::ReserveFailed(_))
    ));
    release_blob_vec(items, &mut accounting)?;
    let KernelAccounting::Standalone {
        allocations, work, ..
    } = accounting
    else {
        unreachable!()
    };
    assert_eq!(allocations.live_bytes(), 0);
    assert_eq!(work.allocation_operations, 2);
    Ok(())
}
