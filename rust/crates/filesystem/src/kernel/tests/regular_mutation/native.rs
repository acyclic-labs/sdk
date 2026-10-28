#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_regular_copy_denial_and_written_prefix_use_original_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let content = blob(&store, b"native source").await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let denied = (apply_regular_mutation_with_accounting(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"old")?),
        RegularMutation::Write {
            offset: 0,
            length: 3,
            content,
            content_offset: 0,
        },
        config(),
        &mut accounting,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("zero-copy write accepted")?;
    assert!(matches!(
        denied,
        FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)) | FsError::Work(_)
    ));
    assert_eq!(original.receipt(), before);
    assert_eq!(original.live_allocation_bytes(), baseline);

    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let written = (apply_regular_mutation_with_accounting(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"old")?),
        RegularMutation::Write {
            offset: 0,
            length: 3,
            content,
            content_offset: 0,
        },
        config(),
        &mut accounting,
        &cancellation,
    ))
    .await?;
    let FilePayload::InlineRegular(data) = written.payload else {
        return Err("native inline payload promoted unexpectedly".into());
    };
    assert_eq!(data.as_bytes(), b"nat");
    assert!(original.receipt().backend_read_operations > 0);
    assert!(original.receipt().bytes_copied >= 3);

    let store = MemoryObjectStore::default();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            page_writes: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let denied = (apply_regular_mutation_with_accounting(
        &store,
        FilePayload::InlineRegular(InlineFileData::new(b"prefix!")?),
        RegularMutation::Resize {
            logical_bytes: usize_to_u64(MAXIMUM_INLINE_FILE_BYTES) + 1,
        },
        config(),
        &mut accounting,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("second page-write admitted")?;
    assert!(matches!(
        denied,
        FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)) | FsError::Work(_)
    ));
    assert_eq!(original.receipt().page_writes, 1);
    assert_eq!(original.receipt().backend_write_operations, 2);
    let chunk = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(b"prefix!"),
    };
    assert_eq!(
        {
            let __object_args = (
                chunk,
                7,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = crate::async_storage::AsyncObjectStore::read(
                &store,
                __object_args.0,
                __object_args.1,
                &mut __object_work,
            )
            .await;
            __object_work.receipt(__object_result)
        }?
        .value
        .as_ref(),
        b"prefix!"
    );
    let prefix = original.receipt();
    assert!(matches!(
        original.claim(u64::MAX, 1, crate::WorkCounters::UNCHARGED),
        Err(crate::NativeTreeMaterializationError::Work(
            WorkError::Overflow
        ))
    ));
    assert!(matches!(
        original.spent(WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::UNCHARGED
        }),
        Err(crate::NativeTreeMaterializationError::Work(
            WorkError::Overflow
        ))
    ));
    assert_eq!(original.receipt(), prefix);
    drop(store);
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}
