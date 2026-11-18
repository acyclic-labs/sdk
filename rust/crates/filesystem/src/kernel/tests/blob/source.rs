#[tokio::test]
async fn measured_async_source_work_replaces_inferred_bytes_and_obeys_budget()
-> Result<(), Box<dyn std::error::Error>> {
    struct MeasuredSource {
        returned: bool,
        work: Option<WorkCounters>,
    }

    impl AsyncBlobSource for MeasuredSource {
        async fn read<'a>(
            &'a mut self,
            _destination: &'a mut [u8],
            _budget: WorkBudget,
            _cancellation: &'a CancellationToken,
        ) -> std::io::Result<usize> {
            Err(std::io::Error::other("owned source should not be copied"))
        }

        async fn read_owned(
            &mut self,
            _maximum: usize,
            _budget: WorkBudget,
            _cancellation: &CancellationToken,
        ) -> std::io::Result<Option<OwnedSourceChunk>> {
            if self.returned {
                self.work = Some(WorkCounters::default());
                return Ok(Some(OwnedSourceChunk::claimed(
                    acyclic_stream::OwnedBytes::new(),
                    0,
                )));
            }
            self.returned = true;
            self.work = Some(WorkCounters {
                source_bytes_read: 3,
                source_path_components: 7,
                ..WorkCounters::default()
            });
            Ok(Some(OwnedSourceChunk::claimed(
                acyclic_stream::OwnedBytes::from_static(b"abc"),
                0,
            )))
        }

        fn take_work(&mut self) -> Option<WorkCounters> {
            self.work.take()
        }
    }

    let options = BlobBuildOptions {
        chunk_bytes: 3,
        page_items: 2,
        page_bytes: 129,
        maximum_blob_bytes: Some(3),
    };
    let mut source = MeasuredSource {
        returned: false,
        work: None,
    };
    let built = (build_blob(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(built.work.source_bytes_read, 3);
    assert_eq!(built.work.source_path_components, 7);

    let mut source = MeasuredSource {
        returned: false,
        work: None,
    };
    let mut budget = WorkBudget::UNBOUNDED;
    budget.source_path_components = 6;
    let failed = (build_blob(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("underbudgeted measured source unexpectedly built")?;
    assert!(matches!(failed.error, BlobBuildError::Work(_)));
    assert_eq!(failed.work.as_deref().unwrap().backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn owned_chunk_retains_actual_backing_capacity_not_visible_length()
-> Result<(), Box<dyn std::error::Error>> {
    struct RetainedSource(Option<OwnedSourceChunk>);
    impl AsyncBlobSource for RetainedSource {
        async fn read<'a>(
            &'a mut self,
            _destination: &'a mut [u8],
            _budget: WorkBudget,
            _cancellation: &'a CancellationToken,
        ) -> std::io::Result<usize> {
            panic!("the owned source must not be copied");
        }

        async fn read_owned(
            &mut self,
            _maximum: usize,
            _budget: WorkBudget,
            _cancellation: &CancellationToken,
        ) -> std::io::Result<Option<OwnedSourceChunk>> {
            Ok(Some(self.0.take().unwrap_or(OwnedSourceChunk::claimed(
                acyclic_stream::OwnedBytes::new(),
                0,
            ))))
        }
    }

    // The producer transfers an existing allocation, including spare capacity.
    // Constructing it outside the callback is intentional: this is an input
    // ownership/retention control, not a claim of newly allocated source work.
    let make_source = || {
        let mut bytes = Vec::with_capacity(65_536);
        bytes.extend_from_slice(b"abc");
        let capacity = usize_to_u64(bytes.capacity());
        (
            RetainedSource(Some(OwnedSourceChunk::claimed(bytes.into(), capacity))),
            capacity,
        )
    };
    let options = BlobBuildOptions {
        chunk_bytes: 3,
        page_items: 2,
        page_bytes: 129,
        maximum_blob_bytes: Some(3),
    };
    let (mut source, capacity) = make_source();
    let built = (build_blob(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(built.logical_bytes, 3);
    assert_eq!(built.work.source_bytes_read, 3);
    assert!(built.work.peak_allocation_bytes >= capacity);

    let (mut source, capacity) = make_source();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = capacity - 1;
    let failed = (build_blob(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("spare-capacity input escaped its hard bound")?;
    assert!(matches!(failed.error, BlobBuildError::Work(_)));
    assert_eq!(failed.work.as_deref().unwrap().source_bytes_read, 3);
    assert_eq!(failed.work.as_deref().unwrap().backend_write_operations, 0);
    assert!(failed.work.as_deref().unwrap().peak_allocation_bytes >= capacity);
    Ok(())
}

#[tokio::test]
async fn partial_async_source_failure_and_midstream_cancellation_preserve_exact_work()
-> Result<(), Box<dyn std::error::Error>> {
    let options = BlobBuildOptions {
        chunk_bytes: 4,
        page_items: 2,
        page_bytes: 129,
        maximum_blob_bytes: Some(6),
    };
    let store = MemoryObjectStore::default();
    let mut partial = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: None,
    };
    let built = (build_blob(
        &store,
        &mut partial,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(built.logical_bytes, 6);
    assert_eq!(built.work.source_bytes_read, 6);
    assert_eq!(
        read_blob_range(
            &store,
            built.root,
            ByteRange {
                offset: 0,
                length: 6,
            },
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .bytes,
        OwnedBytes::from_static(b"abcdef")
    );

    let mut failing = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: Some(1),
        cancel_at: None,
    };
    let failure = (build_blob(
        &MemoryObjectStore::default(),
        &mut failing,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("failing source unexpectedly built")?;
    assert!(matches!(failure.error, BlobBuildError::Source(_)));
    assert_eq!(failure.work.as_deref().unwrap().source_bytes_read, 1);
    assert_eq!(failure.work.as_deref().unwrap().backend_write_operations, 0);

    let cancellation = CancellationToken::new();
    let mut cancelling = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: Some(1),
    };
    let cancelled = (build_blob(
        &MemoryObjectStore::default(),
        &mut cancelling,
        options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("midstream-cancelled source unexpectedly built")?;
    assert!(matches!(cancelled.error, BlobBuildError::Cancelled));
    assert_eq!(cancelled.work().unwrap().source_bytes_read, 1);
    assert_eq!(cancelled.work().unwrap().backend_write_operations, 0);

    let cancellation = CancellationToken::new();
    let mut queued = ScriptedSource {
        bytes: OwnedBytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: Some(5),
    };
    let cancelled = (build_blob(
        &MemoryObjectStore::default(),
        &mut queued,
        options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("queued midstream cancellation unexpectedly built")?;
    assert!(matches!(cancelled.error, BlobBuildError::Cancelled));
    assert_eq!(cancelled.work().unwrap().source_bytes_read, 5);
    assert_eq!(cancelled.work().unwrap().backend_write_operations, 0);

    let mut tight_budget = WorkBudget::UNBOUNDED;
    tight_budget.peak_allocation_bytes = cancelled
        .work()
        .unwrap()
        .peak_allocation_bytes
        .saturating_sub(1);
    let mut bounded = std::io::Cursor::new(OwnedBytes::from_static(b"abcdef"));
    let rejected = (build_blob(
        &MemoryObjectStore::default(),
        &mut bounded,
        options,
        tight_budget,
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("one-byte-short retained-buffer budget unexpectedly built")?;
    assert!(matches!(rejected.error, BlobBuildError::Work(_)));
    assert_eq!(rejected.work().unwrap().source_bytes_read, 4);
    assert_eq!(rejected.work().unwrap().backend_write_operations, 0);
    Ok(())
}
