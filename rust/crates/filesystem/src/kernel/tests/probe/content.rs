use super::*;

#[tokio::test]
async fn content_digest_is_layout_independent_and_hash_budget_is_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let (_, file_id, metadata) = fixture(&store, None).await?;
    let domain = b"acyclic-fs-dependency-content-range-v1\0";
    let hash_bytes = u64::try_from(domain.len())?.saturating_add(16);
    let exact_budget = WorkBudget {
        bytes_hashed: hash_bytes,
        ..WorkBudget::UNBOUNDED
    };
    let inline = capture_content_range_bytes(&[0; 8], 8, exact_budget)?;
    let extents = crate::test_support::put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::Hole,
        }]),
        8,
    )
    .await?;
    let sparse = probe
        .capture_record_async(
            Some(FileRecord {
                file_id,
                kind: FileKind::Regular,
                link_count: 1,
                metadata,
                payload: FilePayload::Regular {
                    logical_bytes: 8,
                    extents,
                },
            }),
            &DependencyRegion::ContentRange {
                file_id,
                offset: 0,
                length: 8,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(inline.value, sparse.value);
    assert_eq!(inline.work.bytes_hashed, hash_bytes);
    let failure = capture_content_range_bytes(
        &[0; 8],
        8,
        WorkBudget {
            bytes_hashed: hash_bytes - 1,
            ..WorkBudget::UNBOUNDED
        },
    )
    .err()
    .ok_or("underbudgeted inline hash unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AuthenticatedProbeError::Work(WorkError::BudgetExceeded {
            counter: "bytes_hashed",
            observed,
            maximum,
        }) if observed == hash_bytes && maximum == hash_bytes - 1
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn canonical_content_range_surface_matches_golden_vector_and_fails_before_work()
-> Result<(), Box<dyn std::error::Error>> {
    let expected = hex::decode("e06aee566a0811dd9212ffd2d478c71bcc6fab338bfe63af1bc0ddc513366f64")?;
    let receipt = capture_content_range_bytes(b"hello", 5, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        receipt.value,
        DependencyState::Present(Digest::from_bytes(
            expected
                .try_into()
                .map_err(|_: Vec<u8>| "golden content-range digest has the wrong length",)?
        ))
    );
    assert_eq!(receipt.work.bytes_hashed, 52);

    let cases = [
        capture_content_range_bytes(b"hello", 0, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("zero maximum unexpectedly succeeded")?,
        capture_content_range_bytes(b"", 1, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("empty range unexpectedly succeeded")?,
        capture_content_range_bytes(b"hello", 4, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("oversized range unexpectedly succeeded")?,
        capture_content_range_bytes(
            b"hello",
            5,
            WorkBudget {
                bytes_hashed: 51,
                ..WorkBudget::UNBOUNDED
            },
        )
        .err()
        .ok_or("underbudgeted range unexpectedly succeeded")?,
    ];
    assert!(matches!(
        cases[0].error,
        AuthenticatedProbeError::InvalidLimits
    ));
    assert!(matches!(
        cases[1].error,
        AuthenticatedProbeError::InvalidDependency(DependencyError::EmptyContentRange)
    ));
    assert!(matches!(
        cases[2].error,
        AuthenticatedProbeError::ContentRangeTooLarge
    ));
    assert!(matches!(
        cases[3].error,
        AuthenticatedProbeError::Work(WorkError::BudgetExceeded {
            counter: "bytes_hashed",
            observed: 52,
            maximum: 51,
        })
    ));
    assert!(
        cases
            .iter()
            .all(|failure| *failure.work == WorkCounters::default())
    );
    Ok(())
}

#[tokio::test]
async fn inline_content_probe_rejects_overflow_and_eof_before_hashing()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
    let file_id = FileId::from_bytes([1; 16]);
    let record = Some(FileRecord {
        file_id,
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            storage: None,
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([0; 32]),
        },
        payload: FilePayload::InlineRegular(InlineFileData::new(b"abcdefgh")?),
    });
    for (offset, length) in [(u64::MAX, 1), (7, 2)] {
        let failure = probe
            .capture_record_async(
                record,
                &DependencyRegion::ContentRange {
                    file_id,
                    offset,
                    length,
                },
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .err()
            .ok_or("invalid inline content range unexpectedly succeeded")?;
        if offset == u64::MAX {
            assert!(matches!(
                failure.error,
                AuthenticatedProbeError::InvalidDependency(DependencyError::RangeOverflow)
            ));
        } else {
            assert!(matches!(
                failure.error,
                AuthenticatedProbeError::Extent(ExtentReadError::InvalidRange)
            ));
        }
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let exact = probe
        .capture_record_async(
            record,
            &DependencyRegion::ContentRange {
                file_id,
                offset: 4,
                length: 4,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert!(matches!(exact.value, DependencyState::Present(_)));
    assert_eq!(exact.work.backend_read_operations, 0);
    assert!(exact.work.bytes_hashed > 4);
    Ok(())
}

#[tokio::test]
async fn every_sparse_dependency_region_preserves_ordinary_and_native_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let SparseProbeFixture {
        generation,
        root_id,
        inline_id,
        regular_id,
        regular_name,
    } = sparse_probe_fixture(&store).await?;
    let missing_name = LogicalName::new(NameEncoding::Utf8, b"missing".to_vec(), 255)?;
    let regions = vec![
        DependencyRegion::FileRecord(regular_id),
        DependencyRegion::Metadata(regular_id),
        DependencyRegion::FileLength(inline_id),
        DependencyRegion::FileLength(regular_id),
        DependencyRegion::ContentRange {
            file_id: inline_id,
            offset: 2,
            length: 4,
        },
        DependencyRegion::ContentRange {
            file_id: regular_id,
            offset: 0,
            length: 16,
        },
        DependencyRegion::SparseSeek {
            file_id: inline_id,
            offset: 0,
            target: ExtentSeekTarget::Data,
        },
        DependencyRegion::SparseSeek {
            file_id: regular_id,
            offset: 0,
            target: ExtentSeekTarget::Data,
        },
        DependencyRegion::SparseSeek {
            file_id: regular_id,
            offset: 0,
            target: ExtentSeekTarget::Hole,
        },
        DependencyRegion::SparseSeek {
            file_id: regular_id,
            offset: 17,
            target: ExtentSeekTarget::Data,
        },
        DependencyRegion::SparseSeek {
            file_id: regular_id,
            offset: 17,
            target: ExtentSeekTarget::Hole,
        },
        DependencyRegion::FileRecord(FileId::from_bytes([99; 16])),
        DependencyRegion::DirectoryName {
            directory_id: root_id,
            name: regular_name.clone(),
        },
        DependencyRegion::DirectoryName {
            directory_id: root_id,
            name: missing_name,
        },
        DependencyRegion::DirectoryRange {
            directory_id: root_id,
            after: None,
            maximum_entries: 1,
        },
        DependencyRegion::DirectoryRange {
            directory_id: root_id,
            after: Some(regular_name),
            maximum_entries: 1,
        },
    ];
    let mut page_states = Vec::new();
    for region in regions {
        let asynchronous_probe = AuthenticatedGenerationProbe::new(&store, ProbeLimits::default())?;
        let asynchronous = (asynchronous_probe.probe_async(
            generation,
            &region,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))
        .await?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancellation = CancellationToken::new();
            let original =
                crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(error, _)| error)?;
            let baseline = original.live_allocation_bytes();
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let (native_probe, constructor) = AuthenticatedGenerationProbe::new_with_accounting(
                &store,
                ProbeLimits::default(),
                &mut accounting,
            )?;
            let native = (native_probe.probe_async_with_accounting(
                generation,
                &region,
                &mut accounting,
                &cancellation,
            ))
            .await?;
            assert_eq!(native, asynchronous.value);
            native_probe.retire_with_accounting(&mut accounting)?;
            drop(constructor);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        if matches!(region, DependencyRegion::DirectoryRange { .. }) {
            page_states.push(asynchronous.value);
        }
    }
    assert_eq!(page_states.len(), 2);
    assert_ne!(page_states[0], page_states[1]);
    Ok(())
}
