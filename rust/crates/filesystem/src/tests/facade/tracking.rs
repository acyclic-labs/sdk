use super::*;

#[tokio::test]
async fn tracking_reads_capture_only_their_terminal_regions() -> TestResult {
    #[cfg(not(target_arch = "wasm32"))]
    mutation_subsumption_cost_and_custody()?;
    for observation in 0..3 {
        let cancel = CancellationToken::new();
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        create(
            &mut seed,
            "file",
            OwnedBytes::from(vec![b'a'; 128]),
            &cancel,
        )
        .await?;
        commit(&mut seed, &cancel).await?;
        let file = path("file")?;
        let mut reader = open_checkout(&volume, tracking(), &cancel).await?;
        match observation {
            0 => {
                reader
                    .read_file_range(
                        &file,
                        ByteRange {
                            offset: 0,
                            length: 8,
                        },
                        B,
                        &cancel,
                    )
                    .await?;
            }
            1 => {
                reader.read_metadata(&file, B, &cancel).await?;
            }
            _ => {
                reader.lookup_no_follow(&file, B, &cancel).await?;
            }
        }
        let mut writer = open_checkout(&volume, writable_pinned(), &cancel).await?;
        if observation == 0 {
            writer
                .set_attributes(
                    file,
                    FileMetadata {
                        modified_ns: MetadataField::Value(7),
                        ..FileMetadata::default()
                    },
                    None,
                    B,
                    &cancel,
                )
                .await?;
        } else {
            let (offset, bytes) = if observation == 1 {
                (64, b"changed".as_slice())
            } else {
                (96, b"different".as_slice())
            };
            writer
                .change_content(
                    file,
                    ContentChange::Write {
                        offset,
                        bytes: OwnedBytes::copy_from_slice(bytes),
                    },
                    ContentTimes::Preserve,
                    B,
                    &cancel,
                )
                .await?;
        }
        commit(&mut writer, &cancel).await?;
        let rebase = reader.rebase_head(8, B, &cancel).await?.value;
        if observation == 2 {
            assert!(
                matches!(rebase, RebaseDecision::Conflicted { conflicts, .. }
                if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::FileRecord(_))))
            );
        } else {
            assert!(matches!(rebase, RebaseDecision::Safe { .. }));
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn mutation_subsumption_cost_and_custody() -> TestResult {
    use crate::kernel::KernelAccounting;
    use crate::kernel::allocation::retained::RetainedAllocation;
    type MemoryCheckout = Checkout<crate::MemoryAuthorityStore, crate::MemoryObjectStore>;
    type Region = (Option<FileRecord>, DependencyRegion);
    let bytes = std::mem::size_of::<Region>() as u64;
    let mut counts = Vec::new();
    for population in [1_u64, 100_000] {
        let mut regions = Vec::new();
        let mut expected = Vec::new();
        // Reverse identities exercise the old insertion shifts. Narrow regions
        // alternate with survivors, exercising the old repeated suffix moves.
        for ordinal in (1..=population).rev() {
            let id = FileId::from_bytes(u128::from(ordinal).to_be_bytes());
            regions.push((None, DependencyRegion::FileLength(id)));
            regions.push((None, DependencyRegion::FileRecord(id)));
            regions.push((
                None,
                DependencyRegion::ContentRange {
                    file_id: id,
                    offset: ordinal,
                    length: 1,
                },
            ));
            regions.push((None, DependencyRegion::Metadata(id)));
            regions.push((
                None,
                DependencyRegion::SparseSeek {
                    file_id: id,
                    offset: ordinal,
                    target: ExtentSeekTarget::Hole,
                },
            ));
            expected.push((None, DependencyRegion::FileRecord(id)));
            expected.push((None, DependencyRegion::Metadata(id)));
        }
        let bound = acyclic_collections::FixedIndex::<(), RetainedAllocation, 16>::insertion_bound(
            population,
        )
        .ok_or("mutation membership bound overflow")?;
        let budget = WorkBudget {
            items_examined: bound.examined_items + population * (3 * 145 + 10),
            bytes_copied: 3 * regions.len() as u64 * bytes,
            allocation_operations: 2 * population + 8,
            peak_allocation_bytes: bound.allocation_bytes + 65_536,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let cancel = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(budget, &cancel).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancel);
        let prior = RetainedAllocation::claim(29, 1, &mut accounting)?;
        let retained = original.live_allocation_bytes();
        let before = accounting.spent();
        let capacity = regions.capacity();
        MemoryCheckout::subsume_mutation_record_dependencies(&mut regions, &mut accounting)?;
        let spent = accounting.spent();
        assert_eq!(regions, expected);
        assert_eq!(regions.capacity(), capacity);
        assert!(spent.bytes_copied - before.bytes_copied <= budget.bytes_copied);
        assert!(spent.items_examined - before.items_examined <= budget.items_examined);
        assert_eq!(original.live_allocation_bytes(), retained);
        drop(prior);
        assert_eq!(original.live_allocation_bytes(), baseline);
        counts.push((
            spent.items_examined - before.items_examined,
            spent.bytes_copied - before.bytes_copied,
        ));
    }
    eprintln!("mutation subsumption input=[1,100000] examinations/copy-bytes={counts:?}");

    // Full-width branch coordinates, duplicate records, uncovered narrow
    // regions and owned names share the same original pass and reference rule.
    let ids: Vec<_> = (0..128)
        .map(|bit| FileId::from_bytes((1_u128 << bit).to_be_bytes()))
        .collect();
    let mut input = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        input.push((None, DependencyRegion::FileLength(*id)));
        if index % 2 == 0 {
            input.push((None, DependencyRegion::FileRecord(*id)));
            input.push((None, DependencyRegion::FileRecord(*id)));
        }
    }
    input.push((
        None,
        DependencyRegion::DirectoryName {
            directory_id: ids[0],
            name: LogicalName::new(NameEncoding::Utf8, b"retained-name".to_vec(), 255)?,
        },
    ));
    let mut expected = input.clone();
    expected.retain(|(_, region)| !matches!(region, DependencyRegion::FileLength(id) if input.iter().any(|(_, candidate)| matches!(candidate, DependencyRegion::FileRecord(record) if record == id))));
    let cancel = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    MemoryCheckout::subsume_mutation_record_dependencies(&mut input, &mut accounting)?;
    assert_eq!(input, expected);

    let id = ids[0];
    let input = vec![
        (None, DependencyRegion::FileLength(id)),
        (None, DependencyRegion::FileRecord(id)),
        (
            None,
            DependencyRegion::DirectoryName {
                directory_id: id,
                name: LogicalName::new(NameEncoding::Utf8, b"owner".to_vec(), 255)?,
            },
        ),
        (None, DependencyRegion::FileLength(ids[1])),
    ];
    let mut successful = input.clone();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    MemoryCheckout::subsume_mutation_record_dependencies(&mut successful, &mut accounting)?;
    let spent = accounting.spent();
    // Every examined-item refusal and the first denied swap keep all original
    // owners. No new work is needed to retire the private membership index.
    for allowance in 0..spent.items_examined {
        let budget = WorkBudget {
            items_examined: allowance,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let original =
            crate::NativeMaterializationWork::new(budget, &cancel).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancel);
        let mut regions = input.clone();
        assert!(
            MemoryCheckout::subsume_mutation_record_dependencies(&mut regions, &mut accounting)
                .is_err()
        );
        assert_eq!(regions.len(), input.len());
        assert!(input.iter().all(|entry| regions.contains(entry)));
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    for budget in [
        WorkBudget {
            bytes_copied: spent.bytes_copied - 1,
            ..crate::performance::BOUNDARY_BUDGET
        },
        WorkBudget {
            allocation_operations: 0,
            ..crate::performance::BOUNDARY_BUDGET
        },
    ] {
        let mut regions = input.clone();
        let mut accounting = KernelAccounting::standalone(budget, &cancel);
        assert!(
            MemoryCheckout::subsume_mutation_record_dependencies(&mut regions, &mut accounting)
                .is_err()
        );
        assert_eq!(regions.len(), input.len());
        assert!(input.iter().all(|entry| regions.contains(entry)));
    }
    cancel.cancel();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    assert!(matches!(
        MemoryCheckout::subsume_mutation_record_dependencies(&mut Vec::new(), &mut accounting),
        Err(FsError::Cancelled(_))
    ));
    Ok(())
}

#[tokio::test]
async fn sparse_seek_tracks_local_holes_and_remote_boundary_changes() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    create(
        &mut seed,
        "sparse",
        OwnedBytes::from(vec![b'a'; 256]),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let file = path("sparse")?;
    let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
    local
        .change_content(
            file.clone(),
            ContentChange::ZeroRange {
                range: ByteRange {
                    offset: 128,
                    length: 16,
                },
                allocated: false,
                extend: false,
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        local
            .seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancel)
            .await?
            .value,
        Some(128)
    );
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    remote
        .change_content(
            file.clone(),
            ContentChange::ZeroRange {
                range: ByteRange {
                    offset: 0,
                    length: 64,
                },
                allocated: false,
                extend: false,
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut remote, &cancel).await?;
    assert!(matches!(
        local.rebase_head(8, B, &cancel).await?.value,
        RebaseDecision::Safe { .. }
    ));
    assert_eq!(
        local
            .seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancel)
            .await?
            .value,
        Some(128)
    );
    let mut reader = open_checkout(&volume, tracking(), &cancel).await?;
    assert_eq!(
        reader
            .seek_file_extent(&file, 128, ExtentSeekTarget::Hole, B, &cancel)
            .await?
            .value,
        Some(256)
    );
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    remote
        .change_content(
            file,
            ContentChange::ZeroRange {
                range: ByteRange {
                    offset: 192,
                    length: 16,
                },
                allocated: false,
                extend: false,
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut remote, &cancel).await?;
    assert!(
        matches!(reader.rebase_head(8, B, &cancel).await?.value, RebaseDecision::Conflicted { conflicts, .. }
        if conflicts.iter().any(|conflict| matches!(conflict.region,
            DependencyRegion::SparseSeek { offset: 128, target: ExtentSeekTarget::Hole, .. })))
    );
    Ok(())
}

#[tokio::test]
async fn clipped_identity_read_at_eof_tracks_file_length() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let id = create(&mut seed, "file", OwnedBytes::from_static(b"abc"), &cancel).await?;
    commit(&mut seed, &cancel).await?;
    let mut reader = open_checkout(&volume, tracking(), &cancel).await?;
    assert!(
        reader
            .read_file_up_to(id, 3, 16, B, &cancel)
            .await?
            .value
            .bytes
            .is_empty()
    );
    let mut writer = open_checkout(&volume, writable_pinned(), &cancel).await?;
    writer
        .change_content(
            path("file")?,
            ContentChange::Write {
                offset: 3,
                bytes: OwnedBytes::from_static(b"d"),
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut writer, &cancel).await?;
    assert!(
        matches!(reader.rebase_head(8, B, &cancel).await?.value, RebaseDecision::Conflicted { conflicts, .. }
        if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::FileLength(actual) if actual == id)))
    );
    Ok(())
}

#[tokio::test]
async fn sparse_local_writes_rebase_across_disjoint_remote_ranges_and_conflict_on_overlap()
-> TestResult {
    sparse_rebase_across_remote_ranges(false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_native_rebase_preserves_disjoint_ranges_and_is_atomic_on_denial_and_conflict()
-> TestResult {
    sparse_rebase_across_remote_ranges(true).await
}

#[tokio::test]
async fn repeated_single_and_batch_lookups_preserve_binding_conflicts() -> TestResult {
    for (batch, authored) in [(false, true), (true, false), (true, true)] {
        let cancel = CancellationToken::new();
        let (volume, _) = fixture(config(), &cancel).await?;
        let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
        if authored {
            create(
                &mut local,
                "authored",
                OwnedBytes::from_static(b"local"),
                &cancel,
            )
            .await?;
        }
        let paths = [path("first")?, path("second")?];
        for _ in 0..2 {
            if batch {
                assert!(
                    local
                        .lookup_batch_no_follow(&paths, B, &cancel)
                        .await?
                        .value
                        .entries
                        .iter()
                        .all(|entry| entry.record.is_none())
                );
            } else {
                assert!(
                    local
                        .lookup_no_follow_with_metadata(&paths[1], B, &cancel)
                        .await?
                        .value
                        .is_none()
                );
            }
        }
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        create(
            &mut remote,
            "second",
            OwnedBytes::from_static(b"remote"),
            &cancel,
        )
        .await?;
        commit(&mut remote, &cancel).await?;
        assert!(matches!(
            local.rebase_head(8, B, &cancel).await?.value,
            RebaseDecision::Conflicted { .. }
        ));
    }
    Ok(())
}
