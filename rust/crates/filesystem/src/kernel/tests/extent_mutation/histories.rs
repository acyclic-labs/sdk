use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ByteKind {
    Hole,
    AllocatedZero,
    Content { object: ObjectId, offset: u64 },
}

fn bytes_to_extents(bytes: &[ByteKind]) -> Result<Vec<Extent>, ExtentMutationError> {
    let mut extents = Vec::new();
    for (index, byte) in bytes.iter().enumerate() {
        let offset = u64::try_from(index).map_err(|_| ExtentMutationError::RangeOverflow)?;
        let kind = match byte {
            ByteKind::Hole => ExtentKind::Hole,
            ByteKind::AllocatedZero => ExtentKind::AllocatedZero,
            ByteKind::Content { object, offset } => ExtentKind::Content {
                object: *object,
                object_offset: *offset,
            },
        };
        extents.push(Extent {
            offset,
            length: 1,
            kind,
        });
    }
    coalesce(&mut extents)?;
    Ok(extents)
}

fn extents_to_bytes(extents: &[Extent]) -> Result<Vec<ByteKind>, ExtentMutationError> {
    let mut bytes = Vec::new();
    for extent in extents {
        for relative in 0..extent.length {
            bytes.push(match extent.kind {
                ExtentKind::Hole => ByteKind::Hole,
                ExtentKind::AllocatedZero => ByteKind::AllocatedZero,
                ExtentKind::Content {
                    object,
                    object_offset,
                } => ByteKind::Content {
                    object,
                    offset: object_offset
                        .checked_add(relative)
                        .ok_or(ExtentMutationError::RangeOverflow)?,
                },
            });
        }
    }
    Ok(bytes)
}

async fn build_extent_tree(
    store: &MemoryObjectStore,
    extents: &[Extent],
    width: usize,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    if extents.is_empty() {
        return put(store, &ExtentPage::Leaf(Vec::new()), u32::try_from(width)?).await;
    }
    let mut level = Vec::new();
    for chunk in extents.chunks(width) {
        level.push(ExtentChild {
            first_offset: chunk[0].offset,
            end_offset: extent_end(&chunk[chunk.len() - 1])?,
            page: put(
                store,
                &ExtentPage::Leaf(chunk.to_vec()),
                u32::try_from(width)?,
            )
            .await?,
        });
    }
    while level.len() > 1 {
        let mut parent = Vec::new();
        for chunk in level.chunks(width) {
            parent.push(ExtentChild {
                first_offset: chunk[0].first_offset,
                end_offset: chunk[chunk.len() - 1].end_offset,
                page: put(
                    store,
                    &ExtentPage::Internal(chunk.to_vec()),
                    u32::try_from(width)?,
                )
                .await?,
            });
        }
        level = parent;
    }
    Ok(level[0].page)
}

async fn flatten_extent_tree(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let bytes = {
        let __object_args = (
            root,
            u64::MAX,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?
    .value;
    Ok(
        match decode_extent_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })? {
            ExtentPage::Leaf(extents) => extents,
            ExtentPage::Internal(children) => {
                let mut extents = Vec::new();
                for child in children {
                    extents.extend(Box::pin(flatten_extent_tree(store, child.page)).await?);
                }
                extents
            }
        },
    )
}

fn apply_reference(bytes: &mut Vec<ByteKind>, mutation: &ExtentMutation) {
    match mutation {
        ExtentMutation::Resize { logical_bytes } => {
            bytes.resize(
                usize::try_from(*logical_bytes).unwrap_or(usize::MAX),
                ByteKind::Hole,
            );
        }
        ExtentMutation::Replace {
            offset,
            length,
            kind,
            ..
        } => {
            let start = usize::try_from(*offset).unwrap_or(usize::MAX);
            let end = usize::try_from(offset.saturating_add(*length)).unwrap_or(usize::MAX);
            if start > bytes.len() {
                bytes.resize(start, ByteKind::Hole);
            }
            bytes.resize(bytes.len().max(end), ByteKind::Hole);
            for (relative, byte) in bytes[start..end].iter_mut().enumerate() {
                *byte = match kind {
                    ExtentKind::Hole => ByteKind::Hole,
                    ExtentKind::AllocatedZero => ByteKind::AllocatedZero,
                    ExtentKind::Content {
                        object,
                        object_offset,
                    } => ByteKind::Content {
                        object: *object,
                        offset: object_offset
                            .saturating_add(u64::try_from(relative).unwrap_or(u64::MAX)),
                    },
                };
            }
        }
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

#[tokio::test]
async fn generated_sparse_histories_match_independent_byte_model()
-> Result<(), Box<dyn std::error::Error>> {
    let blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([9; 32]),
    };
    for history in 0..256_u64 {
        let store = MemoryObjectStore::default();
        let mut seed = history.saturating_add(1);
        let initial_size = usize::try_from(next(&mut seed) % 33)?;
        let mut expected = Vec::with_capacity(initial_size);
        for index in 0..initial_size {
            expected.push(match next(&mut seed) % 3 {
                0 => ByteKind::Hole,
                1 => ByteKind::AllocatedZero,
                _ => ByteKind::Content {
                    object: blob,
                    offset: u64::try_from(index)?,
                },
            });
        }
        let root = build_extent_tree(&store, &bytes_to_extents(&expected)?, 4).await?;
        let old_size = u64::try_from(expected.len())?;
        let mutation_count = usize::try_from(next(&mut seed) % 8 + 1)?;
        let mut mutations = Vec::with_capacity(mutation_count);
        for _ in 0..mutation_count {
            let mutation = if next(&mut seed).is_multiple_of(4) {
                ExtentMutation::Resize {
                    logical_bytes: next(&mut seed) % 65,
                }
            } else {
                let offset = next(&mut seed) % 65;
                let length = next(&mut seed) % 16 + 1;
                let kind = match next(&mut seed) % 3 {
                    0 => ExtentKind::Hole,
                    1 => ExtentKind::AllocatedZero,
                    _ => ExtentKind::Content {
                        object: blob,
                        object_offset: next(&mut seed) % 128,
                    },
                };
                ExtentMutation::Replace {
                    offset,
                    length,
                    kind,
                    extend: true,
                }
            };
            apply_reference(&mut expected, &mutation);
            mutations.push(mutation);
        }
        let limits = DecodeLimits {
            maximum_page_items: 4,
            ..DecodeLimits::default()
        };
        let receipt = apply_extent_mutations(
            &store,
            (root, old_size),
            &mutations,
            8,
            limits,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(receipt.logical_bytes, u64::try_from(expected.len())?);
        assert_eq!(
            extents_to_bytes(&flatten_extent_tree(&store, receipt.root).await?)?,
            expected,
            "history {history} diverged"
        );
    }
    Ok(())
}

#[test]
fn patch_admission_and_coalescing_reject_every_boundary_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let empty = admit_patch_plan(
        8,
        &[ExtentMutation::Replace {
            offset: 0,
            length: 0,
            kind: ExtentKind::Hole,
            extend: false,
        }],
        &mut accounting,
    )
    .err()
    .ok_or("empty replacement unexpectedly admitted")?;
    assert!(matches!(
        project_extent_error(empty),
        ExtentMutationError::EmptyRange
    ));

    let overflow = admit_patch_plan(
        u64::MAX,
        &[ExtentMutation::Replace {
            offset: u64::MAX,
            length: 1,
            kind: ExtentKind::AllocatedZero,
            extend: true,
        }],
        &mut accounting,
    )
    .err()
    .ok_or("overflowing replacement unexpectedly admitted")?;
    assert!(matches!(
        project_extent_error(overflow),
        ExtentMutationError::RangeOverflow
    ));

    assert!(!equivalent_continuation(&hole(0, 1), &hole(2, 1))?);
    assert!(equivalent_continuation(&hole(0, 1), &hole(1, 1))?);
    assert!(equivalent_continuation(&allocated(0, 1), &allocated(1, 1))?);
    assert!(!equivalent_continuation(&hole(0, 1), &allocated(1, 1))?);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_extent_compilation_denial_restores_original_scratch_before_backend()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &ExtentPage::Leaf(vec![hole(0, 16)]), 8).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let mutation = ExtentMutation::Replace {
        offset: 4,
        length: 4,
        kind: ExtentKind::AllocatedZero,
        extend: false,
    };
    let denied = (apply_extent_mutations_with_accounting(
        &store,
        (root, 16),
        &[mutation.clone(), mutation.clone()],
        2,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("unadmitted radix copy succeeded")?;
    assert!(matches!(
        denied,
        FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)) | FsError::Work(_)
    ));
    assert_eq!(original.receipt().bytes_copied, 0);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);

    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let receipt = (apply_extent_mutations_with_accounting(
        &store,
        (root, 16),
        &[mutation],
        1,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    ))
    .await?;
    let read = {
        let __object_args = (
            receipt.root,
            u64::MAX,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert_eq!(
        decode_extent_page(&read.value, DecodeLimits::default(), &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))?,
        ExtentPage::Leaf(vec![hole(0, 4), allocated(4, 4), hole(8, 8)])
    );
    assert!(original.receipt().backend_read_operations > 0);
    assert_eq!(original.receipt().page_writes, 1);
    assert_eq!(original.receipt().backend_write_operations, 1);
    drop(read);
    let cloned = (crate::kernel::regular_mutation::clone_extent_range_with_accounting(
        &store,
        crate::kernel::ExtentCloneRequest {
            source_root: receipt.root,
            source_logical_bytes: 16,
            source_range: crate::ByteRange {
                offset: 2,
                length: 8,
            },
            destination_root: receipt.root,
            destination_logical_bytes: 16,
            destination_offset: 4,
            maximum_spans: 8,
            maximum_mutations: 8,
        },
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    ))
    .await?;
    let read = {
        let __object_args = (
            cloned.root,
            u64::MAX,
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert_eq!(
        decode_extent_page(&read.value, DecodeLimits::default(), &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(()))?,
        ExtentPage::Leaf(vec![hole(0, 6), allocated(6, 4), hole(10, 6)])
    );
    assert_eq!(original.receipt().page_writes, 2);
    assert_eq!(original.receipt().backend_write_operations, 2);
    drop(read);
    drop(store);
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}
