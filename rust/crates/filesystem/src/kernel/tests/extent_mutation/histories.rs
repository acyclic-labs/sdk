use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ByteKind {
    Hole,
    AllocatedZero,
    Content { object: ObjectId, offset: u64 },
}

fn bytes_to_extents(bytes: &[ByteKind]) -> Result<Vec<Extent>, ExtentMutationError> {
    let mut extents = Vec::with_capacity(bytes.len());
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
        push_joined(
            &mut extents,
            Extent {
                offset,
                length: 1,
                kind,
            },
        )?;
    }
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
            first: chunk[0].offset,
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
                first: chunk[0].first,
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
    let bytes = store
        .read(
            root,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
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

fn apply_reference(
    bytes: &mut Vec<ByteKind>,
    mutation: &ExtentMutation,
) -> Result<(), ExtentMutationError> {
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
            extend,
        } => {
            if *length == 0 {
                return Err(ExtentMutationError::EmptyRange);
            }
            if let ExtentKind::Content {
                object,
                object_offset,
            } = kind
            {
                if object.kind != ObjectKind::Blob {
                    return Err(ExtentMutationError::WrongContentKind);
                }
                object_offset
                    .checked_add(*length)
                    .ok_or(ExtentMutationError::RangeOverflow)?;
            }
            let end = offset
                .checked_add(*length)
                .ok_or(ExtentMutationError::RangeOverflow)?;
            let start = usize::try_from(*offset).map_err(|_| ExtentMutationError::RangeOverflow)?;
            let end = usize::try_from(end).map_err(|_| ExtentMutationError::RangeOverflow)?;
            if !extend && end > bytes.len() {
                return Err(ExtentMutationError::OutsideFile);
            }
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
                            .checked_add(
                                u64::try_from(relative)
                                    .map_err(|_| ExtentMutationError::RangeOverflow)?,
                            )
                            .ok_or(ExtentMutationError::RangeOverflow)?,
                    },
                };
            }
        }
    }
    Ok(())
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
        storage: None,
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([9; 32]),
    };
    let mut refusal_mismatches = [false; 5];
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
                let extend = expected.is_empty() || next(&mut seed) >> 63 != 0;
                let size = u64::try_from(expected.len())?;
                let offset = next(&mut seed) % if extend { 65 } else { size };
                let length =
                    next(&mut seed) % if extend { 16 } else { (size - offset).min(16) } + 1;
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
                    extend,
                }
            };
            apply_reference(&mut expected, &mutation)?;
            mutations.push(mutation);
        }
        for reject in [false, true] {
            // Refused variants exercise each admission cause before storage.
            let refusal = usize::try_from(history % 5)?;
            let expected_admission = if reject {
                let (offset, length, kind, extend) = match refusal {
                    0 => (
                        u64::try_from(expected.len())?,
                        1,
                        ExtentKind::AllocatedZero,
                        false,
                    ),
                    1 => (u64::MAX, 0, ExtentKind::Hole, false),
                    2 => (u64::MAX, 1, ExtentKind::AllocatedZero, true),
                    3 => (
                        0,
                        1,
                        ExtentKind::Content {
                            object: ObjectId {
                                kind: ObjectKind::Metadata,
                                ..blob
                            },
                            object_offset: u64::MAX,
                        },
                        false,
                    ),
                    _ => (
                        0,
                        1,
                        ExtentKind::Content {
                            object: blob,
                            object_offset: u64::MAX,
                        },
                        true,
                    ),
                };
                let mutation = ExtentMutation::Replace {
                    offset,
                    length,
                    kind,
                    extend,
                };
                let outcome = apply_reference(&mut expected, &mutation);
                mutations.push(mutation);
                outcome
            } else {
                Ok(())
            };
            let limits = DecodeLimits {
                maximum_page_items: 4,
                ..DecodeLimits::default()
            };
            let result = apply_extent_mutations(
                &store,
                (root, old_size),
                &mutations,
                limits,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await;
            if let Err(expected_error) = expected_admission {
                let failure = result.expect_err("extent admission differs from byte model");
                refusal_mismatches[refusal] |= std::mem::discriminant(&failure.error)
                    != std::mem::discriminant(&expected_error);
                assert_eq!(failure.work.items_examined, u64::try_from(mutations.len())?);
                assert_eq!(failure.work.allocation_operations, 0);
                assert_eq!(failure.work.peak_allocation_bytes, 0);
                assert_eq!(failure.work.backend_read_operations, 0);
                assert_eq!(failure.work.backend_write_operations, 0);
                assert_eq!(failure.work.object_bytes_read, 0);
                assert_eq!(failure.work.object_bytes_written, 0);
                continue;
            }
            let receipt = result?;
            assert_eq!(receipt.logical_bytes, u64::try_from(expected.len())?);
            assert_eq!(
                extents_to_bytes(&flatten_extent_tree(&store, receipt.root).await?)?,
                expected,
                "history {history} diverged"
            );
        }
    }
    assert_eq!(
        refusal_mismatches, [false; 5],
        "extent refusal causes differ from byte model"
    );
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
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    ))
    .await?;
    let read = store
        .read(
            receipt.root,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    assert_eq!(
        decode_extent_page(&read, DecodeLimits::default(), &mut |_| Ok::<
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
            maximum_spans: Some(8),
        },
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    ))
    .await?;
    let read = store
        .read(
            cloned.root,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    assert_eq!(
        decode_extent_page(&read, DecodeLimits::default(), &mut |_| Ok::<
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

#[path = "canonical_history.rs"]
mod canonical_history;
