use super::*;

#[tokio::test]
async fn generated_generation_histories_match_independent_namespace_and_identity_model()
-> Result<(), Box<dyn std::error::Error>> {
    let names = std::iter::once("a".to_owned())
        .chain((0..8).map(|index| format!("f{index}")))
        .collect::<Vec<_>>();
    for history in 0..96_u64 {
        let store = MemoryObjectStore::default();
        let fixture = fixture(&store).await?;
        let mut generation = fixture.generation.clone();
        let mut model = GenerationModel::fixture(fixture.metadata);
        let mut seed = history + 1;
        let mut created_identity = 10_u8;
        assert_generation_matches_model(&store, &generation, &model, &names).await?;

        for step in 0..40_u64 {
            let existing = model.bindings.keys().cloned().collect::<Vec<_>>();
            let absent = names
                .iter()
                .filter(|candidate| !model.bindings.contains_key(*candidate))
                .cloned()
                .collect::<Vec<_>>();
            let mut operation_kind = generated_next(&mut seed) % 8;
            if existing.is_empty() {
                operation_kind = 0;
            } else if absent.is_empty() && matches!(operation_kind, 0 | 1) {
                operation_kind = 3;
            }
            let mut next_model = model.clone();
            let operation = match operation_kind {
                0 => {
                    let destination = selected_name(&absent, &mut seed).to_owned();
                    let file_id = FileId::from_bytes([created_identity; 16]);
                    created_identity = created_identity
                        .checked_add(1)
                        .ok_or("generated identity space exhausted")?;
                    let initial = vec![
                        u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0);
                        usize::try_from(generated_next(&mut seed) % 4 + 1)?
                    ];
                    next_model.bindings.insert(destination.clone(), file_id);
                    next_model.bytes.insert(file_id, initial.clone());
                    next_model.identities_seen.insert(file_id);
                    Mutation::Create {
                        path: path(&[&destination])?,
                        record: FileRecord {
                            file_id,
                            kind: FileKind::Regular,
                            link_count: 1,
                            metadata: fixture.metadata,
                            payload: FilePayload::InlineRegular(InlineFileData::new(&initial)?),
                        },
                    }
                }
                1 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let destination = selected_name(&absent, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    next_model.bindings.insert(destination.clone(), file_id);
                    Mutation::Link {
                        source: path(&[&source])?,
                        destination: path(&[&destination])?,
                    }
                }
                2 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let destinations = names
                        .iter()
                        .filter(|candidate| **candidate != source)
                        .cloned()
                        .collect::<Vec<_>>();
                    let destination = selected_name(&destinations, &mut seed).to_owned();
                    let source_id = model.bindings[&source];
                    let destination_id = model.bindings.get(&destination).copied();
                    if destination_id != Some(source_id) {
                        next_model.bindings.remove(&source);
                        next_model.bindings.insert(destination.clone(), source_id);
                        if let Some(replaced) = destination_id {
                            next_model.prune(replaced);
                        }
                    }
                    Mutation::Rename {
                        source: path(&[&source])?,
                        destination: path(&[&destination])?,
                        replace: destination_id.is_some(),
                    }
                }
                3 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    next_model.bindings.remove(&source);
                    next_model.prune(file_id);
                    Mutation::Remove {
                        path: path(&[&source])?,
                        expected_file_id: MetadataField::Value(file_id),
                    }
                }
                4 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    let current = next_model
                        .bytes
                        .get(&file_id)
                        .ok_or("modeled resize identity missing")?
                        .len();
                    let logical_bytes =
                        usize::try_from(generated_next(&mut seed) % u64::try_from(current + 1)?)?;
                    next_model
                        .bytes
                        .get_mut(&file_id)
                        .ok_or("modeled resize identity missing")?
                        .resize(logical_bytes, 0);
                    Mutation::Resize {
                        path: path(&[&source])?,
                        logical_bytes: u64::try_from(logical_bytes)?,
                    }
                }
                5 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    let current = next_model
                        .bytes
                        .get(&file_id)
                        .ok_or("modeled write identity missing")?
                        .len();
                    let maximum_offset = current.min(MAXIMUM_INLINE_FILE_BYTES - 1);
                    let offset = usize::try_from(
                        generated_next(&mut seed) % u64::try_from(maximum_offset + 1)?,
                    )?;
                    let maximum_length = (MAXIMUM_INLINE_FILE_BYTES - offset).min(4);
                    let length = usize::try_from(
                        generated_next(&mut seed) % u64::try_from(maximum_length)? + 1,
                    )?;
                    let content = (0..length)
                        .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
                        .collect::<Vec<_>>();
                    let content_root = blob(&store, &content).await?;
                    let bytes = next_model
                        .bytes
                        .get_mut(&file_id)
                        .ok_or("modeled write identity disappeared")?;
                    bytes.resize(bytes.len().max(offset + length), 0);
                    bytes[offset..offset + length].copy_from_slice(&content);
                    Mutation::Write {
                        path: path(&[&source])?,
                        offset: u64::try_from(offset)?,
                        length: u64::try_from(length)?,
                        content: content_root,
                        content_offset: 0,
                    }
                }
                6 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    let current = next_model
                        .bytes
                        .get(&file_id)
                        .ok_or("modeled identity resize missing")?
                        .len();
                    let logical_bytes =
                        usize::try_from(generated_next(&mut seed) % u64::try_from(current + 1)?)?;
                    next_model
                        .bytes
                        .get_mut(&file_id)
                        .ok_or("modeled identity resize missing")?
                        .resize(logical_bytes, 0);
                    Mutation::File {
                        file_id,
                        mutation: FileMutation::Resize {
                            logical_bytes: u64::try_from(logical_bytes)?,
                        },
                    }
                }
                _ => {
                    let nonempty = existing
                        .iter()
                        .filter(|candidate| {
                            let file_id = model.bindings[*candidate];
                            !model.bytes[&file_id].is_empty()
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if nonempty.is_empty() {
                        continue;
                    }
                    let source = selected_name(&nonempty, &mut seed).to_owned();
                    let destination = selected_name(&existing, &mut seed).to_owned();
                    let source_id = model.bindings[&source];
                    let destination_id = model.bindings[&destination];
                    let source_bytes = model.bytes[&source_id].clone();
                    let source_offset = usize::try_from(
                        generated_next(&mut seed) % u64::try_from(source_bytes.len())?,
                    )?;
                    let length = usize::try_from(
                        generated_next(&mut seed)
                            % u64::try_from((source_bytes.len() - source_offset).min(4))?
                            + 1,
                    )?;
                    let destination_bytes = next_model
                        .bytes
                        .get_mut(&destination_id)
                        .ok_or("modeled clone destination missing")?;
                    let maximum_destination = destination_bytes
                        .len()
                        .min(MAXIMUM_INLINE_FILE_BYTES - length);
                    let destination_offset = usize::try_from(
                        generated_next(&mut seed) % u64::try_from(maximum_destination + 1)?,
                    )?;
                    let snapshot = source_bytes[source_offset..source_offset + length].to_vec();
                    destination_bytes
                        .resize(destination_bytes.len().max(destination_offset + length), 0);
                    destination_bytes[destination_offset..destination_offset + length]
                        .copy_from_slice(&snapshot);
                    Mutation::CloneRange {
                        source: path(&[&source])?,
                        source_offset: u64::try_from(source_offset)?,
                        destination: path(&[&destination])?,
                        destination_offset: u64::try_from(destination_offset)?,
                        length: u64::try_from(length)?,
                    }
                }
            };
            let receipt = apply_generation_mutations(
                &store,
                &generation,
                vec![operation.clone()],
                config(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|error| {
                format!("history {history}, step {step}, operation {operation:?}: {error:?}")
            })?;
            generation = receipt.root;
            model = next_model;
            assert_generation_matches_model(&store, &generation, &model, &names)
                .await
                .map_err(|error| format!("history {history}, step {step}: {error}"))?;
        }

        assert_eq!(
            inline_at(&store, &fixture.generation, "a").await?,
            b"payload"
        );
    }
    Ok(())
}

#[tokio::test]
async fn generated_multi_operation_transactions_observe_the_candidate_in_order()
-> Result<(), Box<dyn std::error::Error>> {
    let names = ["a", "x", "y", "alias", "z", "large"]
        .map(str::to_owned)
        .to_vec();
    let use_before_create_store = MemoryObjectStore::default();
    let use_before_create_fixture = fixture(&use_before_create_store).await?;
    let use_before_create_id = FileId::from_bytes([13; 16]);
    let use_before_create = apply_generation_mutations(
        &use_before_create_store,
        &use_before_create_fixture.generation,
        vec![
            Mutation::File {
                file_id: use_before_create_id,
                mutation: FileMutation::ValidateRegular,
            },
            Mutation::Create {
                path: path(&["later"])?,
                record: regular(use_before_create_id, use_before_create_fixture.metadata)?,
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("identity use before its ordered create unexpectedly succeeded")?;
    assert!(matches!(
        use_before_create.error,
        GenerationMutationError::MissingSource
    ));
    assert_eq!(use_before_create.work.backend_write_operations, 0);
    for history in 0..128_u64 {
        let store = MemoryObjectStore::default();
        let fixture = fixture(&store).await?;
        let mut seed = history + 1;
        let x_id = FileId::from_bytes([10; 16]);
        let y_id = FileId::from_bytes([11; 16]);
        let z_id = FileId::from_bytes([12; 16]);
        let large_id = FileId::from_bytes([14; 16]);
        let x_initial = (0..4)
            .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let y_initial = (0..4)
            .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let z_initial = (0..4)
            .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let patch = (0..2)
            .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let patch_root = blob(&store, &patch).await?;
        let large_patch = [
            u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0),
            u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0),
        ];
        let large_patch_root = blob(&store, &large_patch).await?;
        let operations = vec![
            Mutation::Create {
                path: path(&["x"])?,
                record: FileRecord {
                    file_id: x_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(&x_initial)?),
                },
            },
            Mutation::Create {
                path: path(&["y"])?,
                record: FileRecord {
                    file_id: y_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(&y_initial)?),
                },
            },
            Mutation::Link {
                source: path(&["x"])?,
                destination: path(&["alias"])?,
            },
            Mutation::Write {
                path: path(&["alias"])?,
                offset: 1,
                length: 2,
                content: patch_root,
                content_offset: 0,
            },
            Mutation::Rename {
                source: path(&["x"])?,
                destination: path(&["alias"])?,
                replace: true,
            },
            Mutation::Rename {
                source: path(&["x"])?,
                destination: path(&["y"])?,
                replace: true,
            },
            Mutation::File {
                file_id: x_id,
                mutation: FileMutation::Resize { logical_bytes: 3 },
            },
            Mutation::CloneFileRange {
                source_file_id: x_id,
                source_offset: 0,
                destination_file_id: x_id,
                destination_offset: 1,
                length: 2,
            },
            Mutation::Create {
                path: path(&["z"])?,
                record: FileRecord {
                    file_id: z_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(&z_initial)?),
                },
            },
            Mutation::CloneFileRange {
                source_file_id: x_id,
                source_offset: 0,
                destination_file_id: z_id,
                destination_offset: 1,
                length: 3,
            },
            Mutation::Create {
                path: path(&["large"])?,
                record: FileRecord {
                    file_id: large_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(b"seed")?),
                },
            },
            Mutation::Write {
                path: path(&["large"])?,
                offset: 63,
                length: 2,
                content: large_patch_root,
                content_offset: 0,
            },
            Mutation::Remove {
                path: path(&["alias"])?,
                expected_file_id: MetadataField::Value(x_id),
            },
            Mutation::ValidateRegular {
                path: path(&["y"])?,
            },
        ];
        let receipt = apply_generation_mutations(
            &store,
            &fixture.generation,
            operations,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|error| format!("history {history}: {error:?}"))?;

        let mut expected_x = x_initial;
        expected_x[1..3].copy_from_slice(&patch);
        expected_x.truncate(3);
        let x_snapshot = expected_x[..2].to_vec();
        expected_x[1..3].copy_from_slice(&x_snapshot);
        let mut expected_z = z_initial;
        expected_z[1..4].copy_from_slice(&expected_x);
        let mut expected_large = b"seed".to_vec();
        expected_large.resize(65, 0);
        expected_large[63..65].copy_from_slice(&large_patch);
        let model = GenerationModel {
            bindings: BTreeMap::from([
                ("a".to_owned(), FileId::from_bytes([2; 16])),
                ("y".to_owned(), x_id),
                ("z".to_owned(), z_id),
                ("large".to_owned(), large_id),
            ]),
            bytes: BTreeMap::from([
                (FileId::from_bytes([2; 16]), b"payload".to_vec()),
                (x_id, expected_x),
                (z_id, expected_z),
                (large_id, expected_large),
            ]),
            identities_seen: BTreeSet::from([
                FileId::from_bytes([2; 16]),
                x_id,
                y_id,
                z_id,
                large_id,
            ]),
            metadata: fixture.metadata,
        };
        assert_generation_matches_model(&store, &receipt.root, &model, &names)
            .await
            .map_err(|error| format!("history {history}: {error}"))?;
        assert_eq!(
            inline_at(&store, &fixture.generation, "a").await?,
            b"payload"
        );
    }
    Ok(())
}
