#[tokio::test]
async fn generated_regular_file_histories_match_independent_logical_and_physical_model()
-> Result<(), Box<dyn std::error::Error>> {
    for history in 0..192_u64 {
        let store = MemoryObjectStore::default();
        let mut seed = history + 1;
        let initial_len = usize::try_from(next_generated(&mut seed) % 65)?;
        let initial = (0..initial_len)
            .map(|_| u8::try_from(next_generated(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let mut model = initial
            .iter()
            .copied()
            .map(ModeledByte::content)
            .collect::<Vec<_>>();
        let mut payload = FilePayload::InlineRegular(InlineFileData::new(&initial)?);
        assert_payload_matches_model(&store, payload, &model).await?;

        for step in 0..32_u64 {
            match next_generated(&mut seed) % 7 {
                0 => {
                    let logical_bytes = usize::try_from(next_generated(&mut seed) % 129)?;
                    payload = apply(
                        &store,
                        payload,
                        RegularMutation::Resize {
                            logical_bytes: u64::try_from(logical_bytes)?,
                        },
                    )
                    .await
                    .map_err(|error| format!("history {history}, step {step}, resize: {error:?}"))?
                    .payload;
                    model.resize(logical_bytes, ModeledByte::HOLE);
                }
                1 => {
                    let offset = usize::try_from(next_generated(&mut seed) % 129)?;
                    let length = usize::try_from(next_generated(&mut seed) % 12 + 1)?;
                    let bytes = (0..length)
                        .map(|_| u8::try_from(next_generated(&mut seed) & 0xff).unwrap_or(0))
                        .collect::<Vec<_>>();
                    let content = blob(&store, &bytes).await?;
                    payload = apply(
                        &store,
                        payload,
                        RegularMutation::Write {
                            offset: u64::try_from(offset)?,
                            length: u64::try_from(length)?,
                            content,
                            content_offset: 0,
                        },
                    )
                    .await
                    .map_err(|error| format!("history {history}, step {step}, write: {error:?}"))?
                    .payload;
                    model_replace(&mut model, offset, &bytes);
                }
                2 | 3 => {
                    let offset = usize::try_from(next_generated(&mut seed) % 129)?;
                    let length = usize::try_from(next_generated(&mut seed) % 16 + 1)?;
                    let allocated = step.is_multiple_of(2);
                    let extend = next_generated(&mut seed).is_multiple_of(2);
                    let old_length = model.len();
                    let requested_end = offset + length;
                    let allocated_is_inline_content = allocated
                        && matches!(payload, FilePayload::InlineRegular(_))
                        && (!extend || offset <= old_length)
                        && (!extend || requested_end.max(old_length) <= MAXIMUM_INLINE_FILE_BYTES);
                    payload = apply(
                        &store,
                        payload,
                        RegularMutation::ZeroRange {
                            offset: u64::try_from(offset)?,
                            length: u64::try_from(length)?,
                            allocated,
                            extend,
                        },
                    )
                    .await
                    .map_err(|error| format!("history {history}, step {step}, zero: {error:?}"))?
                    .payload;
                    model_zero(
                        &mut model,
                        offset,
                        length,
                        allocated,
                        extend,
                        allocated_is_inline_content,
                    );
                }
                4 | 5 => {
                    let offset = usize::try_from(next_generated(&mut seed) % 129)?;
                    let length = usize::try_from(next_generated(&mut seed) % 16 + 1)?;
                    let keep_size = step.is_multiple_of(2);
                    let before = payload;
                    let outcome = apply(
                        &store,
                        payload,
                        RegularMutation::Preallocate {
                            offset: u64::try_from(offset)?,
                            length: u64::try_from(length)?,
                            keep_size,
                        },
                    )
                    .await;
                    if model_preallocate(&mut model, offset, length, keep_size) {
                        payload = outcome?.payload;
                    } else {
                        let failure = outcome
                            .err()
                            .ok_or("keep-size preallocation beyond EOF succeeded")?;
                        assert!(matches!(
                            failure.error,
                            RegularMutationError::KeepSizeBeyondEofUnsupported
                        ));
                        assert_eq!(failure.work.backend_write_operations, 0);
                        assert_eq!(payload, before);
                    }
                }
                _ if !model.is_empty() => {
                    let source_offset =
                        usize::try_from(next_generated(&mut seed) % u64::try_from(model.len())?)?;
                    let maximum_length = model.len() - source_offset;
                    let length = usize::try_from(
                        next_generated(&mut seed) % u64::try_from(maximum_length)? + 1,
                    )?;
                    let destination_offset = usize::try_from(next_generated(&mut seed) % 129)?;
                    let source = model[source_offset..source_offset + length].to_vec();
                    payload = clone_range(
                        &store,
                        payload,
                        u64::try_from(source_offset)?,
                        payload,
                        u64::try_from(destination_offset)?,
                        u64::try_from(length)?,
                    )
                    .await
                    .map_err(|error| format!("history {history}, step {step}, clone: {error:?}"))?
                    .destination;
                    model.resize(
                        model.len().max(destination_offset + length),
                        ModeledByte::HOLE,
                    );
                    model[destination_offset..destination_offset + length].copy_from_slice(&source);
                }
                _ => {}
            }
            assert_payload_matches_model(&store, payload, &model)
                .await
                .map_err(|error| format!("history {history}, step {step}: {error}"))?;
        }
    }
    Ok(())
}
