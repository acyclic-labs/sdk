use super::*;

/// Consumes the original encoder buffer and transfers native allocation custody into the SDK carrier before any request alias or authority write.
pub(in crate::store) async fn stage_immutable_vec_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    path: (StreamPath, u64),
    bytes: Result<Vec<u8>>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(error) => {
            let (path, retained) = path;
            drop(path);
            return operations::release_owned_after_drop(Err(error), retained, work);
        }
    };
    let (path, retained) = path;
    let (bytes, external_owned) = match operations::owned_bytes_from_retained_vec(bytes, work) {
        Ok(bytes) => bytes,
        Err(error) => {
            drop(path);
            return operations::release_owned_after_drop(Err(error), retained, work);
        }
    };
    let staged = stage_owned_immutable_record_with_work(client, path, bytes, retained, work).await;
    operations::release_owned_after_drop(staged, external_owned, work)
}

pub(in crate::store) async fn stage_immutable_record_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    path: StreamPath,
    bytes: OwnedBytes,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    stage_owned_immutable_record_with_work(client, path, bytes, 0, work).await
}

pub(super) async fn stage_owned_immutable_record_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    path: StreamPath,
    bytes: OwnedBytes,
    path_retained: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let mut retained = path_retained;
    let result = async {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            if path_retained == 0 {
                observer.admit_owned(path.as_str().len() as u64)?;
                observer.retain_owned(path.as_str().len() as u64)?;
                retained += path.as_str().len() as u64;
            }
            observer.hashed_bytes(path.as_str().len() as u64)?;
        }
        let path_owned = retained;
        let digest = *blake3::hash(path.as_str().as_bytes()).as_bytes();
        let key = if let Some(observer) = work.as_deref_mut() {
            let mut key = Vec::new();
            observer.reserve_bytes(&mut key, 32)?;
            let copied = observer.copy_bytes(32);
            if let Err(error) = copied {
                let capacity = key.capacity() as u64;
                drop(key);
                return operations::release_owned_after_drop(Err(error), capacity, work);
            }
            key.extend_from_slice(&digest);
            let (key, external_owned) = operations::owned_bytes_from_retained_vec(key, work)?;
            retained += external_owned;
            IdempotencyKey::new(key)?
        } else {
            IdempotencyKey::new(OwnedBytes::copy_from_slice(&digest))?
        };
        let native = work
            .as_deref()
            .is_some_and(operations::original_native_decode_mode);
        let (outcome, native_received) = if native {
            let key_owned = retained - path_owned;
            retained -= key_owned;
            let mut request = crate::distributed::commit_request(key, key_owned, work);
            crate::distributed::add_condition(
                &mut request,
                CommitCondition::Absent { path: path.clone() },
            )?;
            crate::distributed::add_record(&mut request, path.clone(), bytes.clone())?;
            crate::distributed::commit(request, client, None, None).await?
        } else {
            let mut conditions = if work.is_none() {
                Vec::with_capacity(1)
            } else {
                Vec::new()
            };
            reserve_chunk_slot(&mut conditions, 1, work)?;
            retained +=
                conditions.capacity() as u64 * std::mem::size_of::<CommitCondition>() as u64;
            conditions.push(CommitCondition::Absent { path: path.clone() });
            let mut records = if work.is_none() {
                Vec::with_capacity(1)
            } else {
                Vec::new()
            };
            reserve_chunk_slot(&mut records, 1, work)?;
            retained += records.capacity() as u64 * std::mem::size_of::<OwnedBytes>() as u64;
            records.push(bytes.clone());
            let mut mutations = if work.is_none() {
                Vec::with_capacity(1)
            } else {
                Vec::new()
            };
            reserve_chunk_slot(&mut mutations, 1, work)?;
            retained += mutations.capacity() as u64 * std::mem::size_of::<CommitMutation>() as u64;
            mutations.push(CommitMutation::Append {
                path: path.clone(),
                records,
            });
            let request = CommitRequest {
                conditions,
                mutations,
                idempotency_key: key,
            };
            if let Some(observer) = work.as_deref_mut() {
                observer.admit_acknowledgment(bytes.len() as u64, 1)?;
                observer.begin_write()?;
            }
            let request_owned = retained - path_owned;
            let outcome = client.commit(request).await.map_err(Error::from);
            let outcome = operations::release_owned_after_drop(outcome, request_owned, work);
            retained -= request_owned;
            (outcome?, 0)
        };
        if let CommitOutcome::Committed(envelope) = outcome {
            let mut received_failure = None;
            let mut received_bytes = 0_u64;
            let mut received_owned = native_received;
            if let Some(observer) = work.as_deref_mut().filter(|_| !native) {
                received_owned = envelope.mutations.capacity() as u64
                    * std::mem::size_of::<CommittedMutation>() as u64;
                received_failure = observer.retain_owned(received_owned).err();
                for mutation in &envelope.mutations {
                    let records = match mutation {
                        CommittedMutation::Append(append) => &append.records,
                        CommittedMutation::Fork(fork) => &fork.records,
                    };
                    let slots = records.capacity() as u64
                        * std::mem::size_of::<acyclic_stream::Record>() as u64;
                    received_owned += slots;
                    let received = observer.retain_owned(slots);
                    if received_failure.is_none() {
                        received_failure = received.err();
                    }
                    for record in records {
                        received_bytes += record.value.len() as u64;
                        let received = observer.receive_record(record.value.len() as u64);
                        if received_failure.is_none() {
                            received_failure = received.err();
                        }
                    }
                }
            }
            let valid = matches!(envelope.mutations.as_slice(),
                [CommittedMutation::Append(append)]
                    if append.path == path && append.start == 0 && append.end == 1
                        && append.tail == 1
                        && matches!(append.records.as_slice(), [record]
                            if record.sequence == 0 && record.value == bytes
                                && record.commit_id == envelope.commit_id));
            let verified = if let Some(error) = received_failure {
                Err(error)
            } else if valid {
                match work.as_deref_mut().filter(|_| !native) {
                    Some(observer) => observer.acknowledged(bytes.len() as u64, 1),
                    None => Ok(()),
                }
            } else {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work
                    .as_deref()
                    .is_some_and(operations::original_native_decode_mode)
                {
                    Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "immutable record receipt is invalid",
                    })
                } else {
                    Err(Error::Storage("immutable record receipt is invalid".into()))
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                {
                    Err(Error::Storage("immutable record receipt is invalid".into()))
                }
            };
            drop(envelope);
            let released = match work.as_deref_mut() {
                Some(observer) => {
                    let records = observer.release_owned(received_bytes);
                    let slots = observer.release_owned(received_owned);
                    records.and(slots)
                }
                None => Ok(()),
            };
            return match verified {
                Ok(()) => released,
                Err(error) => Err(error),
            };
        }
        drop(outcome);
        if let Some(observer) = work.as_deref_mut() {
            observer.release_owned(native_received)?;
        }
        let record = operations::one_record(client, &path, 0, work).await?;
        let identical = record.as_ref().is_some_and(|record| record.value == bytes);
        let received_bytes = record.as_ref().map_or(0, |record| {
            operations::sdk_record_retained_bytes(record, work)
        });
        drop(record);
        let verified = if identical {
            Ok(())
        } else {
            Err(Error::Conflict("immutable record identity differs".into()))
        };
        let released = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(received_bytes),
            None => Ok(()),
        };
        match verified {
            Ok(()) => released?,
            Err(error) => return Err(error),
        }
        let tail = operations::tail_with_work(client, &path, work).await?;
        if tail == 1 {
            Ok(())
        } else {
            Err(Error::Conflict("immutable record identity differs".into()))
        }
    }
    .await;
    drop(bytes);
    drop(path);
    operations::release_owned_after_drop(result, retained, work)
}

pub(in crate::store) async fn publication_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    reducer: &Reducer,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<operations::IndexedPublication> {
    let mut publication = operations::IndexedPublication::new(
        reducer.authority(),
        event,
        crate::wire_codec::encode_event(reducer.authority(), event, work)?,
        work,
    )?;
    let result = async {
        crate::store::effects::add_publication_index_with_work(
            client,
            reducer,
            event,
            &mut publication,
            work,
        )
        .await?;
        if !event
            .revision
            .is_multiple_of(u64::from(DEFAULT_PROJECTION_EVENTS))
        {
            return Ok(());
        }
        let (snapshot, snapshot_reservation) =
            reducer.publication_snapshot_with_work(event, 1, work)?;
        let encoded =
            crate::store::history::canonical_bytes_with_work(&snapshot, MAX_PROJECTION_BYTES, work);
        drop(snapshot);
        let bytes = operations::release_owned_after_drop(encoded, snapshot_reservation, work)?;
        let source_capacity = bytes.capacity() as u64;
        let mut descriptors = Vec::new();
        let mut location = None;
        let mut manifest = Vec::new();
        let staged = async {
            if let Some(observer) = work.as_deref_mut() {
                observer.hashed_bytes(bytes.len() as u64)?;
            }
            let digest = *blake3::hash(&bytes).as_bytes();
            descriptors = {
                let mut quota = crate::wire_codec::CodecQuota::from_retained(work, 0);
                let descriptors = crate::core::reserve_copy_vector(
                    bytes.len().div_ceil(acyclic_stream::MAX_RECORD_BYTES),
                    &mut quota,
                )?;
                quota.transfer();
                descriptors
            };
            for chunk in bytes.chunks(acyclic_stream::MAX_RECORD_BYTES) {
                let length = u32::try_from(chunk.len())
                    .map_err(|_| Error::Invalid("projection chunk exceeds platform".into()))?;
                if let Some(observer) = work.as_deref_mut() {
                    observer.hashed_bytes(chunk.len() as u64)?;
                }
                descriptors.push(ProjectionChunk {
                    digest: *blake3::hash(chunk).as_bytes(),
                    bytes: length,
                });
            }
            let authority = match work.as_deref_mut() {
                Some(observer) => observer.clone_authority(reducer.authority())?,
                None => reducer.authority().clone(),
            };
            location = Some(ProjectionLocation {
                authority,
                revision: event.revision,
                bytes: bytes.len() as u64,
                digest,
                chunks: std::mem::take(&mut descriptors),
            });
            let location = location.as_ref().expect("original projection location");
            manifest = crate::store::history::canonical_bytes_with_work(
                location,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )?;
            if manifest.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "projection manifest exceeds one record".into(),
                ));
            }
            for (descriptor, chunk) in location
                .chunks
                .iter()
                .zip(bytes.chunks(acyclic_stream::MAX_RECORD_BYTES))
            {
                let path = if work.is_none() {
                    (chunk_path(&location.authority, descriptor)?, 0)
                } else {
                    let hash = blake3::Hash::from_bytes(descriptor.digest).to_hex();
                    operations::publication_path(
                        &location.authority,
                        "harness/v3/projection-chunks/",
                        Some((&hash, hash.len())),
                        work,
                    )?
                };
                let mut copied = Vec::new();
                let copied_result = (|| {
                    match work.as_deref_mut() {
                        Some(observer) => observer.reserve_bytes(&mut copied, chunk.len())?,
                        None => copied.reserve_exact(chunk.len()),
                    }
                    if let Some(observer) = work.as_deref_mut() {
                        observer.copy_bytes(chunk.len() as u64)?;
                    }
                    copied.extend_from_slice(chunk);
                    Ok(())
                })();
                let copied = match copied_result {
                    Ok(()) => Ok(copied),
                    Err(error) => {
                        let capacity = copied.capacity() as u64;
                        drop(copied);
                        operations::release_owned_after_drop(Err(error), capacity, work)
                    }
                };
                stage_immutable_vec_with_work(client, path, copied, work).await?;
            }
            let path = if work.is_none() {
                (head_path(reducer.authority())?, 0)
            } else {
                operations::publication_path(
                    reducer.authority(),
                    "harness/v3/projection-checkpoints/",
                    None,
                    work,
                )?
            };
            publication.add_derived_index_with_work(
                path,
                event.revision / u64::from(DEFAULT_PROJECTION_EVENTS) - 1,
                Ok(std::mem::take(&mut manifest)),
                work,
            )
        }
        .await;
        let location_owned = location.as_ref().map_or(0, |location| {
            location.authority.id.capacity() as u64
                + location.chunks.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64
        });
        let owned = source_capacity
            + manifest.capacity() as u64
            + location_owned
            + descriptors.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64;
        drop(manifest);
        drop(location);
        drop(descriptors);
        drop(bytes);
        operations::release_owned_after_drop(staged, owned, work)
    }
    .await;
    match result {
        Ok(()) => Ok(publication),
        Err(error) => {
            let owned = publication.retained_owned();
            drop(publication);
            operations::release_owned_after_drop(Err(error), owned, work)
        }
    }
}
