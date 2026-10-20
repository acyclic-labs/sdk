use super::*;

/// Stages one immutable record using the caller's retained encoder buffer.
pub(in crate::store) async fn stage_immutable<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    stream: (StreamPath, u64),
    encoded: Result<Vec<u8>>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let (path, path_owned) = stream;
    let (bytes, owned) =
        match encoded.and_then(|bytes| operations::owned_bytes_from_retained_vec(bytes, work)) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(path);
                return operations::release_owned_after_drop(Err(error), path_owned, work);
            }
        };
    let mut quota = CodecQuota::from_retained(work, owned + path_owned);
    let result = async {
        if let Some(io) = quota.work().as_deref_mut() {
            io.check_cancellation()?;
            if path_owned == 0 {
                io.admit_owned(path.as_str().len() as u64)?;
                io.retain_owned(path.as_str().len() as u64)?;
                quota.adopt(path.as_str().len() as u64)?;
            }
        }
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(path.as_str().len() as u64)?;
        }
        let digest = *blake3::hash(path.as_str().as_bytes()).as_bytes();
        let (key, key_owned) = crate::distributed::digest_key(&digest, quota.work())?;
        let mut request = crate::distributed::commit_request(key, key_owned, quota.work());
        crate::distributed::add_condition(
            &mut request,
            CommitCondition::Absent { path: path.clone() },
        )?;
        crate::distributed::add_record(&mut request, path.clone(), bytes.clone())?;
        let (outcome, received) = crate::distributed::commit(request, client, None, None).await?;
        quota.adopt(received)?;
        if let CommitOutcome::Committed(envelope) = &outcome {
            if let Some(io) = quota
                .work()
                .as_deref_mut()
                .filter(|io| !operations::original_native_decode_mode(*io))
            {
                let slots = envelope.mutations.capacity()
                    * std::mem::size_of::<CommittedMutation>()
                    + envelope
                        .mutations
                        .iter()
                        .map(|mutation| {
                            let records = match mutation {
                                CommittedMutation::Append(append) => &append.records,
                                CommittedMutation::Fork(fork) => &fork.records,
                            };
                            records.capacity() * std::mem::size_of::<acyclic_stream::Record>()
                        })
                        .sum::<usize>();
                io.retain_owned(slots as u64)?;
                quota.adopt(slots as u64)?;
            }
            if !matches!(envelope.mutations.as_slice(),
                [CommittedMutation::Append(append)]
                    if append.path == path && append.start == 0 && append.end == 1
                        && append.tail == 1
                        && matches!(append.records.as_slice(), [record]
                            if record.sequence == 0 && record.value == bytes
                                && record.commit_id == envelope.commit_id))
            {
                return Err(super::super::guarded_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Storage,
                    "immutable record receipt is invalid",
                ));
            }
            return Ok(());
        }
        drop(outcome);
        quota.relinquish(received)?;
        let record = operations::one_record(client, &path, 0, quota.work()).await?;
        let held = record.as_ref().map_or(0, |record| {
            operations::sdk_record_retained_bytes(record, quota.work())
        });
        quota.adopt(held)?;
        if !record.as_ref().is_some_and(|record| record.value == bytes)
            || operations::tail_with_work(client, &path, quota.work()).await? != 1
        {
            return Err(Error::Conflict("immutable record identity differs".into()));
        }
        Ok(())
    }
    .await;
    drop(bytes);
    drop(path);
    result
}

pub(in crate::store) async fn publication_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    reducer: &Reducer,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<operations::IndexedPublication> {
    let mut publication =
        crate::store::effect_heads::publication(client, reducer, event, work).await?;
    let result = async {
        if !event
            .revision
            .is_multiple_of(u64::from(DEFAULT_PROJECTION_EVENTS))
        {
            return Ok(());
        }
        let (snapshot, snapshot_owned) = reducer.publication_snapshot_with_work(event, 1, work)?;
        let encoded = snapshot.encode_canonical_with_work(MAX_PROJECTION_BYTES, work);
        drop(snapshot);
        let bytes = operations::release_owned_after_drop(encoded, snapshot_owned, work)?;
        let mut quota = CodecQuota::from_retained(work, bytes.capacity() as u64);
        let staged = async {
            let location = projection_location(&bytes, &mut quota)?;
            let manifest = crate::store::history::canonical_bytes_with_work(
                &location,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                quota.work(),
            )?;
            let manifest_owned = manifest.capacity() as u64;
            quota.adopt(manifest_owned)?;
            if manifest.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "projection manifest exceeds one record".into(),
                ));
            }
            let mut offset = 0;
            for descriptor in &location.chunks {
                let end = offset + descriptor.bytes as usize;
                let chunk = bytes
                    .get(offset..end)
                    .ok_or_else(|| Error::Invalid("projection chunk boundary is invalid".into()))?;
                offset = end;
                let hash = blake3::Hash::from_bytes(descriptor.digest).to_hex();
                let path = operations::publication_path(
                    reducer.authority(),
                    "harness/v3/projection-chunks/",
                    Some(&hash),
                    quota.work(),
                )?;
                let copied = CodecQuota::produce(quota.work(), |copy| {
                    crate::core::copy_copy_vector(chunk, copy)
                })
                .map(|(bytes, _)| bytes);
                stage_immutable(client, path, copied, quota.work()).await?;
            }
            let path = operations::publication_path(
                reducer.authority(),
                "harness/v3/projection-checkpoints/",
                None,
                quota.work(),
            )?;
            // The publication takes the path and manifest; this quota keeps the snapshot inputs.
            let retained = quota.retained() - manifest_owned;
            quota.transfer();
            quota.adopt(retained)?;
            let manifest = operations::owned_bytes_from_retained_vec(manifest, quota.work());
            publication.add_index(
                path,
                event.revision / u64::from(DEFAULT_PROJECTION_EVENTS) - 1,
                manifest,
                quota.work(),
            )
        }
        .await;
        drop(bytes);
        staged
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

fn projection_location(bytes: &[u8], quota: &mut CodecQuota<'_, '_>) -> Result<ProjectionLocation> {
    if let Some(io) = quota.work().as_deref_mut() {
        io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
            items: bytes.len() as u64,
        })?;
    }
    let mut chunks =
        crate::core::reserve_copy_vector(bytes.len().div_ceil(MIN_PROJECTION_CHUNK_BYTES), quota)?;
    let mut start = 0;
    let mut rolling = 0_u64;
    for (index, byte) in bytes.iter().enumerate() {
        let mut gear = u64::from(*byte).wrapping_add(0x9e37_79b9_7f4a_7c15);
        gear = (gear ^ (gear >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        gear = (gear ^ (gear >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        rolling = (rolling << 1).wrapping_add(gear ^ (gear >> 31));
        let length = index + 1 - start;
        if index + 1 == bytes.len()
            || length == acyclic_stream::MAX_RECORD_BYTES
            || (length >= MIN_PROJECTION_CHUNK_BYTES && rolling & 0x3fff == 0)
        {
            let chunk = bytes
                .get(start..=index)
                .ok_or_else(|| Error::Invalid("projection chunk boundary is invalid".into()))?;
            if let Some(io) = quota.work().as_deref_mut() {
                io.hashed_bytes(chunk.len() as u64)?;
            }
            chunks.push(ProjectionChunk {
                digest: *blake3::hash(chunk).as_bytes(),
                bytes: u32::try_from(chunk.len())
                    .map_err(|_| Error::Invalid("projection chunk exceeds platform".into()))?,
            });
            start = index + 1;
        }
    }
    Ok(ProjectionLocation {
        bytes: bytes.len() as u64,
        chunks,
    })
}
