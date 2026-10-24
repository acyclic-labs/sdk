use super::*;
use crate::{filesystem::PreparationIo, store::HistoryIo};

impl crate::filesystem::PreparationRecord for Header {}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    pub(super) async fn open_native(
        provider: Arc<P>,
        path: StreamPath,
        io: &PreparationIo,
    ) -> crate::Result<Self> {
        let work = io
            .original_native_work()
            .ok_or(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task-volume replay requires original native work",
            })?;
        let tail = initialize_header(&provider, &path, io).await?;
        let mut observer = io;
        let mut history = Some(&mut observer as &mut dyn HistoryIo);
        let mut records = crate::store::operations::read_records(
            &provider,
            &path,
            0,
            u32::try_from(tail.max(1).min(acyclic_stream::MAX_ITEMS as u64))
                .map_err(|_| Error::InvalidSegment)?,
            &mut history,
        )
        .await?
        .map_err(Error::from)?;
        let record = records
            .next(&mut history)
            .await?
            .ok_or(Error::ResetRequired)?;
        if record.sequence != 0 {
            return Err(Error::InvalidSegment.into());
        }
        let (header, _header_hold) = io.decode::<Header>(
            std::marker::PhantomData,
            &record.value,
            crate::wire::ErrorCode::Storage,
        )?;
        if !matches!(header.schema, 2 | 3 | SCHEMA) {
            return Err(Error::InvalidSegment.into());
        }
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: (path.as_str().len() + header.volume.as_bytes().len()) as u64,
            ..Default::default()
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(path.as_str().as_bytes());
        hasher.update(header.volume.as_bytes());
        let mut index = Index {
            volume: *hasher.finalize().as_bytes(),
            epoch: header.epoch,
            next: 1,
            commit: record.commit_id,
            ..Index::default()
        };
        drop(_header_hold);
        drop(record);
        // Freeze one admitted tail. Records appended after entry belong to
        // subsequent refreshes, rather than extending this reopen indefinitely.
        let tail = tail.max(1);
        while index.next < tail {
            let Some(record) = records.next(&mut history).await? else {
                records = crate::store::operations::read_records(
                    &provider,
                    &path,
                    index.next - 1,
                    u32::try_from((tail - index.next + 1).min(acyclic_stream::MAX_ITEMS as u64))
                        .map_err(|_| Error::InvalidSegment)?,
                    &mut history,
                )
                .await?
                .map_err(Error::from)?;
                let anchor = records
                    .next(&mut history)
                    .await?
                    .ok_or(Error::ResetRequired)?;
                if anchor.sequence != index.next - 1 || anchor.commit_id != index.commit {
                    return Err(Error::ResetRequired.into());
                }
                let record = records
                    .next(&mut history)
                    .await?
                    .ok_or(Error::ResetRequired)?;
                replay(&mut index, &record, io)?;
                continue;
            };
            replay(&mut index, &record, io)?;
        }
        Ok(Self {
            provider,
            path,
            index: Mutex::new(index),
        })
    }
}

async fn initialize_header<P: StreamProvider + ?Sized>(
    provider: &Arc<P>,
    path: &StreamPath,
    io: &PreparationIo,
) -> crate::Result<u64> {
    let work = io
        .original_native_work()
        .ok_or(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "task-volume replay requires original native work",
        })?;
    let tail = crate::distributed::native_call_result(
        work,
        provider
            .native_tail(path.clone(), work.runtime_meter())
            .await,
    )?;
    let tail = match tail {
        Ok(tail) => *tail.value(),
        Err(StreamError::NotFound) => 0,
        Err(error) => return Err(Error::from(error).into()),
    };
    if tail == 0 {
        let header = io.encode(
            &Header {
                volume: crate::identity::fresh_uuid(),
                epoch: crate::identity::fresh_uuid(),
                schema: SCHEMA,
            },
            acyclic_stream::MAX_RECORD_BYTES as u64,
            false,
        )?;
        crate::distributed::native_call_result(
            work,
            provider
                .native_append(
                    AppendRequest {
                        path: path.clone(),
                        records: vec![header.into()],
                        if_tail: Some(0),
                        idempotency_key: None,
                    },
                    work.runtime_meter(),
                )
                .await,
        )?
        .map_err(Error::from)?;
    }
    Ok(tail)
}

fn replay(
    index: &mut Index,
    record: &acyclic_stream::Record,
    io: &PreparationIo,
) -> crate::Result<()> {
    if record.sequence != index.next {
        return Err(Error::ResetRequired.into());
    }
    io.charge(acyclic_fs::WorkCounters {
        items_examined: 1,
        ..Default::default()
    })?;
    let (file, metadata) = segment::decode_native(&record.value, io)?;
    let existing = index
        .files
        .get(&file.key)
        .map(|stored| stored.native_metadata_bytes);
    let key = file.key.clone();
    let owner = command_query::owner(&key);
    let before = slots(index, &key, &owner);
    validate_file(index, &file)?;
    // Logical entries in the primary and secondary indexes. Components share
    // their Arc payloads; only InputPath has a deep-copying primary map key.
    let entries = std::mem::size_of::<(FileKey, StoredFile)>()
        + std::mem::size_of::<((Option<Component>, u64), FileKey)>()
        + std::mem::size_of::<(Option<Component>, std::collections::BTreeSet<FileKey>)>()
        + std::mem::size_of::<FileKey>()
        + std::mem::size_of::<((Component, u8, u64), FileKey)>()
        + std::mem::size_of::<(Component, FileKey)>()
        + std::mem::size_of::<(
            Component,
            BTreeMap<Component, std::collections::BTreeSet<FileKey>>,
        )>()
        + std::mem::size_of::<(Component, std::collections::BTreeSet<FileKey>)>()
        + std::mem::size_of::<(Component, Vec<FileKey>)>()
        + std::mem::size_of::<FileKey>();
    let vector = child_capacity(index, &key)
        .saturating_mul(2)
        .max(4)
        .checked_mul(std::mem::size_of::<FileKey>())
        .ok_or(Error::LimitExceeded)?;
    let reserved = entries.checked_add(vector).ok_or(Error::LimitExceeded)? as u64;
    io.retain_owned(reserved)?;
    let next = index.next.checked_add(1).ok_or(Error::LimitExceeded)?;
    apply(index, file, next);
    index.next = next;
    index.commit = record.commit_id;
    let after = slots(index, &key, &owner);
    let unused = reserved
        .checked_add(before)
        .and_then(|n| n.checked_sub(after))
        .ok_or(Error::LimitExceeded)?;
    io.release(unused)?;
    let retained = metadata.bytes();
    io.absorb_native_custody(metadata)?;
    if let Some(previous) = existing {
        // Keep the original map key's metadata, and only the latest replacement
        // metadata. All custody stays in the one shared binding, not per file.
        io.release(previous)?;
        match index.files.entry(key) {
            std::collections::btree_map::Entry::Occupied(mut stored) => {
                stored.get_mut().native_metadata_bytes = retained;
            }
            std::collections::btree_map::Entry::Vacant(_) => {
                return Err(Error::InvalidSegment.into());
            }
        }
    }
    Ok(())
}

fn child_capacity(index: &Index, key: &FileKey) -> usize {
    match key {
        FileKey::Child { reference, .. } => {
            index.child_inputs.get(reference).map_or(0, Vec::capacity)
        }
        _ => 0,
    }
}

fn slots(index: &Index, key: &FileKey, owner: &Option<Component>) -> u64 {
    (index.files.len() * std::mem::size_of::<(FileKey, StoredFile)>()
        + index.commands.native_index_bytes(owner)
        + index.views.native_index_bytes()
        + index.approvals.native_index_bytes()
        + index.child_inputs.len() * std::mem::size_of::<(Component, Vec<FileKey>)>()
        + child_capacity(index, key) * std::mem::size_of::<FileKey>()) as u64
}
