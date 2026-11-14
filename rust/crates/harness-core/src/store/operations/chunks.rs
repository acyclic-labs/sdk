//! Immutable byte prefixes shared by diagnostic and indexed-content writers.
//! The caller publishes its reference only after every bounded chunk is durable.
use super::*;
use crate::executor::PreparationIo;
use acyclic_stream::{AppendOutcome, AppendRecord, AppendRequest, IdempotencyKey};

pub(crate) async fn stage_chunks<P, F>(
    client: &Arc<P>,
    path: &StreamPath,
    bytes: &[u8],
    io: Option<&PreparationIo>,
    key: F,
) -> Result<()>
where
    P: crate::store::JournalProvider + ?Sized,
    F: Fn(u64) -> Result<IdempotencyKey> + Send + Sync,
{
    for (index, chunk) in bytes.chunks(acyclic_stream::MAX_RECORD_BYTES).enumerate() {
        let sequence = index as u64;
        let value = crate::model::vec_with_capacity(chunk.len(), io)?;
        let hold = io
            .map(|io| io.guard_owned(value.capacity() as u64))
            .transpose()?;
        let mut value = (value, hold);
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                bytes_copied: chunk.len() as u64,
                ..Default::default()
            })?;
        }
        value.0.extend_from_slice(chunk);
        let mut history = io.map(PreparationIo::history_io);
        let mut observer = history.as_mut().map(|work| work as &mut dyn HistoryIo);
        let (value, hold) = value;
        if let Some(hold) = hold {
            hold.into_retained()?;
        }
        let (value, retained) = owned_bytes_from_retained_vec(value, &mut observer)?;
        let _value_hold = io.map(|io| io.guard_owned(retained)).transpose()?;
        let idempotency_key = key(sequence)?;
        let _key_hold = io
            .map(|io| io.guard_owned(idempotency_key.as_bytes().len() as u64))
            .transpose()?;
        let mut records = crate::model::vec_with_capacity(1, io)?;
        let _records_hold = io
            .map(|io| io.guard_owned(std::mem::size_of::<AppendRecord>() as u64))
            .transpose()?;
        records.push(AppendRecord {
            value,
            headers: Default::default(),
        });
        let outcome = append_with_work(
            client,
            AppendRequest {
                path: path.clone(),
                records,
                if_tail: Some(sequence),
                idempotency_key,
            },
            &mut observer,
        )
        .await?;
        if matches!(&*outcome, AppendOutcome::TailConflict { .. }) {
            // The retry-key retention window may have elapsed. Only the
            // original immutable bytes can complete this already-staged cut.
            let existing = one_record(client, path, sequence, &mut observer).await?;
            if existing
                .as_ref()
                .is_none_or(|record| record.value.as_ref() != chunk)
            {
                return Err(crate::executor::stock_boundary(
                    io,
                    Storage,
                    "immutable content chunk differs",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) async fn read_chunks<'a, P: crate::store::JournalProvider + ?Sized>(
    client: &Arc<P>,
    path: &StreamPath,
    length: u64,
    digest: &[u8; 32],
    io: &'a PreparationIo,
) -> Result<(
    OwnedBytes,
    crate::filesystem::bounded_io::DecodeReservation<'a>,
)> {
    let length = usize::try_from(length).map_err(|_| {
        crate::executor::stock_boundary(Some(io), Storage, "content length overflows")
    })?;
    let bytes = crate::model::vec_with_capacity(length, Some(io))?;
    let hold = io.guard_owned(bytes.capacity() as u64)?;
    let mut owned = (bytes, hold);
    let mut history = io.history_io();
    let mut observer = Some(&mut history as &mut dyn HistoryIo);
    let mut sequence = 0;
    while owned.0.len() < length {
        let record = one_record(client, path, sequence, &mut observer)
            .await?
            .ok_or_else(|| {
                crate::executor::stock_boundary(Some(io), Storage, "content chunk is missing")
            })?;
        let expected = (length - owned.0.len()).min(acyclic_stream::MAX_RECORD_BYTES);
        if record.value.len() != expected {
            return Err(crate::executor::stock_boundary(
                Some(io),
                Storage,
                "content chunk length differs",
            ));
        }
        io.charge(acyclic_fs::WorkCounters {
            bytes_copied: expected as u64,
            ..Default::default()
        })?;
        owned.0.extend_from_slice(&record.value);
        sequence += 1;
    }
    if &crate::model::hash_bytes(&owned.0, Some(io))? != digest {
        return Err(crate::executor::stock_boundary(
            Some(io),
            Storage,
            "content digest differs",
        ));
    }
    let (bytes, hold) = owned;
    hold.into_retained()?;
    let (bytes, retained) = owned_bytes_from_retained_vec(bytes, &mut observer)?;
    Ok((bytes, io.guard_owned(retained)?))
}
