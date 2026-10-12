use super::*;

pub(super) async fn load_json_bytes(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "execution journal references non-private JSON content",
        ));
    }
    let bytes = journal.load(reference, io)?.await?;
    with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
    Ok(bytes)
}

pub(crate) async fn load_model_request(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<crate::model::PreparedModelRequest> {
    let bytes = load_json_bytes(journal, reference, io).await?;
    #[cfg(feature = "filesystem")]
    let _bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    crate::model::PreparedModelRequest::decode(&bytes, limits, io)
}

pub(crate) fn retain_preparation<T>(
    decoded: (T, Option<crate::conversation::ContentHold<'_>>),
) -> T {
    #[cfg(feature = "filesystem")]
    if let Some(hold) = decoded.1 {
        hold.into_retained();
    }
    decoded.0
}

#[cfg(feature = "filesystem")]
pub(crate) fn receive_preparation<T>(
    io: &PreparationIo,
    result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, Error>,
) -> Result<T> {
    let held = result
        .as_ref()
        .map_or(0, |receipt| receipt.work.peak_allocation_bytes);
    let value = io.receive_content(result)?;
    io.retain_owned(held)?;
    Ok(value)
}

#[cfg(feature = "filesystem")]
pub(crate) fn reserve_preparation_workspace<'a, T: Serialize>(
    value: &T,
    io: &'a PreparationIo,
) -> Result<crate::filesystem::DecodeReservation<'a>> {
    let before_items = io.receipt().items_examined;
    let _encoded_hold;
    let encoded = io.encode(value, io.remaining()?.bytes_encoded, false)?;
    _encoded_hold = io.guard_owned(encoded.len() as u64)?;
    let items = io
        .receipt()
        .items_examined
        .checked_sub(before_items)
        .ok_or_else(|| Error::Invalid("preparation item accounting underflow".into()))?;
    let slot = [
        std::mem::size_of::<T>() as u64,
        <crate::context::Context as PreparationRecord>::maximum_slot_bytes(),
        std::mem::size_of::<crate::conversation::ConversationMessage>() as u64,
        std::mem::size_of::<crate::context::CompactionReference>() as u64,
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    let bytes = items
        .checked_mul(slot)
        .and_then(|slots| slots.checked_mul(8))
        .and_then(|slots| {
            (encoded.len() as u64)
                .checked_mul(4)
                .and_then(|strings| slots.checked_add(strings))
        })
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<T>() as u64))
        .ok_or_else(|| Error::Invalid("preparation allowance overflow".into()))?;
    io.reserve_decode(bytes)
}

pub(crate) fn preparation_digest<T: Serialize>(
    value: &T,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let _bytes_hold;
        let bytes = io.encode(value, io.remaining()?.bytes_encoded, true)?;
        _bytes_hold = io.guard_bytes(&bytes)?;
        return crate::model::hash_bytes(&bytes, Some(io));
    }
    crate::contract::canonical_json_digest(value, u64::MAX, &mut None)
}

// Values precede their temporary guards.
pub(crate) async fn load_json<'a, T: PreparationRecord>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&'a PreparationIo>,
) -> Result<(T, Option<crate::conversation::ContentHold<'a>>)> {
    let bytes = load_json_bytes(journal, reference, io).await?;
    #[cfg(feature = "filesystem")]
    let _bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    decode_json(&bytes, io)
}

pub(crate) fn decode_json<'a, T: PreparationRecord>(
    bytes: &[u8],
    io: Option<&'a PreparationIo>,
) -> Result<(T, Option<crate::conversation::ContentHold<'a>>)> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        {
            let parsed = io.decode(
                std::marker::PhantomData::<Value>,
                bytes,
                crate::wire::ErrorCode::Storage,
            )?;
            let _canonical_hold;
            let canonical = io.encode(&parsed.0, bytes.len() as u64, true)?;
            _canonical_hold = io.guard_bytes(&canonical)?;
            if canonical.as_ref() != bytes {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "execution journal JSON is not canonical",
                ));
            }
        }
        let (value, hold) = io.decode(
            std::marker::PhantomData::<T>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        return Ok((value, Some(hold)));
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    let parsed: Value = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Storage(format!("execution journal JSON is invalid: {error}")))?;
    if crate::contract::canonical_json_bytes(&parsed)? != bytes {
        return Err(Error::Storage(
            "execution journal JSON is not canonical".into(),
        ));
    }
    let value = crate::contract::json_from_slice(bytes).map_err(|error| {
        Error::Storage(format!("execution journal content is invalid: {error}"))
    })?;
    Ok((value, None))
}
