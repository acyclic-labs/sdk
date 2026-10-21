use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn preparation_output<'a>(
    io: Option<&'a PreparationIo>,
    before: u64,
    retained: u64,
) -> Result<Option<crate::conversation::ContentHold<'a>>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let bytes = io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "preparation ownership underflows",
            )
        })?;
        let mut hold = io.guard_owned(bytes)?;
        hold.substitute(retained)?;
        return Ok(Some(hold));
    }
    let _ = (io, before, retained);
    Ok(None)
}

pub(super) async fn load_json_bytes(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    validate_journal(journal, io)?;
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
) -> Result<T> {
    #[cfg(feature = "filesystem")]
    if let Some(hold) = decoded.1 {
        hold.into_retained()?;
    }
    Ok(decoded.0)
}

#[doc(hidden)]
pub fn preparation_digest<T: Serialize>(value: &T, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        return io.canonical_digest(value);
    }
    crate::executor::json::canonical_json_digest(value, u64::MAX, &mut None)
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

#[doc(hidden)]
pub fn decode_json<'a, T: PreparationRecord>(
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
