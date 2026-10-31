//! Original-ledger codecs shared by the approval owner and its retained files.
use crate::{Result, executor::PreparationIo, wire::ErrorCode};
use acyclic_stream::OwnedBytes;

pub(super) fn error(
    io: Option<&PreparationIo>,
    code: ErrorCode,
    reason: &'static str,
) -> crate::Error {
    crate::executor::stock_boundary(io, code, reason)
}

pub(super) fn text(parts: std::fmt::Arguments<'_>, io: Option<&PreparationIo>) -> Result<String> {
    crate::executor::with_history_io(io, |work| crate::executor::preparation_format(parts, work))
}

pub(super) fn copy(text: &str, io: Option<&PreparationIo>) -> Result<String> {
    crate::executor::stock_string_copy(text, io)
}

pub(super) fn step(io: Option<&PreparationIo>, items: u64) -> Result<()> {
    crate::executor::with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
            work.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items })?;
        }
        Ok(())
    })
}

pub(super) fn encode<T: serde::Serialize>(
    value: &T,
    maximum: u64,
    canonical: bool,
    io: Option<&PreparationIo>,
) -> Result<OwnedBytes> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        return io.encode(value, maximum, canonical);
    }
    let _ = io;
    let bytes = if canonical {
        crate::contract::canonical_json_bytes(value)?
    } else {
        serde_json::to_vec(value).map_err(|error| crate::Error::Storage(error.to_string()))?
    };
    super::files::check_bound(bytes.len(), maximum)?;
    Ok(bytes.into())
}

/// Closed approval records have no untyped conversion or copied Value tree.
#[cfg(feature = "filesystem")]
pub(super) trait Record: crate::filesystem::PreparationRecord {}
#[cfg(not(feature = "filesystem"))]
pub(super) trait Record: serde::de::DeserializeOwned + crate::HeapLayout {}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for crate::interaction::Interaction {
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        &crate::executor::json::JsonValueSchema::Interaction
    }
}
impl Record for crate::interaction::Interaction {}
impl Record for super::ApprovalReviewFile {}
impl Record for crate::interaction::ApprovalDecisionFile {}
#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for super::ApprovalReviewFile {}
#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for crate::interaction::ApprovalDecisionFile {}

pub(super) fn decode<T: Record>(bytes: &[u8], io: Option<&PreparationIo>) -> Result<T> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let (value, custody) =
            io.decode(std::marker::PhantomData::<T>, bytes, ErrorCode::Storage)?;
        custody.into_retained()?;
        return Ok(value);
    }
    let _ = io;
    crate::contract::json_from_slice(bytes)
        .map_err(|error| crate::Error::Storage(error.to_string()))
}

pub(super) fn principal_file(principal: &str, io: Option<&PreparationIo>) -> Result<String> {
    crate::executor::with_history_io(io, |work| {
        crate::interaction::ApprovalDecisionFile::principal_file_with_work(principal, work)
    })
}

pub(crate) async fn until_stopped<T>(
    io: Option<&PreparationIo>,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        use std::{future::Future as _, task::Poll};
        io.check_cancellation()?;
        let stopped = io.cancellation().cancelled();
        futures::pin_mut!(operation, stopped);
        return std::future::poll_fn(|cx| {
            if stopped.as_mut().poll(cx).is_ready() {
                return Poll::Ready(io.check_cancellation().and_then(|()| {
                    Err(error(
                        Some(io),
                        ErrorCode::Storage,
                        "approval cancellation event lost its cause",
                    ))
                }));
            }
            let result = operation.as_mut().poll(cx);
            if result.is_ready() {
                io.check_cancellation()?;
            }
            result
        })
        .await;
    }
    let _ = io;
    operation.await
}
