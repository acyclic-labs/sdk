use crate::{Error, wire};
use acyclic_stream::{StreamError, wire_codec::session::FrameDecoder};
use prost::Message as _;

pub(crate) fn decoder(maximum: Option<usize>) -> FrameDecoder<wire::AvailabilityCursor> {
    FrameDecoder::with_decoder(
        maximum.map_or(acyclic_stream::MAX_COMMAND_BYTES, |maximum| {
            maximum.min(acyclic_stream::MAX_COMMAND_BYTES)
        }),
        |bytes| wire::AvailabilityCursor::decode(bytes).map_err(|_| StreamError::InvalidResponse),
    )
}

pub(crate) fn failure(error: StreamError) -> Error {
    match error {
        StreamError::LimitExceeded => wire::ErrorCode::QuotaExceeded.into(),
        _ => wire::ErrorCode::Unavailable.into(),
    }
}
