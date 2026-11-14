use crate::{Error, wire};
use acyclic_stream::{StreamError, wire_codec::session::FrameDecoder};
use prost::Message as _;

pub(crate) fn decoder(maximum: Option<usize>) -> FrameDecoder<wire::AvailabilityCursor> {
    FrameDecoder::with_decoder(
        maximum.map_or(acyclic_stream::MAX_COMMAND_BYTES, |maximum| {
            maximum.min(acyclic_stream::MAX_COMMAND_BYTES)
        }),
        |bytes| {
            wire::AvailabilityCursor::decode(bytes)
                .map_err(|source| StreamError::InvalidResponse.with_source(source))
        },
    )
}

pub(crate) fn failure(error: StreamError) -> Error {
    let code = match error.category() {
        StreamError::LimitExceeded => wire::ErrorCode::QuotaExceeded,
        _ => wire::ErrorCode::Unavailable,
    };
    let mut failure = Error::from(code);
    failure.source = Some(error);
    failure
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_availability_frame_retains_quota_and_original_cause() {
        let original = decoder(Some(1)).read(&mut [2].as_slice()).unwrap_err();
        assert_eq!(original.category(), StreamError::LimitExceeded);
        let error = failure(original.clone());
        assert_eq!(error.code, wire::ErrorCode::QuotaExceeded);
        let source = std::error::Error::source(&error).unwrap();
        assert_eq!(source.downcast_ref::<StreamError>(), Some(&original));
        assert!(source.source().is_some());
    }
}
