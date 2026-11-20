//! One record framing projection for native and browser provider cursors.
use futures::{Stream, StreamExt as _};
use prost::Message as _;

/// Encodes records lazily, eliding only the followed path's origin.
pub fn record_frames(
    records: impl Stream<Item = Result<acyclic_stream_contract_core::Record, acyclic_stream_contract_core::StreamError>>,
    path: acyclic_stream_contract_core::StreamPath,
) -> impl Stream<Item = Result<Vec<u8>, acyclic_stream_contract_core::StreamError>> {
    records.map(move |record| {
        record.map(|record| {
            super::read_response_wire(vec![super::record_wire(record, Some(&path))]).encode_to_vec()
        })
    })
}
