//! Byte framing vectors use records returned by the real public memory provider.
use acyclic_stream::{
    AppendRequest, IdempotencyKey, MemoryStream, OwnedBytes, ReadRequest, StreamError, StreamPath,
    StreamProvider, wire, wire_codec,
};
use futures::TryStreamExt as _;
use prost::Message as _;
use wire_codec::session::FrameDecoder;

async fn frames() -> Result<Vec<wire::SessionFrame>, StreamError> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("framing/records")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![
                OwnedBytes::from(vec![0x5a; 4096]).into(),
                OwnedBytes::from_static(b"after-large-record").into(),
            ],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(b"framing-records".to_vec())?,
        })
        .await?;
    let records = provider
        .read(ReadRequest {
            path,
            from: 0,
            limit: 2,
            at_least: Default::default(),
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(records
        .into_iter()
        .map(|record| wire::SessionFrame {
            channel_id: 1,
            generation: 1,
            body: Some(wire::session_frame::Body::Data(wire::SessionData {
                after: Some(wire::SessionCursor {
                    next: record.sequence + 1,
                    opaque: Default::default(),
                }),
                accepted: record.sequence + 1,
                durable: 0,
                item: Some(wire::session_data::Item::Record(wire_codec::record_wire(
                    record,
                ))),
            })),
        })
        .collect())
}

#[tokio::test]
async fn provider_frames_survive_split_prefixes_bodies_and_coalesced_messages()
-> Result<(), StreamError> {
    let expected = frames().await?;
    let bytes = expected
        .iter()
        .flat_map(|frame| frame.encode_length_delimited_to_vec())
        .collect::<Vec<_>>();
    for chunk_size in [1, 2, 4096, bytes.len()] {
        let mut decoder = FrameDecoder::default();
        let mut observed = Vec::new();
        for chunk in bytes.chunks(chunk_size) {
            let mut remaining = chunk;
            while let Some(frame) = decoder.read(&mut remaining)? {
                observed.push(frame);
            }
            assert!(remaining.is_empty());
        }
        decoder.finish()?;
        assert_eq!(observed, expected);
    }
    Ok(())
}

#[tokio::test]
async fn producer_loss_and_malformed_lengths_never_accept_a_partial_frame()
-> Result<(), StreamError> {
    let bytes = frames().await?.remove(0).encode_length_delimited_to_vec();
    for end in [1, 2, 4096, bytes.len() - 1] {
        let mut decoder = FrameDecoder::default();
        let mut partial = &bytes[..end];
        assert_eq!(decoder.read(&mut partial)?, None);
        assert_eq!(decoder.finish(), Err(StreamError::InvalidResponse));
    }
    for malformed in [vec![0x80; 10], vec![0x80, 0]] {
        assert_eq!(
            FrameDecoder::default().read(&mut malformed.as_slice()),
            Err(StreamError::InvalidResponse)
        );
    }
    let mut excessive = Vec::new();
    prost::encode_length_delimiter(acyclic_stream::MAX_COMMAND_BYTES + 1, &mut excessive)
        .map_err(|_| StreamError::InvalidArgument)?;
    assert_eq!(
        FrameDecoder::default().read(&mut excessive.as_slice()),
        Err(StreamError::LimitExceeded)
    );
    Ok(())
}
