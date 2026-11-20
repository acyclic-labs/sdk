#[cfg(test)]
mod tests {
    use crate::*;
    use futures::StreamExt as _;
    use prost::Message as _;

    #[tokio::test]
    async fn a_native_host_serves_the_ports_read_and_refuses_a_stale_anchor_at_open()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = MemoryStream::default();
        let path = StreamPath::new("events")?;
        let AppendOutcome::Committed(receipt) = provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![(OwnedBytes::from_static(b"one")).into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?
        else {
            return Err("append was not committed".into());
        };
        let read = wire::ReadRequest {
            path: path.to_string(),
            limit: 1,
            ..Default::default()
        };
        let mut chunks =
            wire_codec::provider_cursor(&provider, "read", &read.encode_to_vec(), &[]).await?;
        let mut decoder = wire_codec::session::FrameDecoder::default();
        let mut frames = Vec::new();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            let mut input = chunk.as_slice();
            while let Some(frame) = decoder.read(&mut input)? {
                frames.push(frame);
            }
        }
        decoder.finish()?;
        assert!(
            frames
                .iter()
                .all(|f| f.channel_id == 1 && f.generation == 1)
        );
        let [opened, data] = frames.as_slice() else {
            return Err(format!("expected Opened and one record, got {}", frames.len()).into());
        };
        assert!(matches!(
            opened.body,
            Some(wire::session_frame::Body::Opened(_))
        ));
        let Some(wire::session_frame::Body::Data(data)) = &data.body else {
            return Err("expected the record".into());
        };
        assert_eq!(data.after.as_ref().map(|after| after.next), Some(1));
        // Negative control: a checked follow whose anchor is not the commit
        // before `from` is refused at open, before any frame is produced.
        let follow = wire::FollowRequest {
            path: path.to_string(),
            from: 1,
            ..Default::default()
        };
        let stale = [42_u8; 32];
        assert_ne!(stale, *receipt.commit_id.as_bytes());
        let refused =
            wire_codec::provider_cursor(&provider, "follow", &follow.encode_to_vec(), &stale)
                .await
                .err();
        assert_eq!(
            refused.map(|error| error.category()),
            Some(StreamCategory::PrefixNotRetained)
        );
        Ok(())
    }
}
