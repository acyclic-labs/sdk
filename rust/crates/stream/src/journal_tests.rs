pub use acyclic_stream_contract::journal::*;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;
    use crate::OwnedBytes as Bytes;
    use crate::*;
    use crate::{AppendRequest, CommitEffect, CommitRequest, ForkRequest, StreamProviderExt};

    #[test]
    fn current_writer_roundtrips_and_rejects_noncanonical_payloads() -> Result<(), StreamError> {
        let append = WriteRequest::Append(AppendRequest {
            path: crate::StreamPath::new("codec/events")?,
            records: vec![(Bytes::from_static(b"one")).into(), (Bytes::new()).into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::new(Bytes::from_static(b"key"))?,
        });
        let fork = WriteRequest::Fork(ForkRequest {
            exclude: Vec::new(),
            source: crate::StreamPath::new("codec/events")?,
            destination: crate::StreamPath::new("codec/fork")?,
            at_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        });
        let commit = WriteRequest::Commit(CommitRequest {
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(
                vec![crate::CommitCondition::Tail {
                    path: crate::StreamPath::new("codec/events")?,
                    expected: 2,
                }],
                vec![crate::CommitMutation::Append {
                    path: crate::StreamPath::new("codec/events")?,
                    records: vec![(Bytes::from_static(b"two")).into()],
                }],
            )],
            idempotency_key: crate::IdempotencyKey::new(Bytes::from_static(b"commit"))?,
        });
        for command in [append, fork, commit] {
            let encoded = command.encode_to_vec();
            assert_eq!(decode_command(&encoded)?.encode_to_vec(), encoded);
            for end in 0..encoded.len() {
                assert!(decode_command(&encoded[..end]).is_err());
            }
            let mut previous = encoded.clone();
            previous[b"ACYCLIC-STREAM-COMMAND-V".len()] = b'1';
            assert!(matches!(
                decode_command(&previous),
                Err(error) if error.category() == crate::StreamCategory::Unsupported
            ));
            let mut unknown = encoded.clone();
            unknown.extend_from_slice(&[0x78, 0]);
            assert!(decode_command(&unknown).is_err());
            let mut duplicate = encoded.clone();
            duplicate.extend_from_slice(&encoded);
            assert!(decode_command(&duplicate).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn native_journal_matches_the_browser_correspondence_fixture() -> Result<(), StreamError>
    {
        let provider = MemoryStream::default();
        let request = AppendRequest {
            path: crate::StreamPath::new("events")?,
            records: vec![(Bytes::from_static(&[1])).into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::new(Bytes::from_static(b"first"))?,
        };
        let encoded = WriteRequest::Append(request.clone()).encode_to_vec();
        replay(&provider, decode_command(&encoded)?).await?;
        let crate::AppendOutcome::Committed(receipt) = provider.append(request).await? else {
            return Err(StreamError::Unavailable);
        };
        assert_eq!(receipt.start, 0);
        assert_eq!(receipt.end, 1);
        // The actual Chromium test asserts this same complete content-bound ID,
        assert_eq!(
            *receipt.commit_id.as_bytes(),
            [
                38, 143, 36, 24, 145, 152, 169, 224, 7, 28, 82, 129, 78, 208, 207, 70, 142, 223,
                186, 183, 148, 195, 120, 23, 80, 176, 19, 93, 220, 251, 47, 177,
            ]
        );
        Ok(())
    }
}
