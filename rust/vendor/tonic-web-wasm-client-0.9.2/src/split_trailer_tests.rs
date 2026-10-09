mod split_trailers {
    use bytes::{BufMut, Bytes, BytesMut};

    use crate::{
        Error,
        response_body::TRAILER_BIT,
        test_support::{GRPC_WEB, body_from_chunks, drain, frame, next_frame},
    };

    const TRAILERS: &[u8] = b"grpc-status:0\r\ngrpc-message:\r\n";

    /// A grpc-web response: one data frame per message length, followed by a
    /// trailers frame. Returns the wire bytes and the length of the data frames.
    fn wire(message_lens: &[usize], trailers: &[u8]) -> (Bytes, usize) {
        let mut wire = BytesMut::new();
        for &message_len in message_lens {
            let message: Vec<u8> = (0..message_len).map(|i| i as u8).collect();
            wire.put(frame(0x00, &message));
        }
        let data_frames_len = wire.len();
        wire.put(frame(TRAILER_BIT, trailers));
        (wire.freeze(), data_frames_len)
    }

    fn assert_complete(chunks: Vec<Bytes>, expected_data: &[u8], case: &str) {
        let (data, trailers) =
            drain(body_from_chunks(GRPC_WEB, chunks)).unwrap_or_else(|e| panic!("{case}: {e}"));

        assert_eq!(data, expected_data, "{case}");
        let trailers = trailers.unwrap_or_else(|| panic!("{case}: no trailers"));
        assert_eq!(trailers.get("grpc-status").unwrap(), "0", "{case}");
    }

    #[test]
    fn trailers_are_returned_wherever_the_chunk_boundary_lands() {
        let (wire, data_frames_len) = wire(&[1024], TRAILERS);

        for split in 0..=wire.len() {
            let chunks = vec![wire.slice(..split), wire.slice(split..)];
            assert_complete(
                chunks,
                &wire[..data_frames_len],
                &format!("split at {split}"),
            );
        }
    }

    #[test]
    fn trailers_are_returned_when_every_byte_is_its_own_chunk() {
        let (wire, data_frames_len) = wire(&[300, 0, 7], TRAILERS);

        let chunks = (0..wire.len()).map(|i| wire.slice(i..i + 1)).collect();
        assert_complete(chunks, &wire[..data_frames_len], "one byte per chunk");
    }

    #[test]
    fn stream_ending_inside_the_trailers_frame_is_malformed() {
        let (wire, data_frames_len) = wire(&[16], TRAILERS);

        for end in data_frames_len + 1..wire.len() {
            let result = drain(body_from_chunks(GRPC_WEB, vec![wire.slice(..end)]));
            assert!(
                matches!(result, Err(Error::MalformedResponse)),
                "truncated at {end}"
            );
        }
    }

    #[test]
    fn body_ends_after_a_trailers_frame_that_fails_to_parse() {
        let (wire, data_frames_len) = wire(&[16], b"not a header\r\n");
        // More bytes after the trailers frame must not be read as trailers.
        let mut body = body_from_chunks(GRPC_WEB, vec![wire.clone(), wire.clone()]);

        let mut data = BytesMut::new();
        let mut errors = 0;
        while let Some(frame) = next_frame(&mut body) {
            match frame {
                Ok(frame) => data.put(frame.into_data().expect("no trailers were parsed")),
                Err(e) => {
                    assert!(matches!(e, Error::HeaderParsingError), "{e}");
                    errors += 1;
                }
            }
        }

        assert_eq!(errors, 1);
        assert_eq!(data, &wire[..data_frames_len]);
    }
}
