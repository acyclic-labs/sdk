//! Bounded byte framing, independent of provider event schemas.
use crate::ProviderError;
use futures::{Stream, StreamExt, stream, stream::BoxStream};

/// Bounds unfinished SSE events before a parser can retain them. This is also
/// used by adapters in other crates, including Daytona's toolbox log stream.
pub fn bounded_sse<S, B, E>(source: S) -> impl Stream<Item = Result<B, ProviderError>> + Send
where
    S: Stream<Item = Result<B, E>> + Send,
    B: AsRef<[u8]>,
{
    let (mut retained, mut line) = (0usize, 0usize);
    source.map(move |chunk| {
        let chunk = chunk.map_err(|_| ProviderError::Unavailable)?;
        if chunk.as_ref().len() > 512 * 1024 {
            return Err(ProviderError::Unavailable);
        }
        for byte in chunk.as_ref() {
            retained += 1;
            match byte {
                b'\n' => {
                    if line == 0 {
                        retained = 0;
                    }
                    line = 0;
                }
                b'\r' => {}
                _ => line += 1,
            }
            if retained > 512 * 1024 {
                return Err(ProviderError::Unavailable);
            }
        }
        Ok(chunk)
    })
}

/// Streaming envelope supported by the shared bounded decoder.
pub enum Framing {
    /// Newline-delimited records, retaining their terminating newline.
    Lines,
    /// Connect's flags byte and big-endian four-byte payload length.
    Connect,
}

/// One complete envelope. The adapter interprets its payload schema.
pub struct Frame {
    /// Connect envelope flags; zero for newline-delimited records.
    pub flags: u8,
    /// Complete payload without a Connect header.
    pub data: Vec<u8>,
}

/// Encodes one uncompressed Connect JSON request, without dispatching it.
pub fn connect_message(message: &serde_json::Value) -> Result<Vec<u8>, ProviderError> {
    let data = serde_json::to_vec(message).map_err(|_| ProviderError::Invalid(
        "Connect request serialization failed; recovery: inspect the command without redispatching".into()))?;
    let length = u32::try_from(data.len()).map_err(|_| {
        ProviderError::Invalid("Connect request too large; recovery: reduce argv".into())
    })?;
    let mut framed = vec![0];
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend(data);
    Ok(framed)
}

impl Framing {
    fn capacity(&self) -> usize {
        match self {
            Self::Lines => 512 * 1024,
            Self::Connect => 2 * 1024 * 1024,
        }
    }

    fn next(
        &self,
        pending: &[u8],
        scanned: &mut usize,
    ) -> Result<Option<(usize, usize, u8)>, ProviderError> {
        match self {
            Self::Lines => {
                let end = pending[*scanned..].iter().position(|byte| *byte == b'\n');
                let frame = end.map(|end| (0, *scanned + end + 1, 0));
                *scanned = pending.len();
                Ok(frame)
            }
            Self::Connect => {
                let Some(header) = pending.get(..5) else {
                    return Ok(None);
                };
                let length =
                    u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
                if length > 1024 * 1024 || header[0] & !2 != 0 {
                    return Err(ProviderError::Unavailable);
                }
                Ok((pending.len() >= length + 5).then_some((5, length + 5, header[0])))
            }
        }
    }
}

struct Decoder<B> {
    source: BoxStream<'static, Result<B, ProviderError>>,
    framing: Framing,
    pending: Vec<u8>,
    consumed: usize,
    scanned: usize,
    incoming: Option<(B, usize)>,
}

/// Buffer ownership, fragmentation, bounds and truncated EOF handling live once;
/// adapters only interpret complete protocol frames.
pub fn frames<S, B, E>(
    source: S,
    framing: Framing,
) -> BoxStream<'static, Result<Frame, ProviderError>>
where
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: Send + 'static,
{
    let decoder = Decoder {
        source: source
            .map(|chunk| chunk.map_err(|_| ProviderError::Unavailable))
            .boxed(),
        framing,
        pending: Vec::new(),
        consumed: 0,
        scanned: 0,
        incoming: None,
    };
    stream::try_unfold(decoder, |mut decoder| async move {
        loop {
            let pending = &decoder.pending[decoder.consumed..];
            if let Some((start, end, flags)) =
                decoder.framing.next(pending, &mut decoder.scanned)?
            {
                let data = pending[start..end].to_vec();
                decoder.consumed += end;
                decoder.scanned = 0;
                return Ok(Some((Frame { flags, data }, decoder)));
            }
            if decoder.consumed != 0 {
                // Compact only an incomplete suffix when more input is
                // needed. Yielding a frame never shifts the remaining batch.
                let incomplete = pending.len();
                decoder.pending.copy_within(decoder.consumed.., 0);
                decoder.pending.truncate(incomplete);
                decoder.consumed = 0;
            }
            if let Some((data, offset)) = &mut decoder.incoming {
                let remaining = &data.as_ref()[*offset..];
                let count = remaining
                    .len()
                    .min(decoder.framing.capacity() - decoder.pending.len());
                if count == 0 && !remaining.is_empty() {
                    return Err(ProviderError::Unavailable);
                }
                decoder.pending.extend_from_slice(&remaining[..count]);
                *offset += count;
                if *offset == data.as_ref().len() {
                    decoder.incoming = None;
                }
            } else {
                match decoder.source.next().await {
                    Some(Ok(data)) => decoder.incoming = Some((data, 0)),
                    Some(Err(error)) => return Err(error),
                    None if decoder.pending.is_empty() => return Ok(None),
                    None => return Err(ProviderError::Unavailable),
                }
            }
        }
    })
    .boxed()
}

/// Decode complete JSON records once, including Connect's terminal envelope.
/// Provider adapters interpret application events, never transport trailers.
pub(super) fn json_frames<T, S, B, E>(
    source: S,
    framing: Framing,
) -> BoxStream<'static, Result<T, ProviderError>>
where
    T: serde::de::DeserializeOwned + Send + 'static,
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: Send + 'static,
{
    stream::try_unfold(frames(source, framing), |mut source| async move {
        let Some(frame) = source.next().await else {
            return Ok(None);
        };
        let frame = frame?;
        if frame.flags == 2 {
            let trailer: serde_json::Value =
                serde_json::from_slice(&frame.data).map_err(|_| ProviderError::Unavailable)?;
            return if trailer.get("error").is_some() {
                Err(ProviderError::Unavailable)
            } else {
                Ok(None)
            };
        }
        let value = serde_json::from_slice(&frame.data).map_err(|_| ProviderError::Unavailable)?;
        Ok(Some((value, source)))
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    // These are generated envelope vectors for the internal byte buffer, not
    // fixtures or purported responses from any external provider.
    #[tokio::test]
    async fn typed_json_records_share_decoding_and_connect_trailer_ownership()
    -> Result<(), ProviderError> {
        let values = [7u32, 9];
        let lines: Vec<u8> = values
            .iter()
            .flat_map(|value| format!("{value}\n").into_bytes())
            .collect();
        let actual: Vec<u32> =
            json_frames::<u32, _, _, _>(stream::iter([Ok::<_, ()>(lines)]), Framing::Lines)
                .try_collect()
                .await?;
        assert_eq!(actual, values);

        let mut connect = Vec::new();
        for value in values {
            connect.extend(connect_message(&serde_json::json!(value))?);
        }
        let mut terminal = connect_message(&serde_json::json!({}))?;
        terminal[0] = 2;
        connect.extend(terminal);
        // A terminal envelope owns termination; later input is never decoded.
        connect.extend(connect_message(&serde_json::json!(11))?);
        let actual: Vec<u32> = json_frames::<u32, _, _, _>(
            stream::iter(connect.into_iter().map(|byte| Ok::<_, ()>(vec![byte]))),
            Framing::Connect,
        )
        .try_collect()
        .await?;
        assert_eq!(actual, values);
        Ok(())
    }

    #[tokio::test]
    async fn typed_json_and_connect_trailer_failures_are_terminal() -> Result<(), ProviderError> {
        let mut trailer = connect_message(&serde_json::json!({"error": {}}))?;
        trailer[0] = 2;
        for (wire, framing) in [
            (trailer, Framing::Connect),
            (
                connect_message(&serde_json::json!(false))?,
                Framing::Connect,
            ),
            (b"false\n".to_vec(), Framing::Lines),
        ] {
            let mut output =
                json_frames::<u32, _, _, _>(stream::iter([Ok::<_, ()>(wire)]), framing);
            assert!(output.next().await.is_some_and(|result| result.is_err()));
            assert!(output.next().await.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn connect_envelopes_survive_every_single_split() -> Result<(), ProviderError> {
        let payload = serde_json::json!({"bytes": [0, 255, 10]});
        let one = connect_message(&payload)?;
        let mut wire = one.clone();
        wire.extend_from_slice(&one);
        for split in 0..=wire.len() {
            let source = stream::iter([
                Ok::<_, ()>(wire[..split].to_vec()),
                Ok(wire[split..].to_vec()),
            ]);
            let actual: Vec<_> = frames(source, Framing::Connect).try_collect().await?;
            assert_eq!(actual.len(), 2);
            for frame in actual {
                assert_eq!(frame.flags, 0);
                assert_eq!(frame.data, one[5..]);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn truncation_invalid_flags_and_oversize_are_terminal() {
        for bytes in [
            vec![0],
            vec![0, 0, 0, 0, 2, 1],
            vec![4, 0, 0, 0, 0],
            vec![0, 0, 16, 0, 1],
        ] {
            let mut output = frames(stream::iter([Ok::<_, ()>(bytes)]), Framing::Connect);
            assert!(output.next().await.is_some_and(|item| item.is_err()));
            assert!(output.next().await.is_none());
        }
        let mut output = frames(stream::iter([Err::<Vec<u8>, _>(())]), Framing::Lines);
        assert!(output.next().await.is_some_and(|item| item.is_err()));
        assert!(output.next().await.is_none());
    }

    #[tokio::test]
    async fn line_boundaries_and_incomplete_eof_are_not_confused() -> Result<(), ProviderError> {
        let source = stream::iter([Ok::<_, ()>(b"a".to_vec()), Ok(b"\nb\n".to_vec())]);
        let actual: Vec<_> = frames(source, Framing::Lines).try_collect().await?;
        assert_eq!(
            actual
                .iter()
                .map(|frame| frame.data.as_slice())
                .collect::<Vec<_>>(),
            [b"a\n", b"b\n"]
        );
        let mut truncated = frames(stream::iter([Ok::<_, ()>(b"a".to_vec())]), Framing::Lines);
        assert!(truncated.next().await.is_some_and(|item| item.is_err()));
        Ok(())
    }

    #[tokio::test]
    async fn valid_batches_larger_than_incomplete_capacity_are_consumed_incrementally()
    -> Result<(), ProviderError> {
        let count = 512 * 1024 / 2 + 1;
        let source = stream::iter([Ok::<_, ()>(b"a\n".repeat(count))]);
        let actual = frames(source, Framing::Lines)
            .try_fold(0, |count, frame| async move {
                assert_eq!(frame.data, b"a\n");
                Ok(count + 1)
            })
            .await?;
        assert_eq!(actual, count);

        let payload = serde_json::json!({"value": "a".repeat(64 * 1024)});
        let one = connect_message(&payload)?;
        let source = stream::iter([Ok::<_, ()>(one.repeat(40))]);
        let actual = frames(source, Framing::Connect)
            .try_fold(0, |count, frame| {
                assert_eq!(frame.data, one[5..]);
                async move { Ok(count + 1) }
            })
            .await?;
        assert_eq!(actual, 40);
        Ok(())
    }

    #[tokio::test]
    async fn fragmented_lines_keep_the_individual_frame_limit() -> Result<(), ProviderError> {
        let mut line = vec![b'a'; 16 * 1024];
        line.push(b'\n');
        let source = stream::iter(
            line.iter()
                .copied()
                .map(|byte| Ok::<_, ()>(vec![byte]))
                .collect::<Vec<_>>(),
        );
        let actual: Vec<_> = frames(source, Framing::Lines).try_collect().await?;
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].data, line);

        let mut limit = vec![b'a'; 512 * 1024 - 1];
        limit.push(b'\n');
        let actual: Vec<_> = frames(stream::iter([Ok::<_, ()>(limit.clone())]), Framing::Lines)
            .try_collect()
            .await?;
        assert_eq!(actual[0].data, limit);
        limit.insert(0, b'a');
        let mut oversized = frames(stream::iter([Ok::<_, ()>(limit)]), Framing::Lines);
        assert!(oversized.next().await.is_some_and(|item| item.is_err()));
        assert!(oversized.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn sse_limit_spans_chunks_and_resets_only_at_event_boundaries() {
        let source = stream::iter([
            Ok::<_, ()>(vec![b'a'; 256 * 1024]),
            Ok(vec![b'b'; 256 * 1024]),
            Ok(vec![b'c']),
        ]);
        let result: Vec<_> = bounded_sse(source).collect().await;
        assert!(result[0].is_ok());
        assert!(result[1].is_ok());
        assert!(result[2].is_err());
        let source = stream::iter([
            Ok::<_, ()>(vec![b'a'; 256 * 1024]),
            Ok(b"\n\n".to_vec()),
            Ok(vec![b'b'; 256 * 1024]),
        ]);
        assert!(
            bounded_sse(source)
                .all(|item| async move { item.is_ok() })
                .await
        );
    }
}
