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

    fn next(&self, pending: &[u8]) -> Result<Option<(usize, usize, u8)>, ProviderError> {
        match self {
            Self::Lines => Ok(pending
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|end| (0, end + 1, 0))),
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
    stream::try_unfold(
        (source.boxed(), Vec::<u8>::new(), framing),
        |(mut source, mut pending, framing)| async move {
            loop {
                if let Some((start, end, flags)) = framing.next(&pending)? {
                    let data = pending.drain(..end).skip(start).collect();
                    return Ok(Some((Frame { flags, data }, (source, pending, framing))));
                }
                match source.next().await {
                    Some(Ok(data))
                        if pending.len().saturating_add(data.as_ref().len())
                            <= framing.capacity() =>
                    {
                        pending.extend_from_slice(data.as_ref())
                    }
                    Some(_) => return Err(ProviderError::Unavailable),
                    None if pending.is_empty() => return Ok(None),
                    None => return Err(ProviderError::Unavailable),
                }
            }
        },
    )
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    // These are generated envelope vectors for the internal byte buffer, not
    // fixtures or purported responses from any external provider.
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
