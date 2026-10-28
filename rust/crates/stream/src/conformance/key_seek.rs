use super::*;

pub(crate) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    verify_with(provider, async |request| provider.read(request).await).await
}

pub(crate) async fn verify_with<P: StreamProvider + ?Sized>(
    provider: &P,
    mut read_records: impl AsyncFnMut(ReadRequest) -> Result<crate::RecordStream, StreamError>,
) -> Result<(), StreamError> {
    let source = path("key-seek/source")?;
    let record =
        |key: &'static [u8], value: &'static [u8]| -> Result<crate::AppendRecord, StreamError> {
            Ok(crate::AppendRecord {
                value: Bytes::from_static(value),
                headers: crate::RecordHeaders::new(vec![Bytes::from_static(key)], Vec::new())?,
            })
        };
    provider
        .append(AppendRequest {
            path: source.clone(),
            if_tail: None,
            idempotency_key: key(b"key-seek-original")?,
            records: vec![
                record(b"a", b"one")?,
                record(b"b", b"two")?,
                record(b"a", b"opaque\0\xff")?,
            ],
        })
        .await?;
    let read = |value: &'static [u8], from, before, limit| ReadRequest {
        key: Some(crate::ReadKeySeek {
            key: Bytes::from_static(value),
            before,
        }),
        path: source.clone(),
        from,
        limit,
        at_least: Default::default(),
    };
    let mut pinned = read_records(read(b"a", 0, u64::MAX, 10)).await?;
    provider
        .append(AppendRequest {
            path: source.clone(),
            if_tail: None,
            idempotency_key: key(b"key-seek-later")?,
            records: vec![record(b"a", b"later")?],
        })
        .await?;
    let first = pinned
        .next()
        .await
        .transpose()?
        .ok_or(StreamError::Unavailable)?;
    let second = pinned
        .next()
        .await
        .transpose()?
        .ok_or(StreamError::Unavailable)?;
    if first.sequence != 0
        || second.sequence != 2
        || second.value.as_ref() != b"opaque\0\xff"
        || pinned.next().await.is_some()
        || read_records(read(b"a\0", 0, 4, 1))
            .await?
            .next()
            .await
            .is_some()
        || read_records(read(b"a", 1, 2, 1))
            .await?
            .next()
            .await
            .is_some()
    {
        return Err(StreamError::Unavailable);
    }
    let resumed: Vec<_> = read_records(read(b"a", 3, 4, 1))
        .await?
        .try_collect()
        .await?;
    if resumed.len() != 1 || resumed[0].sequence != 3 {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}
