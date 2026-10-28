use super::*;
use crate::{AppendRecord, ReadKeySeek, RecordHeaders};
use futures::TryStreamExt as _;

fn record(key: &[u8], value: &[u8]) -> AppendRecord {
    AppendRecord {
        value: Bytes::copy_from_slice(value),
        headers: RecordHeaders::new(vec![Bytes::copy_from_slice(key)], Vec::new()).unwrap(),
    }
}

async fn append_records(provider: &MemoryStream, path: &StreamPath, records: Vec<AppendRecord>) {
    provider
        .append(AppendRequest {
            path: path.clone(),
            records,
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await
        .unwrap();
}

fn request(path: &StreamPath, key: &[u8], from: u64, before: u64, limit: u32) -> ReadRequest {
    ReadRequest {
        path: path.clone(),
        from,
        limit,
        at_least: Default::default(),
        key: Some(ReadKeySeek {
            key: Bytes::copy_from_slice(key),
            before,
        }),
    }
}

#[tokio::test]
async fn key_seek_pins_real_opaque_records_and_inherited_cuts() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let source = StreamPath::new("seek/source")?;
    let destination = StreamPath::new("seek/fork")?;
    let mut multiple = record(b"a", b"{ invalid JSON \0\xff");
    multiple.headers = RecordHeaders::new(
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        Vec::new(),
    )?;
    append_records(
        &provider,
        &source,
        vec![
            record(b"b", b"first"),
            record(b"a", b"second"),
            multiple,
            record(b"a", b"source suffix"),
        ],
    )
    .await;
    let expected: Vec<_> = provider
        .read(ReadRequest {
            path: source.clone(),
            from: 0,
            limit: 4,
            key: None,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    let pinned = provider
        .read(request(&source, b"a", 0, u64::MAX, 10))
        .await?;
    append_records(&provider, &source, vec![record(b"a", b"after pin")]).await;
    assert_eq!(pinned.try_collect::<Vec<_>>().await?, expected[1..4]);
    provider
        .fork(ForkRequest {
            source: source.clone(),
            destination: destination.clone(),
            at_tail: Some(3),
            exclude: Vec::new(),
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    append_records(
        &provider,
        &destination,
        vec![
            record(b"b", b"reused sequence"),
            record(b"a", b"destination suffix"),
        ],
    )
    .await;
    let values: Vec<_> = provider
        .read(request(&destination, b"a", 0, 5, 2))
        .await?
        .try_collect()
        .await?;
    assert_eq!(values, expected[1..3]);
    let rest: Vec<_> = provider
        .read(request(&destination, b"a", 3, 5, 2))
        .await?
        .try_collect()
        .await?;
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].sequence, 4);
    assert_eq!(rest[0].value.as_ref(), b"destination suffix");
    assert!(
        provider
            .read(request(&destination, b"a\0", 0, 5, 2))
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );
    assert!(
        provider
            .read(request(&destination, b"a", 3, 3, 2))
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );
    let restored = MemoryStream::default();
    restored
        .install_state(&provider.encode_state().await)
        .await?;
    assert_eq!(
        restored
            .read(request(&destination, b"a", 3, 5, 2))
            .await?
            .try_collect::<Vec<_>>()
            .await?,
        rest
    );
    Ok(())
}

#[test]
fn disabling_the_index_exposes_the_historical_scan_slope() {
    futures::executor::block_on(async {
        let mut measured = Vec::new();
        for count in [64usize, 1024, 16384] {
            let provider = MemoryStream::default();
            let path = StreamPath::new("seek/slope").unwrap();
            let records: Vec<_> = (0..count)
                .map(|position| {
                    record(
                        if position + 1 == count {
                            b"target"
                        } else {
                            b"other"
                        },
                        &position.to_le_bytes(),
                    )
                })
                .collect();
            for page in records.chunks(crate::MAX_ITEMS) {
                append_records(&provider, &path, page.to_vec()).await;
            }
            let path = provider
                .state
                .read()
                .await
                .paths
                .get(&path)
                .cloned()
                .unwrap();
            let mut probes = 0usize;
            let sequence = path
                .history
                .as_ref()
                .unwrap()
                .keys()
                .seek(b"target", 0, path.tail, &mut || {
                    probes += 1;
                    Ok::<_, StreamError>(())
                })
                .unwrap()
                .unwrap();
            let mut scanned = 0;
            let mut cursor = HistoryCursor::new(&path, path.tail, 0);
            let expected = loop {
                let record = cursor.next_record().await.unwrap().unwrap();
                scanned += 1;
                if record
                    .headers
                    .keys()
                    .iter()
                    .any(|key| key.as_ref() == b"target")
                {
                    break record;
                }
            };
            assert_eq!(sequence, expected.sequence);
            assert_eq!(scanned, count);
            assert!(probes <= 2 * (usize::BITS - count.leading_zeros()) as usize + 2);
            measured.push((count, probes, scanned));
        }
        println!("indexed key probes / disabled-index records examined: {measured:?}");
        assert!(measured[2].1 < measured[0].2);
        assert!(measured[2].2 > measured[1].2);
    });
}
