use super::*;

#[tokio::test]
async fn packed_effect_codec_refuses_all_truncations_and_malformed_rows()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::staged::effects::{BYTES, HEADER, MAGIC};
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let staged = stage_tokens(
        &storage,
        Submission {
            commit_id: CommitId::from_bytes([72; 32]),
            idempotency_key: IdempotencyKey::new("packed-codec")?,
            family: Family::Commit,
            deadline_unix_millis: None,
            effects: 2,
            accepted: false,
        },
        futures::stream::iter((0..2_u64).flat_map(|ordinal| {
            [
                Ok(Token::Effect {
                    soft: false,
                    conditions: 0,
                    mutations: 1,
                }),
                Ok(Token::Append {
                    path: Field::Inline(OwnedBytes::from(format!("output/{ordinal}"))),
                    records: 1,
                }),
                Ok(Token::Record {
                    value: Field::Inline(OwnedBytes::new()),
                    keys: 0,
                    attributes: 0,
                }),
            ]
        })),
    )
    .await?;
    let prepared = Directory::build(&storage, &staged).await?;
    let root = prepared.effects(&storage).await?.root();
    let mut reader = tree::Reader::new(&storage, root)?;
    let mut bytes = Vec::new();
    while let Some(page) = reader.next().await? {
        bytes.extend_from_slice(page.as_slice());
    }
    async fn refuses(
        storage: &impl Storage,
        staged: &StagedWrite,
        bytes: &[u8],
    ) -> Result<bool, StreamError> {
        let mut writer = tree::Writer::new(storage)?;
        writer.write(bytes).await?;
        let root = writer.finish().await?;
        let Ok(mut effects) = Effects::open(storage, staged, root).await else {
            return Ok(true);
        };
        loop {
            match effects.next().await {
                Ok(Some(_)) => {}
                Err(_) => return Ok(true),
                Ok(None) => return Ok(false),
            }
        }
    }
    for end in 0..bytes.len() {
        assert!(
            refuses(&storage, &staged, &bytes[..end]).await?,
            "packed effect metadata accepted a truncated prefix at byte {end}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(refuses(&storage, &staged, &trailing).await?);
    for offset in [MAGIC.len() - 2, MAGIC.len(), HEADER, HEADER + BYTES - 1] {
        let mut malformed = bytes.clone();
        malformed[offset] = 255;
        assert!(
            refuses(&storage, &staged, &malformed).await?,
            "packed effect codec accepted corruption at byte {offset}"
        );
    }
    for offset in [HEADER + 1, HEADER + 9] {
        let mut malformed = bytes.clone();
        malformed[offset..offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(refuses(&storage, &staged, &malformed).await?);
    }
    let mut wrong = staged.clone();
    wrong.effects = 1;
    assert!(Reader::open(&storage, &wrong).await.is_err());
    assert!(Effects::open(&storage, &wrong, root).await.is_err());
    wrong = staged.clone();
    wrong.deadline = Some(1);
    assert!(Reader::open(&storage, &wrong).await.is_err());
    assert!(Effects::open(&storage, &wrong, root).await.is_err());
    Ok(())
}

#[tokio::test]
async fn staged_100k_empty_condition_effects_use_packed_metadata_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let staged = stage_tokens(
        &storage,
        Submission {
            commit_id: CommitId::from_bytes([71; 32]),
            idempotency_key: IdempotencyKey::new("packed-100k-directory")?,
            family: Family::Commit,
            deadline_unix_millis: None,
            effects: 100_000,
            accepted: false,
        },
        futures::stream::iter((0..100_000_u64).flat_map(|ordinal| {
            [
                Ok(Token::Effect {
                    soft: false,
                    conditions: 0,
                    mutations: 1,
                }),
                Ok(Token::Append {
                    path: Field::Inline(OwnedBytes::from(format!("output/{ordinal:06}"))),
                    records: 1,
                }),
                Ok(Token::Record {
                    value: Field::Inline(OwnedBytes::from(ordinal.to_le_bytes().to_vec())),
                    keys: 0,
                    attributes: 0,
                }),
            ]
        })),
    )
    .await?;
    let prepared = Directory::build(&storage, &staged).await?;
    let root = prepared.root();
    assert!(storage.largest.load(Ordering::Relaxed) <= tree::PAGE_BYTES);
    drop(prepared);
    drop(storage);
    let storage = Disk::open(directory.path());
    let prepared = Directory::open(&storage, &staged, root).await?;
    let mut effects = prepared.effects(&storage).await?;
    let metadata = effects.root();
    let graph = reachable(&storage, metadata).await?;
    let frames = metadata.part.length.div_ceil(tree::PAGE_BYTES as u64);
    assert!(
        graph.len() as u64 <= frames * 4 + 4,
        "effect metadata retained one object per effect instead of packed frames"
    );
    let before = storage.reads.load(Ordering::Relaxed);
    for ordinal in 0..100_000_u64 {
        let effect = effects.next().await?.ok_or("missing packed effect")?;
        assert_eq!(effect.ordinal, ordinal);
        assert!(!effect.soft);
        let mut conditions = effect.conditions(&storage).await?;
        assert!(conditions.next().await?.is_none());
    }
    assert!(effects.next().await?.is_none());
    let reads = storage.reads.load(Ordering::Relaxed) - before;
    assert!(
        reads <= graph.len() * 2,
        "empty conditions performed one object read per effect: reads={reads} frames={}",
        graph.len()
    );
    // Original token-boundary recovery is independent of the metadata pages.
    let mut cursor = prepared.scan(&storage).await?;
    for ordinal in 0..3_u64 {
        let (_, location) = cursor.next().await?.ok_or("missing original mutation")?;
        assert_eq!((location.effect, location.ordinal), (ordinal, 0));
        let mut reader = location.reader(&storage, &staged)?;
        assert!(matches!(
            reader.next().await?,
            Some(Token::Append { records: 1, .. })
        ));
        assert_eq!(
            reader.next().await?,
            Some(Token::Record {
                value: Field::Inline(OwnedBytes::from(ordinal.to_le_bytes().to_vec())),
                keys: 0,
                attributes: 0,
            })
        );
    }
    Ok(())
}
