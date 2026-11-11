use super::*;
use crate::content::tree::tests::Disk;
use crate::{AppendRecord, CommitEffect, CommitRequest, StreamPath};
use std::sync::atomic::Ordering;

fn inline(field: Field) -> Result<OwnedBytes, Box<dyn std::error::Error>> {
    match field {
        Field::Inline(bytes) => Ok(bytes),
        _ => Err("expected inline token".into()),
    }
}

#[tokio::test]
async fn staged_100k_effects_keep_counts_and_caller_record_order()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let mut effects = Vec::new();
    for index in 0..100_000_u64 {
        let path = StreamPath::new(format!("files/{index:06}"))?;
        effects.push(CommitEffect::hard(
            vec![CommitCondition::Tail {
                path: path.clone(),
                expected: index,
            }],
            vec![CommitMutation::Append {
                path,
                records: vec![
                    AppendRecord::from(index.to_le_bytes().to_vec()),
                    AppendRecord::from((!index).to_le_bytes().to_vec()),
                ],
            }],
        ));
    }
    let request = WriteRequest::Commit(CommitRequest {
        idempotency_key: IdempotencyKey::new("staged-100k")?,
        // Staging must not settle a condition, deadline, sequence or revision.
        deadline_unix_millis: Some(1),
        effects,
    });
    let staged = stage(&storage, CommitId::from_bytes([1; 32]), &request).await?;
    assert!(staged.body().part.length > crate::MAX_COMMAND_BYTES as u64);
    let mut reader = Reader::open(&storage, &staged).await?;
    assert_eq!(reader.deadline_unix_millis(), Some(1));
    for index in 0..100_000_u64 {
        assert_eq!(
            reader.next().await?,
            Some(Token::Effect {
                soft: false,
                conditions: 1,
                mutations: 1
            })
        );
        let Some(Token::Tail {
            path,
            expected,
            commit_id,
        }) = reader.next().await?
        else {
            return Err("missing staged condition".into());
        };
        assert_eq!(
            inline(path)?.as_slice(),
            format!("files/{index:06}").as_bytes()
        );
        assert_eq!(expected, index);
        assert_eq!(commit_id, None);
        let Some(Token::Append { records: 2, .. }) = reader.next().await? else {
            return Err("missing staged append".into());
        };
        for expected in [index, !index] {
            let Some(Token::Record {
                value,
                keys: 0,
                attributes: 0,
            }) = reader.next().await?
            else {
                return Err("missing ordered record".into());
            };
            assert_eq!(inline(value)?.as_slice(), &expected.to_le_bytes());
        }
    }
    assert!(reader.next().await?.is_none());
    assert!(storage.largest.load(Ordering::Relaxed) <= tree::PAGE_BYTES);
    Ok(())
}

#[tokio::test]
async fn staged_large_record_is_a_content_reference_and_projection_keeps_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let size = 64 * 1024 * 1024;
    let request = CommitRequest {
        idempotency_key: IdempotencyKey::new("staged-large")?,
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path: StreamPath::new("values")?,
                records: vec![
                    OwnedBytes::from_static(b"before").into(),
                    OwnedBytes::from(vec![0xa5; size]).into(),
                    OwnedBytes::from_static(b"after").into(),
                ],
            }],
        )],
    };
    let commit_id = CommitId::from_bytes([2; 32]);
    let staged = stage(&storage, commit_id, &WriteRequest::Commit(request.clone())).await?;
    let selected = stage(&storage, commit_id, &WriteRequest::CommitAccepted(request)).await?;
    assert_eq!(staged.body(), selected.body());
    assert!(!staged.accepted());
    assert!(selected.accepted());
    assert!(staged.body().part.length < INLINE_BYTES as u64);
    let mut reader = Reader::open(&storage, &selected).await?;
    assert!(matches!(
        reader.next().await?,
        Some(Token::Effect { mutations: 1, .. })
    ));
    assert!(matches!(
        reader.next().await?,
        Some(Token::Append { records: 3, .. })
    ));
    let Some(Token::Record { value, .. }) = reader.next().await? else {
        return Err("missing first record".into());
    };
    assert_eq!(inline(value)?.as_slice(), b"before");
    let Some(Token::Record {
        value: Field::Content(root),
        ..
    }) = reader.next().await?
    else {
        return Err("large value was not content addressed".into());
    };
    assert_eq!(root.part.length, size as u64);
    let mut content = tree::Reader::new(&storage, root)?;
    let mut read = 0;
    while let Some(bytes) = content.next().await? {
        assert!(bytes.as_slice().iter().all(|byte| *byte == 0xa5));
        read += bytes.len();
    }
    assert_eq!(read, size);
    let Some(Token::Record { value, .. }) = reader.next().await? else {
        return Err("missing final record".into());
    };
    assert_eq!(inline(value)?.as_slice(), b"after");
    assert!(reader.next().await?.is_none());
    let forged = StagedWrite {
        commit_id: CommitId::from_bytes([3; 32]),
        ..staged
    };
    assert!(matches!(
        Reader::open(&storage, &forged).await,
        Err(StreamError::InvalidResponse)
    ));
    Ok(())
}

#[tokio::test]
async fn staged_decoder_refuses_every_truncation_and_trailing_tokens()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let request = WriteRequest::Append(crate::AppendRequest {
        path: StreamPath::new("a")?,
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::new("truncation")?,
        records: vec![OwnedBytes::from_static(b"payload").into()],
    });
    let staged = stage(&storage, CommitId::from_bytes([4; 32]), &request).await?;
    let mut source = tree::Reader::new(&storage, staged.body())?;
    let mut bytes = Vec::new();
    while let Some(page) = source.next().await? {
        bytes.extend_from_slice(page.as_slice());
    }
    for end in 0..bytes.len() {
        let mut writer = tree::Writer::new(&storage)?;
        writer.write(&bytes[..end]).await?;
        let partial = StagedWrite {
            body: writer.finish().await?,
            ..staged.clone()
        };
        if let Ok(mut reader) = Reader::open(&storage, &partial).await {
            loop {
                match reader.next().await {
                    Ok(Some(_)) => {}
                    Err(_) => break,
                    Ok(None) => return Err(format!("truncated stage sealed at byte {end}").into()),
                }
            }
            assert!(matches!(reader.next().await, Err(StreamError::Unavailable)));
        }
    }
    bytes.push(1);
    let mut writer = tree::Writer::new(&storage)?;
    writer.write(&bytes).await?;
    let trailing = StagedWrite {
        body: writer.finish().await?,
        ..staged
    };
    let mut reader = Reader::open(&storage, &trailing).await?;
    for _ in 0..4 {
        reader
            .next()
            .await?
            .ok_or("missing complete original token")?;
    }
    assert!(matches!(
        reader.next().await,
        Err(StreamError::InvalidResponse)
    ));
    Ok(())
}

#[tokio::test]
async fn cancelled_content_write_cannot_seal_or_resume_a_staged_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    struct Delayed {
        disk: Disk,
        gate: std::sync::Mutex<
            Option<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        >,
    }
    #[async_trait::async_trait]
    impl Storage for Delayed {
        async fn put(&self, digest: [u8; 32], page: tree::Page) -> Result<(), StreamError> {
            self.disk.put(digest, page).await?;
            let gate = self
                .gate
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .take();
            if let Some((stored, resume)) = gate {
                stored.send(()).map_err(|_| StreamError::Unavailable)?;
                resume.await.map_err(|_| StreamError::Unavailable)?;
            }
            Ok(())
        }
        async fn get(&self, digest: [u8; 32]) -> Result<tree::Page, StreamError> {
            self.disk.get(digest).await
        }
    }
    let directory = tempfile::tempdir()?;
    let (stored, event) = tokio::sync::oneshot::channel();
    let (_resume, wait) = tokio::sync::oneshot::channel();
    let storage = Delayed {
        disk: Disk::open(directory.path()),
        gate: std::sync::Mutex::new(Some((stored, wait))),
    };
    let mut writer = tree::Writer::new(&storage)?;
    let input = vec![42; 2 * tree::PAGE_BYTES];
    {
        let writing = writer.write(&input);
        tokio::pin!(writing);
        tokio::select! {
            result = &mut writing => return Err(format!("write completed before its stored-page event: {result:?}").into()),
            result = event => result?,
        }
    }
    assert!(
        std::fs::read_dir(directory.path())?.next().is_some(),
        "no original page was stored"
    );
    assert_eq!(
        writer.write(b"replacement").await,
        Err(StreamError::Unavailable),
        "cancelled staging writer accepted replacement bytes"
    );
    assert_eq!(writer.finish().await, Err(StreamError::Unavailable));
    Ok(())
}

#[tokio::test]
async fn staged_fork_exclusions_and_headers_keep_typed_token_order()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let source = StreamPath::new("source")?;
    let destination = StreamPath::new("fork")?;
    let head = CommitId::from_bytes([6; 32]);
    let request = WriteRequest::Commit(CommitRequest {
        deadline_unix_millis: None,
        idempotency_key: IdempotencyKey::new("fork-headers")?,
        effects: vec![CommitEffect {
            soft: true,
            conditions: vec![
                CommitCondition::Present {
                    path: source.clone(),
                },
                CommitCondition::Absent {
                    path: destination.clone(),
                },
                CommitCondition::Head {
                    path: source.clone(),
                    expected: 7,
                    commit_id: head,
                },
            ],
            mutations: vec![CommitMutation::Fork {
                source,
                destination,
                at_tail: 7,
                exclude: vec![StreamPath::new("a/b")?, StreamPath::new("z")?],
                records: vec![AppendRecord::latest("body", "key", true)?],
            }],
        }],
    });
    let staged = stage(&storage, CommitId::from_bytes([5; 32]), &request).await?;
    let mut reader = Reader::open(&storage, &staged).await?;
    assert_eq!(
        reader.next().await?,
        Some(Token::Effect {
            soft: true,
            conditions: 3,
            mutations: 1
        })
    );
    assert!(matches!(
        reader.next().await?,
        Some(Token::Presence { present: true, .. })
    ));
    assert!(matches!(
        reader.next().await?,
        Some(Token::Presence { present: false, .. })
    ));
    assert!(
        matches!(reader.next().await?, Some(Token::Tail { expected: 7, commit_id: Some(id), .. }) if id == head)
    );
    assert!(matches!(
        reader.next().await?,
        Some(Token::Fork {
            at_tail: Some(7),
            exclusions: 2,
            records: 1,
            ..
        })
    ));
    for expected in [b"a/b".as_slice(), b"z"] {
        let Some(Token::Exclusion(path)) = reader.next().await? else {
            return Err("missing fork exclusion".into());
        };
        assert_eq!(inline(path)?.as_slice(), expected);
    }
    assert!(matches!(
        reader.next().await?,
        Some(Token::Record {
            keys: 1,
            attributes: 1,
            ..
        })
    ));
    let Some(Token::Key(key)) = reader.next().await? else {
        return Err("missing indexed key".into());
    };
    assert_eq!(inline(key)?.as_slice(), b"key");
    let Some(Token::Attribute { name, value }) = reader.next().await? else {
        return Err("missing indexed attribute".into());
    };
    assert_eq!(inline(name)?.as_slice(), b"attention");
    assert_eq!(inline(value)?.as_slice(), b"true");
    assert!(reader.next().await?.is_none());
    Ok(())
}
