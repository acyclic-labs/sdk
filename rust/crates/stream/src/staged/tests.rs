use super::*;
use crate::content::tree::tests::{Delayed, Disk, reachable};
use crate::{AppendRecord, CommitEffect, CommitRequest, StreamPath};
use std::sync::atomic::Ordering;

fn inline(field: Field) -> Result<OwnedBytes, Box<dyn std::error::Error>> {
    match field {
        Field::Inline(bytes) => Ok(bytes),
        _ => Err("expected inline token".into()),
    }
}

#[tokio::test]
async fn staged_paths_validate_grammar_across_pages_without_size_limits()
-> Result<(), Box<dyn std::error::Error>> {
    fn tokens(role: usize, path: Field) -> Vec<Result<Token, StreamError>> {
        let ordinary = || Field::Inline(OwnedBytes::from_static(b"ordinary"));
        let exclusion = (role == 5).then(|| path.clone());
        let mut tokens = vec![Ok(Token::Effect {
            soft: false,
            conditions: u64::from(role < 2),
            mutations: u64::from(role >= 2),
        })];
        tokens.push(Ok(match role {
            0 => Token::Presence {
                path,
                present: true,
            },
            1 => Token::Tail {
                path,
                expected: 0,
                commit_id: None,
            },
            2 => Token::Append { path, records: 1 },
            3 => Token::Fork {
                source: path,
                destination: ordinary(),
                at_tail: Some(0),
                exclusions: 0,
                records: 0,
            },
            4 => Token::Fork {
                source: ordinary(),
                destination: path,
                at_tail: Some(0),
                exclusions: 0,
                records: 0,
            },
            _ => Token::Fork {
                source: ordinary(),
                destination: ordinary(),
                at_tail: Some(0),
                exclusions: 1,
                records: 0,
            },
        }));
        if role == 2 {
            tokens.push(Ok(Token::Record {
                value: Field::Inline(OwnedBytes::new()),
                keys: 0,
                attributes: 0,
            }));
        } else if let Some(path) = exclusion {
            tokens.push(Ok(Token::Exclusion(path)));
        }
        tokens
    }
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let submission = Submission {
        commit_id: CommitId::from_bytes([17; 32]),
        idempotency_key: IdempotencyKey::new("paged-path-grammar")?,
        family: Family::Commit,
        deadline_unix_millis: None,
        effects: 1,
        accepted: false,
    };
    let path = OwnedBytes::from(format!("{}end", "a/".repeat(tree::PAGE_BYTES)));
    assert!(path.len() > crate::MAX_PATH_BYTES);
    let mut content = tree::Writer::new(&storage)?;
    content.write(path.as_slice()).await?;
    let root = content.finish().await?;
    let mut pages = tree::Reader::new(&storage, root)?;
    let first_page = pages.next().await?.ok_or("missing path page")?.len();
    for role in 0..6 {
        for field in [Field::Inline(path.clone()), Field::Content(root)] {
            let staged = stage_tokens(
                &storage,
                submission.clone(),
                futures::stream::iter(tokens(role, field)),
            )
            .await?;
            let mut reader = Reader::open(&storage, &staged).await?;
            while reader.next().await?.is_some() {}
        }
        for path in [
            "", "/a", "a/", "a//b", ".", "..", "a/./b", "a/../b", "a\\b", "a b", "é",
        ] {
            let result = stage_tokens(
                &storage,
                submission.clone(),
                futures::stream::iter(tokens(role, Field::Inline(OwnedBytes::from(path)))),
            )
            .await;
            assert!(
                matches!(result, Err(StreamError::InvalidPath)),
                "staged path grammar accepted malformed input: role={role} path={path:?} result={result:?}"
            );
        }
        for boundary in [tree::PAGE_BYTES, first_page] {
            let path = OwnedBytes::from(format!("{}/../b", "a".repeat(boundary - 2)));
            let mut content = tree::Writer::new(&storage)?;
            content.write(path.as_slice()).await?;
            let root = content.finish().await?;
            for field in [Field::Inline(path.clone()), Field::Content(root)] {
                let result = stage_tokens(
                    &storage,
                    submission.clone(),
                    futures::stream::iter(tokens(role, field)),
                )
                .await;
                assert!(
                    matches!(result, Err(StreamError::InvalidPath)),
                    "staged path grammar accepted a dot segment across pages: role={role} boundary={boundary} result={result:?}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn staged_100k_effects_keep_counts_and_caller_record_order()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    // The producer retains one effect at a time, never a 100k-effect request.
    let tokens = futures::stream::iter((0..100_000_u64).flat_map(|index| {
        let path = OwnedBytes::from(format!("files/{index:06}"));
        [
            Ok(Token::Effect {
                soft: false,
                conditions: 1,
                mutations: 1,
            }),
            Ok(Token::Tail {
                path: Field::Inline(path.clone()),
                expected: index,
                commit_id: None,
            }),
            Ok(Token::Append {
                path: Field::Inline(path),
                records: 2,
            }),
            Ok(Token::Record {
                value: Field::Inline(OwnedBytes::from(index.to_le_bytes().to_vec())),
                keys: 0,
                attributes: 0,
            }),
            Ok(Token::Record {
                value: Field::Inline(OwnedBytes::from((!index).to_le_bytes().to_vec())),
                keys: 0,
                attributes: 0,
            }),
        ]
    }));
    let staged = stage_tokens(
        &storage,
        Submission {
            commit_id: CommitId::from_bytes([1; 32]),
            idempotency_key: IdempotencyKey::new("staged-100k")?,
            family: Family::Commit,
            effects: 100_000,
            deadline_unix_millis: Some(1),
            accepted: false,
        },
        tokens,
    )
    .await?;
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
    let mut value = tree::Writer::new(&storage)?;
    let fragment = vec![0xa5; tree::PAGE_BYTES];
    for _ in 0..size / fragment.len() {
        value.write(&fragment).await?;
    }
    let root = value.finish().await?;
    let tokens = || {
        futures::stream::iter([
            Ok(Token::Effect {
                soft: false,
                conditions: 0,
                mutations: 1,
            }),
            Ok(Token::Append {
                path: Field::Inline(b"values".as_slice()),
                records: 3,
            }),
            Ok(Token::Record {
                value: Field::Inline(b"before".as_slice()),
                keys: 0,
                attributes: 0,
            }),
            Ok(Token::Record {
                value: Field::Content(root),
                keys: 0,
                attributes: 0,
            }),
            Ok(Token::Record {
                value: Field::Inline(b"after".as_slice()),
                keys: 0,
                attributes: 0,
            }),
        ])
    };
    let submission = Submission {
        commit_id: CommitId::from_bytes([2; 32]),
        idempotency_key: IdempotencyKey::new("staged-large")?,
        family: Family::Commit,
        effects: 1,
        deadline_unix_millis: None,
        accepted: false,
    };
    let staged = stage_tokens(&storage, submission.clone(), tokens()).await?;
    let selected = stage_tokens(
        &storage,
        Submission {
            accepted: true,
            ..submission
        },
        tokens(),
    )
    .await?;
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
    let body_page = storage.get(staged.body().part.digest).await?;
    assert_eq!(
        body_page.kind,
        tree::Kind::Manifest,
        "staged body omitted content dependencies"
    );
    let body_manifest = crate::content::Manifest::decode(body_page.bytes.as_slice())?;
    assert_eq!(
        body_manifest.edges().collect::<Vec<_>>(),
        vec![root.part.digest]
    );
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
    for references in [false, true] {
        let directory = tempfile::tempdir()?;
        let disk = Disk::open(directory.path());
        let root = if references {
            Some(
                tree::upload(
                    &disk,
                    futures::stream::iter([Ok(OwnedBytes::from_static(b"dependency"))]),
                )
                .await?,
            )
        } else {
            None
        };
        let (stored, event) = tokio::sync::oneshot::channel();
        let (_resume, wait) = tokio::sync::oneshot::channel();
        let storage = Delayed {
            disk,
            gate: std::sync::Mutex::new(Some((stored, wait))),
        };
        let mut writer = tree::Writer::new(&storage)?;
        if let Some(root) = root {
            for _ in 0..tree::EDGE_FANOUT - 1 {
                writer.reference(root).await?;
            }
        }
        let input = vec![42; 2 * tree::PAGE_BYTES];
        {
            let writing = async {
                match root {
                    Some(root) => writer.reference(root).await,
                    None => writer.write(&input).await,
                }
            };
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
        if let Some(root) = root {
            assert_eq!(writer.reference(root).await, Err(StreamError::Unavailable));
        }
        assert_eq!(writer.finish().await, Err(StreamError::Unavailable));
    }
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

#[tokio::test]
async fn staged_bookmarks_resume_boundaries_across_content_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let tokens = || {
        [
            Ok(Token::Effect {
                soft: false,
                conditions: 0,
                mutations: 1,
            }),
            Ok(Token::Append {
                path: Field::Inline(OwnedBytes::from_static(b"bookmarks")),
                records: 20,
            }),
        ]
        .into_iter()
        .chain((0..20_u8).map(|index| {
            Ok(Token::Record {
                value: Field::Inline(OwnedBytes::from(vec![index; INLINE_BYTES])),
                keys: 0,
                attributes: 0,
            })
        }))
    };
    let staged = stage_tokens(
        &storage,
        Submission {
            commit_id: CommitId::from_bytes([9; 32]),
            idempotency_key: IdempotencyKey::new("bookmarks")?,
            family: Family::Commit,
            deadline_unix_millis: Some(42),
            effects: 1,
            accepted: false,
        },
        futures::stream::iter(tokens()),
    )
    .await?;
    assert!(staged.body().part.length > tree::PAGE_BYTES as u64);
    let mut reader = Reader::open(&storage, &staged).await?;
    let mut projection = staged.clone();
    projection.accepted = true;
    for expected in tokens() {
        let expected = expected?;
        let bookmark = reader.bookmark()?;
        let before = reader.position()?;
        let mut resumed = Reader::resume(&storage, &projection, &bookmark)?;
        assert_eq!(resumed.deadline_unix_millis(), Some(42));
        assert_eq!(resumed.position()?, before);
        assert_eq!(resumed.next().await?, Some(expected.clone()));
        assert_eq!(reader.next().await?, Some(expected));
        assert_eq!(resumed.position()?, reader.position()?);
        assert!(reader.position()? > before);

        let mut rebound = staged.clone();
        rebound.commit_id = CommitId::from_bytes([10; 32]);
        assert!(matches!(
            Reader::resume(&storage, &rebound, &bookmark),
            Err(StreamError::InvalidArgument)
        ));
    }
    assert!(reader.next().await?.is_none());
    let end = reader.bookmark()?;
    let mut resumed = Reader::resume(&storage, &staged, &end)?;
    assert_eq!(resumed.position()?, staged.body().part.length);
    assert!(resumed.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn streamed_ingress_refuses_missing_extra_and_wrong_family_tokens()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let submission = Submission {
        commit_id: CommitId::from_bytes([7; 32]),
        idempotency_key: IdempotencyKey::new("shape")?,
        family: Family::Append,
        effects: 1,
        deadline_unix_millis: None,
        accepted: false,
    };
    let effect = Token::<&[u8]>::Effect {
        soft: false,
        conditions: 0,
        mutations: 1,
    };
    let append = Token::Append {
        path: Field::Inline(b"a".as_slice()),
        records: 1,
    };
    let record = Token::Record {
        value: Field::Inline(b"body".as_slice()),
        keys: 0,
        attributes: 0,
    };
    let wrong = Token::Fork {
        source: Field::Inline(b"a".as_slice()),
        destination: Field::Inline(b"b".as_slice()),
        at_tail: None,
        exclusions: 0,
        records: 0,
    };
    for tokens in [
        vec![effect.clone(), append.clone()],
        vec![
            effect.clone(),
            append.clone(),
            record.clone(),
            record.clone(),
        ],
        vec![effect.clone(), wrong],
    ] {
        assert!(matches!(
            stage_tokens(
                &storage,
                submission.clone(),
                futures::stream::iter(tokens.into_iter().map(Ok))
            )
            .await,
            Err(StreamError::InvalidArgument)
        ));
    }
    // Addressed values need no minimum size. A pre-staged small object is a
    // legitimate field, rather than a hidden inline-only API restriction.
    let mut value = tree::Writer::new(&storage)?;
    value.write(b"body").await?;
    let root = value.finish().await?;
    let addressed = stage_tokens(
        &storage,
        submission,
        futures::stream::iter([
            Ok(effect),
            Ok(append),
            Ok(Token::<&[u8]>::Record {
                value: Field::Content(root),
                keys: 0,
                attributes: 0,
            }),
        ]),
    )
    .await?;
    let mut reader = Reader::open(&storage, &addressed).await?;
    reader.next().await?.ok_or("missing effect")?;
    reader.next().await?.ok_or("missing append")?;
    assert!(
        matches!(reader.next().await?, Some(Token::Record { value: Field::Content(value), .. }) if value == root)
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn staged_destination_directory_reopens_original_ordinals_and_records()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let submission = Submission {
        commit_id: CommitId::from_bytes([63; 32]),
        idempotency_key: IdempotencyKey::new("destination-directory")?,
        family: Family::Commit,
        deadline_unix_millis: Some(72),
        effects: 257,
        accepted: false,
    };
    let tokens = futures::stream::iter((0..514_u64).flat_map(|ordinal| {
        let mut tokens = Vec::new();
        if ordinal % 2 == 0 {
            tokens.push(Ok(Token::Effect {
                soft: false,
                conditions: 0,
                mutations: 2,
            }));
        }
        tokens.push(Ok(Token::Append {
            path: Field::Inline(OwnedBytes::from(format!(
                "directory/{:04}",
                ordinal * 259 % 514
            ))),
            records: 2,
        }));
        for record in [ordinal * 2, ordinal * 2 + 1] {
            tokens.push(Ok(Token::Record {
                value: Field::Inline(OwnedBytes::from(record.to_le_bytes().to_vec())),
                keys: 0,
                attributes: 0,
            }));
        }
        tokens
    }));
    let staged = stage_tokens(&storage, submission.clone(), tokens).await?;
    let indexed = Directory::build(&storage, &staged).await?;
    let root = indexed.root();
    drop(indexed);
    drop(storage);
    let storage = Disk::open(directory.path());
    let indexed = Directory::open(&storage, &staged, root).await?;
    let model: std::collections::BTreeMap<_, _> = (0..514_u64)
        .map(|ordinal| (format!("directory/{:04}", ordinal * 259 % 514), ordinal))
        .collect();
    let mut cursor = indexed.scan(&storage).await?;
    for (path, ordinal) in model {
        let (key, location) = cursor.next().await?.ok_or("missing directory entry")?;
        assert_eq!(inline(key)?.as_slice(), path.as_bytes());
        assert_eq!(
            (location.effect, location.ordinal),
            (ordinal / 2, ordinal % 2),
            "directory lost original mutation ordinals"
        );
        let found = indexed
            .get(&storage, Field::Inline(OwnedBytes::from(path.clone())))
            .await?
            .ok_or("missing point lookup")?;
        assert_eq!(
            (found.effect, found.ordinal),
            (location.effect, location.ordinal)
        );
        let mut reader = found.reader(&storage, &staged)?;
        assert_eq!(reader.deadline_unix_millis(), Some(72));
        assert_eq!(
            reader.next().await?,
            Some(Token::Append {
                path: Field::Inline(OwnedBytes::from(path)),
                records: 2,
            })
        );
        for record in [ordinal * 2, ordinal * 2 + 1] {
            assert_eq!(
                reader.next().await?,
                Some(Token::Record {
                    value: Field::Inline(OwnedBytes::from(record.to_le_bytes().to_vec())),
                    keys: 0,
                    attributes: 0,
                }),
                "directory reordered original records"
            );
        }
    }
    assert!(cursor.next().await?.is_none());
    assert!(
        indexed
            .get(&storage, Field::Inline(OwnedBytes::from_static(b"missing")))
            .await?
            .is_none()
    );
    let mut wrong = staged.clone();
    wrong.commit_id = CommitId::from_bytes([64; 32]);
    assert!(Directory::open(&storage, &wrong, root).await.is_err());
    wrong = staged.clone();
    wrong.body.part.length += 1;
    assert!(Directory::open(&storage, &wrong, root).await.is_err());
    let mut projected = staged.clone();
    projected.accepted = true;
    Directory::open(&storage, &projected, root).await?;
    let duplicate = stage_tokens(
        &storage,
        Submission {
            effects: 2,
            ..submission
        },
        futures::stream::iter((0..2).flat_map(|_| {
            [
                Ok(Token::<OwnedBytes>::Effect {
                    soft: false,
                    conditions: 0,
                    mutations: 1,
                }),
                Ok(Token::Append {
                    path: Field::Inline(OwnedBytes::from_static(b"duplicate")),
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
    assert!(
        matches!(
            Directory::build(&storage, &duplicate).await,
            Err(StreamError::InvalidArgument)
        ),
        "directory accepted duplicate destinations across effects"
    );
    Ok(())
}

#[tokio::test]
async fn staged_condition_directory_preserves_effect_scope_order_and_retention()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let count = |effect| match effect {
        0 => 4097_u64,
        1 => 2,
        _ => 0,
    };
    let path = |effect, ordinal| {
        format!(
            "condition/{:04}",
            if effect == 0 {
                ordinal * 2053 % 4097
            } else {
                ordinal * 4096
            }
        )
    };
    let submission = Submission {
        commit_id: CommitId::from_bytes([69; 32]),
        idempotency_key: IdempotencyKey::new("condition-directory")?,
        family: Family::Commit,
        deadline_unix_millis: Some(69),
        effects: 3,
        accepted: false,
    };
    let tokens = futures::stream::iter((0..3_u64).flat_map(|effect| {
        std::iter::once(Ok(Token::Effect {
            soft: effect == 1,
            conditions: count(effect),
            mutations: 1,
        }))
        .chain((0..count(effect)).map(move |ordinal| {
            Ok(Token::Presence {
                path: Field::Inline(OwnedBytes::from(path(effect, ordinal))),
                present: ordinal % 2 == 0,
            })
        }))
        .chain([
            Ok(Token::Append {
                path: Field::Inline(OwnedBytes::from(format!("output/{effect}"))),
                records: 1,
            }),
            Ok(Token::Record {
                value: Field::Inline(OwnedBytes::from(vec![effect as u8])),
                keys: 0,
                attributes: 0,
            }),
        ])
    }));
    let staged = stage_tokens(&storage, submission.clone(), tokens).await?;
    let prepared = Directory::build(&storage, &staged).await?;
    let root = prepared.root();
    drop(prepared);
    drop(storage);
    let storage = Disk::open(directory.path());
    let prepared = Directory::open(&storage, &staged, root).await?;
    let graph = reachable(&storage, root).await?;
    let mut effects = prepared.effects(&storage).await?;
    for expected in 0..3_u64 {
        let effect = effects.next().await?.ok_or("missing original effect")?;
        assert_eq!(effect.ordinal, expected);
        assert_eq!(effect.soft, expected == 1);
        assert!(
            graph.contains(&effect.condition_root().part.digest),
            "effect metadata lost condition-checkpoint ownership"
        );
        let mut original = effect.reader(&storage, &staged)?;
        assert_eq!(original.deadline_unix_millis(), Some(69));
        assert_eq!(
            original.next().await?,
            Some(Token::Effect {
                soft: expected == 1,
                conditions: count(expected),
                mutations: 1
            })
        );
        let model: std::collections::BTreeMap<_, _> = (0..count(expected))
            .map(|ordinal| (path(expected, ordinal), ordinal))
            .collect();
        let mut conditions = effect.conditions(&storage).await?;
        for (path, ordinal) in model {
            let (key, location) = conditions.next().await?.ok_or("missing condition")?;
            assert_eq!(inline(key)?.as_slice(), path.as_bytes());
            assert_eq!((location.effect, location.ordinal), (expected, ordinal));
            let mut reader = location.reader(&storage, &staged)?;
            assert_eq!(
                reader.next().await?,
                Some(Token::Presence {
                    path: Field::Inline(OwnedBytes::from(path.clone())),
                    present: ordinal % 2 == 0,
                })
            );
            if ordinal == 0 {
                let found = effect
                    .condition(&storage, Field::Inline(OwnedBytes::from(path)))
                    .await?
                    .ok_or("missing condition lookup")?;
                assert_eq!((found.effect, found.ordinal), (expected, ordinal));
            }
        }
        assert!(conditions.next().await?.is_none());
    }
    assert!(effects.next().await?.is_none());
    let duplicate = stage_tokens(
        &storage,
        Submission {
            effects: 1,
            ..submission
        },
        futures::stream::iter([
            Ok(Token::Effect {
                soft: false,
                conditions: 2,
                mutations: 1,
            }),
            Ok(Token::Presence {
                path: Field::Inline(OwnedBytes::from_static(b"duplicate")),
                present: true,
            }),
            Ok(Token::Presence {
                path: Field::Inline(OwnedBytes::from_static(b"duplicate")),
                present: false,
            }),
            Ok(Token::Append {
                path: Field::Inline(OwnedBytes::from_static(b"output")),
                records: 1,
            }),
            Ok(Token::Record {
                value: Field::Inline(OwnedBytes::new()),
                keys: 0,
                attributes: 0,
            }),
        ]),
    )
    .await?;
    assert!(
        matches!(
            Directory::build(&storage, &duplicate).await,
            Err(StreamError::InvalidArgument)
        ),
        "directory accepted duplicate conditions within one effect"
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_effect_metadata_cursor_cannot_skip_the_consumed_effect()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let staged = stage_tokens(
        &disk,
        Submission {
            commit_id: CommitId::from_bytes([70; 32]),
            idempotency_key: IdempotencyKey::new("effect-cursor-cancel")?,
            family: Family::Commit,
            deadline_unix_millis: None,
            effects: 2,
            accepted: false,
        },
        futures::stream::iter((0..2_u64).flat_map(|effect| {
            [
                Ok(Token::Effect {
                    soft: false,
                    conditions: 1,
                    mutations: 1,
                }),
                Ok(Token::Presence {
                    path: Field::Inline(OwnedBytes::from(format!("check/{effect}"))),
                    present: true,
                }),
                Ok(Token::Append {
                    path: Field::Inline(OwnedBytes::from(format!("output/{effect}"))),
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
    let prepared = Directory::build(&disk, &staged).await?;
    let storage = Delayed {
        disk,
        put: Default::default(),
        get: Default::default(),
    };
    let mut effects = prepared.effects(&storage).await?;
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (_resume, resumed) = tokio::sync::oneshot::channel();
    *storage.get.lock().map_err(|_| "gate poisoned")? = Some((entered, resumed));
    {
        let next = effects.next();
        tokio::pin!(next);
        tokio::select! {
            _ = waiting => {},
            result = &mut next => { result?; return Err("effect metadata read did not suspend".into()); },
        }
    }
    assert!(
        matches!(effects.next().await, Err(StreamError::Unavailable)),
        "cancelled effect cursor skipped consumed metadata"
    );
    let mut fresh = prepared.effects(&storage).await?;
    assert_eq!(
        fresh.next().await?.ok_or("missing first effect")?.ordinal,
        0
    );
    assert_eq!(
        fresh.next().await?.ok_or("missing second effect")?.ordinal,
        1
    );
    assert!(fresh.next().await?.is_none());
    Ok(())
}

mod packed;
