use super::*;
use crate::config::{AdmissionStatus, utc};
use acyclic_stream::{MemoryStream, Stream, StreamProviderExt};
use futures::TryStreamExt as _;

fn work() -> Result<acyclic_fs::NativeMaterializationWork> {
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

#[tokio::test]
async fn decisions_are_indexed_live_and_retained_without_an_agent_root() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let journal = SettingsAdmissions::new(provider.clone());
    let agent = AgentId::new();
    let original = Admission {
        path: "agent.ts".into(),
        version: "original".into(),
        status: AdmissionStatus::Applied,
        error: None,
        at: utc(0),
    };
    let work = work()?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    journal
        .record(
            agent,
            (&original.path, "cut-a", &original.version),
            &original,
            None,
            &io,
        )
        .await?;
    journal
        .record(
            agent,
            (&original.path, "cut-a", &original.version),
            &original,
            None,
            &io,
        )
        .await?;
    let path = SettingsAdmissions::path(agent)?;
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, 1);
    let mut other = original.clone();
    other.path = "plugins.ts".into();
    journal
        .record(
            agent,
            (&other.path, "cut-a", &other.version),
            &other,
            None,
            &io,
        )
        .await?;
    assert_eq!(
        journal
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(original.clone())
    );
    assert!(
        journal
            .latest(AgentId::new(), "agent.ts", "cut-a", "original", &work)
            .await?
            .is_none()
    );
    assert!(
        journal
            .latest(agent, "agent.ts", "cut-a", "different", &work)
            .await?
            .is_none()
    );
    let stream = Stream::new(provider.clone(), path.as_str()).map_err(invalid)?;
    let mut follow = stream
        .follow(2, Default::default())
        .await
        .map_err(invalid)?;
    let refused = Admission {
        status: AdmissionStatus::Rejected,
        error: Some("model refused".into()),
        at: utc(1),
        ..original.clone()
    };
    journal
        .record(
            agent,
            (&refused.path, "cut-a", &refused.version),
            &refused,
            None,
            &io,
        )
        .await?;
    let event = follow.try_next().await.map_err(invalid)?.unwrap();
    let decision: Decision =
        serde_json::from_slice(&event.value).map_err(|error| Error::Storage(error.to_string()))?;
    let Decision::Inline { admission, .. } = decision else {
        return Err(Error::Storage("small admission was not inline".into()));
    };
    assert_eq!(admission, refused);
    assert_eq!(event.sequence, 2);
    let reopened = SettingsAdmissions::new(provider.clone());
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(refused.clone())
    );
    // All prior decisions remain in the same ordinary, bounded history query.
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, 3);
    let mut history = stream.read(0, 3).await.map_err(invalid)?;
    let first = history.try_next().await.map_err(invalid)?.unwrap();
    let first: Decision =
        serde_json::from_slice(&first.value).map_err(|error| Error::Storage(error.to_string()))?;
    let Decision::Inline { admission, .. } = first else {
        return Err(Error::Storage("small admission was not inline".into()));
    };
    assert_eq!(admission, original);
    assert_eq!(
        journal
            .record(
                agent,
                (&original.path, "cut-a", &original.version),
                &original,
                Some(1),
                &io
            )
            .await
            .err()
            .expect("oversized admission must refuse")
            .code(),
        crate::wire::ErrorCode::Invalid
    );
    assert_eq!(
        provider.tail(path.clone()).await.map_err(invalid)?,
        3,
        "bound refusal must not publish"
    );
    // The same file version under another complete source cut has its own
    // outcome. A path/version-only index would redirect both reads here.
    journal
        .record(
            agent,
            (&original.path, "cut-b", &original.version),
            &original,
            None,
            &io,
        )
        .await?;
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(refused)
    );
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "cut-b", "original", &work)
            .await?,
        Some(original)
    );
    assert!(
        reopened
            .latest(agent, "agent.ts", "missing-cut", "original", &work)
            .await?
            .is_none()
    );
    assert_eq!(provider.tail(path).await.map_err(invalid)?, 4);
    Ok(())
}

#[tokio::test]
async fn large_decisions_replay_from_bounded_chunks_and_refuse_incomplete_payloads() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let journal = SettingsAdmissions::new(provider.clone());
    let agent = AgentId::new();
    let original = Admission {
        path: "agent.ts".into(),
        version: "original".into(),
        status: AdmissionStatus::Invalid,
        // Raw chunks may split UTF-8 and escaped JSON tokens. Only the complete
        // retained value is decoded, so neither can shorten the original cause.
        error: Some("設定\"\n\\".repeat(20_000)),
        at: utc(0),
    };
    let work = work()?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let mut retained = None;
    for _ in 0..2 {
        journal
            .record(
                agent,
                (&original.path, "cut-a", &original.version),
                &original,
                None,
                &io,
            )
            .await?;
        let live = work.live_allocation_bytes();
        if let Some(original) = retained {
            assert_eq!(live, original, "chunk replay must retire temporary storage");
        }
        retained = Some(live);
    }
    let root = SettingsAdmissions::path(agent)?;
    assert_eq!(provider.tail(root.clone()).await.map_err(invalid)?, 1);
    let stream = Stream::new(provider.clone(), root.as_str()).map_err(invalid)?;
    let mut records = stream.read(0, 1).await.map_err(invalid)?;
    let record = records.try_next().await.map_err(invalid)?.unwrap();
    assert!(
        record.value.len() + record.headers.payload_bytes() <= acyclic_stream::MAX_RECORD_BYTES
    );
    let Decision::Chunks {
        version: 1,
        digest,
        bytes,
    } = serde_json::from_slice::<Decision>(&record.value)
        .map_err(|error| Error::Storage(error.to_string()))?
    else {
        return Err(Error::Storage("large admission was not chunked".into()));
    };
    let path = SettingsAdmissions::payload_path(agent, &digest, None)?;
    let count = bytes.div_ceil(acyclic_stream::MAX_RECORD_BYTES as u64);
    assert!(count > 1);
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, count);
    let reopened = SettingsAdmissions::new(provider.clone());
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await?,
        Some(original.clone())
    );
    assert!(
        reopened
            .latest(agent, "agent.ts", "cut-b", "original", &work)
            .await?
            .is_none()
    );

    // A complete indexed reference cannot make a truncated or substituted
    // prefix valid. Its length and digest both bind the complete decision.
    for (name, digest, bytes) in [("short", digest, bytes - 1), ("missing", [0; 32], bytes)] {
        let query = SettingsAdmissions::query(agent, "agent.ts", "cut-a", "original", None)?;
        provider
            .append(AppendRequest {
                path: query.path,
                records: vec![AppendRecord {
                    value: serde_json::to_vec(&Decision::<Admission>::Chunks {
                        version: 1,
                        digest,
                        bytes,
                    })
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
                    headers: RecordHeaders::new(vec![query.key.unwrap().key], vec![])
                        .map_err(invalid)?,
                }],
                if_tail: None,
                idempotency_key: IdempotencyKey::new(name).map_err(invalid)?,
            })
            .await
            .map_err(invalid)?;
        let error = reopened
            .latest(agent, "agent.ts", "cut-a", "original", &work)
            .await
            .expect_err("an incomplete payload must refuse");
        assert_eq!(error.code(), crate::wire::ErrorCode::Storage);
    }

    // Stage failure must precede the indexed publication. A conflicting raw
    // prefix is a real provider failure, rather than an injected append mock.
    let other = AgentId::new();
    let body = io.encode(
        &Decision::Inline {
            version: 1,
            agent: other,
            cut: "cut-a",
            admission: &original,
        },
        None,
        true,
    )?;
    let path = SettingsAdmissions::payload_path(other, blake3::hash(&body).as_bytes(), None)?;
    provider
        .append(AppendRequest {
            path,
            records: vec![b"different prefix".to_vec().into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new("conflicting-prefix").map_err(invalid)?,
        })
        .await
        .map_err(invalid)?;
    let error = journal
        .record(
            other,
            (&original.path, "cut-a", &original.version),
            &original,
            None,
            &io,
        )
        .await
        .expect_err("a conflicting prefix must refuse before index publication");
    assert_eq!(error.code(), crate::wire::ErrorCode::Storage);
    assert!(
        journal
            .latest(other, "agent.ts", "cut-a", "original", &work)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn indexed_decisions_refuse_substituted_identity_and_unknown_formats() -> Result<()> {
    for (format, wrong_agent, wrong_path, wrong_version, wrong_cut) in [
        (2, false, false, false, false),
        (1, true, false, false, false),
        (1, false, true, false, false),
        (1, false, false, true, false),
        (1, false, false, false, true),
    ] {
        let provider = Arc::new(MemoryStream::default());
        let agent = AgentId::new();
        let query = SettingsAdmissions::query(agent, "agent.ts", "cut-a", "original", None)?;
        let decision = Decision::Inline {
            version: format,
            cut: if wrong_cut { "cut-b" } else { "cut-a" }.to_owned(),
            agent: if wrong_agent { AgentId::new() } else { agent },
            admission: Admission {
                path: if wrong_path { "plugins.ts" } else { "agent.ts" }.into(),
                version: if wrong_version {
                    "substitute"
                } else {
                    "original"
                }
                .into(),
                status: AdmissionStatus::Applied,
                error: None,
                at: utc(0),
            },
        };
        provider
            .append(AppendRequest {
                path: query.path,
                records: vec![AppendRecord {
                    value: serde_json::to_vec(&decision)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                        .into(),
                    headers: RecordHeaders::new(vec![query.key.unwrap().key], vec![])
                        .map_err(invalid)?,
                }],
                if_tail: None,
                idempotency_key: IdempotencyKey::new("substitution").map_err(invalid)?,
            })
            .await
            .map_err(invalid)?;
        let result = SettingsAdmissions::new(provider)
            .latest(agent, "agent.ts", "cut-a", "original", &work()?)
            .await;
        if format != 1 {
            assert!(matches!(result, Err(Error::Unsupported(_))));
        } else {
            assert!(matches!(result, Err(Error::Storage(_))));
        }
    }
    Ok(())
}
