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
    journal.record(agent, original.clone(), None, &work).await?;
    journal.record(agent, original.clone(), None, &work).await?;
    let path = SettingsAdmissions::path(agent)?;
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, 1);
    let mut other = original.clone();
    other.path = "plugins.ts".into();
    journal.record(agent, other, None, &work).await?;
    assert_eq!(
        journal.latest(agent, "agent.ts", "original", &work).await?,
        Some(original.clone())
    );
    assert!(
        journal
            .latest(AgentId::new(), "agent.ts", "original", &work)
            .await?
            .is_none()
    );
    assert!(
        journal
            .latest(agent, "agent.ts", "different", &work)
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
    journal.record(agent, refused.clone(), None, &work).await?;
    let event = follow.try_next().await.map_err(invalid)?.unwrap();
    let decision: Decision =
        serde_json::from_slice(&event.value).map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(decision.admission, refused);
    assert_eq!(event.sequence, 2);
    let reopened = SettingsAdmissions::new(provider.clone());
    assert_eq!(
        reopened
            .latest(agent, "agent.ts", "original", &work)
            .await?,
        Some(refused)
    );
    // All prior decisions remain in the same ordinary, bounded history query.
    assert_eq!(provider.tail(path.clone()).await.map_err(invalid)?, 3);
    let mut history = stream.read(0, 3).await.map_err(invalid)?;
    let first = history.try_next().await.map_err(invalid)?.unwrap();
    let first: Decision =
        serde_json::from_slice(&first.value).map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(first.admission, original);
    assert!(matches!(
        journal.record(agent, original, Some(1), &work).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        provider.tail(path).await.map_err(invalid)?,
        3,
        "bound refusal must not publish"
    );
    Ok(())
}

#[tokio::test]
async fn indexed_decisions_refuse_substituted_identity_and_unknown_formats() -> Result<()> {
    for (format, wrong_agent, wrong_path, wrong_version) in [
        (2, false, false, false),
        (1, true, false, false),
        (1, false, true, false),
        (1, false, false, true),
    ] {
        let provider = Arc::new(MemoryStream::default());
        let agent = AgentId::new();
        let query = SettingsAdmissions::query(agent, "agent.ts", "original")?;
        let decision = Decision {
            version: format,
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
            .latest(agent, "agent.ts", "original", &work()?)
            .await;
        if format != 1 {
            assert!(matches!(result, Err(Error::Unsupported(_))));
        } else {
            assert!(matches!(result, Err(Error::Storage(_))));
        }
    }
    Ok(())
}
