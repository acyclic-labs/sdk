//! Directory scale is independent of actors that are not selected or running.
use super::*;
use crate::agent::Phase;
use acyclic_stream::{CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey};

#[tokio::test]
async fn ten_thousand_sessions_keep_discovery_at_two_bounded_head_views() -> Result {
    async fn work(count: u128) -> Result<acyclic_stream::testing::Requests> {
        let bound = binding()?;
        let stream = bound.agents().provider().clone();
        let owner = Arc::new(Owner::default());
        owner.sessions.store(true, Ordering::SeqCst);
        let Serving::Served(server) = serve(
            bound,
            owner,
            &IntentId::try_from("directory-scale")?,
            RootRequest {
                agent: AgentId::from_bytes([1; 16]),
                fresh: vec![VolumeBinding {
                    name: PRIVATE_VOLUME.into(),
                    volume: VolumeRef::new(
                        provider()?,
                        "directory-owner".into(),
                        VolumeClass::AgentPrivate,
                        VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
                        &mut None,
                    )?,
                }],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        )
        .await?
        else {
            return Err("directory owner publication was uncertain".into());
        };
        let roots = server.bound().session_roots()?;
        let status = StatusRecord {
            child_settlement: None,
            child_wakes: None,
            phase: Phase::Idle,
            cursor: 0,
            conversation: 0,
        }
        .append_record()?;
        // Seed committed directory/status facts, without starting ten thousand
        // idle runtimes. Root creation and actor admission have separate real
        // lifecycle controls; this query must read only these original facts.
        let mut mutations = Vec::new();
        for index in 0..count {
            let agent = AgentPath::root(&AgentId::from_bytes((index + 2).to_be_bytes()));
            mutations.extend([
                CommitMutation::Append {
                    path: acyclic_stream::StreamPath::new(format!(
                        "{}/{}",
                        roots.as_str(),
                        agent.id()
                    ))?,
                    records: vec![
                        serde_json::to_vec(&serde_json::json!({
                            "kind": "root", "version": 1, "agent": agent,
                        }))?
                        .into(),
                    ],
                },
                CommitMutation::Append {
                    path: agent.status()?,
                    records: vec![status.clone()],
                },
            ]);
            if mutations.len() == 128 || index + 1 == count {
                assert!(matches!(
                    stream
                        .commit(CommitRequest {
                            deadline_unix_millis: None,
                            effects: vec![acyclic_stream::CommitEffect::hard(
                                Vec::new(),
                                std::mem::take(&mut mutations)
                            )],
                            idempotency_key: IdempotencyKey::unique(),
                        })
                        .await?,
                    CommitOutcome::Committed(_)
                ));
            }
        }
        let client = Client::in_process_with_products(
            stream.clone(),
            acyclic_stream::session::testing::authority(),
            None,
            server.clone(),
            Some(server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        client.register_credential(
            0,
            credentials(&[server.resource(), &server.session_resource()]),
        )?;
        let before = stream.requests();
        let Ok(AgentReply::Children { items, next }) = call(
            &client,
            "agents",
            &AgentCommand::Sessions {
                page: AgentPageRequest::new(16, 64 * 1024, None)?,
            },
        )
        .await?
        else {
            return Err("session discovery returned another result".into());
        };
        let work = stream.requests().since(before);
        assert_eq!(items.len(), 16);
        assert!(next.is_some());
        assert!(items.iter().all(|item| item.status.is_some()
            && item.created_at_micros.is_some()
            && item.updated_at_micros.is_some()));
        assert_eq!(
            work.control(),
            2,
            "one hierarchy page and one bounded row view"
        );
        assert_eq!(
            work.data(),
            0,
            "discovery must not read transcripts or volumes"
        );
        Ok(work)
    }
    assert_eq!(work(32).await?, work(10_000).await?);
    Ok(())
}
