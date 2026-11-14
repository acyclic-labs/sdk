//! Deterministic commit interleavings, using the original Streams backend.
use super::*;
use crate::agent::{
    AgentId, AgentRecord as _, AgentState, ForkAdopted, IntentId, PeerMembership, Queued, Routing,
    Sender, TurnDriver, TurnFuture,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    guest::CancellationToken,
    local::SystemClock,
    route::{NodeId, Paths},
};
use acyclic_stream::{CommitOutcome, MemoryStream, StreamPath, StreamProviderExt as _};
use std::sync::Arc;

struct Unused;

impl TurnDriver for Unused {
    fn step<'a>(
        &'a self,
        _: &'a AgentState,
        _: u64,
        _: Option<Queued>,
        _: CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async { Err(Error::Record("no model work in a control interleaving")) })
    }
}

#[tokio::test]
async fn unchanged_membership_fences_a_concurrent_opposite_decision()
-> Result<(), Box<dyn std::error::Error>> {
    for initial in [false, true] {
        let (provider, host, root) = root_with_membership(initial).await?;
        let user = Sender::User {
            principal: "owner".into(),
        };
        let base = crate::agent::set_peer_membership(
            provider.as_ref(),
            &root,
            &user,
            &IntentId::try_from("unchanged-decision")?,
            initial,
        )
        .await?;
        let mut stale = host.presence_control(&root, base, initial).await?;
        host.set_peer_membership(
            &root,
            &user,
            &IntentId::try_from("opposite-decision")?,
            !initial,
        )
        .await?;
        assert!(
            matches!(
                provider.commit(stale.clone()).await?,
                CommitOutcome::Conflict(_)
            ),
            "a stale unchanged decision overwrote a later membership choice"
        );
        let path = SessionPublication::row_path(&host.actor(&root)?)?;
        let record = provider
            .head(path.clone())
            .await?
            .last
            .ok_or("row missing")?;
        let row = SessionRow::decode(&record.value)?;
        let member = provider
            .head(root.peer_membership()?)
            .await?
            .last
            .ok_or("membership missing")?;
        assert_eq!(PeerMembership::decode(&member.value)?.enabled(), !initial);
        assert_eq!(row.profile.is_some(), !initial);

        // Negative control: removing the captured row and membership fences
        // accepts the stale command and reproduces directory divergence.
        let membership = root.peer_membership()?;
        for effect in &mut stale.effects {
            effect
                .conditions
                .retain(|condition| condition.path() != &path && condition.path() != &membership);
        }
        assert!(matches!(
            provider.commit(stale).await?,
            CommitOutcome::Committed(_)
        ));
        let member = provider
            .head(root.peer_membership()?)
            .await?
            .last
            .ok_or("membership missing")?;
        assert_ne!(
            PeerMembership::decode(&member.value)?.enabled(),
            row.profile.is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn rejoining_requires_original_display_metadata() -> Result<(), Box<dyn std::error::Error>> {
    for inherited in [false, true] {
        let (provider, host, root) = root_with_membership(false).await?;
        let path = SessionPublication::row_path(&host.actor(&root)?)?;
        let original = if inherited {
            StreamPath::new("other-session-row")?
        } else {
            path.clone()
        };
        let profile = crate::agent::SessionProfile::project(
            &crate::agent::Phase::Idle,
            "Original title".into(),
            &[],
            "Original goal",
            64,
        );
        let row = SessionRow::new(NodeId::new(b"node")?, profile.clone());
        let mut effects = vec![row.effect(original.clone())?];
        if inherited {
            effects.push(acyclic_stream::CommitEffect::hard(
                vec![acyclic_stream::CommitCondition::Absent { path: path.clone() }],
                vec![CommitMutation::Fork {
                    source: original.clone(),
                    destination: path.clone(),
                    at_tail: 1,
                    exclude: Vec::new(),
                    records: Vec::new(),
                }],
            ));
        }
        // Forks select pre-commit history, so publish their source first.
        for effect in effects {
            assert!(matches!(
                provider
                    .commit(CommitRequest {
                        deadline_unix_millis: None,
                        idempotency_key: acyclic_stream::IdempotencyKey::unique(),
                        effects: vec![effect],
                    })
                    .await?,
                CommitOutcome::Committed(_)
            ));
        }
        let preceding = provider
            .record_at(path.clone(), 0)
            .await?
            .ok_or("row missing")?;
        assert_eq!(SessionRow::decode(&preceding.value)?, row);
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: acyclic_stream::IdempotencyKey::unique(),
                    effects: vec![
                        row.changed(None)?
                            .ok_or("removal missing")?
                            .effect(path.clone())?
                    ],
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        let result = host
            .set_peer_membership(
                &root,
                &Sender::User {
                    principal: "owner".into(),
                },
                &IntentId::try_from("rejoin")?,
                true,
            )
            .await;
        let head = provider.head(path).await?;
        let member = provider
            .head(root.peer_membership()?)
            .await?
            .last
            .ok_or("membership missing")?;
        if inherited {
            assert!(result.is_err());
            assert_eq!(head.tail, 2);
            assert!(!PeerMembership::decode(&member.value)?.enabled());
        } else {
            result?;
            assert_eq!(head.tail, 3);
            let current = SessionRow::decode(&head.last.ok_or("rejoined row missing")?.value)?;
            assert_eq!(current.profile, Some(profile));
            assert!(PeerMembership::decode(&member.value)?.enabled());
        }
    }
    Ok(())
}

async fn root_with_membership(
    initial: bool,
) -> Result<(Arc<MemoryStream>, AgentHost<MemoryStream>, AgentPath), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider.clone(),
        Unused,
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::default(),
        Arc::new(SystemClock),
    )?
    .with_root_placement(Some(Routing {
        paths: Paths::new(StreamPath::new("account/routing")?),
        node: NodeId::new(b"node")?,
    }));
    let created = host.prepare_root_with_peer(
        &IntentId::try_from("create")?,
        ForkAdopted {
            agent: AgentId::from_bytes([1; 16]),
            parent: None,
            profile: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        },
        false,
        initial,
    )?;
    let root = created.child;
    assert!(matches!(
        provider.commit(created.request).await?,
        CommitOutcome::Committed(_)
    ));
    Ok((provider, host, root))
}
