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
        let user = Sender::User {
            principal: "owner".into(),
        };
        let base = crate::agent::set_peer_membership(
            &root,
            &user,
            &IntentId::try_from("unchanged-decision")?,
            initial,
        )?;
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
        assert_eq!(PeerMembership::decode(&member.value)?.enabled, !initial);
        assert_eq!(row.profile.is_some(), !initial);

        // Negative control: removing only the row guard accepts the stale
        // command and reproduces membership/directory divergence on this store.
        for effect in &mut stale.effects {
            effect
                .conditions
                .retain(|condition| condition.path() != &path);
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
            PeerMembership::decode(&member.value)?.enabled,
            row.profile.is_some()
        );
    }
    Ok(())
}
