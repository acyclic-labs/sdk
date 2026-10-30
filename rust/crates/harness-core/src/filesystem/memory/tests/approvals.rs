//! Memory storage opens approvals through the one harness approval owner.
use super::*;
use crate::{
    InteractionId,
    interaction::{ApprovalRoute, ApprovalSubject, Interaction, InteractionOutcome},
};

fn approval() -> Interaction {
    Interaction::Approval {
        prompt: "write approved".into(),
        operation_id: OperationId::new(),
        action_digest: [7; 32],
        subject: ApprovalSubject::Tool {
            route: ApprovalRoute::Policy,
            tool: "write".into(),
            call_id: "call".into(),
            arguments: json!({"path": "approved"}),
            target: None,
        },
    }
}

async fn open(
    storage: &MemoryHarnessStorage,
) -> Result<(InteractionId, Option<InteractionOutcome>)> {
    let journal = storage.journal();
    let id = InteractionId::new();
    let request = approval();
    journal.open_interaction(id, &request, None)?.await?;
    // Opening is exact-once: the same request reopens, a different one conflicts.
    journal.open_interaction(id, &request, None)?.await?;
    assert!(
        journal
            .open_interaction(id, &approval(), None)?
            .await
            .is_err()
    );
    Ok((id, journal.interaction_outcome(id, None)?.await?))
}

#[tokio::test]
async fn default_policy_opens_an_approval_that_waits_for_its_decider() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    assert_eq!(open(&storage).await?.1, None);
    Ok(())
}

#[tokio::test]
async fn human_decision_settles_a_waiting_approval() -> Result<()> {
    for (approved, settled) in [
        (true, InteractionOutcome::Approved {}),
        (false, InteractionOutcome::Declined {}),
    ] {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let (id, waiting) = open(&storage).await?;
        assert_eq!(waiting, None);
        // Only the policy's own principal may decide.
        assert!(
            storage
                .decide_approval(id, ApprovalResponder::Human("intruder"), approved, None)
                .await
                .is_err()
        );
        let human = ApprovalResponder::Human("human");
        assert_eq!(
            storage.decide_approval(id, human, approved, None).await?,
            Some(settled.clone())
        );
        // The same decision replays; the journal sees the settled outcome.
        assert_eq!(
            storage.decide_approval(id, human, approved, None).await?,
            Some(settled.clone())
        );
        assert!(
            storage
                .decide_approval(id, human, !approved, None)
                .await
                .is_err()
        );
        assert_eq!(
            storage.journal().interaction_outcome(id, None)?.await?,
            Some(settled)
        );
    }
    Ok(())
}

#[tokio::test]
async fn graphcoder_preset_opens_an_approval_it_accepts() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536)
        .await?
        .with_approval_policy(crate::preset::Preset::graphcoder().approval_policy)?;
    assert_eq!(
        open(&storage).await?.1,
        Some(InteractionOutcome::Approved {})
    );
    Ok(())
}

#[tokio::test]
async fn approval_policy_binds_before_the_journal_is_shared() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let _shared = storage.journal();
    assert!(
        storage
            .with_approval_policy(ApprovalPolicy::default())
            .is_err()
    );
    Ok(())
}
