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
        },
    }
}

async fn open(storage: &MemoryHarnessStorage) -> Result<Option<InteractionOutcome>> {
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
    journal.interaction_outcome(id, None)?.await
}

#[tokio::test]
async fn default_policy_opens_an_approval_that_waits_for_its_decider() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    assert_eq!(open(&storage).await?, None);
    Ok(())
}

#[tokio::test]
async fn graphcoder_preset_opens_an_approval_it_accepts() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536)
        .await?
        .with_approval_policy(crate::preset::Preset::graphcoder().approval_policy)?;
    assert_eq!(open(&storage).await?, Some(InteractionOutcome::Approved {}));
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
