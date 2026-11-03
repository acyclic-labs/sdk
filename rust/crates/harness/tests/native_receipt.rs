//! Receipt admission through the public native workspace owner.
#[path = "support/native_receipt.rs"]
mod contract;

use acyclic_harness::OperationId;
use acyclic_harness::{
    agent::AgentPath,
    local::{
        LocalHarness, LocalHarnessConfig,
        host_owner::{HostAction, HostWorkspaceOwner, SessionAction},
    },
};
use std::sync::Arc;

/// Capture real replies from the shipped native owner, including siblings,
/// a nested child, and a fresh owner. No native model process is executed.
#[tokio::test]
async fn native_receipt_contract_uses_original_owner_replies()
-> Result<(), Box<dyn std::error::Error>> {
    let checkout = tempfile::tempdir()?;
    let state = tempfile::tempdir()?;
    let open = || {
        LocalHarnessConfig::native_host(checkout.path()).state_directory(state.path().join("owner"))
    };
    let owner = HostWorkspaceOwner::new(Arc::new(LocalHarness::open(open()).await?))?;
    let root = owner
        .dispatch(
            OperationId::new(),
            HostAction::Open {
                host: "codex".into(),
                session: "receipt-contract".into(),
            },
        )
        .await?;
    let parent: AgentPath = serde_json::from_value(root["task"].clone())?;
    let spawn = |parent, reference: &str| HostAction::Session {
        parent,
        reference: reference.into(),
        action: SessionAction::Spawn {
            prompt: "receipt contract".into(),
        },
        next: false,
    };
    let first = owner
        .dispatch(OperationId::new(), spawn(parent.clone(), "first"))
        .await?;
    let sibling = owner
        .dispatch(OperationId::new(), spawn(parent.clone(), "sibling"))
        .await?;
    let child: AgentPath = serde_json::from_value(first["task"].clone())?;
    let nested = owner
        .dispatch(OperationId::new(), spawn(child, "nested"))
        .await?;
    owner.retire().await?;
    let owner = HostWorkspaceOwner::new(Arc::new(LocalHarness::open(open()).await?))?;
    let reopened = owner
        .dispatch(
            OperationId::new(),
            HostAction::Session {
                parent,
                reference: "first".into(),
                action: SessionAction::Observe {},
                next: false,
            },
        )
        .await?;
    owner.retire().await?;
    let replies = vec![first, root, sibling, nested, reopened];
    let original = serde_json::to_vec(&replies)?;
    contract::verify(&original);
    println!("NATIVE_RECEIPT_CAPTURE={}", String::from_utf8(original)?);
    Ok(())
}
