#![cfg(all(feature = "filesystem-local", feature = "native-execution"))]

#[path = "../fuzz-native-journal/oracles/native_receipt.rs"]
mod contract;

use acyclic_harness::OperationId;
use acyclic_harness::{
    agent::AgentPath,
    local::{
        LocalHarness, LocalHarnessConfig,
        host_owner::{HostAction, HostWorkspaceOwner, SessionAction},
    },
};
use serde_json::Value;
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
                action: SessionAction::Observe,
                next: false,
            },
        )
        .await?;
    owner.retire().await?;
    let replies = vec![first, root, sibling, nested, reopened];
    let original = serde_json::to_vec(&replies)?;
    contract::verify(&original);
    let mut remounted = replies.clone();
    remounted
        .last_mut()
        .ok_or("original reopened reply absent")?["cwd"] = remounted[3]["cwd"].clone();
    contract::verify(&serde_json::to_vec(&remounted)?);
    // These are explicit mutations of actual producer bytes, not invented
    // external responses. Cover absent/wrong task and view types at both cuts.
    for field in ["task", "cwd"] {
        for replacement in [
            Value::Null,
            Value::Bool(false),
            Value::from(0),
            Value::String(String::new()),
            serde_json::json!([]),
            serde_json::json!({}),
        ] {
            for index in [0, replies.len() - 1] {
                let mut mutated = replies.clone();
                mutated[index][field] = replacement.clone();
                contract::verify(&serde_json::to_vec(&mutated)?);
            }
        }
        for index in [0, replies.len() - 1] {
            let mut mutated = replies.clone();
            mutated[index]
                .as_object_mut()
                .ok_or("original reply is not an object")?
                .remove(field);
            contract::verify(&serde_json::to_vec(&mutated)?);
        }
    }
    println!("NATIVE_RECEIPT_CAPTURE={}", String::from_utf8(original)?);
    Ok(())
}
