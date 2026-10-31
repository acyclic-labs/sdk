//! Corpus inputs built by the current production record encoder.
use acyclic_harness_core::agent::*;
use std::collections::BTreeMap;

pub fn seeds() -> Result<BTreeMap<&'static str, Vec<u8>>, Box<dyn std::error::Error>> {
    let root = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let child_id = AgentId::from_bytes([2; 16]);
    let child = root.child(&child_id)?;
    let mut seeds = BTreeMap::new();
    for (name, record) in [
        (
            "approval",
            InboxRecord::Approval(Approval {
                id: IntentId::try_from("q1")?,
            }),
        ),
        (
            "approval-requested",
            InboxRecord::ApprovalRequested(ApprovalRequested {
                child: child.clone(),
                id: IntentId::try_from("q1")?,
            }),
        ),
        (
            "archive",
            InboxRecord::Archive(Control {
                by: Sender::Agent { path: root.clone() },
                id: IntentId::try_from("a1")?,
            }),
        ),
        (
            "cancel",
            InboxRecord::Cancel(Control {
                by: Sender::User {
                    principal: "user-1".into(),
                },
                id: IntentId::try_from("c1")?,
            }),
        ),
        (
            "child-done-archived",
            InboxRecord::ChildDone(ChildDone {
                child: child.clone(),
                outcome: AgentOutcome::Archived,
            }),
        ),
        (
            "child-done-failed",
            InboxRecord::ChildDone(ChildDone {
                child: child.clone(),
                outcome: AgentOutcome::Failed {
                    message: "tool failed".into(),
                },
            }),
        ),
        (
            "fork-adopted",
            InboxRecord::ForkAdopted(Box::new(ForkAdopted {
                agent: child_id,
                parent: Some(root),
                forked_at: 1,
                conversation_tail: 2,
                checkpoint: None,
                volumes: Vec::new(),
                brief: None,
                policy: None,
            })),
        ),
    ] {
        let mut bytes = vec![0];
        bytes.extend(record.encode()?);
        seeds.insert(name, bytes);
    }
    for (name, phase, cursor) in [
        (
            "status-alarm",
            Phase::WaitingAlarm {
                deadline_unix_ms: 1_700_000_000_000,
            },
            3,
        ),
        (
            "status-done",
            Phase::Done {
                outcome: AgentOutcome::Succeeded { result: None },
            },
            4,
        ),
    ] {
        let mut bytes = vec![1];
        bytes.extend(
            StatusRecord {
                phase,
                cursor,
                conversation: 0,
            }
            .encode()?,
        );
        seeds.insert(name, bytes);
    }
    let mut closed = vec![2];
    closed.extend(
        Closed {
            by: Sender::Host {
                host: "codex".into(),
            },
            id: IntentId::try_from("a1")?,
        }
        .encode()?,
    );
    seeds.insert("closed", closed);
    let mut path = vec![3];
    path.extend(child.as_str().as_bytes());
    seeds.insert("path-child", path);
    let mut invalid_path = vec![3];
    invalid_path.extend(b"harness/agents/0101010101010101010101010101010101");
    seeds.insert("path-bad-id", invalid_path);
    // Mutate a current encoded record, so the negative fails on its field,
    // independently of record-header version changes.
    let mut unknown: serde_json::Value = serde_json::from_slice(&seeds["cancel"][1..])?;
    unknown["record"]["cancel"]["extra"] = serde_json::json!(1);
    let mut bytes = vec![0];
    bytes.extend(serde_json::to_vec(&unknown)?);
    seeds.insert("unknown-field", bytes);
    Ok(seeds)
}
