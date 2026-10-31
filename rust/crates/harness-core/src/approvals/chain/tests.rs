use super::*;
use crate::{
    approvals::ApprovalCreate,
    interaction::{ApprovalRoute, ApprovalSubject, Interaction},
    runtime::ToolPolicy,
};
use acyclic_stream::BoxProviderFuture;
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
struct Files(Mutex<BTreeMap<String, Vec<u8>>>);
impl ApprovalFiles for Files {
    fn read<'a>(
        &'a self,
        path: &'a str,
        bound: u64,
    ) -> BoxProviderFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .get(path)
                .map(|bytes| {
                    super::super::files::check_bound(bytes.len(), bound)?;
                    Ok(bytes.clone())
                })
                .transpose()
        })
    }
    fn create<'a>(
        &'a self,
        path: &'a str,
        bytes: Vec<u8>,
        bound: u64,
    ) -> BoxProviderFuture<'a, Result<ApprovalCreate>> {
        Box::pin(async move {
            super::super::files::check_bound(bytes.len(), bound)?;
            let mut files = self.0.lock().unwrap();
            match files.entry(path.to_owned()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(bytes);
                    Ok(ApprovalCreate::Created)
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    Ok(ApprovalCreate::Existing(entry.get().clone()))
                }
            }
        })
    }
}

fn request(route: ApprovalRoute) -> Vec<u8> {
    serde_json::to_vec(
        &Interaction::approval(
            "review".into(),
            crate::OperationId::from_bytes([1; 16]),
            [2; 32],
            ApprovalSubject::Tool {
                route,
                tool: "write".into(),
                call_id: "call".into(),
                arguments: serde_json::json!({"path":"approved"}),
            },
        )
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn agent_reviews_escalate_in_order_and_external_files_never_grant_authority() -> Result<()> {
    let policy = ApprovalPolicy {
        approvers: vec![
            Approver::Rules {
                principal: "rules".into(),
                rules: vec![],
                fallback: RuleVerdict::Ask,
            },
            Approver::Llm {
                principal: "model".into(),
                model: "independent".into(),
            },
            Approver::Parent,
            Approver::Human {
                principal: "human".into(),
            },
        ],
    };
    let digest = policy.bind()?.identity().digest;
    let bytes = request(ApprovalRoute::Policy);
    let request = ApprovalRequest::new("request", &bytes, 4096)?;
    let files = Files::default();
    let parent = AgentId::from_bytes([3; 16]);
    for stage in 0..3 {
        let NextStage::Review {
            mut expected,
            approver,
            ..
        } = next_stage(&policy, digest, Some(parent.clone()), &request, &files, 0).await?
        else {
            panic!("chain skipped a required reviewer");
        };
        assert_eq!(expected.stage, stage);
        expected.verdict = if stage == 2 {
            RuleVerdict::Allow
        } else {
            RuleVerdict::Ask
        };
        if stage == 2 {
            assert!(matches!(approver, Approver::Parent));
            assert_eq!(expected.principal, format!("parent:{parent}"));
        }
        super::super::files::create_exact(
            &files,
            &request.review_path(&expected.principal),
            serde_json::to_vec(&expected).unwrap(),
            4096,
        )
        .await?;
    }
    assert!(matches!(
        next_stage(&policy, digest, Some(parent.clone()), &request, &files, 0).await?,
        NextStage::Review {
            approver: Approver::Parent,
            retained: Some(_),
            ..
        }
    ));
    // A gone parent refuses the action even if a later human could approve it.
    assert!(matches!(
        next_stage(&policy, digest, None, &request, &files, 0).await?,
        NextStage::Decided(ApprovalReviewFile {
            verdict: RuleVerdict::Deny,
            ..
        })
    ));
    // Parent mode only advances through its authenticated owner, never through
    // the Allow review above. A caller may inspect the human stage explicitly.
    assert!(matches!(
        next_stage(&policy, digest, Some(parent.clone()), &request, &files, 3).await?,
        NextStage::Review {
            approver: Approver::Human { .. },
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
async fn human_override_ignores_policy_allow_and_policy_digest_must_match() -> Result<()> {
    let policy = ApprovalPolicy {
        approvers: vec![Approver::Rules {
            principal: "rules".into(),
            rules: vec![],
            fallback: RuleVerdict::Allow,
        }],
    };
    let digest = policy.bind()?.identity().digest;
    let bytes = request(ApprovalRoute::Human);
    let request = ApprovalRequest::new("request", &bytes, 4096)?;
    let files = Files::default();
    assert!(matches!(
        next_stage(&policy, digest, None, &request, &files, 0).await?,
        NextStage::Review {
            approver: Approver::Human { .. },
            ..
        }
    ));
    let descriptor =
        crate::conversation::FileDescriptor::from_bytes(&bytes, "application/json", &mut None)?;
    let decision = ApprovalDecisionFile::new("human", descriptor.sha256(), 1, true, None)?;
    validate_agent_decision(
        &policy,
        digest,
        None,
        &request,
        &files,
        ApprovalResponder::Human("human"),
        1,
        &decision,
    )
    .await?;
    assert!(
        validate_agent_decision(
            &policy,
            digest,
            None,
            &request,
            &files,
            ApprovalResponder::Human("forged"),
            1,
            &decision
        )
        .await
        .is_err()
    );
    assert!(
        validate_agent_decision(
            &policy,
            digest,
            None,
            &request,
            &files,
            ApprovalResponder::Rules("human"),
            1,
            &decision
        )
        .await
        .is_err()
    );
    assert!(
        validate_agent_decision(
            &policy,
            digest,
            None,
            &request,
            &files,
            ApprovalResponder::Human("human"),
            2,
            &decision
        )
        .await
        .is_err()
    );
    assert!(
        next_stage(&policy, [255; 32], None, &request, &files, 0)
            .await
            .is_err()
    );
    assert!(ApprovalRequest::new("../request", &bytes, 4096).is_err());
    assert!(ApprovalRequest::new("request", &bytes, 1).is_err());
    Ok(())
}

#[tokio::test]
async fn dispatch_creation_has_one_winner_and_never_replaces_conflicting_bytes() -> Result<()> {
    let files = Files::default();
    let create = || {
        super::super::files::create_exact(
            &files,
            "approvals/request/reviews/model.json",
            b"dispatch".to_vec(),
            4096,
        )
    };
    let (first, second) = futures::join!(create(), create());
    assert_ne!(first?, second?);
    assert!(
        super::super::files::create_exact(
            &files,
            "approvals/request/reviews/model.json",
            b"changed".to_vec(),
            4096
        )
        .await
        .is_err()
    );
    assert_eq!(
        files
            .read("approvals/request/reviews/model.json", 4096)
            .await?,
        Some(b"dispatch".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn automatic_and_parent_decisions_require_their_authenticated_role() -> Result<()> {
    let policy = ApprovalPolicy {
        approvers: vec![Approver::Rules {
            principal: "rules".into(),
            rules: vec![],
            fallback: RuleVerdict::Allow,
        }],
    };
    let digest = policy.bind()?.identity().digest;
    let bytes = request(ApprovalRoute::Policy);
    let request = ApprovalRequest::new("request", &bytes, 4096)?;
    let files = Files::default();
    let NextStage::Review {
        approver, expected, ..
    } = next_stage(&policy, digest, None, &request, &files, 0).await?
    else {
        panic!("rules must evaluate before deciding");
    };
    let review = super::super::automatic_agent_review(
        &BTreeMap::new(),
        &files,
        &request,
        approver,
        expected,
        crate::conversation::Limits::default(),
    )
    .await?;
    assert_eq!(review.verdict, RuleVerdict::Allow);
    let descriptor =
        crate::conversation::FileDescriptor::from_bytes(&bytes, "application/json", &mut None)?;
    let decision = ApprovalDecisionFile::new("rules", descriptor.sha256(), 1, true, review.reason)?;
    validate_agent_decision(
        &policy,
        digest,
        None,
        &request,
        &files,
        ApprovalResponder::Rules("rules"),
        1,
        &decision,
    )
    .await?;
    assert!(
        validate_agent_decision(
            &policy,
            digest,
            None,
            &request,
            &files,
            ApprovalResponder::Llm("rules"),
            1,
            &decision,
        )
        .await
        .is_err()
    );

    let policy = ApprovalPolicy {
        approvers: vec![Approver::Parent],
    };
    let digest = policy.bind()?.identity().digest;
    let parent = AgentId::from_bytes([3; 16]);
    let decision = ApprovalDecisionFile::new(
        &format!("parent:{parent}"),
        descriptor.sha256(),
        1,
        true,
        None,
    )?;
    validate_agent_decision(
        &policy,
        digest,
        Some(parent.clone()),
        &request,
        &files,
        ApprovalResponder::Parent(&parent),
        1,
        &decision,
    )
    .await?;
    let foreign_parent = AgentId::from_bytes([4; 16]);
    for responder in [
        ApprovalResponder::Parent(&foreign_parent),
        ApprovalResponder::Human(&decision.principal),
    ] {
        assert!(
            validate_agent_decision(
                &policy,
                digest,
                Some(parent.clone()),
                &request,
                &files,
                responder,
                1,
                &decision,
            )
            .await
            .is_err()
        );
    }
    assert!(
        validate_agent_decision(
            &policy,
            digest,
            None,
            &request,
            &files,
            ApprovalResponder::Parent(&parent),
            1,
            &decision,
        )
        .await
        .is_err()
    );
    Ok(())
}
