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
        bound: Option<u64>,
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
        bound: Option<u64>,
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
                target: None,
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
            Some(4096),
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
    // A root has no parent stage: the same chain advances to its human.
    assert!(matches!(
        next_stage(&policy, digest, None, &request, &files, 0).await?,
        NextStage::Review {
            approver: Approver::Human { .. },
            expected: ApprovalReviewFile { stage: 3, .. },
            ..
        }
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

/// A root's parent stage is absent, not a refusal; exhaustion is the only
/// implicit deny. A child's parent stage still routes to its parent.
#[tokio::test]
async fn a_root_skips_its_absent_parent_stage_and_only_exhaustion_denies() -> Result<()> {
    let bytes = request(ApprovalRoute::Policy);
    let request = ApprovalRequest::new("request", &bytes, 4096)?;
    let files = Files::default();
    let parent = AgentId::from_bytes([3; 16]);
    let alone = ApprovalPolicy {
        approvers: vec![Approver::Parent],
    };
    let digest = alone.bind()?.identity().digest;
    let NextStage::Decided(review) = next_stage(&alone, digest, None, &request, &files, 0).await?
    else {
        panic!("a root with only a parent stage did not decide");
    };
    assert_eq!(
        (review.principal.as_str(), review.verdict, review.stage),
        ("policy:exhausted", RuleVerdict::Deny, 1)
    );
    let allowing = ApprovalPolicy {
        approvers: vec![
            Approver::Parent,
            Approver::Rules {
                principal: "rules".into(),
                rules: vec![],
                fallback: RuleVerdict::Allow,
            },
        ],
    };
    let digest = allowing.bind()?.identity().digest;
    let NextStage::Review {
        approver: approver @ Approver::Rules { .. },
        expected,
        ..
    } = next_stage(&allowing, digest, None, &request, &files, 0).await?
    else {
        panic!("a root did not advance past its absent parent stage");
    };
    assert_eq!(expected.stage, 1);
    let journal = crate::executor::tests::Journal::default();
    let review = super::super::automatic_agent_review(
        (&journal, None),
        &BTreeMap::new(),
        &files,
        &request,
        approver,
        expected,
        crate::conversation::Limits::default(),
    )
    .await?;
    assert_eq!(review.verdict, RuleVerdict::Allow);
    assert!(matches!(
        next_stage(&allowing, digest, None, &request, &files, 0).await?,
        NextStage::Decided(ApprovalReviewFile {
            verdict: RuleVerdict::Allow,
            stage: 1,
            ..
        })
    ));
    let NextStage::Review {
        approver: Approver::Parent,
        expected,
        ..
    } = next_stage(&allowing, digest, Some(parent.clone()), &request, &files, 0).await?
    else {
        panic!("a child's parent stage did not route to its parent");
    };
    assert_eq!(
        (expected.principal, expected.stage),
        (format!("parent:{parent}"), 0)
    );
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
        (1, &decision),
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
            (1, &decision),
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
            (1, &decision),
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
            (2, &decision),
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
            Some(4096),
        )
    };
    let (first, second) = futures::join!(create(), create());
    assert_ne!(first?, second?);
    assert!(
        super::super::files::create_exact(
            &files,
            "approvals/request/reviews/model.json",
            b"changed".to_vec(),
            Some(4096)
        )
        .await
        .is_err()
    );
    assert_eq!(
        files
            .read("approvals/request/reviews/model.json", Some(4096))
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
    let journal = crate::executor::tests::Journal::default();
    let review = super::super::automatic_agent_review(
        (&journal, None),
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
        (1, &decision),
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
            (1, &decision),
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
        (1, &decision),
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
                (1, &decision),
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
            (1, &decision),
        )
        .await
        .is_err()
    );
    Ok(())
}

/// Every kind of approver, with a rules stage that would allow anything.
fn every_approver() -> ApprovalPolicy {
    ApprovalPolicy {
        approvers: vec![
            Approver::Rules {
                principal: "rules".into(),
                rules: vec![],
                fallback: RuleVerdict::Allow,
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
    }
}

/// The request a plugin `{ask, to}` decision produces, routed by the shipping
/// decoder rather than by the test.
async fn asked(to: &str) -> Result<Vec<u8>> {
    let route = crate::hooks::plugin_ask_route(serde_json::json!({"ask": "write?", "to": to}))
        .await?
        .expect("an ask decision asks");
    Ok(request(route))
}

fn denied(stage: NextStage<'_>, reason: &str) -> bool {
    matches!(
        stage,
        NextStage::Decided(ApprovalReviewFile {
            verdict: RuleVerdict::Deny,
            reason: Some(given),
            ..
        }) if given == reason
    )
}

#[tokio::test]
async fn a_person_ask_is_reviewed_by_a_person_alone() -> Result<()> {
    let policy = every_approver();
    let digest = policy.bind()?.identity().digest;
    let bytes = asked("person").await?;
    let request = ApprovalRequest::new("request", &bytes, 4096)?;
    let parent = AgentId::from_bytes([3; 16]);
    assert!(matches!(
        next_stage(
            &policy,
            digest,
            Some(parent),
            &request,
            &Files::default(),
            0
        )
        .await?,
        NextStage::Review {
            approver: Approver::Human { .. },
            expected: ApprovalReviewFile { stage: 0, .. },
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
async fn a_model_ask_is_reviewed_by_the_model_alone() -> Result<()> {
    let policy = every_approver();
    let digest = policy.bind()?.identity().digest;
    let bytes = asked("model").await?;
    let request = ApprovalRequest::new("request", &bytes, 4096)?;
    let files = Files::default();
    let parent = AgentId::from_bytes([3; 16]);
    // The allowing rules stage is not consulted: the model is the first reviewer.
    let NextStage::Review {
        approver: Approver::Llm { .. },
        mut expected,
        ..
    } = next_stage(&policy, digest, Some(parent.clone()), &request, &files, 0).await?
    else {
        panic!("a model ask was not reviewed by the model");
    };
    assert_eq!(expected.stage, 1);
    // A model that will not decide denies; no parent or person decides instead.
    expected.verdict = RuleVerdict::Ask;
    super::super::files::create_exact(
        &files,
        &request.review_path(&expected.principal),
        serde_json::to_vec(&expected).unwrap(),
        Some(4096),
    )
    .await?;
    assert!(denied(
        next_stage(&policy, digest, Some(parent.clone()), &request, &files, 0).await?,
        "approval chain exhausted without permission"
    ));
    // Without a model in the chain, nobody else is asked in its place.
    let policy = ApprovalPolicy {
        approvers: vec![
            Approver::Parent,
            Approver::Human {
                principal: "human".into(),
            },
        ],
    };
    let digest = policy.bind()?.identity().digest;
    assert!(denied(
        next_stage(
            &policy,
            digest,
            Some(parent),
            &request,
            &Files::default(),
            0
        )
        .await?,
        "approval model is unavailable"
    ));
    Ok(())
}

#[tokio::test]
async fn a_parent_ask_is_reviewed_by_the_parent_alone() -> Result<()> {
    let policy = every_approver();
    let digest = policy.bind()?.identity().digest;
    let bytes = asked("parent").await?;
    let request = ApprovalRequest::new("request", &bytes, 4096)?;
    let files = Files::default();
    let parent = AgentId::from_bytes([3; 16]);
    let NextStage::Review {
        approver: Approver::Parent,
        expected,
        ..
    } = next_stage(&policy, digest, Some(parent.clone()), &request, &files, 0).await?
    else {
        panic!("a parent ask was not reviewed by the parent");
    };
    assert_eq!(expected.principal, format!("parent:{parent}"));
    // Without a parent, the ask denies rather than reaching a person.
    assert!(denied(
        next_stage(&policy, digest, None, &request, &files, 0).await?,
        "approval parent is unavailable"
    ));
    Ok(())
}
