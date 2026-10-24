//! A human connection submits to the same owner as every automatic approver.
use super::*;

pub(super) enum NextStage<'a> {
    Review {
        approver: &'a Approver,
        expected: ApprovalReviewFile,
        retained: Option<ApprovalReviewFile>,
    },
    Decided(ApprovalReviewFile),
}

// One traversal decides the active policy stage for execution and every client.
pub(super) fn next_stage<'a>(
    policy: &'a crate::approvals::ApprovalPolicy,
    digest: [u8; 32],
    parent: Option<TaskId>,
    request: &FileValue,
    files: &BTreeMap<FileKey, FileValue>,
    start: usize,
) -> Result<NextStage<'a>> {
    let hash = fingerprint(request)?;
    for (stage, approver) in policy.approvers.iter().enumerate().skip(start) {
        let principal = match approver {
            Approver::Rules { principal, .. }
            | Approver::Llm { principal, .. }
            | Approver::Human { principal } => principal.clone(),
            Approver::Parent => match parent {
                Some(parent) => format!("parent:{parent}"),
                None => continue,
            },
        };
        let expected = ApprovalReviewFile {
            principal,
            request_sha256: hash.clone(),
            policy_digest: digest,
            stage,
            verdict: RuleVerdict::Ask,
            reason: None,
        };
        let retained = retained_review(
            files.get(&review_key(request, &expected.principal)?),
            &expected,
        )?;
        if !matches!(approver, Approver::Human { .. } | Approver::Parent) {
            if let Some(review) = &retained {
                if review.verdict == RuleVerdict::Ask {
                    continue;
                }
                return Ok(NextStage::Decided(review.clone()));
            }
        }
        return Ok(NextStage::Review {
            approver,
            expected,
            retained,
        });
    }
    Ok(NextStage::Decided(ApprovalReviewFile {
        principal: "policy:exhausted".into(),
        request_sha256: hash,
        policy_digest: digest,
        stage: policy.approvers.len(),
        verdict: RuleVerdict::Deny,
        reason: Some("approval chain exhausted without permission".into()),
    }))
}

pub(crate) struct HumanConnection {
    runtime: Arc<LocalTaskRuntime>,
    policy: crate::approvals::ApprovalPolicy,
    maximum_bytes: u64,
}
impl HumanConnection {
    pub(super) fn new(
        runtime: Arc<LocalTaskRuntime>,
        policy: crate::approvals::ApprovalPolicy,
        maximum_bytes: u64,
    ) -> Self {
        Self {
            runtime,
            policy,
            maximum_bytes,
        }
    }

    async fn request(&self, task: TaskId, request: &Component) -> Result<(FileValue, String)> {
        let identity = self.policy.bind()?.identity();
        if self.runtime.approval_policy_identity().as_ref() != Some(&identity) {
            return Err(Error::Unauthorized(
                "human connection policy differs from runtime".into(),
            ));
        }
        let volume = self.runtime.approval_files(task).await?;
        let snapshot = volume
            .snapshot(
                &Query::Approval {
                    task: Component::new(task.to_string())?,
                    request: request.clone(),
                },
                self.policy.approvers.len() * 2 + 3,
            )
            .await?;
        let files = snapshot
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect::<BTreeMap<_, _>>();
        let key = FileKey::Task {
            task: Component::new(task.to_string())?,
            file: TaskFile::ApprovalRequest(request.clone()),
        };
        let request = files
            .get(&key)
            .ok_or_else(|| Error::NotFound("approval request is absent".into()))?;
        let parent = self
            .runtime
            .task_host()
            .observe_admission(task)
            .await?
            .parent;
        match next_stage(&self.policy, identity.digest, parent, request, &files, 0)? {
            NextStage::Review {
                approver: Approver::Human { .. },
                expected,
                retained,
            } => {
                ensure_waiting_review(&volume, request, &expected, retained.is_some()).await?;
                Ok((request.clone(), expected.principal))
            }
            _ => Err(Error::Unauthorized(
                "human is not the current approval stage".into(),
            )),
        }
    }

    pub(crate) async fn submit(&self, file: &FileValue) -> Result<()> {
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision { request, .. },
        } = &file.key
        else {
            return Err(Error::Unauthorized(
                "only an approval decision can be submitted".into(),
            ));
        };
        let task = TaskId::parse(task.as_str())?;
        let (_, principal) = self.request(task, request).await?;
        let decision = ApprovalDecisionFile::decode(&file.bytes, self.maximum_bytes)?;
        if decision.principal != principal {
            return Err(Error::Unauthorized(
                "decision principal differs from the active human".into(),
            ));
        }
        self.runtime.submit_reviewed_approval(task, file).await
    }

    pub(crate) async fn decide(&self, request: &FileValue, approved: bool) -> Result<()> {
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalRequest(id),
        } = &request.key
        else {
            return Err(Error::Invalid("not an approval request".into()));
        };
        let task = TaskId::parse(task.as_str())?;
        let (original, principal) = self.request(task, id).await?;
        if original != *request {
            return Err(Error::Unauthorized(
                "client request differs from its owner".into(),
            ));
        }
        let descriptor = crate::conversation::FileDescriptor::from_bytes(
            &request.bytes,
            "application/json",
            &mut None,
        )?;
        let decision =
            ApprovalDecisionFile::new(&principal, descriptor.sha256(), 1, approved, None)?;
        let file = FileValue::new(
            FileKey::Task {
                task: Component::new(task.to_string())?,
                file: TaskFile::ApprovalDecision {
                    request: id.clone(),
                    principal: Component::new(ApprovalDecisionFile::principal_file(&principal))?,
                },
            },
            serde_json::to_vec(&decision).map_err(storage)?,
        );
        self.runtime.submit_reviewed_approval(task, &file).await
    }
}

impl crate::client::ApprovalResponder for HumanConnection {
    fn decide<'a>(
        &'a self,
        request: &'a FileValue,
        approved: bool,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(self.decide(request, approved))
    }
}

pub(super) async fn ensure_waiting_review(
    files: &crate::task_volume::TaskFiles<LocalStream>,
    request: &FileValue,
    expected: &ApprovalReviewFile,
    retained: bool,
) -> Result<()> {
    if !retained {
        publish_review(
            files,
            review_key(request, &expected.principal)?,
            &ApprovalReviewFile {
                reason: Some("awaiting authenticated decision".into()),
                ..expected.clone()
            },
        )
        .await?;
    }
    Ok(())
}
