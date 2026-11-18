//! Parent controls retain original input and policy cuts before independent review.
use super::*;
use crate::agent::{Approval, ApprovalSelection, InboxRecord, OpenTriage, Out};
use crate::approvals::{ApprovalResponder, RuleVerdict};
use serde::Serialize;

/// An agent that completed or was archived runs no turn and reviews nothing,
/// yet its children's asks still reach it; it refuses each one.
fn ended(agent: &AgentState) -> bool {
    agent.closed || matches!(agent.phase, crate::agent::Phase::Done { .. })
}

#[derive(Serialize)]
struct Escalation<'a> {
    child: &'a AgentPath,
    request: &'a crate::agent::IntentId,
    reason: Option<&'a str>,
    action: &'a serde_json::value::RawValue,
}

impl<P, A, O> FilesystemTurnHost<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn select_approval_page(
        &self,
        agent: &AgentState,
        inbox: u64,
        cursor: u64,
    ) -> Result<ApprovalSelection> {
        let policy = if ended(agent) {
            None
        } else {
            let (_, bindings, _) = self.parent_review_binding(agent, None).await?;
            bindings
                .content
                .reader
                .read_private_path(
                    &bindings.content.volume,
                    "",
                    "policy/attention.md",
                    None,
                    self.executors.content_limits().file_bytes,
                    crate::conversation::ContentAccess::default(),
                )?
                .await?
                .map(|(file, _, _)| file)
        };
        let selection = ApprovalSelection {
            source: agent.path.clone(),
            range: inbox..cursor.min(inbox.saturating_add(crate::agent::MAX_QUEUED as u64)),
            policy,
        };
        selection
            .validate(&agent.path, inbox)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(selection)
    }

    pub(super) async fn review_approval_page(
        &self,
        agent: &AgentState,
        selection: &ApprovalSelection,
    ) -> Result<OpenTriage> {
        selection
            .validate(&agent.path, selection.range.start)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let review = if ended(agent) {
            None
        } else {
            Some(
                self.parent_review_binding(agent, selection.policy.as_ref())
                    .await?,
            )
        };
        let policy = review
            .as_ref()
            .map(|(_, _, bytes)| {
                std::str::from_utf8(bytes)
                    .map_err(|_| Error::Invalid("review policy is not UTF-8".into()))
            })
            .transpose()?;
        let inbox = agent
            .path
            .inbox()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let mut records = self
            .stream
            .read(ReadRequest {
                key: None,
                path: inbox.clone(),
                from: selection.range.start,
                limit: u32::try_from(selection.range.end - selection.range.start)
                    .map_err(|_| Error::Invalid("approval page size overflows".into()))?,
                at_least: Default::default(),
            })
            .await?;
        // A committed closed marker means the parent's closing cascade will
        // archive every open child. Its asks are answered by that archive, so
        // review must not read or write their approval journals.
        let closing = if agent.closed {
            true
        } else {
            match self.stream.tail(agent.path.closed()?).await {
                Ok(_) => true,
                Err(error) if error.category() == acyclic_stream::StreamCategory::NotFound => false,
                Err(error) => return Err(Error::from(error)),
            }
        };
        let parent = agent.path.id();
        let principal = format!("parent:{parent}");
        let mut outs = Vec::new();
        let mut summary = Vec::new();
        let mut next = selection.range.start;
        while let Some(record) = records.try_next().await? {
            if record.sequence != next
                || record.sequence >= selection.range.end
                || record.origin != inbox
            {
                return Err(Error::Invalid(
                    "approval source page differs from its original cut".into(),
                ));
            }
            next += 1;
            let InboxRecord::ApprovalRequested(request) = InboxRecord::decode(&record.value)
                .map_err(|error| Error::Invalid(error.to_string()))?
            else {
                continue;
            };
            if !request.child.is_child_of(&agent.path) {
                return Err(Error::Unauthorized(
                    "review source is not a direct child".into(),
                ));
            }
            let child = crate::agent::observe_state(&self.stream, &self.within, &request.child)
                .await
                .map_err(Error::from)?;
            // A page interrupted after this answer was saved, before its wake
            // was published, keeps that answer and only repairs the wake: a
            // second review, or an ended parent's refusal, would contradict it.
            // This holds even when the parent has since been archived, so the
            // saved answer is checked before the closing cascade's skip.
            if self
                .decided_approval(&child, &request.id, &principal)
                .await?
                .is_some()
            {
                if !child.closed {
                    outs.push(Out::Send {
                        to: request.child,
                        record: InboxRecord::Approval(Approval { id: request.id }),
                    });
                }
                continue;
            }
            // A closing parent answers each new ask with its archive cascade.
            // Saved answers above still publish their wakes; no new child
            // journal write is admitted. The committed closed marker covers a
            // parent whose archive has not yet been admitted to its actor.
            if closing {
                continue;
            }
            let (verdict, reason, action) = match (&review, policy) {
                (Some((journal, _, _)), Some(policy)) => {
                    self.review_child_action(agent, &child, &request.id, journal, policy)
                        .await?
                }
                _ => (
                    RuleVerdict::Deny,
                    Some(crate::approvals::PARENT_ENDED.to_owned()),
                    Vec::new(),
                ),
            };
            let decision = match verdict {
                RuleVerdict::Allow => Some(crate::interaction::ApprovalVerdict::Approve),
                RuleVerdict::Deny => Some(crate::interaction::ApprovalVerdict::Deny),
                RuleVerdict::Ask => None,
            };
            if let Some(decision) = decision {
                self.decide(
                    &child,
                    request.id.as_str(),
                    ApprovalResponder::Parent(&parent),
                    decision,
                    reason,
                )
                .await?;
                // A child its parent's closing cascade already archived cannot
                // receive a wake; the retained decision is its answer.
                if !child.closed {
                    outs.push(Out::Send {
                        to: request.child,
                        record: InboxRecord::Approval(Approval { id: request.id }),
                    });
                }
            } else {
                if summary.is_empty() {
                    summary
                        .extend_from_slice(b"Child approval decisions requiring parent action:\n");
                }
                serde_json::to_writer(
                    &mut summary,
                    &Escalation {
                        child: &request.child,
                        request: &request.id,
                        reason: reason.as_deref(),
                        action: serde_json::from_slice(&action)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    },
                )
                .map_err(|error| Error::Invalid(error.to_string()))?;
                summary.push(b'\n');
            }
        }
        if next != selection.range.end {
            return Err(Error::Invalid("approval source page is incomplete".into()));
        }
        let summary = if summary.is_empty() {
            None
        } else {
            use crate::conversation::ContentPublisher as _;
            let (_, bindings, _) = review
                .as_ref()
                .ok_or_else(|| Error::Invalid("an ended agent escalates no approval".into()))?;
            let mut hasher = blake3::Hasher::new_derive_key("acyclic.parent-approval-page.v1");
            let identity =
                serde_json::to_vec(selection).map_err(|error| Error::Invalid(error.to_string()))?;
            hasher.update(&identity);
            let mut bytes = [0; 16];
            hasher.finalize_xof().fill(&mut bytes);
            let operation = OperationId::from_bytes(bytes);
            // Details are an ordinary private document, read on demand through
            // the same bounded file ranges as every other private file. They
            // are not another copy of every action in the worker's input.
            let path = format!("attention/approval-{operation}.txt");
            bindings
                .content
                .writer
                .as_ref()
                .ok_or_else(|| Error::Unsupported("private content writer is not bound".into()))?
                .stage_once(
                    operation,
                    &path,
                    &summary,
                    "text/plain",
                    "approval-details",
                    crate::conversation::ContentAccess::default(),
                )?
                .await?;
            let summary = crate::conversation::wire::Notice {
                source: "harness".into(),
                kind: "approval.required".into(),
                body: serde_json::json!({
                    "text": "Child approvals require a decision.",
                    "path": format!("{}{path}", crate::layers::PRIVATE_PATH),
                }),
                wake: true,
                coalesce_key: None,
            };
            let summary = crate::contract::canonical_json_bytes(&summary)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let publisher = FilesystemContentPublisher::new(
                Arc::clone(&self.host),
                &bindings.journal.volume,
                &bindings.journal.verifier,
                &bindings.journal.scope,
                self.limits.maximum_bytes,
                None,
            )?;
            Some(
                publisher
                    .stage_once(
                        operation,
                        &format!("inputs/approval-summary-{operation}.json"),
                        &summary,
                        crate::agent::NOTICE_MEDIA_TYPE,
                        "approval-summary",
                        crate::conversation::ContentAccess::default(),
                    )?
                    .await?
                    .0,
            )
        };
        Ok(OpenTriage { summary, outs })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "approval_triage_tests.rs"]
mod tests;
