//! Parent controls retain original input and policy cuts before independent review.
use super::*;
use crate::agent::{Approval, ApprovalSelection, InboxRecord, OpenTriage, Out};
use crate::approvals::{ApprovalResponder, RuleVerdict};
use serde::Serialize;

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
        let (_, bindings, _) = self.parent_review_binding(agent, None).await?;
        let selected = bindings
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
            .await?;
        let selection = ApprovalSelection {
            source: agent.path.clone(),
            range: inbox..cursor.min(inbox.saturating_add(crate::agent::MAX_QUEUED as u64)),
            policy: selected.map(|(file, _, _)| file),
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
        let (journal, bindings, policy_bytes) = self
            .parent_review_binding(agent, selection.policy.as_ref())
            .await?;
        let policy = std::str::from_utf8(&policy_bytes)
            .map_err(|_| Error::Invalid("review policy is not UTF-8".into()))?;
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
            let (verdict, reason, action) = self
                .review_child_action(agent, &child, &request.id, &journal, policy)
                .await?;
            let decision = match verdict {
                RuleVerdict::Allow => Some(crate::interaction::ApprovalVerdict::Approve),
                RuleVerdict::Deny => Some(crate::interaction::ApprovalVerdict::Deny),
                RuleVerdict::Ask => None,
            };
            if let Some(decision) = decision {
                self.decide(
                    &child,
                    request.id.as_str(),
                    ApprovalResponder::Parent(&agent.path.id()),
                    decision,
                    reason,
                )
                .await?;
                outs.push(Out::Send {
                    to: request.child,
                    record: InboxRecord::Approval(Approval { id: request.id }),
                });
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
            let summary = format!(
                "Child approvals require a decision. Read {}{path}\n",
                crate::layers::PRIVATE_PATH,
            );
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
                        &format!("inputs/approval-summary-{operation}.txt"),
                        summary.as_bytes(),
                        "text/plain",
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
