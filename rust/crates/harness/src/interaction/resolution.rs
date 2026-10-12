use super::*;

/// One compare-and-set resolution against an open interaction version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionResolution {
    /// Address of the open request.
    pub id: Uuid,
    /// Exact open version; stale responses conflict.
    pub expected_version: u64,
    /// One explicit closed state.
    pub outcome: InteractionOutcome,
    /// Optional pinned JSON response preserving an approval explanation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<FileRef>,
}

impl InteractionResolution {
    /// Checks version, address, and outcome kind against the open ticket.
    pub fn validate(&self, ticket: &InteractionTicket) -> Result<()> {
        if self.id != ticket.id || self.expected_version == 0 {
            return Err(Error::Conflict(
                "interaction resolution version mismatch".into(),
            ));
        }
        if let Some(detail) = &self.detail {
            detail.validate(&mut None)?;
            if ticket.kind != InteractionKind::Approval
                || detail.descriptor().media_type() != "application/json"
                || !matches!(
                    &self.outcome,
                    InteractionOutcome::Approved | InteractionOutcome::Declined
                )
            {
                return Err(Error::Invalid(
                    "interaction decision artifact is invalid".into(),
                ));
            }
        }
        match &self.outcome {
            InteractionOutcome::Answered { answer } if ticket.kind != InteractionKind::Approval => {
                answer.validate(&mut None)?;
                if answer.descriptor().media_type() != "application/json" {
                    return Err(Error::Invalid("interaction answer must be JSON".into()));
                }
                Ok(())
            }
            InteractionOutcome::Approved if ticket.kind == InteractionKind::Approval => Ok(()),
            InteractionOutcome::Answered { .. } | InteractionOutcome::Approved => Err(
                Error::Invalid("interaction resolution kind mismatch".into()),
            ),
            InteractionOutcome::Expired if ticket.deadline_unix_ms.is_none() => {
                Err(Error::Invalid("interaction has no expiry deadline".into()))
            }
            _ => Ok(()),
        }
    }
}
