//! Typed, durable, executor-neutral open interactions.

use crate::{Error, OperationId, Result, conversation::FileRef};
use serde::{Deserialize, Serialize, de::Error as _};
use serde_json::Value;
use std::collections::BTreeSet;
use uuid::Uuid;

/// Discriminator retained in the event without copying a participant-facing prompt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    /// Free-form schema-constrained answer.
    Question,
    /// Stable option selection.
    Choice,
    /// Structured schema-constrained form.
    Form,
    /// Approval of one exact operation intent.
    Approval,
}

/// Exact effect intent that an approval authorizes, independent of display text.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalBinding {
    /// Operation to which the decision applies.
    pub operation_id: OperationId,
    /// Digest of the exact approved arguments and action.
    pub action_digest: [u8; 32],
}

impl ApprovalBinding {
    /// Rejects the all-zero sentinel so every approval names an exact intent.
    pub fn validate(&self) -> Result<()> {
        if self.action_digest == [0; 32] {
            return Err(Error::Invalid(
                "approval action digest cannot be zero".into(),
            ));
        }
        Ok(())
    }
}

/// Addressable, ref-only request admitted before presentation to a responder.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionTicket {
    /// Stable address used by responders and retries.
    pub id: Uuid,
    /// Validated request discriminator.
    pub kind: InteractionKind,
    /// Immutable provider-owned request JSON.
    pub request: FileRef,
    /// Optional absolute deadline in Unix milliseconds.
    pub deadline_unix_ms: Option<u64>,
    /// Exact action identity for an approval, absent otherwise.
    pub approval: Option<ApprovalBinding>,
}

impl InteractionTicket {
    /// A scoped observer may see the ticket and outcome refs without being
    /// authorized to answer it or read the referenced private bytes.
    #[must_use]
    pub fn viewer_grant(&self) -> String {
        format!("interaction:view:{}", self.id)
    }

    /// The one exact capability that may resolve this request.
    #[must_use]
    pub fn responder_grant(&self) -> String {
        format!("interaction:respond:{}", self.id)
    }

    /// Checks the ref-only envelope before provider byte admission.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_nil()
            || self.request.descriptor().media_type() != "application/json"
            || self.deadline_unix_ms == Some(0)
            || (self.kind == InteractionKind::Approval) != self.approval.is_some()
            || self
                .approval
                .as_ref()
                .is_some_and(|binding| binding.validate().is_err())
        {
            return Err(Error::Invalid("invalid interaction ticket".into()));
        }
        self.request.validate()
    }

    /// Provider admission validates the staged bytes before publishing their ref.
    /// Checks staged request JSON, content identity, and approval binding.
    pub fn validate_request_bytes(&self, bytes: &[u8]) -> Result<Interaction> {
        self.validate()?;
        self.request.descriptor().verify(bytes)?;
        let request: Interaction = serde_json::from_slice(bytes).map_err(|error| {
            Error::Invalid(format!("invalid interaction request JSON: {error}"))
        })?;
        request.validate()?;
        if request.kind() != self.kind {
            return Err(Error::Invalid("interaction request kind mismatch".into()));
        }
        match (&request, &self.approval) {
            (
                Interaction::Approval {
                    operation_id,
                    action_digest,
                    ..
                },
                Some(binding),
            ) if *operation_id == binding.operation_id
                && *action_digest == binding.action_digest => {}
            (Interaction::Approval { .. }, _) => {
                return Err(Error::Conflict("approval action binding mismatch".into()));
            }
            (_, None) => {}
            _ => {
                return Err(Error::Invalid(
                    "non-approval carries an approval binding".into(),
                ));
            }
        }
        Ok(request)
    }

    /// Provider admission verifies a version-pinned answer against the exact request.
    /// Checks staged answer JSON against the admitted request schema.
    pub fn validate_answer_bytes(
        &self,
        request: &Interaction,
        answer: &FileRef,
        bytes: &[u8],
    ) -> Result<()> {
        if self.kind == InteractionKind::Approval {
            return Err(Error::Invalid(
                "approval cannot use a free-form answer".into(),
            ));
        }
        answer.validate()?;
        if answer.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("interaction answer must be JSON".into()));
        }
        answer.descriptor().verify(bytes)?;
        let response: InteractionResponse = serde_json::from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("invalid interaction answer JSON: {error}")))?;
        request.validate_response(&response)
    }

    /// Verifies an optional approval explanation against the committed decision.
    pub fn validate_decision_bytes(
        &self,
        request: &Interaction,
        outcome: &InteractionOutcome,
        detail: &FileRef,
        bytes: &[u8],
    ) -> Result<()> {
        if self.kind != InteractionKind::Approval
            || detail.descriptor().media_type() != "application/json"
        {
            return Err(Error::Invalid(
                "approval decision artifact is invalid".into(),
            ));
        }
        detail.validate()?;
        detail.descriptor().verify(bytes)?;
        let response: InteractionResponse = serde_json::from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("invalid approval decision JSON: {error}")))?;
        request.validate_response(&response)?;
        match (outcome, response) {
            (
                InteractionOutcome::Approved,
                InteractionResponse::Approval { approved: true, .. },
            )
            | (
                InteractionOutcome::Declined,
                InteractionResponse::Approval {
                    approved: false, ..
                },
            ) => Ok(()),
            _ => Err(Error::Conflict(
                "approval artifact disagrees with outcome".into(),
            )),
        }
    }
}

/// Closed interaction outcome. Unknown provider outcomes remain explicitly unresolved.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InteractionOutcome {
    /// Schema-valid response content, retained only as a ref.
    Answered {
        /// Immutable response JSON.
        answer: Box<FileRef>,
    },
    /// Exact approval binding was accepted.
    Approved,
    /// Participant explicitly refused.
    Declined,
    /// Requester or responder cancelled.
    Cancelled,
    /// Deadline elapsed before an answer.
    Expired,
    /// Policy denied the interaction.
    Denied,
    /// Provider outcome needs reconciliation by stable operation identity.
    Indeterminate {
        /// Operation to reconcile.
        operation_id: OperationId,
    },
}

impl<'de> Deserialize<'de> for InteractionOutcome {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let Value::Object(mut fields) = Value::deserialize(deserializer)? else {
            return Err(D::Error::custom("interaction outcome must be an object"));
        };
        let kind = fields
            .remove("kind")
            .ok_or_else(|| D::Error::missing_field("kind"))?;
        let Value::String(kind) = kind else {
            return Err(D::Error::custom(
                "interaction outcome kind must be a string",
            ));
        };
        let outcome = match kind.as_str() {
            "answered" => {
                let answer = fields
                    .remove("answer")
                    .ok_or_else(|| D::Error::missing_field("answer"))?;
                Self::Answered {
                    answer: serde_json::from_value(answer).map_err(D::Error::custom)?,
                }
            }
            "approved" => Self::Approved,
            "declined" => Self::Declined,
            "cancelled" => Self::Cancelled,
            "expired" => Self::Expired,
            "denied" => Self::Denied,
            "indeterminate" => {
                let operation_id = fields
                    .remove("operation_id")
                    .ok_or_else(|| D::Error::missing_field("operation_id"))?;
                Self::Indeterminate {
                    operation_id: serde_json::from_value(operation_id).map_err(D::Error::custom)?,
                }
            }
            _ => {
                return Err(D::Error::unknown_variant(
                    &kind,
                    &[
                        "answered",
                        "approved",
                        "declined",
                        "cancelled",
                        "expired",
                        "denied",
                        "indeterminate",
                    ],
                ));
            }
        };
        if let Some(field) = fields.keys().next() {
            return Err(D::Error::custom(format!(
                "unknown interaction outcome field {field}"
            )));
        }
        Ok(outcome)
    }
}

impl InteractionOutcome {
    /// Unknown outcomes remain open for later reconciliation.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        !matches!(self, Self::Indeterminate { .. })
    }
}

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
            detail.validate()?;
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
                answer.validate()?;
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

/// Immutable evidence of one admitted interaction resolution. A replay keeps
/// the original aggregate revision and never creates a second decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionReceipt {
    /// Resolved request identity.
    pub id: Uuid,
    /// Exact resolution version admitted by the conversation owner.
    pub version: u64,
    /// Stable caller operation used for retry and reconciliation.
    pub operation_id: OperationId,
    /// Gapless revision of the authoritative conversation event.
    pub conversation_revision: u64,
    /// Whether this call observed the already committed exact operation.
    pub replayed: bool,
    /// Ref-only terminal or explicitly indeterminate outcome.
    pub outcome: InteractionOutcome,
}

impl ResolutionReceipt {
    /// Checks the independent public receipt shape before trusting its owner.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_nil() || self.version == 0 || self.conversation_revision == 0 {
            return Err(Error::Invalid(
                "interaction resolution receipt is invalid".into(),
            ));
        }
        Ok(())
    }

    /// Converts only a committed interaction-resolution event into its receipt.
    pub fn from_apply_result(result: crate::core::ApplyResult) -> Result<Self> {
        let (event, replayed) = match result {
            crate::core::ApplyResult::Applied { event } => (event, false),
            crate::core::ApplyResult::Replayed { event } => (event, true),
        };
        let crate::core::EventPayload::InteractionResolved { resolution } = event.payload else {
            return Err(Error::Conflict(
                "operation did not resolve an interaction".into(),
            ));
        };
        let receipt = Self {
            id: resolution.id,
            version: resolution.expected_version,
            operation_id: event.operation_id,
            conversation_revision: event.revision,
            replayed,
            outcome: resolution.outcome,
        };
        receipt.validate()?;
        Ok(receipt)
    }
}

#[cfg(test)]
mod fixture_tests {
    use super::*;

    #[test]
    fn v2_interaction_resolution_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/interaction-resolution.json").trim();
        let resolution: InteractionResolution =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        let encoded = serde_json::to_string(&resolution)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn v2_resolution_receipt_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/resolution-receipt.json").trim();
        let receipt: ResolutionReceipt =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(receipt.version, 2);
        assert_eq!(receipt.conversation_revision, 9);
        let encoded =
            serde_json::to_string(&receipt).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        let mut invalid = receipt;
        invalid.version = 0;
        assert!(invalid.validate().is_err());
        Ok(())
    }
}

/// One selectable choice presented to a participant.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChoiceOption {
    /// Stable option identity returned in the response.
    pub id: String,
    /// Human-readable label.
    pub label: String,
    /// Optional explanatory detail.
    pub description: Option<String>,
}

/// Typed interaction request. Executors decide how and where it is presented.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Interaction {
    /// Free-form value constrained by JSON Schema.
    Question {
        /// Human-readable prompt.
        prompt: String,
        /// JSON Schema for the answer value.
        response_schema: Value,
    },
    /// Selection from stable enumerated options.
    Choice {
        /// Human-readable prompt.
        prompt: String,
        /// Stable options.
        options: Vec<ChoiceOption>,
        /// Minimum number of selections.
        min_selections: u32,
        /// Maximum number of selections.
        max_selections: u32,
    },
    /// Structured form constrained by JSON Schema.
    Form {
        /// Human-readable title.
        title: String,
        /// JSON Schema for the submitted object.
        schema: Value,
    },
    /// Approval bound to one exact operation intent.
    Approval {
        /// Human-readable explanation of the requested action.
        prompt: String,
        /// Operation awaiting approval.
        operation_id: OperationId,
        /// Digest of the exact action being approved.
        action_digest: [u8; 32],
    },
}

impl Interaction {
    /// Returns the stable request discriminator without serializing its body.
    #[must_use]
    pub const fn kind(&self) -> InteractionKind {
        match self {
            Self::Question { .. } => InteractionKind::Question,
            Self::Choice { .. } => InteractionKind::Choice,
            Self::Form { .. } => InteractionKind::Form,
            Self::Approval { .. } => InteractionKind::Approval,
        }
    }

    /// Creates a validated free-form question.
    pub fn question(prompt: impl Into<String>, response_schema: Value) -> Result<Self> {
        let value = Self::Question {
            prompt: prompt.into(),
            response_schema,
        };
        value.validate()?;
        Ok(value)
    }

    /// Creates a validated single-choice interaction.
    pub fn choice(prompt: impl Into<String>, options: Vec<ChoiceOption>) -> Result<Self> {
        let value = Self::Choice {
            prompt: prompt.into(),
            options,
            min_selections: 1,
            max_selections: 1,
        };
        value.validate()?;
        Ok(value)
    }

    /// Creates a validated structured form.
    pub fn form(title: impl Into<String>, schema: Value) -> Result<Self> {
        let value = Self::Form {
            title: title.into(),
            schema,
        };
        value.validate()?;
        Ok(value)
    }

    /// Creates an approval bound to one exact action digest.
    pub fn approval(
        prompt: impl Into<String>,
        operation_id: OperationId,
        action_digest: [u8; 32],
    ) -> Result<Self> {
        let value = Self::Approval {
            prompt: prompt.into(),
            operation_id,
            action_digest,
        };
        value.validate()?;
        Ok(value)
    }

    /// Validates the interaction before it is admitted.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Question {
                prompt,
                response_schema,
            } => {
                nonempty(prompt, "question prompt")?;
                validate_schema(response_schema)?;
            }
            Self::Choice {
                prompt,
                options,
                min_selections,
                max_selections,
            } => {
                nonempty(prompt, "choice prompt")?;
                if options.is_empty()
                    || min_selections > max_selections
                    || *max_selections as usize > options.len()
                {
                    return Err(Error::Invalid("invalid choice selection bounds".into()));
                }
                let mut ids = BTreeSet::new();
                for option in options {
                    nonempty(&option.id, "choice option identity")?;
                    nonempty(&option.label, "choice option label")?;
                    if !ids.insert(option.id.as_str()) {
                        return Err(Error::Invalid("duplicate choice option identity".into()));
                    }
                }
            }
            Self::Form { title, schema } => {
                nonempty(title, "form title")?;
                validate_schema(schema)?;
            }
            Self::Approval {
                prompt,
                action_digest,
                ..
            } => {
                nonempty(prompt, "approval prompt")?;
                if action_digest == &[0; 32] {
                    return Err(Error::Invalid("approval action digest is empty".into()));
                }
            }
        }
        Ok(())
    }

    /// Validates one response against this exact interaction request.
    pub fn validate_response(&self, response: &InteractionResponse) -> Result<()> {
        match (self, response) {
            (
                Self::Question {
                    response_schema, ..
                },
                InteractionResponse::Question { value },
            )
            | (
                Self::Form {
                    schema: response_schema,
                    ..
                },
                InteractionResponse::Form { value },
            ) => {
                jsonschema::validator_for(response_schema)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .validate(value)
                    .map_err(|error| {
                        Error::Invalid(format!("interaction response failed validation: {error}"))
                    })?;
                Ok(())
            }
            (
                Self::Choice {
                    options,
                    min_selections,
                    max_selections,
                    ..
                },
                InteractionResponse::Choice { option_ids },
            ) => {
                let selected: BTreeSet<_> = option_ids.iter().map(String::as_str).collect();
                if selected.len() != option_ids.len()
                    || selected.len() < *min_selections as usize
                    || selected.len() > *max_selections as usize
                    || selected
                        .iter()
                        .any(|id| !options.iter().any(|option| option.id == *id))
                {
                    return Err(Error::Invalid("invalid choice response".into()));
                }
                Ok(())
            }
            (Self::Approval { .. }, InteractionResponse::Approval { .. }) => Ok(()),
            _ => Err(Error::Invalid(
                "interaction response type does not match its request".into(),
            )),
        }
    }
}

/// Typed response to one open interaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InteractionResponse {
    /// Free-form question answer.
    Question {
        /// Schema-validated answer.
        value: Value,
    },
    /// Stable selected option identities.
    Choice {
        /// Selected option identities.
        option_ids: Vec<String>,
    },
    /// Structured form submission.
    Form {
        /// Schema-validated object.
        value: Value,
    },
    /// Approval decision.
    Approval {
        /// Whether the exact bound action is approved.
        approved: bool,
        /// Optional participant explanation.
        reason: Option<String>,
    },
}

fn validate_schema(schema: &Value) -> Result<()> {
    jsonschema::validator_for(schema)
        .map(|_| ())
        .map_err(|error| Error::Invalid(format!("invalid interaction JSON Schema: {error}")))
}

fn nonempty(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(Error::Invalid(format!("{name} is empty")))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId,
        conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    use serde_json::json;

    #[test]
    fn outcome_rejects_unrecognized_fields() {
        assert!(
            serde_json::from_str::<InteractionOutcome>(r#"{"kind":"approved","answer":"hidden"}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<InteractionOutcome>(
            r#"{"kind":"indeterminate","operation_id":"00000000-0000-0000-0000-000000000001","extra":true}"#).is_err());
    }

    fn file(path: &str, bytes: &[u8]) -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "interaction-private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
            )?,
            path,
            "immutable-1",
            FileDescriptor::from_bytes(bytes, "application/json")?,
            "interaction.json",
        )
    }

    #[test]
    fn staged_question_and_answer_are_schema_validated_before_ref_publication() -> Result<()> {
        let request = Interaction::question(
            "Choose a positive number",
            json!({"type": "integer", "minimum": 1}),
        )?;
        let request_bytes =
            serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
        let ticket = InteractionTicket {
            id: Uuid::from_bytes([1; 16]),
            kind: InteractionKind::Question,
            request: file("request.json", &request_bytes)?,
            deadline_unix_ms: None,
            approval: None,
        };
        let admitted = ticket.validate_request_bytes(&request_bytes)?;
        let response_bytes = br#"{"type":"question","value":2}"#;
        let answer = file("answer.json", response_bytes)?;
        ticket.validate_answer_bytes(&admitted, &answer, response_bytes)?;
        let non_json = FileRef::new(
            answer.volume().clone(),
            "answer.txt",
            "immutable-1",
            FileDescriptor::from_bytes(response_bytes, "text/plain")?,
            "answer.txt",
        )?;
        assert!(matches!(
            (InteractionResolution {
                id: ticket.id,
                expected_version: 1,
                outcome: InteractionOutcome::Answered {
                    answer: Box::new(non_json)
                },
                detail: None,
            })
            .validate(&ticket),
            Err(Error::Invalid(_))
        ));
        let invalid_bytes = br#"{"type":"question","value":0}"#;
        let invalid = file("invalid-answer.json", invalid_bytes)?;
        assert!(matches!(
            ticket.validate_answer_bytes(&admitted, &invalid, invalid_bytes),
            Err(Error::Invalid(_))
        ));
        assert!(
            ticket
                .validate_answer_bytes(&admitted, &answer, invalid_bytes)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn approval_binding_cannot_change_with_display_json() -> Result<()> {
        let operation_id = OperationId::from_bytes([4; 16]);
        let request = Interaction::approval("Proceed?", operation_id, [5; 32])?;
        let request_bytes =
            serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
        let mut ticket = InteractionTicket {
            id: Uuid::from_bytes([2; 16]),
            kind: InteractionKind::Approval,
            request: file("approval.json", &request_bytes)?,
            deadline_unix_ms: Some(10),
            approval: Some(ApprovalBinding {
                operation_id,
                action_digest: [6; 32],
            }),
        };
        assert!(matches!(
            ticket.validate_request_bytes(&request_bytes),
            Err(Error::Conflict(_))
        ));
        ticket
            .approval
            .as_mut()
            .ok_or_else(|| Error::Invalid("missing approval".into()))?
            .action_digest = [5; 32];
        ticket.validate_request_bytes(&request_bytes)?;
        assert!(matches!(
            InteractionResolution {
                id: ticket.id,
                expected_version: 1,
                outcome: InteractionOutcome::Answered {
                    answer: Box::new(file("answer.json", b"null")?)
                },
                detail: None,
            }
            .validate(&ticket),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }
}
