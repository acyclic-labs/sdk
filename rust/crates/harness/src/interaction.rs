//! Typed, durable, executor-neutral open interactions.

use crate::contract::capability;
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

/// Private lifetime carrier for a request used across the original response IO.
pub(crate) struct ValidatedInteractionRequest {
    request: Interaction,
    // Only the existing legacy observer retains this actual DTO byte delta.
    measured_owned_bytes: u64,
    // LAST: the real request fields physically drop before original custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

impl ValidatedInteractionRequest {
    pub(crate) fn request(&self) -> &Interaction { &self.request }

    /// For legacy Some only: the caller drops this DTO, then releases this original observer-owned delta on every exit.
    pub(crate) fn measured_owned_bytes(&self) -> u64 { self.measured_owned_bytes }
}

impl InteractionTicket {
    /// A scoped observer may see the ticket and outcome refs without being authorized to answer it or read the referenced private bytes.
    #[must_use]
    pub fn viewer_grant(&self) -> String {
        format!("interaction:view:{}", self.id)
    }

    /// The one exact capability that may resolve this request.
    #[must_use]
    pub fn responder_grant(&self) -> String {
        capability::interaction_respond(self.id)
    }

    /// Checks the ref-only envelope before provider byte admission.
    pub(crate) fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if self.id.is_nil()
            || self.request.descriptor().media_type() != "application/json"
            || self.deadline_unix_ms == Some(0)
            || (self.kind == InteractionKind::Approval) != self.approval.is_some()
            || self.approval.as_ref().is_some_and(|binding| binding.action_digest == [0; 32])
        {
            return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                "invalid interaction ticket"));
        }
        self.request.validate(work)
    }

    /// Provider admission validates the staged bytes before publishing their ref.
    pub fn validate_request_bytes(&self, bytes: &[u8]) -> Result<Interaction> {
        self.validate(&mut None)?;
        self.request.descriptor().verify(bytes, &mut None)?;
        let request: Interaction = crate::contract::json_from_slice(bytes).map_err(|error| {
            Error::Invalid(format!("invalid interaction request JSON: {error}"))
        })?;
        self.validate_decoded_request_with_work(&request, &mut None)?;
        Ok(request)
    }

    /// Validates the original typed request without letting its payload escape.
    pub(crate) fn validate_request_bytes_with_work(
        &self,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if work.is_none() {
            return self.validate_request_bytes(bytes).map(drop);
        }
        let request = self.validate_request_bytes_owned_with_work(bytes, work)?;
        let measured = request.measured_owned_bytes();
        drop(request);
        work.as_deref_mut().expect("observed interaction").release_owned(measured)
    }

    /// Decodes and validates before response IO, retaining actual request fields across that await without borrowing the original observer mutably.
    pub(crate) fn validate_request_bytes_owned_with_work(
        &self,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ValidatedInteractionRequest> {
        if work.is_none() {
            return Ok(ValidatedInteractionRequest {
                request: self.validate_request_bytes(bytes)?,
                measured_owned_bytes: 0,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                _custody: None,
            });
        }
        self.validate(work)?;
        self.request.descriptor().verify(bytes, work)?;
        let mut decoded = decode_interaction_json::<Interaction>(
            bytes, crate::contract::JsonValueSchema::Interaction,
            std::mem::size_of::<ChoiceOption>() as u64, work,
        )?;
        let owned = interaction_request_owned_bytes(&decoded.0, decoded.1.work())?;
        let release = decoded.1.retained().checked_sub(owned).ok_or_else(|| interaction_denial(
            decoded.1.work(), crate::wire::ErrorCode::Invalid,
            "interaction DTO exceeds its original decoder custody"))?;
        decoded.1.relinquish(release)?;
        decoded.0.validate(decoded.1.work())?;
        self.validate_request_binding(&decoded.0, decoded.1.work())?;
        let measured_owned_bytes = owned;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (custody, measured_owned_bytes) = if interaction_native_mode(decoded.1.work()) {
            let allocation = decoded.1.work().as_deref_mut().expect("observed interaction")
                .take_native_custody(owned).expect("actual native DTO custody is a pure original split");
            (Some(allocation), 0)
        } else { (None, measured_owned_bytes) };
        decoded.1.transfer();
        let (request, quota) = decoded;
        drop(quota);
        Ok(ValidatedInteractionRequest {
            request, measured_owned_bytes,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _custody: custody,
        })
    }

    /// Validates a funded typed request after its original bytes were verified against this ticket's descriptor.
    pub(crate) fn validate_decoded_request_with_work(
        &self,
        request: &Interaction,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate(work)?;
        request.validate(work)?;
        self.validate_request_binding(request, work)
    }

    fn validate_request_binding(
        &self,
        request: &Interaction,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if request.kind() != self.kind {
            return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                "interaction request kind mismatch"));
        }
        match (request, &self.approval) {
            (
                Interaction::Approval { operation_id, action_digest, .. },
                Some(binding),
            ) if *operation_id == binding.operation_id
                && *action_digest == binding.action_digest => {}
            (Interaction::Approval { .. }, _) => {
                return Err(interaction_denial(work, crate::wire::ErrorCode::Conflict,
                    "approval action binding mismatch"));
            }
            (_, None) => {}
            _ => {
                return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                    "non-approval carries an approval binding"));
            }
        }
        Ok(())
    }

    /// Uses the already validated, caller-custodied request after response IO.
    pub(crate) fn validate_answer_bytes(
        &self,
        request: &Interaction,
        answer: &FileRef,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_answer_envelope(answer, work)?;
        answer.descriptor().verify(bytes, work)?;
        let mut response = decode_interaction_json::<InteractionResponse>(
            bytes, crate::contract::JsonValueSchema::InteractionResponse,
            std::mem::size_of::<String>() as u64, work,
        )?;
        let result = request.validate_response(&response.0, response.1.work());
        drop(response);
        result
    }

    /// Uses the same admitted request through the exact decision artifact check.
    pub(crate) fn validate_decision_bytes(
        &self,
        request: &Interaction,
        outcome: &InteractionOutcome,
        detail: &FileRef,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_decision_envelope(detail, work)?;
        detail.descriptor().verify(bytes, work)?;
        let mut response = decode_interaction_json::<InteractionResponse>(
            bytes, crate::contract::JsonValueSchema::InteractionResponse,
            std::mem::size_of::<String>() as u64, work,
        )?;
        let result = self.validate_decoded_decision(request, outcome, &response.0, response.1.work());
        drop(response);
        result
    }

    /// Borrowed counterpart for already verified/admitted producer-owned DTOs.
    pub(crate) fn validate_decoded_decision_with_work(
        &self,
        request: &Interaction,
        outcome: &InteractionOutcome,
        detail: &FileRef,
        response: &InteractionResponse,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_decision_envelope(detail, work)?;
        self.validate_decoded_decision(request, outcome, response, work)
    }

    fn validate_answer_envelope(
        &self,
        answer: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if self.kind == InteractionKind::Approval {
            return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                "approval cannot use a free-form answer"));
        }
        answer.validate(work)?;
        if answer.descriptor().media_type() != "application/json" {
            return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                "interaction answer must be JSON"));
        }
        Ok(())
    }

    fn validate_decision_envelope(
        &self,
        detail: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if self.kind != InteractionKind::Approval
            || detail.descriptor().media_type() != "application/json"
        {
            return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                "approval decision artifact is invalid"));
        }
        detail.validate(work)
    }

    fn validate_decoded_decision(
        &self,
        request: &Interaction,
        outcome: &InteractionOutcome,
        response: &InteractionResponse,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        request.validate_response(response, work)?;
        match (outcome, response) {
            (InteractionOutcome::Approved, InteractionResponse::Approval { approved: true, .. })
            | (InteractionOutcome::Declined, InteractionResponse::Approval { approved: false, .. }) => Ok(()),
            _ => Err(interaction_denial(work, crate::wire::ErrorCode::Conflict,
                "approval artifact disagrees with outcome")),
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

/// Immutable evidence of one admitted interaction resolution.
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
        value.validate(&mut None)?;
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
        value.validate(&mut None)?;
        Ok(value)
    }

    /// Creates a validated structured form.
    pub fn form(title: impl Into<String>, schema: Value) -> Result<Self> {
        let value = Self::Form {
            title: title.into(),
            schema,
        };
        value.validate(&mut None)?;
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
        value.validate(&mut None)?;
        Ok(value)
    }

    /// Validates the interaction before it is admitted.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        match self {
            Self::Question {
                prompt,
                response_schema,
            } => {
                nonempty(prompt, "question prompt is empty", work)?;
                validate_schema(response_schema, work)?;
            }
            Self::Choice {
                prompt,
                options,
                min_selections,
                max_selections,
            } => {
                nonempty(prompt, "choice prompt is empty", work)?;
                if options.is_empty()
                    || min_selections > max_selections
                    || *max_selections as usize > options.len()
                {
                    return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                        "invalid choice selection bounds"));
                }
                let slots = if work.is_some() {
                    (options.len() as u64).checked_mul(std::mem::size_of::<&str>() as u64)
                        .ok_or_else(|| interaction_denial(work, crate::wire::ErrorCode::Invalid,
                            "choice identity custody overflows"))?
                } else { 0 };
                let mut custody = crate::wire_codec::CodecQuota::new(work, slots)?;
                let mut ids = BTreeSet::new();
                for option in options {
                    if let Some(observer) = custody.work().as_deref_mut() {
                        observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                    }
                    nonempty(&option.id,
                        "choice option identity is empty", custody.work())?;
                    nonempty(&option.label,
                        "choice option label is empty", custody.work())?;
                    if !ids.insert(option.id.as_str()) {
                        return Err(interaction_denial(custody.work(), crate::wire::ErrorCode::Invalid,
                            "duplicate choice option identity"));
                    }
                }
            }
            Self::Form { title, schema } => {
                nonempty(title, "form title is empty", work)?;
                validate_schema(schema, work)?;
            }
            Self::Approval {
                prompt,
                action_digest,
                ..
            } => {
                nonempty(prompt, "approval prompt is empty", work)?;
                if action_digest == &[0; 32] {
                    return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                        "approval action digest is empty"));
                }
            }
        }
        Ok(())
    }

    /// Validates a response on the optional history observer.
    pub fn validate_response(
        &self,
        response: &InteractionResponse,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
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
                crate::contract::validate_json_schema_value_with_work(
                    response_schema, value, "interaction response", work,
                )
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
                let slots = if work.is_some() {
                    (option_ids.len() as u64).checked_mul(2 * std::mem::size_of::<&str>() as u64)
                        .ok_or_else(|| interaction_denial(work, crate::wire::ErrorCode::Invalid,
                            "choice response custody overflows"))?
                } else { 0 };
                let mut custody = crate::wire_codec::CodecQuota::new(work, slots)?;
                if let Some(observer) = custody.work().as_deref_mut() {
                    observer.codec_charge(crate::contract::JsonCodecCharge::Work {
                        bytes: 0, items: option_ids.len() as u64,
                    })?;
                }
                let selected: BTreeSet<_> = option_ids.iter().map(String::as_str).collect();
                if selected.len() != option_ids.len()
                    || selected.len() < *min_selections as usize
                    || selected.len() > *max_selections as usize
                    || selected
                        .iter()
                        .any(|id| !options.iter().any(|option| option.id == *id))
                {
                    return Err(interaction_denial(custody.work(), crate::wire::ErrorCode::Invalid,
                        "invalid choice response"));
                }
                Ok(())
            }
            (Self::Approval { .. }, InteractionResponse::Approval { .. }) => Ok(()),
            _ => Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                "interaction response type does not match its request")),
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

fn decode_interaction_json<'loan, 'io, T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    schema: crate::contract::JsonValueSchema,
    typed_slot: u64,
    work: &'loan mut Option<&'io mut dyn crate::store::history::HistoryIo>,
) -> Result<(T, crate::wire_codec::CodecQuota<'loan, 'io>)> {
    if work.is_none() {
        let value = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        return Ok((value, crate::wire_codec::CodecQuota::new(work, 0)?));
    }
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let diagnostic = interaction_decode_diagnostic_bytes(bytes.len() as u64)?;
    let mut items = 0_u64;
    let native = interaction_native_mode(custody.work());
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: bytes.len() as u64,
        maximum_items: bytes.len() as u64,
        maximum_depth: 256,
    };
    let decoded = {
        let mut charge = |charge| -> Result<()> {
            match charge {
                crate::contract::JsonCodecCharge::Work { items: visited, .. } => {
                    items = items.checked_add(visited).ok_or_else(|| interaction_denial(
                        custody.work(), crate::wire::ErrorCode::Invalid,
                        "interaction JSON item count overflows"))?;
                }
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                    let value_owned = crate::contract::json_value_decode_owned_bytes(value_shape)?;
                    let planned = interaction_decode_owned_bytes(
                        items, typed_slot, maximum_owned_bytes, value_owned, diagnostic,
                    ).ok_or_else(|| interaction_denial(custody.work(),
                        crate::wire::ErrorCode::Invalid, "interaction JSON custody overflows"))?;
                    let observer = custody.work().as_deref_mut().expect("observed interaction");
                    observer.admit_owned(planned)?;
                    observer.retain_owned(planned)?;
                    custody.adopt(planned)?;
                    return Ok(());
                }
                _ => {}
            }
            custody.work().as_deref_mut().expect("observed interaction").codec_charge(charge)
        };
        crate::contract::json_from_slice_bounded(std::marker::PhantomData::<T>,
            bytes, bounds, bytes.len() as u64,
            native.then_some(crate::wire::ErrorCode::Invalid), native.then_some(schema), &mut charge,
        )
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let decoded = decoded.map_err(|mut error| {
        if let Error::NativeDecode { custody: error_custody, .. } = &mut error
            && error_custody.is_none()
        {
            // Pure original split: no admission/cancellation can replace the first Serde cause after its actual diagnostic allocation.
            let bytes = custody.retained();
            *error_custody = Some(custody.work().as_deref_mut()
                .expect("observed interaction").take_native_custody(bytes)
                .expect("preclaimed interaction decoder custody"));
            custody.transfer();
        }
        error
    });
    let value = decoded?;
    Ok((value, custody))
}

fn validate_schema(
    schema: &Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    crate::contract::compile_json_schema_with_work(schema, "interaction", work).map(drop)
}

fn nonempty(
    value: &str,
    reason: &'static str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if value.trim().is_empty() {
        Err(interaction_denial(work, crate::wire::ErrorCode::Invalid, reason))
    } else {
        Ok(())
    }
}

fn interaction_native_mode(work: &Option<&mut dyn crate::store::history::HistoryIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { work.as_deref().is_some_and(|observer| observer.original_native_work().is_some()) }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = work; false }
}

fn interaction_denial(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if interaction_native_mode(work) { return Error::NativeBoundary { code, reason }; }
    let _ = work;
    if code == crate::wire::ErrorCode::Conflict {
        Error::Conflict(reason.into())
    } else {
        Error::Invalid(reason.into())
    }
}

fn interaction_decode_diagnostic_bytes(input_bytes: u64) -> Result<u64> {
    const LABELS: &[&str] = &[
        "Interaction", "Question", "Choice", "Form", "Approval", "ChoiceOption", "OperationId",
        "InteractionResponse", "value", "option_ids", "approved", "reason", "a boolean",
        "internally tagged enum InteractionResponse",
        "struct variant InteractionResponse::Question", "struct variant InteractionResponse::Choice",
        "struct variant InteractionResponse::Form", "struct variant InteractionResponse::Approval",
        "type", "question", "choice", "form", "approval", "prompt", "response_schema",
        "options", "min_selections", "max_selections", "title", "schema", "operation_id",
        "action_digest", "id", "label", "description",
        "internally tagged enum Interaction", "struct variant Interaction::Question",
        "struct variant Interaction::Choice", "struct variant Interaction::Form",
        "struct variant Interaction::Approval", "struct ChoiceOption", "any value",
        "any valid JSON value", "a string key", "a string", "u32", "u8",
        "an array of length 32", "field identifier", "variant identifier",
        "variant index 0 <= i < 4", "field index 0 <= i < 4",
        "1 element in sequence", " elements in sequence", "1 element in map", " elements in map",
        "raw value", "$serde_json::private::RawValue",
        "a formatted UUID string", "a 16 byte array", "UUID parsing failed: ",
        "invalid character: found `` at ", "invalid length: expected 16 bytes, found ",
        "invalid group count: expected 5, found ",
        "invalid group length in group : expected , found ",
        "non-UTF8 input", "invalid length: found ", "failed to parse a UUID",
    ];
    crate::contract::audited_serde_diagnostic_bytes(input_bytes,
        LABELS.iter().map(|label| label.len() as u64 + 4).sum())
}

fn interaction_decode_owned_bytes(
    items: u64,
    typed_slot: u64,
    codec_owned_bytes: u64,
    value_owned_bytes: u64,
    diagnostic_bytes: u64,
) -> Option<u64> {
    let content_slot = std::mem::size_of::<serde::__private229::de::Content<'static>>()
        .max(std::mem::size_of::<(
            serde::__private229::de::Content<'static>,
            serde::__private229::de::Content<'static>,
        )>()) as u64;
    let content_and_typed = content_slot.checked_add(typed_slot)?;
    items.checked_mul(10)?.checked_mul(content_and_typed)?
        .checked_add(value_owned_bytes)?
        .checked_add(codec_owned_bytes)?
        .checked_add(diagnostic_bytes)
}

fn interaction_request_owned_bytes(
    request: &Interaction,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    let overflow = || interaction_denial(work, crate::wire::ErrorCode::Invalid,
        "interaction DTO ownership overflows");
    match request {
        Interaction::Question { prompt, response_schema } => {
            (prompt.capacity() as u64).checked_add(crate::core::value_owned_bytes(response_schema)?)
                .ok_or_else(overflow)
        }
        Interaction::Form { title, schema } => {
            (title.capacity() as u64).checked_add(crate::core::value_owned_bytes(schema)?)
                .ok_or_else(overflow)
        }
        Interaction::Choice { prompt, options, .. } => {
            let slots = options.capacity().checked_mul(std::mem::size_of::<ChoiceOption>())
                .ok_or_else(overflow)? as u64;
            let mut owned = (prompt.capacity() as u64).checked_add(slots).ok_or_else(overflow)?;
            for option in options {
                owned = owned.checked_add(option.id.capacity() as u64)
                    .and_then(|bytes| bytes.checked_add(option.label.capacity() as u64))
                    .and_then(|bytes| bytes.checked_add(option.description.as_ref()
                        .map_or(0, |description| description.capacity() as u64)))
                    .ok_or_else(overflow)?;
            }
            Ok(owned)
        }
        Interaction::Approval { prompt, .. } => Ok(prompt.capacity() as u64),
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
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            source_bytes_read: 1_048_576,
            bytes_hashed: 1_048_576,
            bytes_copied: 1_048_576,
            bytes_encoded: 1_048_576,
            items_examined: 1_048_576,
            allocation_operations: 1_048_576,
            peak_allocation_bytes: 1_048_576,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_typed_request_error_keeps_original_serde_source_and_custody() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let bytes = br#"{"type":"approval","prompt":"Proceed?","operation_id":"00000000-0000-0000-0000-000000000001","action_digest":[3]}"#;
        let ticket = InteractionTicket {
            id: Uuid::from_bytes([1; 16]),
            kind: InteractionKind::Approval,
            request: file("native-approval.json", bytes)?,
            deadline_unix_ms: None,
            approval: Some(ApprovalBinding {
                operation_id: OperationId::from_bytes([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
                ]),
                action_digest: [3; 32],
            }),
        };
        let original = serde_json::from_slice::<Interaction>(bytes).expect_err("fixed digest");
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(native_budget(), &cancellation)
            .expect("original ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        let error = ticket.validate_request_bytes_with_work(bytes, &mut Some(&mut io))
            .expect_err("original typed digest rejects malformed length");
        let Error::NativeDecode { code, source, custody: Some(custody) } = error else {
            panic!("native request must preserve its original typed cause and guard");
        };
        assert_eq!(code, crate::wire::ErrorCode::Invalid);
        assert_eq!(source.classify(), original.classify());
        assert_eq!((source.line(), source.column()), (original.line(), original.column()));
        assert!(custody.bytes() > 0);
        assert!(native.shares_ledger(custody.work()));
        assert_eq!(io.into_custody().bytes(), 0);
        drop(source);
        drop(custody);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_unit_choice_validation_retires_all_temporary_custody() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let bytes = br#"{"type":"choice","prompt":"Choose","options":[{"id":"one","label":"One"},{"id":"two","label":"Two"}],"min_selections":1,"max_selections":1}"#;
        let ticket = InteractionTicket {
            id: Uuid::from_bytes([1; 16]),
            kind: InteractionKind::Choice,
            request: file("native-choice.json", bytes)?,
            deadline_unix_ms: None,
            approval: None,
        };
        ticket.validate_request_bytes_with_work(bytes, &mut None)?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(native_budget(), &cancellation)
            .expect("original ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        ticket.validate_request_bytes_with_work(bytes, &mut Some(&mut io))?;
        assert_eq!(io.into_custody().bytes(), 0);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_request_preserves_original_raw_value_schema_semantics() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let schema = json!({"const": vec![false; 64]});
        let embedded = serde_json::to_string(&schema)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let bytes = serde_json::to_vec(&json!({
            "type": "question",
            "prompt": "Choose",
            "response_schema": {"$serde_json::private::RawValue": embedded},
        })).map_err(|error| Error::Invalid(error.to_string()))?;
        let ticket = InteractionTicket {
            id: Uuid::from_bytes([1; 16]),
            kind: InteractionKind::Question,
            request: file("native-raw-schema.json", &bytes)?,
            deadline_unix_ms: None,
            approval: None,
        };
        let Interaction::Question { response_schema, .. } = ticket.validate_request_bytes(&bytes)? else {
            panic!("original question");
        };
        assert_eq!(response_schema, schema);
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(native_budget(), &cancellation)
            .expect("original ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        ticket.validate_request_bytes_with_work(&bytes, &mut Some(&mut io))?;
        assert_eq!(io.into_custody().bytes(), 0);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_answer_error_retains_original_response_cause_after_request_drop() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let request_bytes = br#"{"type":"choice","prompt":"Choose","options":[{"id":"one","label":"One"}],"min_selections":1,"max_selections":1}"#;
        let bytes = br#"{"type":"choice","option_ids":[1]}"#;
        let ticket = InteractionTicket {
            id: Uuid::from_bytes([1; 16]),
            kind: InteractionKind::Choice,
            request: file("native-request.json", request_bytes)?,
            deadline_unix_ms: None,
            approval: None,
        };
        let answer = file("native-answer.json", bytes)?;
        let original = serde_json::from_slice::<InteractionResponse>(bytes).expect_err("string ids");
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(native_budget(), &cancellation)
            .expect("original ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        let request = ticket.validate_request_bytes_owned_with_work(request_bytes, &mut Some(&mut io))?;
        let error = ticket.validate_answer_bytes(
            request.request(), &answer, bytes, &mut Some(&mut io),
        ).expect_err("original typed response rejects integer ids");
        drop(request);
        let Error::NativeDecode { code, source, custody: Some(custody) } = error else {
            panic!("original response cause and custody");
        };
        assert_eq!(code, crate::wire::ErrorCode::Invalid);
        assert_eq!(source.classify(), original.classify());
        assert_eq!((source.line(), source.column()), (original.line(), original.column()));
        assert!(custody.bytes() > 0);
        assert!(native.shares_ledger(custody.work()));
        assert_eq!(io.into_custody().bytes(), 0);
        drop(source);
        drop(custody);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_request_carrier_retires_parser_and_keeps_actual_fields_through_response() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let padding = "x".repeat(30_000);
        let request_bytes = serde_json::to_vec(&json!({
            "type": "choice", "prompt": padding,
            "options": [{"id": "one", "label": "One"}],
            "min_selections": 1, "max_selections": 1,
        })).map_err(|error| Error::Invalid(error.to_string()))?;
        let bytes = serde_json::to_vec(&json!({
            "type": "choice", "option_ids": [{"label": padding}],
        })).map_err(|error| Error::Invalid(error.to_string()))?;
        let ticket = InteractionTicket {
            id: Uuid::from_bytes([1; 16]),
            kind: InteractionKind::Choice,
            request: file("native-large-request.json", &request_bytes)?,
            deadline_unix_ms: None,
            approval: None,
        };
        let answer = file("native-large-answer.json", &bytes)?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(native_budget(), &cancellation)
            .expect("original ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        let request = ticket.validate_request_bytes_owned_with_work(&request_bytes, &mut Some(&mut io))?;
        let guard = request._custody.as_ref().expect("original native DTO guard");
        assert_eq!(request.measured_owned_bytes(), 0);
        assert_eq!(guard.bytes(), interaction_request_owned_bytes(request.request(), &None)?);
        assert!(native.shares_ledger(guard.work()));
        let error = ticket.validate_answer_bytes(
            request.request(), &answer, &bytes, &mut Some(&mut io),
        ).expect_err("original typed string id rejects the funded response object");
        assert!(matches!(error, Error::NativeDecode { code: crate::wire::ErrorCode::Invalid, .. }));
        assert_eq!(io.into_custody().bytes(), 0);
        drop(error);
        drop(request);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_decision_artifact_keeps_exact_original_outcome_binding() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let operation_id = OperationId::from_bytes([4; 16]);
        let request = Interaction::approval("Proceed?", operation_id, [5; 32])?;
        let request_bytes = serde_json::to_vec(&request)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let ticket = InteractionTicket {
            id: Uuid::from_bytes([1; 16]),
            kind: InteractionKind::Approval,
            request: file("native-approval-request.json", &request_bytes)?,
            deadline_unix_ms: None,
            approval: Some(ApprovalBinding { operation_id, action_digest: [5; 32] }),
        };
        let bytes = br#"{"type":"approval","approved":false,"reason":"Declined"}"#;
        let detail = file("native-approval-decision.json", bytes)?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(native_budget(), &cancellation)
            .expect("original ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        let request = ticket.validate_request_bytes_owned_with_work(&request_bytes, &mut Some(&mut io))?;
        ticket.validate_decision_bytes(
            request.request(), &InteractionOutcome::Declined, &detail, bytes, &mut Some(&mut io),
        )?;
        drop(request);
        let request = ticket.validate_request_bytes_owned_with_work(&request_bytes, &mut Some(&mut io))?;
        assert!(matches!(ticket.validate_decision_bytes(
            request.request(), &InteractionOutcome::Approved, &detail, bytes, &mut Some(&mut io),
        ), Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: "approval artifact disagrees with outcome",
        })));
        drop(request);
        assert_eq!(io.into_custody().bytes(), 0);
        Ok(())
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
        ticket.validate_answer_bytes(&admitted, &answer, response_bytes, &mut None)?;
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
            ticket.validate_answer_bytes(&admitted, &invalid, invalid_bytes, &mut None),
            Err(Error::Invalid(_))
        ));
        assert!(
            ticket
                .validate_answer_bytes(&admitted, &answer, invalid_bytes, &mut None)
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
