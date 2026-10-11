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
        capability::interaction_respond(self.id)
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
        let request: Interaction = crate::contract::json_from_slice(bytes).map_err(|error| {
            Error::Invalid(format!("invalid interaction request JSON: {error}"))
        })?;
        self.validate_decoded_request(&request)?;
        Ok(request)
    }

    pub(crate) fn validate_decoded_request(&self, request: &Interaction) -> Result<()> {
        self.validate()?;
        request.validate()?;
        self.validate_request_binding(&request, &mut None)?;
        Ok(request)
    }

    /// Validates the original typed request without letting its payload escape.
    /// The original source claim outlives every parsed field, including errors.
    pub(crate) fn validate_request_bytes_with_work(
        &self,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if work.is_none() {
            return self.validate_request_bytes(bytes).map(drop);
        }
        self.validate_with_work(work)?;
        self.request.descriptor().verify_with_work(bytes, work)?;

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
                    crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } => {
                        let planned = interaction_decode_owned_bytes(
                            bytes.len() as u64, items, maximum_owned_bytes, diagnostic,
                        ).ok_or_else(|| interaction_denial(custody.work(),
                            crate::wire::ErrorCode::Invalid,
                            "interaction JSON custody overflows"))?;
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let decoded = if native {
                crate::contract::json_from_slice_bounded_native_trusted::<Interaction, _>(
                    bytes, bounds, bytes.len() as u64, crate::wire::ErrorCode::Invalid, &mut charge,
                )
            } else {
                crate::contract::json_from_slice_bounded_trusted::<Interaction, _>(
                    bytes, bounds, bytes.len() as u64, &mut charge,
                )
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let decoded = crate::contract::json_from_slice_bounded_trusted::<Interaction, _>(
                bytes, bounds, bytes.len() as u64, &mut charge,
            );
            decoded
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let decoded = decoded.map_err(|mut error| {
            if let Error::NativeDecode { custody: error_custody, .. } = &mut error
                && error_custody.is_none()
            {
                // Pure split: no new admission/cancellation can replace the
                // first original Serde cause after its allocation occurred.
                let bytes = custody.retained();
                *error_custody = Some(custody.work().as_deref_mut()
                    .expect("observed interaction").take_native_custody(bytes)
                    .expect("preclaimed interaction decoder custody"));
                custody.transfer();
            }
            error
        });
        let _ = native;
        let request = decoded?;
        request.validate_with_work(custody.work())?;
        self.validate_request_binding(&request, custody.work())?;
        drop(request);
        Ok(())
    }

    /// Validates a funded typed request after its original bytes were verified
    /// against this ticket's descriptor. Its caller retains DTO custody through
    /// validation and every subsequent consumer; this method never decodes again.
    pub(crate) fn validate_decoded_request_with_work(
        &self,
        request: &Interaction,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_with_work(work)?;
        request.validate_with_work(work)?;
        self.validate_request_binding(request, work)
    }

    /// Validates the same ref-only envelope with static native denials.
    pub(crate) fn validate_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if work.is_none() { return self.validate(); }
        work.as_deref_mut().expect("observed interaction").check_cancellation()?;
        if self.id.is_nil()
            || self.request.descriptor().media_type() != "application/json"
            || self.deadline_unix_ms == Some(0)
            || (self.kind == InteractionKind::Approval) != self.approval.is_some()
            || self.approval.as_ref().is_some_and(|binding| binding.action_digest == [0; 32])
        {
            return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                "invalid interaction ticket"));
        }
        self.request.validate_with_work(work)
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
        let response: InteractionResponse = crate::contract::json_from_slice(bytes)
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
        let response: InteractionResponse = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("invalid approval decision JSON: {error}")))?;
        self.validate_decoded_decision(request, outcome, &response)
    }

    pub(crate) fn validate_decoded_decision(
        &self, request: &Interaction, outcome: &InteractionOutcome, response: &InteractionResponse,
    ) -> Result<()> {
        if self.kind != InteractionKind::Approval {
            return Err(Error::Invalid("approval decision artifact is invalid".into()));
        }
        request.validate_response(response)?;
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
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        match self {
            Self::Question {
                prompt,
                response_schema,
            } => {
                nonempty_with_work(prompt, "question prompt", "question prompt is empty", work)?;
                validate_schema_with_work(response_schema, work)?;
            }
            Self::Choice {
                prompt,
                options,
                min_selections,
                max_selections,
            } => {
                nonempty_with_work(prompt, "choice prompt", "choice prompt is empty", work)?;
                if options.is_empty()
                    || min_selections > max_selections
                    || *max_selections as usize > options.len()
                {
                    return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                        "invalid choice selection bounds"));
                }
                // Only borrowed logical keys are owned by this original set;
                // opaque BTree node/allocator overhead is not an SDK receipt.
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
                    nonempty_with_work(&option.id, "choice option identity",
                        "choice option identity is empty", custody.work())?;
                    nonempty_with_work(&option.label, "choice option label",
                        "choice option label is empty", custody.work())?;
                    if !ids.insert(option.id.as_str()) {
                        return Err(interaction_denial(custody.work(), crate::wire::ErrorCode::Invalid,
                            "duplicate choice option identity"));
                    }
                }
            }
            Self::Form { title, schema } => {
                nonempty_with_work(title, "form title", "form title is empty", work)?;
                validate_schema_with_work(schema, work)?;
            }
            Self::Approval {
                prompt,
                action_digest,
                ..
            } => {
                nonempty_with_work(prompt, "approval prompt", "approval prompt is empty", work)?;
                if action_digest == &[0; 32] {
                    return Err(interaction_denial(work, crate::wire::ErrorCode::Invalid,
                        "approval action digest is empty"));
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
            ) => crate::contract::validate_json_schema_value(
                response_schema,
                value,
                "interaction response",
            ),
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
    crate::contract::compile_json_schema(schema, "interaction").map(|_| ())
}

fn validate_schema_with_work(
    schema: &Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() { return validate_schema(schema); }
    crate::contract::compile_json_schema_with_work(schema, "interaction", work).map(drop)
}

fn nonempty_with_work(
    value: &str,
    name: &str,
    reason: &'static str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() { return nonempty(value, name); }
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
    // Serde 229 internally-tagged Interaction and ChoiceOption derived visitors;
    // UUID 1.27 human-readable visitor and its original parsing error Display.
    // Common serde/syntax templates and escaped source interpolation are
    // provided by the canonical diagnostic witness, not another JSON parser.
    const LABELS: &[&str] = &[
        "Interaction", "Question", "Choice", "Form", "Approval", "ChoiceOption", "OperationId",
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
    // Pinned serde_json::Error::custom normalizes its trailing line/column
    // suffix, so nested raw parser errors retain only one original position.
    crate::contract::audited_serde_diagnostic_bytes(input_bytes,
        LABELS.iter().map(|label| label.len() as u64 + 4).sum())
}

fn interaction_decode_owned_bytes(
    input_bytes: u64,
    items: u64,
    codec_owned_bytes: u64,
    diagnostic_bytes: u64,
) -> Option<u64> {
    // ONE Content layer overlaps the consuming Value/ChoiceOption collections.
    // Ten slots/item covers minimum-eight/geometric collection capacities.
    // With raw_value enabled, Value's first-key magic map consumes its string
    // through the ORIGINAL nested parser. Its hidden nodes are bounded by input
    // bytes, not exterior lexical items; no additional scanner/parser is run.
    // Each nesting quotes the entire preceding JSON string, so even one inner
    // quote requires at least 2^depth escape bytes in this original slice.
    // usize::BITS bounds the active raw-box/parser chain. Per layer, scratch
    // is <=2*max(input,8), boxed raw text <=input, and the fourth payload covers
    // destination strings/into_boxed_str shrink overlap. Exterior codec scratch
    // remains separately held. These are logical pairs/payloads, not opaque
    // BTree nodes, allocator overhead, RSS, CPU or stack admission. The 256
    // bound is EXTERIOR lexical depth; each original inner parser retains its
    // own pinned 128-depth rule, which is not a 256 semantic-tree guarantee.
    let content_slot = std::mem::size_of::<serde::__private229::de::Content<'static>>()
        .max(std::mem::size_of::<(
            serde::__private229::de::Content<'static>,
            serde::__private229::de::Content<'static>,
        )>()) as u64;
    let value_slot = std::mem::size_of::<Value>()
        .max(std::mem::size_of::<(String, Value)>()) as u64;
    let content_and_options = content_slot
        .checked_add(std::mem::size_of::<ChoiceOption>() as u64)?;
    let wire_value_slots = input_bytes.checked_mul(10)?.checked_mul(value_slot)?;
    let raw_payload_and_scratch = input_bytes.max(8)
        .checked_mul(4 * (u64::from(usize::BITS) + 2))?;
    items.checked_mul(10)?.checked_mul(content_and_options)?
        .checked_add(wire_value_slots)?
        .checked_add(raw_payload_and_scratch)?
        .checked_add(codec_owned_bytes)?
        .checked_add(diagnostic_bytes)
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
        // Original Value deserialization expands this schema from a string;
        // exterior lexical items alone cannot fund its actual collection.
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
