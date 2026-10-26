use super::*;

/// One selectable choice presented to a participant.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
pub struct ChoiceOption {
    /// Stable option identity returned in the response.
    pub id: String,
    /// Human-readable label.
    pub label: String,
    /// Optional explanatory detail.
    pub description: Option<String>,
}

/// The authority that must review this request. Guard requests require a human
/// independently of the ordinary tool policy; the immutable request binds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalRoute {
    /// Traverse the configured rule, model, parent and human policy chain.
    Policy,
    /// A guard explicitly requested a human decision.
    Human,
}

/// The exact action an approval decides, as its approver sees it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalSubject {
    /// One model tool call with its exact admitted arguments. The approval's
    /// operation and digest bind this same invocation, and execution reopens the
    /// request from the invocation it runs, so a differing request conflicts.
    Tool {
        /// Pinned review authority, independent of the tool's display text.
        route: ApprovalRoute,
        /// Model-visible tool name.
        tool: String,
        /// Provider call identity.
        call_id: String,
        /// Exact admitted arguments.
        #[serde(deserialize_with = "crate::deserialize_json_value")]
        #[heap_layout(as = serde_json::Value)]
        arguments: Value,
    },
    /// An action whose preimage stays private to its owner (credentials,
    /// authorization URLs, sealed native requests). Only the digest is public.
    Opaque {},
    /// Read-only marker for requests saved before subjects were recorded.
    /// It cannot be supplied as an explicit subject on the wire.
    #[serde(skip_deserializing)]
    Legacy {},
}

impl ApprovalSubject {
    pub(crate) const fn is_legacy(&self) -> bool {
        matches!(self, Self::Legacy {})
    }
}

fn legacy_approval_subject() -> ApprovalSubject {
    ApprovalSubject::Legacy {}
}

/// Typed interaction request. Executors decide how and where it is presented.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
        /// The exact action, so approvers decide what actually runs.
        /// Missing subjects retain the original pre-upgrade request encoding.
        #[serde(
            default = "legacy_approval_subject",
            skip_serializing_if = "ApprovalSubject::is_legacy"
        )]
        subject: ApprovalSubject,
    },
}

impl Interaction {
    pub(crate) const fn is_legacy_approval(&self) -> bool {
        matches!(
            self,
            Self::Approval {
                subject: ApprovalSubject::Legacy {},
                ..
            }
        )
    }

    /// A pre-subject request keeps its original bytes when its owner resumes
    /// the same operation and action; supplied metadata never rewrites it.
    pub(crate) fn matches_reopened(&self, request: &Self) -> bool {
        self == request
            || matches!(
                (self, request),
                (
                    Self::Approval {
                        prompt,
                        operation_id,
                        action_digest,
                        subject: ApprovalSubject::Legacy {},
                    },
                    Self::Approval {
                        prompt: other_prompt,
                        operation_id: other_operation,
                        action_digest: other_digest,
                        ..
                    }
                ) if prompt == other_prompt
                    && operation_id == other_operation
                    && action_digest == other_digest
            )
    }

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
    pub fn question(prompt: String, response_schema: Value) -> Result<Self> {
        let value = Self::Question {
            prompt,
            response_schema,
        };
        value.validate(&mut None)?;
        Ok(value)
    }

    /// Creates a validated single-choice interaction.
    pub fn choice(prompt: String, options: Vec<ChoiceOption>) -> Result<Self> {
        let value = Self::Choice {
            prompt,
            options,
            min_selections: 1,
            max_selections: 1,
        };
        value.validate(&mut None)?;
        Ok(value)
    }

    /// Creates a validated structured form.
    pub fn form(title: String, schema: Value) -> Result<Self> {
        let value = Self::Form { title, schema };
        value.validate(&mut None)?;
        Ok(value)
    }

    /// Creates an approval bound to one exact action digest.
    pub fn approval(
        prompt: String,
        operation_id: OperationId,
        action_digest: [u8; 32],
        subject: ApprovalSubject,
    ) -> Result<Self> {
        let value = Self::Approval {
            prompt,
            operation_id,
            action_digest,
            subject,
        };
        value.validate(&mut None)?;
        Ok(value)
    }

    /// Logical heap bytes this request retains, for native custody checks.
    #[cfg(any(test, all(feature = "filesystem", not(target_arch = "wasm32"))))]
    pub(crate) fn owned_bytes(&self) -> Result<u64> {
        interaction_request_owned_bytes(self, &None)
    }

    /// Validates the interaction before it is admitted.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        match self {
            Self::Question {
                prompt,
                response_schema,
            } => {
                nonempty(prompt, "question prompt is empty", work)?;
                crate::executor::compile_json_schema(response_schema, "interaction", work)?;
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
                    return Err(boundary(work, Invalid, "invalid choice selection bounds"));
                }
                let slots = if work.is_some() {
                    (options.len() as u64)
                        .checked_mul(std::mem::size_of::<&str>() as u64)
                        .ok_or_else(|| {
                            boundary(work, Invalid, "choice identity custody overflows")
                        })?
                } else {
                    0
                };
                let mut custody = crate::wire_codec::CodecQuota::new(work, slots)?;
                let mut ids = BTreeSet::new();
                for option in options {
                    if let Some(observer) = custody.work().as_deref_mut() {
                        observer.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                            bytes: 0,
                            items: 1,
                        })?;
                    }
                    nonempty(
                        &option.id,
                        "choice option identity is empty",
                        custody.work(),
                    )?;
                    nonempty(
                        &option.label,
                        "choice option label is empty",
                        custody.work(),
                    )?;
                    if !ids.insert(option.id.as_str()) {
                        return Err(boundary(
                            custody.work(),
                            Invalid,
                            "duplicate choice option identity",
                        ));
                    }
                }
            }
            Self::Form { title, schema } => {
                nonempty(title, "form title is empty", work)?;
                crate::executor::compile_json_schema(schema, "interaction", work)?;
            }
            Self::Approval {
                prompt,
                action_digest,
                subject,
                ..
            } => {
                nonempty(prompt, "approval prompt is empty", work)?;
                if action_digest == &[0; 32] {
                    return Err(boundary(work, Invalid, "approval action digest is empty"));
                }
                if let ApprovalSubject::Tool { tool, call_id, .. } = subject {
                    nonempty(tool, "approval tool name is empty", work)?;
                    nonempty(call_id, "approval tool call identity is empty", work)?;
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
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
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
            ) => crate::executor::validate_json_schema_value(
                response_schema,
                value,
                "interaction response",
                work,
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
                let slots = if work.is_some() {
                    (option_ids.len() as u64)
                        .checked_mul(2 * std::mem::size_of::<&str>() as u64)
                        .ok_or_else(|| {
                            boundary(work, Invalid, "choice response custody overflows")
                        })?
                } else {
                    0
                };
                let mut custody = crate::wire_codec::CodecQuota::new(work, slots)?;
                if let Some(observer) = custody.work().as_deref_mut() {
                    observer.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                        bytes: 0,
                        items: option_ids.len() as u64,
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
                    return Err(boundary(custody.work(), Invalid, "invalid choice response"));
                }
                Ok(())
            }
            (Self::Approval { .. }, InteractionResponse::Approval { .. }) => Ok(()),
            _ => Err(boundary(
                work,
                Invalid,
                "interaction response type does not match its request",
            )),
        }
    }
}

fn nonempty(
    value: &str,
    reason: &'static str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if value.trim().is_empty() {
        Err(boundary(work, Invalid, reason))
    } else {
        Ok(())
    }
}
