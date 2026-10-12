use super::*;

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
    ) -> Result<Self> {
        let value = Self::Approval {
            prompt,
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
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
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
                    return Err(interaction_denial(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "invalid choice selection bounds",
                    ));
                }
                let slots = if work.is_some() {
                    (options.len() as u64)
                        .checked_mul(std::mem::size_of::<&str>() as u64)
                        .ok_or_else(|| {
                            interaction_denial(
                                work,
                                crate::wire::ErrorCode::Invalid,
                                "choice identity custody overflows",
                            )
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
                        return Err(interaction_denial(
                            custody.work(),
                            crate::wire::ErrorCode::Invalid,
                            "duplicate choice option identity",
                        ));
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
                    return Err(interaction_denial(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "approval action digest is empty",
                    ));
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
            ) => crate::executor::validate_json_schema_value(response_schema, value, "interaction response", work),
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
                            interaction_denial(
                                work,
                                crate::wire::ErrorCode::Invalid,
                                "choice response custody overflows",
                            )
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
                    return Err(interaction_denial(
                        custody.work(),
                        crate::wire::ErrorCode::Invalid,
                        "invalid choice response",
                    ));
                }
                Ok(())
            }
            (Self::Approval { .. }, InteractionResponse::Approval { .. }) => Ok(()),
            _ => Err(interaction_denial(
                work,
                crate::wire::ErrorCode::Invalid,
                "interaction response type does not match its request",
            )),
        }
    }
}

fn validate_schema(
    schema: &Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    crate::executor::compile_json_schema(schema, "interaction", work).map(drop)
}

fn nonempty(
    value: &str,
    reason: &'static str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if value.trim().is_empty() {
        Err(interaction_denial(
            work,
            crate::wire::ErrorCode::Invalid,
            reason,
        ))
    } else {
        Ok(())
    }
}
