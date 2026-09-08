//! Typed, durable, executor-neutral open interactions.

use crate::{Error, OperationId, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

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
#[serde(tag = "type", rename_all = "snake_case")]
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

    pub(crate) fn validate(&self) -> Result<()> {
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

    pub(crate) fn validate_response(&self, response: &InteractionResponse) -> Result<()> {
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
#[serde(tag = "type", rename_all = "snake_case")]
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
