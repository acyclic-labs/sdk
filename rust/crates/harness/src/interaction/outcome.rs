use super::*;

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
