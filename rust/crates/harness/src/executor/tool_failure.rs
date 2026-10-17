use super::*;
use std::fmt::{self, Write as _};

/// Most UTF-8 bytes of a recorded tool-failure message the model receives.
pub const TOOL_FAILURE_MESSAGE_BYTES: usize = 256;

/// Stable, credential-free category of the error behind a settled tool failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ToolErrorClass {
    /// The call or its input was invalid for the tool.
    Invalid,
    /// A resource the call named does not exist.
    NotFound,
    /// The call conflicted with current state.
    Conflict,
    /// The tool cannot perform the requested operation.
    Unsupported,
    /// The call lacked authority for a resource.
    Unauthorized,
    /// A policy or approval refused the call.
    Denied,
    /// Any other definitive failure.
    Failed,
}

impl ToolErrorClass {
    /// Derives the class from the error's stable wire code.
    #[must_use]
    pub fn of(error: &Error) -> Self {
        use crate::wire::ErrorCode as Code;
        match error.code() {
            Code::Invalid => Self::Invalid,
            Code::NotFound => Self::NotFound,
            Code::Conflict => Self::Conflict,
            Code::Unsupported => Self::Unsupported,
            Code::Unauthorized => Self::Unauthorized,
            Code::InteractionDeclined
            | Code::InteractionDenied
            | Code::InteractionCancelled
            | Code::InteractionExpired => Self::Denied,
            _ => Self::Failed,
        }
    }
    const fn label(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
            Self::Unauthorized => "unauthorized",
            Self::Denied => "denied",
            Self::Failed => "failed",
        }
    }
}

/// Bounded, sanitized cause of a settled tool failure: enough for the model to act on, while
/// the raw error stays in the `warn` log.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ToolFailureCause {
    /// Stable category.
    pub class: ToolErrorClass,
    /// At most [`TOOL_FAILURE_MESSAGE_BYTES`] of the error's text, without control characters.
    pub message: String,
}

impl ToolFailureCause {
    pub(crate) fn of(
        error: &Error,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        Ok(Self {
            class: ToolErrorClass::of(error),
            message: preparation_format(format_args!("{}", Sanitized(error)), work)?,
        })
    }

    /// Whether a recorded cause has the bounded, sanitized shape this module writes.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.message.len() <= TOOL_FAILURE_MESSAGE_BYTES
            && !self.message.chars().any(char::is_control)
    }
}

/// The model-visible text of a recorded failure; replay renders the same bytes.
pub(crate) struct ToolFailureText<'a>(pub ToolFailureKind, pub &'a ToolFailureCause);

impl fmt::Display for ToolFailureText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(reason, cause) = self;
        write!(
            formatter,
            "{} ({}): {}",
            reason.message(),
            cause.class.label(),
            cause.message
        )
    }
}

/// A refusal's text: its class and sanitized, bounded cause.
pub(crate) struct RefusalText<'a>(pub &'a Error);

impl fmt::Display for RefusalText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "({}): {}",
            ToolErrorClass::of(self.0).label(),
            Sanitized(self.0)
        )
    }
}

/// Displays an error with control characters replaced and its UTF-8 bounded on a character.
pub(crate) struct Sanitized<'a>(pub &'a Error);

impl fmt::Display for Sanitized<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bounded = Bounded {
            output: formatter,
            remaining: TOOL_FAILURE_MESSAGE_BYTES,
        };
        write!(bounded, "{}", self.0)
    }
}

struct Bounded<'a, 'b> {
    output: &'a mut fmt::Formatter<'b>,
    remaining: usize,
}

impl fmt::Write for Bounded<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        for character in text.chars() {
            let character = if character.is_control() {
                ' '
            } else {
                character
            };
            if character.len_utf8() > self.remaining {
                self.remaining = 0;
                return Ok(());
            }
            self.remaining -= character.len_utf8();
            self.output.write_char(character)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn causes_are_bounded_on_characters_and_free_of_controls() -> Result<()> {
        let error = Error::NotFound(format!("line\none\u{1b}[31m{}", "é".repeat(400)));
        let cause = ToolFailureCause::of(&error, &mut None)?;
        assert_eq!(cause.class, ToolErrorClass::NotFound);
        assert!(cause.is_canonical());
        assert!(cause.message.len() > TOOL_FAILURE_MESSAGE_BYTES - 2);
        assert!(cause.message.contains("line one [31m"));
        // Negative control: unsanitized text is not canonical.
        let raw = ToolFailureCause {
            class: ToolErrorClass::Failed,
            message: error.to_string(),
        };
        assert!(!raw.is_canonical());
        assert_eq!(
            ToolFailureText(ToolFailureKind::ExecutorRejected, &cause)
                .to_string()
                .split_once(" (not_found): ")
                .map(|(head, _)| head),
            Some(ToolFailureKind::ExecutorRejected.message())
        );
        Ok(())
    }
}
