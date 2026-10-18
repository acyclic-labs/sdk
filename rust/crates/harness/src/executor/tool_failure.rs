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
            message: preparation_format(format_args!("{}", CauseText(error)), work)?,
        })
    }

    /// The cause of a call refused before dispatch: its complete model-visible text, sanitized
    /// and bounded like every cause, classed by the refusing error.
    pub(crate) fn refusal(
        name: &str,
        reason: ToolFailureKind,
        error: &Error,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let class = ToolErrorClass::of(error);
        let message = preparation_format(
            format_args!(
                "{}",
                Sanitized(&RefusalText {
                    name,
                    reason,
                    class,
                    error,
                })
            ),
            work,
        )?;
        Ok(Self { class, message })
    }

    /// Whether a recorded cause has the bounded, sanitized, nonblank shape this module writes.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.message.len() <= TOOL_FAILURE_MESSAGE_BYTES
            && !self.message.chars().any(char::is_control)
            && !self.message.trim().is_empty()
    }
}

/// The model-visible text of a recorded failure; replay renders the same bytes.
pub(crate) struct ToolFailureText<'a>(pub ToolFailureKind, pub &'a ToolFailureCause);

impl fmt::Display for ToolFailureText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(reason, cause) = self;
        // A refusal's cause is already its complete text.
        if reason.precedes_dispatch() {
            return formatter.write_str(&cause.message);
        }
        write!(
            formatter,
            "{} ({}): {}",
            reason.message(),
            cause.class.label(),
            cause.message
        )
    }
}

/// The complete text of a call refused before dispatch.
struct RefusalText<'a> {
    name: &'a str,
    reason: ToolFailureKind,
    class: ToolErrorClass,
    error: &'a Error,
}

impl fmt::Display for RefusalText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            name,
            reason,
            class,
            error,
        } = self;
        match reason {
            ToolFailureKind::Unavailable => write!(
                formatter,
                "tool `{name}` is not available; call one of the declared tools"
            ),
            ToolFailureKind::InvalidInput => write!(
                formatter,
                "tool `{name}` rejected its arguments: {}",
                CauseText(*error)
            ),
            _ => write!(
                formatter,
                "tool `{name}` refused the call ({}): {}",
                class.label(),
                CauseText(*error)
            ),
        }
    }
}

/// The sanitized, bounded text of the error behind a failure, never blank: an error whose text
/// is blank is described by its stable class, so no cause is erased.
pub(crate) struct CauseText<'a>(pub &'a Error);

impl fmt::Display for CauseText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut probe = Visible(false);
        // The probe stops at the first visible character, so the error renders twice at most.
        let _ = write!(probe, "{}", self.0);
        if probe.0 {
            write!(formatter, "{}", Sanitized(self.0))
        } else {
            write!(formatter, "{} error", ToolErrorClass::of(self.0).label())
        }
    }
}

/// Finds a visible character, failing the write once it has one.
struct Visible(bool);

impl fmt::Write for Visible {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = text
            .chars()
            .any(|character| !character.is_control() && !character.is_whitespace());
        if self.0 { Err(fmt::Error) } else { Ok(()) }
    }
}

/// Displays text with control characters replaced and its UTF-8 bounded on a character.
pub(crate) struct Sanitized<'a>(pub &'a dyn fmt::Display);

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
    use proptest::prelude::*;

    /// Every failure kind; `entry` fails to compile when a kind is added without coverage.
    const KINDS: [ToolFailureKind; 7] = [
        ToolFailureKind::ExecutorRejected,
        ToolFailureKind::InvalidOutput,
        ToolFailureKind::ProjectionRejected,
        ToolFailureKind::PublicationRejected,
        ToolFailureKind::Unavailable,
        ToolFailureKind::InvalidInput,
        ToolFailureKind::Refused,
    ];

    /// The production constructor each entry point uses for its kind.
    fn entry(kind: ToolFailureKind, name: &str, error: &Error) -> Result<ToolFailureCause> {
        match kind {
            // Unknown tool, schema-invalid arguments, policy denial: refused before dispatch.
            ToolFailureKind::Unavailable
            | ToolFailureKind::InvalidInput
            | ToolFailureKind::Refused => ToolFailureCause::refusal(name, kind, error, &mut None),
            // Executor error, output contract, projection and publication after a start.
            ToolFailureKind::ExecutorRejected
            | ToolFailureKind::InvalidOutput
            | ToolFailureKind::ProjectionRejected
            | ToolFailureKind::PublicationRejected => ToolFailureCause::of(error, &mut None),
        }
    }

    /// Errors over every class, including ones whose own text is blank.
    fn error(variant: u8, text: String) -> Error {
        match variant % 8 {
            0 => Error::Invalid(text),
            1 => Error::NotFound(text),
            2 => Error::Conflict(text),
            3 => Error::Unauthorized(text),
            4 => Error::Storage(text),
            5 => Error::InteractionRejected(crate::InteractionRejection::Denied),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            6 => Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: ["", " ", "\n\t", "\u{1b}"][text.len() % 4],
            },
            _ => Error::Unsupported(text),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        // Class G: no settled tool failure reaches the model or the journal without its cause.
        #[test]
        fn every_tool_failure_entry_point_carries_a_nonblank_bounded_cause(
            kind in 0..KINDS.len(),
            variant in any::<u8>(),
            text in r"(\PC|[\n\t\x1b ]){0,400}",
            name in "[a-z_.@0-9]{1,300}",
        ) {
            let kind = KINDS[kind];
            let cause = entry(kind, &name, &error(variant, text)).unwrap();
            prop_assert!(cause.is_canonical(), "{cause:?}");
            let rendered = ToolFailureText(kind, &cause).to_string();
            prop_assert!(!rendered.trim().is_empty());
            prop_assert!(!rendered.chars().any(char::is_control));
            // The rendered text is exactly what the result admission contract accepts.
            let result = crate::model::ToolResultContent::Error { message: rendered };
            prop_assert!(result.validate_limits(crate::conversation::Limits::default(), &mut None).is_ok());
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn a_blank_error_is_described_by_its_class() -> Result<()> {
        let blank = Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: " \n",
        };
        let cause = ToolFailureCause::of(&blank, &mut None)?;
        assert_eq!(cause.message, "conflict error");
        assert!(cause.is_canonical());
        // Negative control: the sanitized text alone erases the cause, and admission refuses it.
        let erased = ToolFailureCause {
            class: ToolErrorClass::Conflict,
            message: Sanitized(&blank).to_string(),
        };
        assert!(!erased.is_canonical());
        // An empty error result is refused too.
        let empty = crate::model::ToolResultContent::Error {
            message: String::new(),
        };
        assert!(
            empty
                .validate_limits(crate::conversation::Limits::default(), &mut None)
                .is_err()
        );
        Ok(())
    }

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
