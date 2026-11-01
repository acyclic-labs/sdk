use super::*;
use std::fmt::{self, Write as _};

/// Stable, credential-free category of the error behind a settled tool failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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

/// Sanitized cause of a settled tool failure, admitted on the caller's preparation ledger.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ToolFailureCause {
    /// Stable category.
    pub class: ToolErrorClass,
    /// The complete error text, with control characters replaced by spaces.
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

    /// The reason a call was refused before dispatch, sanitized and admitted like
    /// every cause. The shared result renderer adds the tool-failure prefix.
    pub(crate) fn refusal(
        name: &str,
        reason: ToolFailureKind,
        error: &Error,
        context: Option<&str>,
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
                    context,
                })
            ),
            work,
        )?;
        Ok(Self { class, message })
    }

    /// Whether a recorded cause has the sanitized, nonblank shape this module writes.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        !self.message.chars().any(char::is_control) && !self.message.trim().is_empty()
    }
}

/// The model-visible text of a recorded failure; replay renders the same bytes.
pub(crate) struct ToolFailureText<'a>(pub ToolFailureKind, pub &'a ToolFailureCause);

impl fmt::Display for ToolFailureText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(reason, cause) = self;
        if *reason == ToolFailureKind::ExecutorRejected || reason.precedes_dispatch() {
            return write!(formatter, "tool execution failed: {}", cause.message);
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
    context: Option<&'a str>,
}

impl fmt::Display for RefusalText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            name,
            reason,
            class,
            error,
            context,
        } = self;
        if let Some(context) = context {
            return write!(
                formatter,
                "tool `{name}` refused the call ({}): {context}: {}",
                class.label(),
                CauseText(error)
            );
        }
        match reason {
            ToolFailureKind::Unavailable => write!(
                formatter,
                "tool `{name}` is not available; call one of the declared tools"
            ),
            ToolFailureKind::InvalidInput => write!(
                formatter,
                "tool `{name}` rejected its arguments: {}",
                CauseText(error)
            ),
            _ => write!(
                formatter,
                "tool `{name}` refused the call ({}): {}",
                class.label(),
                CauseText(error)
            ),
        }
    }
}

/// The sanitized text of the error behind a failure, never blank: an error whose text
/// is blank is described by its stable class, so no cause is erased.
pub(crate) struct CauseText<'a>(pub &'a Error);

impl fmt::Display for CauseText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut probe = Visible(false);
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

/// Displays the complete text with control characters replaced by spaces.
pub(crate) struct Sanitized<'a>(pub &'a dyn fmt::Display);

impl fmt::Display for Sanitized<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(Sanitizing(formatter), "{}", self.0)
    }
}

struct Sanitizing<'a, 'b>(&'a mut fmt::Formatter<'b>);

impl fmt::Write for Sanitizing<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let mut start = 0;
        for (index, character) in text.char_indices() {
            if character.is_control() {
                if start != index {
                    self.0.write_str(&text[start..index])?;
                }
                self.0.write_char(' ')?;
                start = index + character.len_utf8();
            }
        }
        if start != text.len() {
            self.0.write_str(&text[start..])?;
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
        ToolFailureKind::Interrupted,
        ToolFailureKind::Unavailable,
        ToolFailureKind::InvalidInput,
    ];

    /// The production constructor each entry point uses for its kind.
    fn entry(
        kind: ToolFailureKind,
        name: &str,
        error: &Error,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ToolFailureCause> {
        match kind {
            // Unknown tool, schema-invalid arguments, policy denial: refused before dispatch.
            ToolFailureKind::Unavailable | ToolFailureKind::InvalidInput => {
                ToolFailureCause::refusal(name, kind, error, None, work)
            }
            // Executor error, output contract, projection and publication after a start.
            ToolFailureKind::ExecutorRejected
            | ToolFailureKind::InvalidOutput
            | ToolFailureKind::ProjectionRejected
            | ToolFailureKind::PublicationRejected
            | ToolFailureKind::Interrupted => ToolFailureCause::of(error, work),
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
        fn every_tool_failure_entry_point_carries_a_nonblank_cause(
            kind in 0..KINDS.len(),
            variant in any::<u8>(),
            text in r"(\PC|[\n\t\x1b ]){0,400}",
            name in "[a-z_.@0-9]{1,300}",
        ) {
            let kind = KINDS[kind];
            let cause = entry(kind, &name, &error(variant, text), &mut None).unwrap();
            prop_assert!(cause.is_canonical(), "{cause:?}");
            let rendered = ToolFailureText(kind, &cause).to_string();
            prop_assert!(!rendered.trim().is_empty());
            prop_assert!(!rendered.chars().any(char::is_control));
            if kind.precedes_dispatch() || kind == ToolFailureKind::ExecutorRejected {
                prop_assert!(rendered.starts_with("tool execution failed: "));
            }
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn native_causes_retain_the_complete_diagnostic_on_original_admission(
            prefix in prop::collection::vec(prop::sample::select(vec![' ', '\t', '\n', '\u{2003}']), 0..400),
            tail in "[a-zé]{300,400}",
        ) {
            let prefix: String = prefix.into_iter().collect();
            let message = format!("{prefix}{tail}");
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
                &cancellation,
            ).map_err(|(cause, _)| TestCaseError::fail(format!("{cause:?}")))?;
            let custody = work.claim(message.capacity() as u64, 1, acyclic_fs::WorkCounters::UNCHARGED).unwrap();
            let error = Error::NativeDiagnostic {
                code: crate::wire::ErrorCode::Conflict,
                message,
                custody,
            };
            let expected = format!("{}{tail}", prefix.replace('\t', " ").replace('\n', " "));
            let baseline = work.live_allocation_bytes();
            for kind in KINDS {
                let io = PreparationIo::from_native_work(&work).unwrap();
                let cause = with_history_io(Some(&io), |work| entry(kind, "example.tool", &error, work)).unwrap();
                prop_assert!(cause.is_canonical(), "{kind:?}: {cause:?}");
                prop_assert_eq!(cause.class, ToolErrorClass::Conflict);
                if kind != ToolFailureKind::Unavailable {
                    prop_assert!(cause.message.ends_with(&expected), "{kind:?}: {cause:?}");
                }
                prop_assert!(work.live_allocation_bytes() >= baseline + cause.message.capacity() as u64);
                let rendered = tool_failure_content(kind, &cause, Some(&io)).unwrap();
                with_history_io(Some(&io), |work| rendered.validate_limits(Limits::default(), work)).unwrap();
                // The persisted cause uses the same codec shape and replays identical text.
                let encoded = serde_json::to_vec(&cause).unwrap();
                let replayed: ToolFailureCause = serde_json::from_slice(&encoded).unwrap();
                prop_assert!(replayed.is_canonical());
                prop_assert_eq!(&replayed, &cause);
                prop_assert_eq!(&tool_failure_content(kind, &replayed, Some(&io)).unwrap(), &rendered);
                drop(rendered);
                drop(replayed);
                drop(cause);
                drop(io);
                prop_assert_eq!(work.live_allocation_bytes(), baseline);
            }
            // No copy capacity refuses rather than silently clipping to an admitted prefix.
            let mut budget = acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024);
            budget.bytes_copied = 0;
            let exhausted = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, _)| TestCaseError::fail(format!("{cause:?}")))?;
            let exhausted_baseline = exhausted.live_allocation_bytes();
            let io = PreparationIo::from_native_work(&exhausted).unwrap();
            let no_capacity = with_history_io(Some(&io), |work| ToolFailureCause::of(&error, work));
            prop_assert!(matches!(no_capacity, Err(Error::NativeWork { .. })));
            drop(no_capacity);
            drop(io);
            prop_assert_eq!(exhausted.live_allocation_bytes(), exhausted_baseline);
            // Cancellation refuses on the same ledger instead of returning partial success.
            let io = PreparationIo::from_native_work(&work).unwrap();
            cancellation.cancel();
            prop_assert!(with_history_io(Some(&io), |work| entry(ToolFailureKind::ExecutorRejected, "example.tool", &error, work)).is_err());
            drop(io);
            prop_assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }

    #[test]
    fn causes_retain_multibyte_diagnostics_and_remove_controls() -> Result<()> {
        let error = Error::NotFound(format!("line\none\u{1b}[31m{}", "é".repeat(400)));
        let cause = ToolFailureCause::of(&error, &mut None)?;
        assert_eq!(cause.class, ToolErrorClass::NotFound);
        assert!(cause.is_canonical());
        assert!(cause.message.ends_with(&"é".repeat(400)));
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
                .split_once(": ")
                .map(|(head, _)| head),
            Some(ToolFailureKind::ExecutorRejected.message())
        );
        Ok(())
    }
}
