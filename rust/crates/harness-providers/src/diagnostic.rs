use acyclic_harness_core::{Error, wire::ErrorCode};
use std::fmt::{self, Write as _};

/// Mark semantic failures only after an HTTP refusal or response protocol error.
/// Storage, cancellation and native accounting failures keep their original class.
pub(crate) fn rejection(error: Error) -> Error {
    let (code, message) = match error {
        Error::Invalid(message) => (ErrorCode::Invalid, message),
        Error::Unauthorized(message) => (ErrorCode::Unauthorized, message),
        error => return error,
    };
    Error::ModelDiagnostic {
        code,
        message,
        custody: None,
    }
}

/// Render directly into a fixed UTF-8 prefix. Transport causes use this only on
/// the portable path; admitted HTTP excerpts share the same bounded renderer.
pub(super) fn excerpt(cause: impl fmt::Display) -> String {
    struct Prefix {
        bytes: [u8; super::EXCERPT_BYTES],
        len: usize,
    }
    impl fmt::Write for Prefix {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            for character in text.chars() {
                let character = if character.is_control() {
                    ' '
                } else {
                    character
                };
                let end = self.len + character.len_utf8();
                if end > self.bytes.len() {
                    return Err(fmt::Error);
                }
                character.encode_utf8(&mut self.bytes[self.len..end]);
                self.len = end;
            }
            Ok(())
        }
    }
    let mut prefix = Prefix {
        bytes: [0; super::EXCERPT_BYTES],
        len: 0,
    };
    let _ = write!(prefix, "{cause}");
    std::str::from_utf8(&prefix.bytes[..prefix.len])
        .unwrap_or_default()
        .trim()
        .to_owned()
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;
    use acyclic_harness_core::ModelTransportFailure::*;
    #[test]
    fn transport_causes_and_http_excerpts_keep_only_bounded_diagnostics() {
        struct Cause(std::cell::Cell<usize>);
        impl fmt::Display for Cause {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.set(self.0.get() + 1);
                formatter.write_str("cause\ntext")
            }
        }
        let cause = Cause(std::cell::Cell::new(0));
        for failure in [Connect, Timeout, Interrupted, Tls, Status(503)] {
            let allocated = acyclic_test_support::measure(|| {
                assert!(matches!(
                    crate::transport_failure(true, failure, &cause),
                    acyclic_harness_core::Error::ModelUnavailable { excerpt, .. } if excerpt.is_empty()
                ));
            });
            assert_eq!(allocated.count_total, 0);
        }
        assert_eq!(cause.0.get(), 0);
        // Negative control: the portable path must render, so a disconnected counter fails.
        assert!(
            crate::transport_failure(false, Connect, &cause)
                .to_string()
                .contains("cause text")
        );
        assert_eq!(cause.0.get(), 1);

        let text = " diagnostic\nα".repeat(64);
        let mut actual = String::new();
        let allocation = acyclic_test_support::measure(|| {
            actual = crate::excerpt(text.as_bytes());
        });
        assert_eq!(allocation.count_total, 1);
        // The former HTTP renderer built a growable string and then copied
        // its trimmed value. Keep that real primitive as the cost negative.
        let mut former = String::new();
        let copied = acyclic_test_support::measure(|| {
            let bounded = &text.as_bytes()[..crate::EXCERPT_BYTES];
            let valid = match std::str::from_utf8(bounded) {
                Ok(text) => text,
                Err(cut) => std::str::from_utf8(&bounded[..cut.valid_up_to()]).unwrap(),
            };
            let mut excerpt = String::new();
            for character in valid.chars() {
                if excerpt.len() + character.len_utf8() > crate::EXCERPT_BYTES {
                    break;
                }
                excerpt.push(if character.is_control() {
                    ' '
                } else {
                    character
                });
            }
            former = excerpt.trim().to_owned();
        });
        assert_eq!(actual, former);
        assert!(copied.count_total > allocation.count_total);
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        #[test]
        fn diagnostics_are_bounded_utf8_without_controls(text in ".{0,600}") {
            let rendered = excerpt(&text);
            proptest::prop_assert!(rendered.len() <= crate::EXCERPT_BYTES);
            proptest::prop_assert!(!rendered.chars().any(char::is_control));
            // A short ordinary string is preserved, not merely erased to meet the bound.
            if text.len() <= crate::EXCERPT_BYTES && !text.chars().any(char::is_control) {
                proptest::prop_assert_eq!(rendered, text.trim());
            }
            let http = crate::excerpt(text.as_bytes());
            proptest::prop_assert!(http.len() <= crate::EXCERPT_BYTES);
            proptest::prop_assert!(!http.chars().any(char::is_control));
            if text.len() <= crate::EXCERPT_BYTES {
                proptest::prop_assert_eq!(http, excerpt(&text));
            }
        }
    }
}
