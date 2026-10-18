use std::fmt::{self, Write as _};

/// Render directly into a fixed UTF-8 prefix. Native errors never invoke this renderer;
/// portable errors allocate only the final bounded diagnostic, not an unbounded temporary.
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use acyclic_harness::ModelTransportFailure::*;
    #[test]
    fn native_failures_never_render_discarded_causes() {
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
                    acyclic_harness::Error::NativeBoundary { .. }
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
        }
    }
}
