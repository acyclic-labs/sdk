//! Shared unary response limits for native HTTP/2 and browser gRPC-Web clients.
/// Weighted metadata bytes, including 32 bytes per field value.
pub const HEADER_LIST_BYTES: u32 = 16 * 1024;
/// Raw gRPC-Web trailer bytes, before whitespace normalization.
pub const TRAILER_BYTES: usize = 16 * 1024;

/// Checked payload, metadata and encoded unary body budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnaryResponseLimits {
    message_bytes: usize,
    binary_body_bytes: usize,
    text_body_bytes: usize,
}
impl UnaryResponseLimits {
    /// Construct a policy for the caller-selected protobuf payload ceiling.
    /// Return `None` if either framed binary or padded text budget overflows `usize`.
    pub const fn new(message_bytes: usize) -> Option<Self> {
        let body = match message_bytes.checked_add(TRAILER_BYTES) {
            Some(n) => n,
            None => return None,
        };
        let body = match body.checked_add(10) {
            Some(n) => n,
            None => return None,
        };
        let text = match body.checked_mul(4) {
            Some(n) => n,
            None => return None,
        };
        Some(Self {
            message_bytes,
            binary_body_bytes: body,
            text_body_bytes: text,
        })
    }
    /// Aggregate body budget applies only to unary responses. Streaming responses
    /// retain the independent protobuf message, trailer and metadata bounds.
    pub const fn body_bytes(self, streaming: bool, text: bool) -> Option<usize> {
        if streaming {
            None
        } else if text {
            Some(self.text_body_bytes)
        } else {
            Some(self.binary_body_bytes)
        }
    }
    /// Maximum decoded protobuf message bytes.
    pub const fn message_bytes(self) -> usize {
        self.message_bytes
    }
    /// Maximum raw gRPC-Web trailer frame bytes.
    pub const fn trailer_bytes(self) -> usize {
        TRAILER_BYTES
    }
    /// Maximum metadata weight, including 32 bytes per field value.
    pub const fn header_list_bytes(self) -> u32 {
        HEADER_LIST_BYTES
    }
    /// Payload and trailer ceilings plus their two five-byte frame headers.
    pub const fn binary_body_bytes(self) -> usize {
        self.binary_body_bytes
    }
    /// Text body ceiling allowing independently padded one-byte base64 segments.
    pub const fn text_body_bytes(self) -> usize {
        self.text_body_bytes
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unary_budget_includes_both_frames_and_one_byte_padded_segments() {
        for payload in [0, 1, 2, 3, 8 * 1024 * 1024, 16 * 1024 * 1024] {
            let limits = UnaryResponseLimits::new(payload).unwrap();
            assert_eq!(limits.message_bytes(), payload);
            assert_eq!(limits.trailer_bytes(), TRAILER_BYTES);
            assert_eq!(limits.header_list_bytes(), HEADER_LIST_BYTES);
            assert_eq!(limits.binary_body_bytes(), payload + TRAILER_BYTES + 10);
            assert_eq!(limits.text_body_bytes(), 4 * limits.binary_body_bytes());
            assert!(limits.text_body_bytes() <= u32::MAX as usize);
        }
        assert_eq!(
            UnaryResponseLimits::new(16 * 1024 * 1024)
                .unwrap()
                .text_body_bytes(),
            67_174_440
        );
    }
    #[test]
    fn every_overflow_stage_fails_closed() {
        assert_eq!(UnaryResponseLimits::new(usize::MAX), None);
        assert_eq!(UnaryResponseLimits::new(usize::MAX - TRAILER_BYTES), None);
        assert_eq!(UnaryResponseLimits::new(usize::MAX / 4), None);
        let maximum = usize::MAX / 4 - TRAILER_BYTES - 10;
        assert_eq!(
            UnaryResponseLimits::new(maximum).unwrap().text_body_bytes(),
            usize::MAX / 4 * 4
        );
        assert_eq!(UnaryResponseLimits::new(maximum + 1), None);
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn streaming_preserves_each_bound_without_a_unary_aggregate(payload in 0_usize..(1<<20), text in any::<bool>()) {
            let limits = UnaryResponseLimits::new(payload).unwrap();
            prop_assert_eq!(limits.body_bytes(true, text), None);
            prop_assert_eq!(limits.body_bytes(false, text), Some((payload + TRAILER_BYTES + 10) * if text { 4 } else { 1 }));
            prop_assert_eq!(limits.message_bytes(), payload);
            prop_assert_eq!(limits.trailer_bytes(), TRAILER_BYTES);
            prop_assert_eq!(limits.header_list_bytes(), HEADER_LIST_BYTES);
        }
    }
    #[test]
    fn negative_control_unary_budget_rejects_an_otherwise_valid_stream() {
        let limits = UnaryResponseLimits::new(8).unwrap();
        let body = limits.binary_body_bytes() + 1;
        assert!(
            limits
                .body_bytes(true, false)
                .is_none_or(|maximum| body <= maximum)
        );
        assert!(
            !limits
                .body_bytes(false, false)
                .is_none_or(|maximum| body <= maximum)
        );
    }
}
