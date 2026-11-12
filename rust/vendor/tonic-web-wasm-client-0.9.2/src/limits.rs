//! Caller-selected response policies for native and browser gRPC clients.
/// Current native HTTP/2 metadata ceiling; transport streaming remains separate.
pub const HEADER_LIST_BYTES: u32 = 16 * 1024;

/// A protobuf payload policy with independently selected metadata policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnaryResponseLimits {
    message_bytes: usize,
    trailer_bytes: Option<usize>,
    header_list_bytes: Option<u32>,
}
impl UnaryResponseLimits {
    /// Select a protobuf payload ceiling without imposing metadata ceilings.
    pub const fn new(message_bytes: usize) -> Self {
        Self {
            message_bytes,
            trailer_bytes: None,
            header_list_bytes: None,
        }
    }
    /// Select metadata policies and a checked aggregate unary body budget.
    /// Return `None` if framing or independently padded base64 can overflow.
    pub const fn with_metadata_limits(
        mut self,
        header_list_bytes: u32,
        trailer_bytes: usize,
    ) -> Option<Self> {
        let Some(body) = self.message_bytes.checked_add(trailer_bytes) else {
            return None;
        };
        let Some(body) = body.checked_add(10) else {
            return None;
        };
        if body.checked_mul(4).is_none() {
            return None;
        }
        self.trailer_bytes = Some(trailer_bytes);
        self.header_list_bytes = Some(header_list_bytes);
        Some(self)
    }
    /// Streaming responses have no aggregate body policy. Unary aggregation
    /// requires an explicit trailer policy as well as the payload policy.
    pub const fn body_bytes(self, streaming: bool, text: bool) -> Option<usize> {
        if streaming {
            return None;
        }
        if text {
            self.text_body_bytes()
        } else {
            self.binary_body_bytes()
        }
    }
    /// Maximum decoded protobuf message bytes.
    pub const fn message_bytes(self) -> usize {
        self.message_bytes
    }
    /// Optional maximum raw gRPC-Web trailer frame bytes.
    pub const fn trailer_bytes(self) -> Option<usize> {
        self.trailer_bytes
    }
    /// Optional metadata weight, including 32 bytes per field value.
    pub const fn header_list_bytes(self) -> Option<u32> {
        self.header_list_bytes
    }
    /// Payload and explicit trailer ceilings plus their frame headers.
    pub const fn binary_body_bytes(self) -> Option<usize> {
        let Some(trailer) = self.trailer_bytes else {
            return None;
        };
        let Some(body) = self.message_bytes.checked_add(trailer) else {
            return None;
        };
        body.checked_add(10)
    }
    /// Explicit text body policy allowing independently padded base64 segments.
    pub const fn text_body_bytes(self) -> Option<usize> {
        let Some(body) = self.binary_body_bytes() else {
            return None;
        };
        body.checked_mul(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_encoded_body_policies_fail_closed_on_overflow() {
        assert_eq!(
            UnaryResponseLimits::new(usize::MAX).with_metadata_limits(128, 0),
            None
        );
        assert_eq!(
            UnaryResponseLimits::new(usize::MAX / 4).with_metadata_limits(128, 32),
            None
        );
        let maximum = usize::MAX / 4 - 32 - 10;
        assert_eq!(
            UnaryResponseLimits::new(maximum)
                .with_metadata_limits(128, 32)
                .unwrap()
                .text_body_bytes(),
            Some(usize::MAX / 4 * 4)
        );
        assert_eq!(
            UnaryResponseLimits::new(maximum + 1).with_metadata_limits(128, 32),
            None
        );
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn streaming_preserves_selected_policies_without_an_aggregate(payload in 0_usize..(1<<20), trailer in 0_usize..(1<<16), text in any::<bool>()) {
            let limits = UnaryResponseLimits::new(payload).with_metadata_limits(128, trailer).unwrap();
            prop_assert_eq!(limits.body_bytes(true, text), None);
            prop_assert_eq!(limits.body_bytes(false, text), Some((payload + trailer + 10) * if text { 4 } else { 1 }));
            prop_assert_eq!(limits.message_bytes(), payload);
            prop_assert_eq!(limits.trailer_bytes(), Some(trailer));
            prop_assert_eq!(limits.header_list_bytes(), Some(128));
        }
    }
    #[test]
    fn negative_control_unary_policy_rejects_an_otherwise_valid_stream() {
        let limits = UnaryResponseLimits::new(8)
            .with_metadata_limits(128, 32)
            .unwrap();
        let body = limits.binary_body_bytes().unwrap() + 1;
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
