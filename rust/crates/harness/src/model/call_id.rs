use super::ModelDispatch;
use std::fmt::Write as _;

impl ModelDispatch {
    /// Exact UTF-8 capacity of a harness-minted tool-call identity.
    pub const TOOL_CALL_ID_BYTES: usize = 62;

    /// Stable identity of one call in this operation/step, independent of provider IDs.
    /// The executor binds each operation/step to one admitted request digest.
    pub fn tool_call_id(&self, ordinal: u64) -> String {
        let mut id = String::with_capacity(Self::TOOL_CALL_ID_BYTES);
        write!(
            &mut id,
            "{}:{:08x}:{ordinal:016x}",
            self.operation_id, self.step
        )
        .expect("formatting an identifier into String cannot fail");
        id
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::OperationId;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn canonical_identity_is_an_injective_dispatch_ordinal_tuple(
            operation in any::<[u8;16]>(), step in any::<u32>(), ordinal in any::<u64>(),
            other_operation in any::<[u8;16]>(), other_step in any::<u32>(), other_ordinal in any::<u64>(),
        ) {
            let dispatch = ModelDispatch { operation_id: OperationId::from_bytes(operation), step, request_digest:[0;32] };
            let other = ModelDispatch { operation_id: OperationId::from_bytes(other_operation), step:other_step, request_digest:[0;32] };
            let id = dispatch.tool_call_id(ordinal);
            prop_assert_eq!(id.len(), ModelDispatch::TOOL_CALL_ID_BYTES);
            let fields: Vec<_> = id.split(':').collect();
            prop_assert_eq!(fields.len(), 3);
            prop_assert_eq!(fields[0], dispatch.operation_id.to_string());
            prop_assert_eq!(u32::from_str_radix(fields[1],16).unwrap(), step);
            prop_assert_eq!(u64::from_str_radix(fields[2],16).unwrap(), ordinal);
            prop_assert_eq!(id == other.tool_call_id(other_ordinal), (operation,step,ordinal)==(other_operation,other_step,other_ordinal));
            prop_assert_eq!(&id, &dispatch.tool_call_id(ordinal), "replay mints the same identity");
            prop_assert_ne!(&id, &dispatch.tool_call_id(ordinal.wrapping_add(1)));
        }
    }
}
