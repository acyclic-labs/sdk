//! A canonical append's additive command bound, retained across combiner rounds.

/// Backend-sealed command and record capacity needed by one proposal.
#[derive(Clone, Copy, Debug, Default)]
pub struct AuthorityGroupCost {
    pub(crate) bytes: usize,
    pub(crate) records: usize,
}

impl AuthorityGroupCost {
    pub(crate) fn admit(&mut self, next: Self) -> bool {
        let Some(bytes) = self.bytes.checked_add(next.bytes) else {
            return false;
        };
        let Some(records) = self.records.checked_add(next.records) else {
            return false;
        };
        // A near-bound single command may not need the conservative composition
        // padding. Keep it alone and let the canonical provider admit it exactly.
        if self.bytes != 0
            && (bytes > acyclic_stream::MAX_COMMAND_BYTES || records > acyclic_stream::MAX_ITEMS)
        {
            return false;
        }
        *self = Self { bytes, records };
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]
        #[test]
        fn accepted_multi_member_capacity_never_exceeds_provider_limits(
            proposals in prop::collection::vec((1usize..=acyclic_stream::MAX_COMMAND_BYTES,
                1usize..=acyclic_stream::MAX_ITEMS), 1..100)) {
            let mut batch = AuthorityGroupCost::default();
            for (bytes, records) in proposals {
                let next = AuthorityGroupCost { bytes, records };
                if !batch.admit(next) {
                    prop_assert!(batch.bytes.checked_add(bytes).is_none_or(|total| total > acyclic_stream::MAX_COMMAND_BYTES)
                        || batch.records.checked_add(records).is_none_or(|total| total > acyclic_stream::MAX_ITEMS));
                    batch = AuthorityGroupCost::default();
                    prop_assert!(batch.admit(next));
                }
                prop_assert!(batch.bytes <= acyclic_stream::MAX_COMMAND_BYTES);
                prop_assert!(batch.records <= acyclic_stream::MAX_ITEMS);
            }
        }
    }

    #[test]
    fn near_bound_singletons_exclude_every_other_member() {
        let mut batch = AuthorityGroupCost::default();
        assert!(batch.admit(AuthorityGroupCost {
            bytes: acyclic_stream::MAX_COMMAND_BYTES + 1,
            records: 1
        }));
        assert!(!batch.admit(AuthorityGroupCost {
            bytes: 1,
            records: 1
        }));
    }
}
