use crate::invalid;
use acyclic_harness_core::{OperationId, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Immutable alloy configuration loaded from ordinary settings/catalog files.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct AlloySpec {
    /// Ordered provider/model identities. Order is part of the policy.
    pub models: Vec<String>,
    /// Deterministic selection with no mutable counter or random generator.
    pub select: AlloySelection,
}

/// Selection policy serialized as a string or an externally tagged object.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub enum AlloySelection {
    /// Always select the first declared model.
    First,
    /// Cycle in declaration order using the durable turn ordinal.
    RoundRobin,
    /// Repeated weighted cycle, with an operation-specific starting phase.
    Weighted {
        /// One nonnegative weight per model; at least one must be positive.
        weights: Vec<u32>,
    },
    /// Choose a declared index by agent role, with an explicit fallback.
    PerRole {
        /// Role names are exact and case-sensitive.
        roles: BTreeMap<String, usize>,
        /// Index for roles absent from the map.
        fallback: usize,
    },
}

/// Caller-supplied durable identity: reuse it for resume and all tool follow-ups.
#[derive(Clone, Copy, Debug)]
pub struct AlloyTurn<'a> {
    /// Operation that owns the turn sequence.
    pub operation_id: OperationId,
    /// Persisted turn ordinal, never an in-memory request counter.
    pub ordinal: u64,
    /// Agent role, independent of message roles such as user/assistant.
    pub role: &'a str,
}

impl AlloySpec {
    /// Rejects policies that cannot select a declared member.
    pub fn validate(&self) -> Result<()> {
        if self.models.is_empty() {
            return Err(invalid("model alloy must contain a model"));
        }
        let mut unique = std::collections::BTreeSet::new();
        for model in &self.models {
            let Some((provider, name)) = model.split_once('/') else {
                return Err(invalid("alloy members require provider/model"));
            };
            if provider.trim().is_empty() || name.trim().is_empty() || !unique.insert(model) {
                return Err(invalid("alloy members must be nonempty and unique"));
            }
        }
        match &self.select {
            AlloySelection::Weighted { weights } => {
                if weights.len() != self.models.len() || total(weights)? == 0 {
                    return Err(invalid(
                        "alloy weights must match models and have a positive sum",
                    ));
                }
            }
            AlloySelection::PerRole { roles, fallback } => {
                if *fallback >= self.models.len()
                    || roles
                        .iter()
                        .any(|(role, index)| role.trim().is_empty() || *index >= self.models.len())
                {
                    return Err(invalid("alloy roles must name declared model indices"));
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn index(&self, turn: AlloyTurn<'_>) -> usize {
        match &self.select {
            AlloySelection::First => 0,
            AlloySelection::RoundRobin => {
                (u128::from(turn.ordinal) % self.models.len() as u128) as usize
            }
            AlloySelection::Weighted { weights } => {
                // validate() established a nonzero sum that fits u64.
                let sum: u64 = weights.iter().map(|&weight| u64::from(weight)).sum();
                let phase = blake3::hash(&turn.operation_id.into_bytes());
                let phase =
                    u64::from_le_bytes(phase.as_bytes()[..8].try_into().expect("eight hash bytes"));
                let mut slot =
                    ((u128::from(phase) + u128::from(turn.ordinal)) % u128::from(sum)) as u64;
                for (index, &weight) in weights.iter().enumerate() {
                    if slot < u64::from(weight) {
                        return index;
                    }
                    slot -= u64::from(weight);
                }
                unreachable!("validated weighted cycle contains the slot")
            }
            AlloySelection::PerRole { roles, fallback } => {
                *roles.get(turn.role).unwrap_or(fallback)
            }
        }
    }
}

fn total(weights: &[u32]) -> Result<u64> {
    weights.iter().try_fold(0u64, |sum, &weight| {
        sum.checked_add(u64::from(weight))
            .ok_or_else(|| invalid("alloy weight sum overflow"))
    })
}
