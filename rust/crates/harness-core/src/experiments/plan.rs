//! Counterbalanced assignment order for one source population. This plans
//! indices only; actual source cuts, configuration and execution stay external.

use serde::{Deserialize, Serialize};

/// One predeclared pair, emitted before outcomes in version-1 assignment files.
/// Population identity and replicate refer to the original experimental design;
/// declaring them does not itself prove source-cut or provider comparability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct PairAssignment {
    version: u32,
    /// The comparison and common source cut selected before execution.
    pub population: [u8; 32],
    /// Replicate selected before execution, including future failed work.
    pub replicate: u64,
}

impl PairAssignment {
    /// Declare one pair without a heap allocation or numerical validation.
    pub fn new(population: [u8; 32], replicate: u64) -> Self {
        Self {
            version: 1,
            population,
            replicate,
        }
    }
}

impl<'de> Deserialize<'de> for PairAssignment {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct File {
            version: u32,
            population: [u8; 32],
            replicate: u64,
        }
        let file: File = crate::executor::json::deserialize_object(decoder)?;
        if file.version != 1 {
            return Err(serde::de::Error::custom("assignment version must be 1"));
        }
        Ok(Self::new(file.population, file.replicate))
    }
}

mod assignments;
pub use assignments::{Assignment, AssignmentPlan, PlanError};

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            Self::NoArms => "at least one arm is required",
            Self::Overflow => "arm-by-replicate count exceeds u64",
        };
        write!(
            f,
            "plan experiment assignments: {reason}; recovery: correct the positive arm count and representable arm-by-replicate count, then call AssignmentPlan::new again"
        )
    }
}
impl std::error::Error for PlanError {}
