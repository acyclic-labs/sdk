//! Actual kernel decisions made by the shipping agent step.
//!
//! This is the adapter boundary to the kernel included by the formal harness.
//! It checks decision correspondence, not committed effects, durable frontiers,
//! distributed ordering or unbounded liveness. Capture is opt-in and uses the
//! one existing differential recorder; no alternate agent driver is introduced.

use super::{AgentPath, AgentState, kernel};
use kernel::{Admit, Admitted, Cascade, Control, Pick, Wait};

/// Input and result of one original kernel call, before shell lowering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Admit(Control, Admit, Admitted),
    Pick(Control, bool, bool, bool, Pick),
    Settle(Control, Wait, (Control, bool)),
    Cascade(Control, usize, Cascade),
    Alarm(Control, bool, bool, usize, Option<u64>),
    Satisfy(Control, Control),
}

impl Decision {
    /// Name used by the source/model correspondence inventory.
    pub const fn action(self) -> &'static str {
        match self {
            Self::Admit(..) => "admit",
            Self::Pick(..) => "pick",
            Self::Settle(..) => "settle",
            Self::Cascade(..) => "cascade",
            Self::Alarm(..) => "alarm",
            Self::Satisfy(..) => "satisfy",
        }
    }

    /// Replay against the same production functions included by the proof.
    /// The recorded result is compared, never adopted as an expected value.
    pub fn conforms(self) -> bool {
        match self {
            Self::Admit(control, input, actual) => kernel::admit(control, input) == actual,
            Self::Pick(control, alarm, queued, approval, actual) => {
                kernel::pick(control, alarm, queued, approval) == actual
            }
            Self::Settle(control, wait, actual) => kernel::settle(control, wait) == actual,
            Self::Cascade(control, pending, actual) => kernel::cascade(control, pending) == actual,
            Self::Alarm(control, queued, approval, pending, actual) => {
                kernel::alarm(control, queued, approval, pending) == actual
            }
            Self::Satisfy(control, actual) => kernel::satisfy(control) == actual,
        }
    }
}

/// Original agent identity and decision. Order is capture-local only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub agent: AgentPath,
    pub decision: Decision,
}

pub(crate) fn record(state: &AgentState, decision: Decision) {
    if acyclic_differential::capturing() {
        acyclic_differential::record(Event {
            agent: state.path.clone(),
            decision,
        });
    }
}
