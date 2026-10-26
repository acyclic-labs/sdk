//! The process-incarnation decision shared by recovery and its state model.
use super::NativeMountError;

pub(super) use acyclic_native_runtime::process_identity::ProcessObservation;
use acyclic_native_runtime::process_identity::{IncarnationState, ProcessIncarnation};

pub(super) fn require_dead_incarnation(
    pid: u32,
    started: u64,
    observation: ProcessObservation,
) -> Result<(), NativeMountError> {
    let owner =
        ProcessIncarnation::new(pid, started).ok_or(NativeMountError::RecoveryOwnerUnknown)?;
    match owner.observe(observation) {
        IncarnationState::Absent | IncarnationState::Replaced | IncarnationState::Exited => Ok(()),
        IncarnationState::Running => Err(NativeMountError::DestinationBusy),
        IncarnationState::Unknown => Err(NativeMountError::RecoveryOwnerUnknown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn custody_lifecycle_tracks_incarnations(pid in 1..u32::MAX, birth in 1..u64::MAX) {
            // Alive -> dead -> PID reused. Unknown evidence never retires custody.
            prop_assert!(matches!(require_dead_incarnation(pid, birth,
                ProcessObservation::Present { started: birth, exited: false }),
                Err(NativeMountError::DestinationBusy)), "live owner retains custody");
            prop_assert!(require_dead_incarnation(pid, birth,
                ProcessObservation::Present { started: birth, exited: true }).is_ok(), "exited owner permits recovery");
            prop_assert!(require_dead_incarnation(pid, birth, ProcessObservation::Absent).is_ok());
            prop_assert!(require_dead_incarnation(pid, birth,
                ProcessObservation::Present { started: birth + 1, exited: false }).is_ok(), "a reused PID identifies another incarnation");
            prop_assert!(matches!(require_dead_incarnation(pid, birth, ProcessObservation::Unknown),
                Err(NativeMountError::RecoveryOwnerUnknown)));
            prop_assert!(require_dead_incarnation(0, birth, ProcessObservation::Absent).is_err());
            prop_assert!(require_dead_incarnation(pid, 0, ProcessObservation::Absent).is_err());
        }
    }
}
