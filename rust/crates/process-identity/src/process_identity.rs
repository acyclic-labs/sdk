//! Process identity decisions shared by custody and native mount recovery.
/// Evidence from one process query, independent of the operating system.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessObservation<T = u64> {
    /// The OS proved that the PID does not exist.
    Absent,
    /// A process with this birth identity currently owns the PID.
    Present {
        /// Birth identity returned by the platform.
        started: T,
        /// Whether that same opened process has exited.
        exited: bool,
    },
    /// Query failure is never evidence that an owner died.
    Unknown,
}

/// Classification of a recorded process incarnation against current evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncarnationState {
    /// No process currently has the PID. A POSIX process group can still exist.
    Absent,
    /// A later process reused this PID.
    Replaced,
    /// The recorded incarnation still runs.
    Running,
    /// The recorded incarnation exited.
    Exited,
    /// Identity or current evidence is incomplete.
    Unknown,
}

/// A PID is meaningful only together with its nonempty birth identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessIncarnation<T = u64> {
    pid: std::num::NonZeroU32,
    started: T,
}

impl<T: PartialEq + Default> ProcessIncarnation<T> {
    /// Rejects absent PIDs and empty/zero birth identities before observation.
    pub fn new(pid: u32, started: T) -> Option<Self> {
        (started != T::default()).then_some(Self {
            pid: std::num::NonZeroU32::new(pid)?,
            started,
        })
    }

    /// The PID named by this incarnation.
    #[must_use]
    pub const fn pid(&self) -> u32 {
        self.pid.get()
    }

    /// The birth identity supplied by the platform query.
    #[must_use]
    pub const fn started(&self) -> &T {
        &self.started
    }

    /// The sole identity comparison used by all process and mount owners.
    #[must_use]
    pub fn observe(&self, observation: ProcessObservation<T>) -> IncarnationState {
        match observation {
            ProcessObservation::Absent => IncarnationState::Absent,
            ProcessObservation::Present { started, .. } if started == T::default() => {
                IncarnationState::Unknown
            }
            ProcessObservation::Present { started, .. } if started != self.started => {
                IncarnationState::Replaced
            }
            ProcessObservation::Present { exited: true, .. } => IncarnationState::Exited,
            ProcessObservation::Present { .. } => IncarnationState::Running,
            ProcessObservation::Unknown => IncarnationState::Unknown,
        }
    }
}

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{ProcessProbe, current_process_started, process_started};

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn only_complete_os_evidence_retires_an_incarnation(pid in 1..u32::MAX, birth in 1..u64::MAX) {
            let owner = ProcessIncarnation::new(pid, birth).unwrap();
            prop_assert_eq!(owner.observe(ProcessObservation::Absent), IncarnationState::Absent);
            prop_assert_eq!(owner.observe(ProcessObservation::Present { started: birth, exited: false }), IncarnationState::Running);
            prop_assert_eq!(owner.observe(ProcessObservation::Present { started: birth, exited: true }), IncarnationState::Exited);
            prop_assert_eq!(owner.observe(ProcessObservation::Present { started: birth + 1, exited: false }), IncarnationState::Replaced);
            prop_assert_eq!(owner.observe(ProcessObservation::Unknown), IncarnationState::Unknown);
            prop_assert_eq!(owner.observe(ProcessObservation::Present { started: 0, exited: true }), IncarnationState::Unknown);
            prop_assert!(ProcessIncarnation::new(0, birth).is_none());
            prop_assert!(ProcessIncarnation::new(pid, 0_u64).is_none());
        }
    }

    #[test]
    fn posix_byte_identity_uses_the_same_comparison_without_allocating() {
        let owner = ProcessIncarnation::new(1, b"start".as_slice()).unwrap();
        assert_eq!(
            owner.observe(ProcessObservation::Present {
                started: b"other",
                exited: false
            }),
            IncarnationState::Replaced
        );
        assert!(ProcessIncarnation::new(1, b"".as_slice()).is_none());
    }
}
