use crate::sync::{Arc, Mutex};
use std::collections::BTreeMap;

// A volume id denotes one authority. Distinct embedded stores reusing an id
// conservatively share exclusion. Remote authorities need a distributed lease
// and are refused by ProjFS admission.
#[derive(Default)]
struct ProjectionPolicies {
    // Terminal lock: neither backend I/O nor another lock runs under it.
    // Leases retain registration, never this lock, across publication/retirement.
    entries: Mutex<BTreeMap<crate::VolumeId, State>>,
}
#[derive(Default)]
struct State {
    projections: usize,
    editors: usize,
}
pub(crate) struct Lease {
    owner: Arc<ProjectionPolicies>,
    volume: crate::VolumeId,
    projection: bool,
}

#[cfg(not(acyclic_fs_loom))]
static POLICIES: std::sync::OnceLock<Arc<ProjectionPolicies>> = std::sync::OnceLock::new();
#[cfg(acyclic_fs_loom)]
loom::lazy_static! { static ref POLICIES: Arc<ProjectionPolicies> = Arc::new(ProjectionPolicies::default()); }

fn register(volume: crate::VolumeId, projection: bool) -> Result<(Lease, bool), crate::FsError> {
    #[cfg(not(acyclic_fs_loom))]
    let policies = POLICIES.get_or_init(|| Arc::new(ProjectionPolicies::default()));
    #[cfg(acyclic_fs_loom)]
    let policies = &*POLICIES;
    ProjectionPolicies::enter(policies, volume, projection)
}

pub(crate) fn enter(volume: crate::VolumeId) -> Result<Lease, crate::FsError> {
    register(volume, true).map(|(lease, _)| lease)
}

// Registration is linearized with projection admission. No backend I/O or CAS
// runs under the registry lock; the returned lease excludes admission until CAS.
pub(crate) fn publication(volume: crate::VolumeId) -> Result<(Lease, bool), crate::FsError> {
    register(volume, false)
}
pub(crate) fn check_policy(projected: bool, changed: bool) -> Result<(), crate::FsError> {
    if projected && changed {
        Err(crate::kernel::PublicationError::ActiveProjection.into())
    } else {
        Ok(())
    }
}
impl ProjectionPolicies {
    fn enter(
        owner: &Arc<Self>,
        volume: crate::VolumeId,
        projection: bool,
    ) -> Result<(Lease, bool), crate::FsError> {
        let mut entries = owner
            .entries
            .lock()
            .map_err(|_| crate::FsError::MutationNotAllowed)?;
        if projection && entries.get(&volume).is_some_and(|state| state.editors != 0) {
            return Err(crate::FsError::MutationNotAllowed);
        }
        let state = entries.entry(volume).or_default();
        let count = if projection {
            &mut state.projections
        } else {
            &mut state.editors
        };
        *count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        Ok((
            Lease {
                owner: Arc::clone(owner),
                volume,
                projection,
            },
            state.projections != 0,
        ))
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut entries = self
            .owner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // An inconsistent registry must keep exclusion, rather than unwind or
        // permit a publication while a projection may still be live.
        let Some(state) = entries.get_mut(&self.volume) else {
            return;
        };
        let count = if self.projection {
            &mut state.projections
        } else {
            &mut state.editors
        };
        let Some(remaining) = count.checked_sub(1) else {
            return;
        };
        *count = remaining;
        if state.projections == 0 && state.editors == 0 {
            entries.remove(&self.volume);
        }
        if entries.is_empty() {
            *entries = BTreeMap::new();
        }
    }
}
#[cfg(all(test, acyclic_fs_loom))]
mod model {
    use super::*;
    fn run(omit_publication_lease: bool, late_admission: bool) {
        // The reporter only aggregates completed schedules. Every value affecting
        // the protocol, including the CAS and admission lock, is instrumented.
        let violated = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = std::sync::Arc::clone(&violated);
        loom::model(move || {
            let volume = crate::VolumeId::from_bytes([1; 16]);
            let revision = Arc::new(crate::sync::atomic::AtomicUsize::new(0));
            let publisher = loom::thread::spawn({
                let revision = Arc::clone(&revision);
                move || {
                    let (lease, projected) = if omit_publication_lease {
                        (None, false)
                    } else {
                        let (lease, projected) = publication(volume).unwrap();
                        (Some(lease), projected)
                    };
                    // The real publication reducer checks the candidate metadata
                    // under this lease and retains it through the authority CAS.
                    if check_policy(projected, true).is_ok() {
                        let _ = revision.compare_exchange(
                            0,
                            1,
                            crate::sync::atomic::Ordering::SeqCst,
                            crate::sync::atomic::Ordering::SeqCst,
                        );
                    }
                    drop(lease);
                }
            });
            let mut overlap = false;
            let before_admission =
                late_admission.then(|| revision.load(crate::sync::atomic::Ordering::SeqCst));
            if let Ok(lease) = enter(volume) {
                let pinned = before_admission
                    .unwrap_or_else(|| revision.load(crate::sync::atomic::Ordering::SeqCst));
                loom::thread::yield_now();
                overlap = pinned != revision.load(crate::sync::atomic::Ordering::SeqCst);
                drop(lease);
            }
            publisher.join().unwrap();
            if overlap {
                observed.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            assert!(publication(volume).is_ok());
        });
        assert!(
            !violated.load(std::sync::atomic::Ordering::Relaxed),
            "policy changed during projection or materialization"
        );
    }
    #[test]
    fn policy_publication_cas_racing_projection_start_is_fenced() {
        run(false, false);
    }
    #[test]
    #[should_panic(expected = "policy changed during projection")]
    fn missing_publication_fence_negative_control() {
        run(true, false);
    }
    #[test]
    fn materialization_admits_policy_under_the_publication_lease() {
        run(false, false);
    }
    #[test]
    #[should_panic(expected = "policy changed during projection or materialization")]
    fn admission_before_the_lease_negative_control() {
        run(false, true);
    }
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod properties {
    use super::*;
    use proptest::prelude::*;
    #[cfg(all(feature = "memory", not(target_arch = "wasm32")))]
    #[test]
    fn unknown_authorities_cannot_claim_a_process_policy_owner() {
        let unknown = crate::Fs::new((), (), crate::EmbeddedCapabilities::MEMORY);
        assert!(!unknown.has_process_policy_owner());
        assert!(crate::Fs::memory().has_process_policy_owner());
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn fresh_owner_handles_share_exclusion_until_the_last_lease_retires(steps in prop::collection::vec(0u8..4, 0..48)) {
            let volume = crate::VolumeId::from_bytes([1; 16]);
            let policies = Arc::new(ProjectionPolicies::default());
            let mut projections = Vec::new();
            let mut editors = Vec::new();
            for step in steps {
                match step {
                    0 | 1 => {
                        let projection = step == 0;
                        let allowed = !projection || editors.is_empty();
                        let entered = ProjectionPolicies::enter(&policies, volume, projection);
                        prop_assert_eq!(entered.is_ok(), allowed);
                        if let Ok((lease, projected)) = entered {
                            prop_assert_eq!(projected, !projections.is_empty() || projection);
                            prop_assert_eq!(check_policy(projected, true).is_ok(), !projected);
                            prop_assert!(check_policy(projected, false).is_ok());
                            if projection { projections.push(lease); } else { editors.push(lease); }
                        }
                    }
                    2 => { projections.pop(); }
                    _ => { editors.pop(); }
                }
            }
            drop(projections); drop(editors);
            prop_assert!(policies.entries.lock().unwrap().is_empty());
            prop_assert!(ProjectionPolicies::enter(&policies, volume, false).is_ok());
        }
    }
}
