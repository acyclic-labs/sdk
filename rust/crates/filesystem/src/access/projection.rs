#[cfg(test)]
use crate::sync::Arc;
use crate::sync::Mutex;
mod registry;

// Exclusion belongs to a provider owner and volume. Independent stores may
// derive identical volume ids; adapters sharing a provider share its owner id.
// Remote authorities still require a distributed lease for ProjFS admission.
#[derive(Default)]
struct ProjectionPolicies {
    // Terminal lock: neither backend I/O nor another lock runs under it.
    // Leases retain registration, never this lock, across publication/retirement.
    entries: Mutex<registry::Registry>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct State {
    projections: usize,
    editors: usize,
}
pub(crate) type Lease = Registration<'static>;
pub(crate) struct Registration<'a> {
    owner: &'a ProjectionPolicies,
    key: registry::Key,
}

#[cfg(not(acyclic_fs_loom))]
static POLICIES: std::sync::OnceLock<ProjectionPolicies> = std::sync::OnceLock::new();
#[cfg(acyclic_fs_loom)]
loom::lazy_static! { static ref POLICIES: ProjectionPolicies = ProjectionPolicies::default(); }

fn register(
    owner: &crate::OperationClaims,
    volume: crate::VolumeId,
    projection: bool,
) -> Result<(Lease, bool), crate::FsError> {
    #[cfg(not(acyclic_fs_loom))]
    let policies = POLICIES.get_or_init(ProjectionPolicies::default);
    #[cfg(acyclic_fs_loom)]
    let policies = &*POLICIES;
    ProjectionPolicies::enter(
        policies,
        registry::Volume {
            owner: owner.owner_identity(),
            id: volume,
        },
        projection,
    )
}

pub(crate) fn enter(
    owner: &crate::OperationClaims,
    volume: crate::VolumeId,
) -> Result<Lease, crate::FsError> {
    register(owner, volume, true).map(|(lease, _)| lease)
}

// Registration is linearized with projection admission. No backend I/O or CAS
// runs under the registry lock; the returned lease excludes admission until CAS.
pub(crate) fn publication(
    owner: &crate::OperationClaims,
    volume: crate::VolumeId,
) -> Result<(Lease, bool), crate::FsError> {
    register(owner, volume, false)
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
        owner: &Self,
        volume: registry::Volume,
        projection: bool,
    ) -> Result<(Registration<'_>, bool), crate::FsError> {
        let mut entries = owner
            .entries
            .lock()
            .map_err(|_| crate::FsError::MutationNotAllowed)?;
        let state = entries.counts(volume);
        if projection && state.editors != 0 {
            return Err(crate::FsError::MutationNotAllowed);
        }
        let key = entries.insert(volume, projection)?;
        Ok((
            Registration { owner, key },
            projection || state.projections != 0,
        ))
    }
}
impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let removed = self
            .owner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(self.key);
        drop(removed);
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
            let owner = Arc::new(crate::OperationClaims::default());
            let publisher = loom::thread::spawn({
                let owner = Arc::clone(&owner);
                let revision = Arc::clone(&revision);
                move || {
                    let (lease, projected) = if omit_publication_lease {
                        (None, false)
                    } else {
                        let (lease, projected) = publication(&owner, volume).unwrap();
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
            if let Ok(lease) = enter(&owner, volume) {
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
            assert!(publication(&owner, volume).is_ok());
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
            let volume = registry::Volume { owner: [0; 16], id: crate::VolumeId::from_bytes([1; 16]) };
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
