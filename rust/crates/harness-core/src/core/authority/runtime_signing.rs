//! Runtime-owned signing custody. Consumers retain only a weak admission port.
use super::*;
use ed25519_dalek::{Signer, SigningKey};
use std::sync::{Arc, Weak};

pub(super) struct RuntimeSigner {
    metadata: SigningKey,
    // None when genesis failed: every issuance then fails closed.
    workload: Option<std::sync::Mutex<workload::RuntimeKeys>>,
    /// The Follower this runtime's Issuer publishes to; the same value is the
    /// public trust every verifier of this runtime reads.
    pub(super) trust: Arc<acyclic_auth::trust::Follower>,
    pub(super) public: Arc<PublicAuthority>,
    /// Serves `trust` while signing custody lives, unless its verifiers own it.
    _serving: Option<TrustServing>,
}

/// Serves public trust while its one owner lives. Dropping it fences the
/// Follower, so every authority check derived from that trust fails closed.
pub(super) struct TrustServing(Arc<acyclic_auth::trust::Follower>);

impl Drop for TrustServing {
    fn drop(&mut self) {
        self.0.close(acyclic_auth::trust::FenceCause::Shutdown);
    }
}

/// Who keeps a runtime's public trust serving.
#[derive(Clone, Copy)]
pub(super) enum TrustOwner {
    /// Signing custody: dropping the root fences every derived verifier.
    Custody,
    /// The root and every verifier derived from it: access a host owner hands
    /// out never outlives the trust it is checked against.
    Verifiers,
}

#[cfg(test)]
#[derive(Debug, PartialEq)]
struct KeyPreparation;

pub(super) fn prepare_key(bytes: &[u8; 32]) -> SigningKey {
    #[cfg(test)]
    acyclic_test_support::differential::record(KeyPreparation);
    SigningKey::from_bytes(bytes)
}

impl RuntimeSigner {
    pub(super) fn new(
        id: &str,
        seed: [u8; 32],
        clock: Option<Arc<dyn acyclic_stream::UnixMillisClock>>,
        owner: TrustOwner,
    ) -> Self {
        let seed = zeroize::Zeroizing::new(seed);
        let metadata = prepare_key(&blake3::derive_key(
            "acyclic.harness.metadata.v1",
            &seed[..],
        ));
        let issuer = workload::issuer(&seed);
        let trust = Arc::new(acyclic_auth::trust::Follower::new(
            workload::ENVIRONMENT,
            issuer.anchor(),
        ));
        let serving = TrustServing(trust.clone());
        let (custody, verifiers) = match owner {
            TrustOwner::Custody => (Some(serving), None),
            TrustOwner::Verifiers => (None, Some(serving)),
        };
        let public = Arc::new(PublicAuthority {
            trust: trust.clone(),
            metadata_key: metadata.verifying_key().to_bytes(),
            workload: Default::default(),
            clock,
            _serving: verifiers,
        });
        let workload = workload::RuntimeKeys::genesis(id, issuer, &seed, &trust, public.now())
            .ok()
            .map(std::sync::Mutex::new);
        Self {
            metadata,
            workload,
            trust,
            public,
            _serving: custody,
        }
    }

    /// Exclusive signing custody for one issuance or publication.
    pub(super) fn keys(&self) -> Result<std::sync::MutexGuard<'_, workload::RuntimeKeys>> {
        self.workload
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("runtime trust genesis failed".into()))?
            .lock()
            .map_err(|_| Error::Storage("runtime signing custody poisoned".into()))
    }
}

pub(super) struct PublicAuthority {
    /// Public trust and its publications, from one owner.
    pub(super) trust: Arc<dyn acyclic_auth::trust::Live>,
    pub(super) metadata_key: [u8; 32],
    // One immutable workload and the canonical certificate verifier. Switching
    // scopes retires the old workload without reauthenticating identical roots.
    // Admission decisions are never retained.
    pub(super) workload: std::sync::Mutex<workload::WorkloadOwner>,
    pub(super) clock: Option<Arc<dyn acyclic_stream::UnixMillisClock>>,
    /// Serves `trust` while any verifier of a verifier-owned root lives.
    pub(super) _serving: Option<TrustServing>,
}

impl PublicAuthority {
    pub(super) fn now(&self) -> u64 {
        self.clock
            .as_ref()
            .map_or_else(workload::now, |clock| clock.now_unix_millis() / 1000)
    }
}

/// The port neither owns a root nor exports one. Runtime teardown disables all
/// admission; independently authenticated history remains publicly verifiable.
#[derive(Clone)]
pub(super) enum AdmissionPort {
    Native(Weak<RuntimeSigner>),
    Platform(
        Weak<AdmittedAuthority>,
        crate::OperationId,
        Option<acyclic_actors::guest::CancellationToken>,
    ),
}
impl AdmissionPort {
    pub(super) fn closed() -> Self {
        Self::Native(Weak::new())
    }
    pub(super) fn new(runtime: &Arc<RuntimeSigner>) -> Self {
        Self::Native(Arc::downgrade(runtime))
    }
    pub(super) fn platform(
        authority: &Arc<AdmittedAuthority>,
        operation: crate::OperationId,
    ) -> Self {
        Self::Platform(Arc::downgrade(authority), operation, None)
    }
    pub(in crate::core::authority) fn runtime(&self) -> Result<Arc<RuntimeSigner>> {
        match self {
            Self::Native(runtime) => runtime
                .upgrade()
                .ok_or_else(|| Error::Unauthorized("runtime signing custody is closed".into())),
            Self::Platform(..) => Err(Error::Unauthorized(
                "platform custody requires an admitted asynchronous operation".into(),
            )),
        }
    }
    pub(super) fn check(&self, scope: &Scope) -> Result<()> {
        if let Self::Platform(owner, ..) = self {
            owner
                .upgrade()
                .ok_or_else(|| Error::Unauthorized("platform authority owner is closed".into()))?
                .check_scope(scope)?;
        }
        Ok(())
    }
    pub(super) fn sign(&self, digest: &[u8; 32]) -> Result<[u8; 64]> {
        Ok(self.runtime()?.metadata.sign(digest).to_bytes())
    }
}

#[cfg(test)]
#[path = "runtime_key_tests.rs"]
mod key_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_verifier_owned_trust_outlives_its_root() -> Result<()> {
        use futures::FutureExt;
        let audience = Authority {
            kind: AggregateKind::Agent,
            id: "owner".into(),
        };
        for (owned, root) in [
            (
                false,
                AuthorityRoot::new("owner", [75; 32], audience.clone()),
            ),
            (
                true,
                AuthorityRoot::host_owned("owner", [75; 32], audience.clone()),
            ),
        ] {
            let scope =
                root.bootstrap_scope("owner", &Capabilities::new(["read"]), None, &mut None)?;
            let verifier = root.verifier(&mut None)?;
            let closed = root.trust_changes().changed();
            drop(root);
            // Custody-owned trust fences with its root; verifier-owned trust
            // serves until the last verifier drops. Signing ends either way.
            assert_eq!(verifier.verify(&scope, &mut None).is_ok(), owned);
            assert!(verifier.admission.sign(&[0; 32]).is_err());
            drop(verifier);
            assert_eq!(
                closed.now_or_never(),
                Some(Err(acyclic_auth::trust::SourceClosed))
            );
        }
        Ok(())
    }

    #[test]
    fn delegated_issuance_requires_a_parent_and_refuses_every_widening() -> Result<()> {
        let root = AuthorityRoot::new(
            "delegation",
            [73; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "delegation".into(),
            },
        );
        let parent = root.bootstrap_scope(
            "parent",
            &Capabilities::new(["read", "list"]),
            Some(AgentId::from_bytes([74; 16])),
            &mut None,
        )?;
        let issuer = root.issuer(&mut None)?;
        let narrowed = issuer.issue(&parent, "child", &Capabilities::new(["read"]))?;
        assert_eq!(narrowed.agent(), parent.agent());
        assert_eq!(narrowed.parent_proof, Some(*parent.proof));
        root.verifier(&mut None)?.verify(&narrowed, &mut None)?;
        for widened in [
            Capabilities::new(["write"]),
            Capabilities::new(["read", "write"]),
        ] {
            assert!(issuer.issue(&parent, "widened", &widened).is_err());
            // Negative control: the trusted bootstrap custodian can create a
            // fresh root with these grants. Delegation must never take that path.
            assert!(
                root.bootstrap_scope("unchecked", &widened, parent.agent(), &mut None)
                    .is_ok()
            );
        }
        root.revoke_workload(&parent)?;
        assert!(
            issuer
                .issue(&parent, "revoked", &Capabilities::new(["read"]))
                .is_err()
        );
        Ok(())
    }
    #[test]
    fn public_verifier_survives_runtime_teardown_but_cannot_admit_or_mint() -> Result<()> {
        let runtime = AuthorityRoot::new(
            "runtime",
            [87; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "task".into(),
            },
        );
        let verifier = runtime.verifier(&mut None)?;
        let scope = runtime.bootstrap_scope(
            "actor",
            &Capabilities::new(["event:append"]),
            None,
            &mut None,
        )?;
        let public = AuthorityVerifier::from_public(
            "runtime".into(),
            runtime.audience.clone(),
            runtime.public_workload_trust()?,
            verifier.public_checkpoint_key(),
        )?;
        public.verify(&scope, &mut None)?;
        let proof = verifier.canonical_proof(b"test-checkpoint", &42u64, &mut None)?;
        for offset in 0..proof.len() {
            let mut corrupted = proof;
            corrupted[offset] ^= 1;
            assert!(matches!(
                public.verify_canonical_proof(b"test-checkpoint", &42u64, &corrupted, &mut None),
                Err(Error::Unauthorized(_))
            ));
        }
        let forged_root = AuthorityRoot::new(
            "runtime",
            verifier.public.metadata_key,
            runtime.audience.clone(),
        );
        let forged = forged_root.bootstrap_scope(
            "actor",
            &Capabilities::new(["event:append"]),
            None,
            &mut None,
        )?;
        assert!(verifier.verify(&forged, &mut None).is_err());
        drop(runtime);
        public.verify_canonical_proof(b"test-checkpoint", &42u64, &proof, &mut None)?;
        assert!(
            public
                .canonical_proof(b"test-checkpoint", &42u64, &mut None)
                .is_err()
        );
        assert!(verifier.verify(&scope, &mut None).is_err());
        verifier.verify_canonical_proof(b"test-checkpoint", &42u64, &proof, &mut None)?;
        assert!(
            verifier
                .verify_canonical_proof(b"test-checkpoint", &43u64, &proof, &mut None)
                .is_err()
        );
        assert!(
            verifier
                .canonical_proof(b"test-checkpoint", &42u64, &mut None)
                .is_err()
        );
        Ok(())
    }
}
