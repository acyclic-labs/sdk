//! Runtime-owned signing custody. Consumers retain only a weak admission port.
use super::*;
use ed25519_dalek::{Signer, SigningKey};
use std::sync::{Arc, Weak};

pub(super) struct RuntimeSigner {
    metadata: SigningKey,
    pub(super) workload: workload::RuntimeKeys,
    pub(super) public: Arc<PublicAuthority>,
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
    pub(super) fn new(id: &str, seed: [u8; 32]) -> Self {
        let seed = zeroize::Zeroizing::new(seed);
        let metadata = prepare_key(&blake3::derive_key(
            "acyclic.harness.metadata.v1",
            &seed[..],
        ));
        let workload = workload::RuntimeKeys::new(&seed);
        let public = Arc::new(PublicAuthority {
            trust: std::sync::RwLock::new(Arc::new(workload::trust(id, &workload))),
            metadata_key: metadata.verifying_key().to_bytes(),
            workload: Default::default(),
            revision: tokio::sync::watch::channel(0).0,
        });
        Self {
            metadata,
            workload,
            public,
        }
    }
}

pub(super) struct PublicAuthority {
    pub(super) trust: std::sync::RwLock<Arc<acyclic_auth::trust::Snapshot>>,
    pub(super) metadata_key: [u8; 32],
    // One immutable workload and the canonical certificate verifier. Switching
    // scopes retires the old workload without reauthenticating identical roots.
    // Admission decisions are never retained.
    pub(super) workload: std::sync::Mutex<workload::WorkloadOwner>,
    pub(super) revision: tokio::sync::watch::Sender<u64>,
}

/// The port neither owns a root nor exports one. Runtime teardown disables all
/// admission; independently authenticated history remains publicly verifiable.
#[derive(Clone)]
pub(super) struct AdmissionPort(Weak<RuntimeSigner>);
impl AdmissionPort {
    pub(super) fn closed() -> Self {
        Self(Weak::new())
    }
    pub(super) fn new(runtime: &Arc<RuntimeSigner>) -> Self {
        Self(Arc::downgrade(runtime))
    }
    pub(in crate::core::authority) fn runtime(&self) -> Result<Arc<RuntimeSigner>> {
        self.0
            .upgrade()
            .ok_or_else(|| Error::Unauthorized("runtime signing custody is closed".into()))
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
        verifier.verify(&scope, &mut None)?;
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
