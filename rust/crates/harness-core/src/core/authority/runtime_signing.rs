//! Runtime-owned signing custody. Consumers retain only a weak admission port.
use super::*;
use ed25519_dalek::{Signer, SigningKey};
use std::sync::{Arc, Weak};

pub(super) struct RuntimeSigner {
    seed: zeroize::Zeroizing<[u8; 32]>,
    pub(super) public: Arc<PublicAuthority>,
}
impl RuntimeSigner {
    pub(super) fn new(id: &str, seed: [u8; 32]) -> Self {
        let key = SigningKey::from_bytes(&blake3::derive_key("acyclic.harness.metadata.v1", &seed));
        Self {
            seed: zeroize::Zeroizing::new(seed),
            public: Arc::new(PublicAuthority {
                trust: std::sync::RwLock::new(workload::trust(id, &seed)),
                metadata_key: key.verifying_key().to_bytes(),
            }),
        }
    }
    pub(super) fn seed(&self) -> &[u8; 32] {
        &self.seed
    }
    fn metadata_signer(&self) -> SigningKey {
        SigningKey::from_bytes(&blake3::derive_key(
            "acyclic.harness.metadata.v1",
            self.seed(),
        ))
    }
}

pub(super) struct PublicAuthority {
    pub(super) trust: std::sync::RwLock<acyclic_auth::trust::Snapshot>,
    pub(super) metadata_key: [u8; 32],
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
        Ok(self.runtime()?.metadata_signer().sign(digest).to_bytes())
    }
    pub(super) fn record(
        &self,
        id: &str,
        audience: &Authority,
        scope: &Scope,
        payload: [u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Vec<u8>> {
        let runtime = self.runtime()?;
        workload::record(&runtime, id, audience, scope, payload, work)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_verifier_survives_runtime_teardown_but_cannot_admit_or_mint() -> Result<()> {
        let runtime = AuthorityIssuer::new(
            "runtime",
            [87; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "task".into(),
            },
        );
        let verifier = runtime.verifier(&mut None)?;
        let scope = runtime.issue(
            "actor",
            &Capabilities::new(["event:append"]),
            None,
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
        let forged_root = AuthorityIssuer::new(
            "runtime",
            verifier.public.metadata_key,
            runtime.audience.clone(),
        );
        let forged = forged_root.issue(
            "actor",
            &Capabilities::new(["event:append"]),
            None,
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

    #[test]
    fn revocation_reaches_existing_verifiers_and_closes_descendant_admission() -> Result<()> {
        let runtime = AuthorityIssuer::new(
            "runtime",
            [88; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "task".into(),
            },
        );
        let verifier = runtime.verifier(&mut None)?;
        let parent = runtime.issue(
            "parent",
            &Capabilities::new(["event:append"]),
            None,
            None,
            &mut None,
        )?;
        let child = runtime.attenuate(&parent, "child", parent.capabilities())?;
        let recorded =
            verifier
                .admission
                .record("runtime", &runtime.audience, &child, [1; 32], &mut None)?;
        workload::verify_record(&verifier, &recorded, [1; 32], None, &[], &mut None)?;
        let online_child = runtime.issue_derived(
            &child,
            "new-agent",
            &Capabilities::new(["event:append"]),
            Some(AgentId::from_bytes([3; 16])),
            &mut None,
        )?;
        verifier.verify(&online_child, &mut None)?;
        runtime.revoke_workload(&parent)?;
        assert!(verifier.verify(&online_child, &mut None).is_err());
        assert!(
            runtime
                .issue_derived(
                    &child,
                    "again",
                    child.capabilities(),
                    child.agent(),
                    &mut None
                )
                .is_err()
        );
        assert!(verifier.verify(&child, &mut None).is_err());
        assert!(
            verifier
                .admission
                .record("runtime", &runtime.audience, &child, [1; 32], &mut None)
                .is_err()
        );
        assert!(
            workload::verify_record(&verifier, &recorded, [1; 32], None, &[], &mut None).is_err()
        );
        Ok(())
    }
}
