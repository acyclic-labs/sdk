//! A mount credential issuer over real custody, for wire tests.
use super::{CredentialGrantRequest, MountOwner, mount_capability};
use acyclic_auth::{
    contract::{
        FilesystemGrant, Grant, GrantKind, GrantSet, Principal, PrincipalKind, ResourceGrant,
        ResourcePattern, Validity,
    },
    trust::{Certification, Follower, Issuer},
    verify::CertifiedSigner,
};
use ed25519_dalek::SigningKey;

pub(crate) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

pub(crate) const OWNER: MountOwner = MountOwner {
    posix_uid: 1000,
    posix_gid: 1000,
};

/// A runtime certified for every volume, following its own trust.
pub(crate) struct Mounts {
    pub(crate) trust: Follower,
    pub(crate) signer: CertifiedSigner,
}

impl Mounts {
    pub(crate) fn new() -> Self {
        // Certified a minute back, so a service clock read before this
        // fixture never precedes its validity.
        let now = now() - 60;
        let issuer = Issuer::new("test", SigningKey::from_bytes(&[1; 32]));
        let (root, genesis) = issuer
            .genesis("account", SigningKey::from_bytes(&[2; 32]), now)
            .expect("genesis");
        let trust = Follower::new("test", issuer.anchor());
        for record in &genesis {
            trust.apply(record, now).expect("genesis record");
        }
        trust.resume();
        trust.serve("account");
        let leaf = SigningKey::from_bytes(&[3; 32]);
        let chain = issuer
            .certify(
                &root,
                Certification {
                    key_id: "mounts",
                    public_key: leaf.verifying_key(),
                    validity: Validity {
                        not_before_unix_seconds: now,
                        expires_at_unix_seconds: now + 3_600,
                    },
                    grants: GrantSet {
                        grants: vec![Grant {
                            kind: GrantKind::Filesystem(FilesystemGrant {
                                grant: ResourceGrant {
                                    resource: ResourcePattern {
                                        name: "fs:/".into(),
                                        prefix: true,
                                    },
                                    actions: ["fs/read", "fs/list", "fs/write"]
                                        .map(String::from)
                                        .to_vec(),
                                },
                                posix_uid: Some(OWNER.posix_uid),
                                posix_gid: Some(OWNER.posix_gid),
                            }),
                        }],
                    },
                    principal: Principal {
                        kind: PrincipalKind::Service,
                        id: "mounts".into(),
                        lineage: vec![],
                        groups: vec![],
                        roles: vec![],
                        attributes: vec![],
                    },
                    parent_revocation_ids: vec![],
                },
            )
            .expect("certify");
        Self {
            trust,
            signer: CertifiedSigner::new(chain, leaf).expect("signer"),
        }
    }

    /// The sealed mount capability for `request`, minted now.
    pub(crate) fn mint(&self, request: &CredentialGrantRequest) -> Vec<u8> {
        mount_capability(
            &self.signer,
            &self.trust,
            request,
            OWNER,
            SigningKey::from_bytes(&[10; 32]),
            &[11; 32],
            now(),
        )
        .expect("mint")
        .to_vec()
    }
}
