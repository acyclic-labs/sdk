use acyclic_auth::contract::*;
pub fn root() -> Block {
    Block {
        certificates: Some(CertificateChain {
            birth: BirthCertificate {
                birth: Birth {
                    account_id: "account".into(),
                    environment: "test".into(),
                    genesis_public_key: vec![1; 32],
                    issued_at_unix_seconds: 1,
                },
                signature: vec![2; 64],
            },
            keys: vec![KeyCertificate {
                key: CertifiedKey {
                    account_id: "account".into(),
                    key_id: "key".into(),
                    public_key: vec![3; 32],
                    key_epoch: 1,
                    validity: Validity {
                        not_before_unix_seconds: 1,
                        expires_at_unix_seconds: 1000,
                    },
                    grants: grants("fs:/", true, "fs/read"),
                    principal: Principal {
                        kind: PrincipalKind::User,
                        id: "issuer".into(),
                        lineage: vec![],
                        groups: vec!["staff".into()],
                        roles: vec!["reader".into()],
                        attributes: vec![],
                    },
                    parent_key_id: None,
                    parent_revocation_ids: vec![],
                },
                signature: vec![4; 64],
            }],
        }),
        caveats: vec![],
        next_public_key: vec![5; 32],
    }
}
pub fn grants(name: &str, prefix: bool, action: &str) -> GrantSet {
    GrantSet {
        grants: vec![Grant {
            kind: GrantKind::Resource(ResourceGrant {
                resource: ResourcePattern {
                    name: name.into(),
                    prefix,
                },
                actions: vec![action.into()],
            }),
        }],
    }
}
pub fn limits(value: Option<u64>) -> Limits {
    Limits {
        rps: value,
        burst: value,
        bytes_per_s: value,
        channels: value,
        concurrency: value,
        bytes: value,
        spend_micros: value,
        tokens: value,
    }
}
