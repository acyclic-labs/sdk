//! Mount credential admission: what Cloud mints is admitted exactly, and each
//! way a credential can exceed or miss its scope is refused.
use super::*;
use crate::wire_service::mount_fixture::{Mounts, OWNER, now};
use crate::{CredentialKind, Digest, IdempotencyKey, WorkBudget, WorkspaceName};
use acyclic_auth::contract::CaveatKind;

fn request(writable: bool, generation: u8) -> CredentialGrantRequest {
    CredentialGrantRequest {
        kind: CredentialKind::Mount,
        workspace_id: WorkspaceId::from_bytes([7; 16]),
        workspace_name: WorkspaceName::new(
            "volume",
            &WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("name")
        .value,
        generation_id: GenerationId::new(Digest::from_bytes([generation; 32])),
        writable,
        expires_after_seconds: 60,
        expires_at_unix_seconds: i64::try_from(now() + 60).expect("deadline"),
        idempotency_key: IdempotencyKey::from_bytes([1; 16]),
    }
}

/// A sealed capability over `grants`, sealed to `audience`, expiring at `expires`.
fn custom(mounts: &Mounts, grants: GrantSet, audience: &str, expires: u64) -> Vec<u8> {
    mounts
        .signer
        .mint(
            vec![
                Caveat {
                    kind: CaveatKind::Grants(grants),
                },
                Caveat {
                    kind: CaveatKind::ExpiresAtUnixSeconds(expires),
                },
            ],
            ed25519_dalek::SigningKey::from_bytes(&[20; 32]),
            &mounts.trust,
            now(),
        )
        .expect("mint")
        .forward(audience, &[21; 32])
        .expect("seal")
        .to_vec()
}

fn filesystem(resource: &str, actions: &[&str]) -> GrantSet {
    GrantSet {
        grants: vec![Grant {
            kind: GrantKind::Filesystem(FilesystemGrant {
                grant: ResourceGrant {
                    resource: ResourcePattern {
                        name: resource.into(),
                        prefix: true,
                    },
                    actions: actions.iter().map(|action| (*action).to_owned()).collect(),
                },
                posix_uid: Some(OWNER.posix_uid),
                posix_gid: Some(OWNER.posix_gid),
            }),
        }],
    }
}

fn admit(capability: &[u8], request: &CredentialGrantRequest) -> Result<(), MountRefusal> {
    let expires = u64::try_from(request.expires_at_unix_seconds).expect("deadline");
    admit_mount(capability, request, now(), expires)
}

#[test]
fn a_minted_mount_capability_admits_exactly_its_request() {
    let mounts = Mounts::new();
    for writable in [false, true] {
        let request = request(writable, 1);
        assert_eq!(admit(&mounts.mint(&request), &request), Ok(()));
    }
    let read = request(false, 1);
    let write = request(true, 1);
    // Negative controls: write on a read-only request, a read-only
    // credential for a write, and another generation.
    assert_eq!(admit(&mounts.mint(&write), &read), Err(MountRefusal::Wider));
    assert_eq!(
        admit(&mounts.mint(&read), &write),
        Err(MountRefusal::Narrower)
    );
    assert_eq!(
        admit(&mounts.mint(&request(false, 2)), &read),
        Err(MountRefusal::Wider)
    );
    // Working now and refusing at the requested end does not establish that
    // the entire advertised interval is admitted. The original certificate
    // and a workload caveat can each impose an earlier end.
    let short = Mounts::with_expiry(now() + 30);
    assert_eq!(
        admit(&short.mint(&read), &read),
        Err(MountRefusal::Narrower)
    );
    let audience = mount_audience(read.workspace_id);
    let resource = mount_resource(read.workspace_id, read.generation_id);
    assert_eq!(
        admit(
            &custom(
                &mounts,
                filesystem(&resource, &["fs/read", "fs/list"]),
                &audience,
                now() + 30,
            ),
            &read,
        ),
        Err(MountRefusal::Narrower)
    );
}

#[test]
fn a_mount_capability_wider_or_unsealed_than_its_request_is_refused() {
    let mounts = Mounts::new();
    let read = request(false, 1);
    let audience = mount_audience(read.workspace_id);
    let resource = mount_resource(read.workspace_id, read.generation_id);
    let expires = u64::try_from(read.expires_at_unix_seconds).expect("deadline");
    let reads = ["fs/read", "fs/list"];
    // Control: the hand-built equivalent of a minted credential is admitted.
    assert_eq!(
        admit(
            &custom(&mounts, filesystem(&resource, &reads), &audience, expires),
            &read
        ),
        Ok(())
    );
    for extra in [
        filesystem(
            &mount_resource(read.workspace_id, request(false, 2).generation_id),
            &reads,
        ),
        filesystem(&format!("{resource}/child"), &["fs/write"]),
    ] {
        let mut grants = filesystem(&resource, &reads);
        grants.grants.extend(extra.grants);
        assert_eq!(
            admit(&custom(&mounts, grants, &audience, expires), &read),
            Err(MountRefusal::Wider)
        );
    }
    let mut point = filesystem(&resource, &reads);
    let GrantKind::Filesystem(grant) = &mut point.grants[0].kind else {
        panic!("filesystem fixture")
    };
    grant.grant.resource.prefix = false;
    assert_eq!(
        admit(&custom(&mounts, point, &audience, expires), &read),
        Err(MountRefusal::Narrower)
    );
    // Every generation of the volume.
    assert_eq!(
        admit(
            &custom(&mounts, filesystem(&audience, &reads), &audience, expires),
            &read
        ),
        Err(MountRefusal::Wider)
    );
    // Valid past the deadline.
    assert_eq!(
        admit(
            &custom(
                &mounts,
                filesystem(&resource, &reads),
                &audience,
                expires + 1
            ),
            &read
        ),
        Err(MountRefusal::Wider)
    );
    // Sealed to another volume.
    let mut other = read.workspace_id.into_bytes();
    other[0] ^= 1;
    let other = mount_audience(WorkspaceId::from_bytes(other));
    assert_eq!(
        admit(
            &custom(&mounts, filesystem(&resource, &reads), &other, expires),
            &read
        ),
        Err(MountRefusal::Narrower)
    );
    // A plain resource grant carries no POSIX owner.
    let plain = GrantSet {
        grants: vec![Grant {
            kind: GrantKind::Resource(ResourceGrant {
                resource: ResourcePattern {
                    name: resource.clone(),
                    prefix: true,
                },
                actions: reads.map(String::from).to_vec(),
            }),
        }],
    };
    assert_eq!(
        admit(&custom(&mounts, plain, &audience, expires), &read),
        Err(MountRefusal::NotFilesystem)
    );
    // Unsealed: the holder could append and re-scope it.
    let unsealed = mounts
        .signer
        .mint(
            vec![Caveat {
                kind: CaveatKind::Grants(filesystem(&resource, &reads)),
            }],
            ed25519_dalek::SigningKey::from_bytes(&[22; 32]),
            &mounts.trust,
            now(),
        )
        .expect("mint")
        .encode();
    assert_eq!(admit(&unsealed, &read), Err(MountRefusal::Unsealed));
    assert_eq!(
        admit(b"not a capability", &read),
        Err(MountRefusal::Malformed)
    );
}
