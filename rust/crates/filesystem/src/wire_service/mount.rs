//! The volume mount credential: one sealed acyclic.auth.v1 capability. Its
//! audience is the volume, `fs:/<volume>`; its one ceiling is a
//! [`FilesystemGrant`] on the volume pinned at the requested generation,
//! `fs:/<volume>~<generation>`, carrying the POSIX owner the mount presents.
//! Deployments mint it with [`mount_capability`]; the wire service admits
//! what an issuer returns with [`admit_mount`] before it reaches a client,
//! and the mount endpoint verifies it as any other capability.
use super::CredentialGrantRequest;
use crate::{GenerationId, WorkspaceId};
use acyclic_auth::{
    authority::{self, PolicyDecision, Request, Scope, Target},
    contract::{
        Block, Caveat, CaveatKind, FilesystemGrant, Grant, GrantKind, GrantSet, Limits,
        RecordAttributes, ResourceGrant, ResourcePattern, TipProof,
    },
    verify::{CertifiedSigner, Trust, VerifyError},
};
use ed25519_dalek::SigningKey;
use zeroize::Zeroizing;

/// The audience of `workspace`'s mount capability: the volume itself.
#[must_use]
pub fn mount_audience(workspace: WorkspaceId) -> String {
    format!("fs:/{}", uuid::Uuid::from_bytes(workspace.into_bytes()))
}

/// The resource a mount capability grants: the volume pinned at one
/// generation. Paths below it are `<resource>/<component>...`.
#[must_use]
pub fn mount_resource(workspace: WorkspaceId, generation: GenerationId) -> String {
    format!(
        "{}~{}",
        mount_audience(workspace),
        hex::encode(generation.to_bytes().as_slice())
    )
}

fn actions(writable: bool) -> Vec<String> {
    let mut actions = vec!["fs/read".to_owned(), "fs/list".to_owned()];
    if writable {
        actions.push("fs/write".to_owned());
    }
    actions
}

/// The POSIX owner a mount presents for every file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MountOwner {
    /// Owning user id.
    pub posix_uid: u32,
    /// Owning group id.
    pub posix_gid: u32,
}

/// Mint the sealed mount capability for `request`: a [`FilesystemGrant`] on
/// [`mount_resource`] owned by `owner`, with `fs/write` only
/// when writable, expiring at the request's deadline, sealed to
/// [`mount_audience`]. Two signatures; `next` and `seal` must be fresh.
///
/// # Errors
/// When `signer`'s ceiling does not cover the grant, its trust is not
/// current, the deadline is unrepresentable, or a key repeats.
pub fn mount_capability(
    signer: &CertifiedSigner,
    trust: &(impl Trust + ?Sized),
    request: &CredentialGrantRequest,
    owner: MountOwner,
    next: SigningKey,
    seal: &[u8; 32],
    now_unix_seconds: u64,
) -> Result<Zeroizing<Vec<u8>>, VerifyError> {
    let expires = u64::try_from(request.expires_at_unix_seconds).map_err(|_| VerifyError::Time)?;
    let grant = FilesystemGrant {
        grant: ResourceGrant {
            resource: ResourcePattern {
                name: mount_resource(request.workspace_id, request.generation_id),
                prefix: true,
            },
            actions: actions(request.writable),
        },
        posix_uid: Some(owner.posix_uid),
        posix_gid: Some(owner.posix_gid),
    };
    let minted = signer.mint(
        vec![
            Caveat {
                kind: CaveatKind::Grants(GrantSet {
                    grants: vec![Grant {
                        kind: GrantKind::Filesystem(grant),
                    }],
                }),
            },
            Caveat {
                kind: CaveatKind::ExpiresAtUnixSeconds(expires),
            },
        ],
        next,
        trust,
        now_unix_seconds,
    )?;
    minted.forward(&mount_audience(request.workspace_id), seal)
}

/// Why an issued mount credential is refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MountRefusal {
    /// Not a canonical, valid chain.
    #[error("mount credential is not a valid capability")]
    Malformed,
    /// Its holder could still append: it must be sealed.
    #[error("mount credential is not sealed")]
    Unsealed,
    /// Its tightest grants are not filesystem grants.
    #[error("mount credential does not carry a filesystem grant")]
    NotFilesystem,
    /// It does not admit the requested access at the requested generation.
    #[error("mount credential does not admit the requested scope")]
    Narrower,
    /// It admits more than requested: another generation, another
    /// audience, a write on a read-only request, or past the deadline.
    #[error("mount credential exceeds the requested scope")]
    Wider,
}

/// Admit an issuer's mount credential for `request` at `now`, valid until
/// `expires`: sealed, carrying a filesystem grant, admitting exactly the
/// requested access at the requested generation for audience
/// [`mount_audience`], and nothing at `expires`. Structural only: the mount
/// endpoint verifies signatures and current trust when it is presented.
///
/// # Errors
/// The first [`MountRefusal`] found.
pub fn admit_mount(
    capability: &[u8],
    request: &CredentialGrantRequest,
    now: u64,
    expires: u64,
) -> Result<(), MountRefusal> {
    let value = authority::decode_capability(capability).map_err(|_| MountRefusal::Malformed)?;
    if !matches!(value.proof, TipProof::Sealed(_)) {
        return Err(MountRefusal::Unsealed);
    }
    let blocks = value
        .blocks
        .iter()
        .map(|signed| authority::decode_block(&signed.payload))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| MountRefusal::Malformed)?;
    // Requests meet every ceiling; the tightest one alone admits them, so
    // every admitted access is admitted by a filesystem grant there.
    if !ceiling(&blocks).is_some_and(|set| {
        !set.grants.is_empty()
            && set
                .grants
                .iter()
                .all(|grant| matches!(grant.kind, GrantKind::Filesystem(_)))
    }) {
        return Err(MountRefusal::NotFilesystem);
    }
    let audience = mount_audience(request.workspace_id);
    let resource = mount_resource(request.workspace_id, request.generation_id);
    let mut other = request.workspace_id.into_bytes();
    other[0] ^= 1;
    let other = mount_audience(WorkspaceId::from_bytes(other));
    let permits = |action: &str, resource: &str, audience: &str, now: u64| {
        let attributes = RecordAttributes::default();
        let usage = Limits {
            rps: None,
            burst: None,
            bytes_per_s: None,
            channels: None,
            concurrency: None,
            bytes: None,
            spend_micros: None,
            tokens: None,
        };
        authority::permits(
            &blocks,
            &Request {
                target: Target::Resource { action, resource },
                audience,
                holder: None,
                now_unix_seconds: now,
                keys: &[],
                attributes: &attributes,
                usage: &usage,
                cpu_millis: 0,
                memory_bytes: 0,
            },
            PolicyDecision::Allow,
            Scope::Exact,
        )
        .is_ok()
    };
    if !permits("fs/read", &resource, &audience, now)
        || !permits("fs/list", &resource, &audience, now)
        || request.writable && !permits("fs/write", &resource, &audience, now)
    {
        return Err(MountRefusal::Narrower);
    }
    // The volume without a generation is refused only when the grant pins
    // one; with the requested one admitted, it pins exactly that one.
    if (!request.writable && permits("fs/write", &resource, &audience, now))
        || permits("fs/read", &audience, &audience, now)
        || permits("fs/read", &resource, &other, now)
        || permits("fs/read", &resource, &audience, expires)
    {
        return Err(MountRefusal::Wider);
    }
    Ok(())
}

/// The last grant set every request must meet: the last `Grants` caveat, else
/// the certified leaf's grants.
fn ceiling(blocks: &[Block]) -> Option<&GrantSet> {
    blocks
        .iter()
        .rev()
        .flat_map(|block| block.caveats.iter().rev())
        .find_map(|caveat| match &caveat.kind {
            CaveatKind::Grants(set) => Some(set),
            _ => None,
        })
        .or_else(|| {
            blocks
                .first()?
                .certificates
                .as_ref()?
                .keys
                .last()
                .map(|certificate| &certificate.key.grants)
        })
}

#[cfg(test)]
#[path = "mount_tests.rs"]
mod tests;
