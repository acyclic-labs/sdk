use crate::{Error, contract::*, valid_action, valid_resource};

fn require(valid: bool) -> Result<(), Error> {
    if valid { Ok(()) } else { Err(Error::Invalid) }
}

pub(crate) fn resource(value: &ResourcePattern) -> Result<(), Error> {
    require(valid_resource(&value.name))
}

pub(crate) fn grant(value: &ResourceGrant) -> Result<(), Error> {
    let product = value
        .resource
        .name
        .split_once(":/")
        .ok_or(Error::Invalid)?
        .0;
    require(
        !value.actions.is_empty()
            && value.actions.iter().all(|action| {
                valid_action(action) && action.split_once('/').is_some_and(|(p, _)| p == product)
            }),
    )
}

pub(crate) fn principal(value: &Principal) -> Result<(), Error> {
    require(
        value.kind != PrincipalKind::Unspecified
            && !value.id.is_empty()
            && value.lineage.iter().all(|id| !id.is_empty())
            && unique_attributes(&value.attributes),
    )
}

pub(crate) fn range(value: &KeyRange) -> Result<(), Error> {
    require(value.end.is_empty() || value.start < value.end)
}

pub(crate) fn port(value: &SocketPortRange) -> Result<(), Error> {
    require(value.first > 0 && value.first <= value.last && value.last <= 65535)
}

pub(crate) fn block(value: &Block) -> Result<(), Error> {
    require(value.next_public_key.len() == 32)
}

pub(crate) fn signed(value: &SignedBlock) -> Result<(), Error> {
    require(!value.payload.is_empty() && value.signature.len() == 64)
}

pub(crate) fn capability(value: &Capability) -> Result<(), Error> {
    if value.version != crate::VERSION {
        return Err(Error::UnsupportedVersion(value.version));
    }
    require(
        !value.blocks.is_empty()
            && match &value.proof {
                TipProof::Secret(bytes) => bytes.len() == 32,
                TipProof::Sealed(bytes) => bytes.len() == 64,
            },
    )
}

const MAX_UNIX_SECONDS: u64 = 253_402_300_799;
pub(crate) fn validity(v: &Validity) -> Result<(), Error> {
    require(
        v.not_before_unix_seconds < v.expires_at_unix_seconds
            && v.expires_at_unix_seconds <= MAX_UNIX_SECONDS
            && v.expires_at_unix_seconds - v.not_before_unix_seconds <= 30 * 86400,
    )
}
pub(crate) fn lineage(v: &LineageAppend) -> Result<(), Error> {
    require(!v.ids.is_empty() && v.ids.iter().all(|id| !id.is_empty()))
}
pub(crate) fn caveat(v: &Caveat) -> Result<(), Error> {
    match &v.kind {
        CaveatKind::ExpiresAtUnixSeconds(t) | CaveatKind::NotBeforeUnixSeconds(t) => {
            require(*t <= MAX_UNIX_SECONDS)
        }
        CaveatKind::Audience(r) => {
            resource(r)?;
            require(!r.prefix)
        }
        _ => Ok(()),
    }
}
pub(crate) fn birth(v: &Birth) -> Result<(), Error> {
    require(
        !v.account_id.is_empty()
            && !v.environment.is_empty()
            && v.genesis_public_key.len() == 32
            && v.issued_at_unix_seconds <= MAX_UNIX_SECONDS,
    )
}
pub(crate) fn birth_certificate(v: &BirthCertificate) -> Result<(), Error> {
    require(v.signature.len() == 64)
}
pub(crate) fn certified_key(v: &CertifiedKey) -> Result<(), Error> {
    require(
        !v.account_id.is_empty()
            && !v.key_id.is_empty()
            && v.public_key.len() == 32
            && v.parent_revocation_ids.iter().all(|id| id.len() == 32),
    )
}
pub(crate) fn key_certificate(v: &KeyCertificate) -> Result<(), Error> {
    require(v.signature.len() == 64)
}
pub(crate) fn certificates(v: &CertificateChain) -> Result<(), Error> {
    require(!v.keys.is_empty())?;
    for (i, cert) in v.keys.iter().enumerate() {
        require(
            cert.key.account_id == v.birth.birth.account_id
                && cert.key.parent_key_id.as_deref()
                    == i.checked_sub(1).map(|p| v.keys[p].key.key_id.as_str())
                && !v.keys[..i]
                    .iter()
                    .any(|old| old.key.key_id == cert.key.key_id),
        )?;
        if i > 0 {
            let parent = &v.keys[i - 1].key;
            require(
                principal_narrows(&parent.principal, &cert.key.principal)
                    && cert.key.validity.not_before_unix_seconds
                        >= parent.validity.not_before_unix_seconds
                    && cert.key.validity.expires_at_unix_seconds
                        <= parent.validity.expires_at_unix_seconds
                    && parent
                        .parent_revocation_ids
                        .iter()
                        .all(|id| cert.key.parent_revocation_ids.contains(id)),
            )?;
        }
    }
    Ok(())
}
pub(crate) fn revocation(v: &Revocation) -> Result<(), Error> {
    require(v.id.len() == 32 && v.expires_at_unix_seconds <= MAX_UNIX_SECONDS)
}
pub(crate) fn keyring_snapshot(v: &KeyringSnapshot) -> Result<(), Error> {
    require(
        v.previous_digest.len() == 32
            && v.root_public_key.len() == 32
            && v.issued_at_unix_seconds <= MAX_UNIX_SECONDS,
    )
}

pub(crate) fn signed_snapshot(v: &SignedKeyringSnapshot) -> Result<(), Error> {
    require(v.signature.len() == 64)
}

fn unique_attributes(values: &[Attribute]) -> bool {
    let mut names = std::collections::HashSet::with_capacity(values.len());
    values.iter().all(|value| names.insert(value.name.as_str()))
}
pub(crate) fn attributes(value: &RecordAttributes) -> Result<(), Error> {
    require(unique_attributes(value.as_slice()))
}
pub(crate) fn socket_host(value: &SocketHostRule) -> Result<(), Error> {
    require(crate::socket::valid_rule(&value.host))
}

fn principal_narrows(parent: &Principal, child: &Principal) -> bool {
    child.kind == parent.kind
        && child.id == parent.id
        && child.lineage.starts_with(&parent.lineage)
        && child
            .groups
            .iter()
            .all(|claim| parent.groups.contains(claim))
        && child.roles.iter().all(|claim| parent.roles.contains(claim))
        && child
            .attributes
            .iter()
            .all(|claim| parent.attributes.contains(claim))
}

pub(crate) fn audit(value: &AuditRecord) -> Result<(), Error> {
    require(value.decision != AuditDecision::Unspecified)
}
