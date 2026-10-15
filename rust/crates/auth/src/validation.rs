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
            && value.lineage.iter().all(|id| !id.is_empty()),
    )
}

pub(crate) fn range(value: &KeyRange) -> Result<(), Error> {
    require(value.end.is_empty() || value.start < value.end)
}

pub(crate) fn port(value: &SocketPortRange) -> Result<(), Error> {
    require(value.first > 0 && value.first <= value.last && value.last <= 65535)
}

pub(crate) fn block(value: &Block) -> Result<(), Error> {
    require(
        value.next_public_key.len() == 32
            && value
                .account_bearer
                .as_ref()
                .is_none_or(|bearer| !bearer.is_empty()),
    )
}

pub(crate) fn signed(value: &SignedBlock) -> Result<(), Error> {
    require(!value.payload.is_empty() && value.signature.len() == 64)
}

pub(crate) fn capability(value: &Capability) -> Result<(), Error> {
    require(
        !value.blocks.is_empty()
            && match &value.proof {
                TipProof::Secret(bytes) => bytes.len() == 32,
                TipProof::Sealed(bytes) => bytes.len() == 64,
            },
    )
}
