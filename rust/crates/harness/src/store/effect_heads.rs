//! Complete effect-head observations at immutable canonical publication cuts.
//!
//! The certificate authenticates the entire compressed index root, including
//! absence. Nodes confer no authority; original effect events still undergo
//! their existing admission/atomic-index checks and core projection.

use super::{checkpoints, operations};
use crate::{
    EffectId, Error, Result,
    core::{Authority, AuthorityVerifier, Event},
};
use acyclic_stream::{StreamClient, StreamPath, StreamProvider};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

type Digest = [u8; 32];
const MAX_NODE_BYTES: u64 = 1_024;
const MAX_CERTIFICATE_BYTES: u64 = 4_096;
const PUBLICATION_READ_BYTES: u64 = 4 * acyclic_stream::MAX_COMMAND_BYTES as u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    pub(super) revision: u64,
    pub(super) position: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Certificate {
    authority: Authority,
    canonical_path: String,
    revision: u64,
    canonical_digest: Digest,
    root: Option<Digest>,
    proof: Digest,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Node {
    Leaf {
        effect: EffectId,
        head: Head,
    },
    Branch {
        bit: u8,
        left: Digest,
        right: Digest,
    },
}

struct Branch {
    hash: Digest,
    bit: u8,
    left: Digest,
    right: Digest,
}

struct Walk {
    path: Vec<Branch>,
    leaf: Option<(Digest, EffectId, Head)>,
}

struct Root {
    hash: Option<Digest>,
    consumed: u64,
}

pub(super) struct Selection {
    pub(super) head: Option<Head>,
    pub(super) consumed_bytes: u64,
}

fn prefix(authority: &Authority) -> Result<String> {
    let canonical = authority.stream_path()?;
    let (version, suffix) = canonical
        .strip_prefix("harness/")
        .and_then(|path| path.split_once('/'))
        .ok_or_else(|| Error::Invalid("effect root authority path is invalid".into()))?;
    Ok(format!("harness/{version}/effect-heads/{suffix}"))
}

fn certificate_path(authority: &Authority) -> Result<StreamPath> {
    Ok(StreamPath::new(format!("{}/cuts", prefix(authority)?))?)
}

fn node_path(authority: &Authority, hash: Digest) -> Result<StreamPath> {
    Ok(StreamPath::new(format!(
        "{}/nodes/{}",
        prefix(authority)?,
        blake3::Hash::from_bytes(hash).to_hex()
    ))?)
}

fn charge(used: &mut u64, amount: u64, maximum: u64) -> Result<()> {
    *used = used
        .checked_add(amount)
        .filter(|total| *total <= maximum)
        .ok_or_else(|| Error::Invalid("effect proof exceeds byte allowance".into()))?;
    Ok(())
}

async fn root_at<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    revision: u64,
    maximum_bytes: u64,
) -> Result<Root> {
    verifier.verify_audience(authority)?;
    if revision == 0 {
        return Ok(Root {
            hash: None,
            consumed: 0,
        });
    }
    let path = certificate_path(authority)?;
    let record = operations::one_record(client, &path, revision - 1)
        .await?
        .ok_or_else(|| {
            Error::Unsupported(
                "effect history requires its original complete cut certificate".into(),
            )
        })?;
    let mut used = 0;
    charge(&mut used, record.value.len() as u64, maximum_bytes)?;
    if record.value.len() as u64 > MAX_CERTIFICATE_BYTES {
        return Err(Error::Storage(
            "effect cut certificate exceeds its encoded bound".into(),
        ));
    }
    let certificate: Certificate = crate::executor::decode_json(&record.value)?;
    if &certificate.authority != authority
        || certificate.canonical_path != authority.stream_path()?
        || certificate.revision != revision
        || certificate.proof
            != verifier.effect_history_root_proof(
                revision,
                certificate.canonical_digest,
                certificate.root,
            )?
    {
        return Err(Error::Unauthorized(
            "effect cut certificate proof differs".into(),
        ));
    }
    let canonical = operations::one_record(
        client,
        &StreamPath::new(authority.stream_path()?)?,
        revision - 1,
    )
    .await?
    .ok_or_else(|| Error::Storage("effect cut canonical anchor is missing".into()))?;
    charge(&mut used, canonical.value.len() as u64, maximum_bytes)?;
    if canonical.commit_id != record.commit_id
        || *blake3::hash(&canonical.value).as_bytes() != certificate.canonical_digest
    {
        return Err(Error::Storage(
            "effect cut differs from its original atomic publication".into(),
        ));
    }
    Ok(Root {
        hash: certificate.root,
        consumed: used,
    })
}

fn right(effect: EffectId, bit: u8) -> bool {
    effect
        .into_bytes()
        .get(usize::from(bit / 8))
        .is_some_and(|byte| byte & (0x80 >> (bit % 8)) != 0)
}

async fn load_node<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    hash: Digest,
    used: &mut u64,
    maximum: u64,
) -> Result<Node> {
    let path = node_path(authority, hash)?;
    let record = operations::one_record(client, &path, 0)
        .await?
        .ok_or_else(|| Error::Storage("authenticated effect index node is missing".into()))?;
    charge(used, record.value.len() as u64, maximum)?;
    if record.value.len() as u64 > MAX_NODE_BYTES
        || client.stream(path.as_str())?.tail().await? != 1
        || *blake3::hash(&record.value).as_bytes() != hash
    {
        return Err(Error::Storage(
            "authenticated effect index node differs".into(),
        ));
    }
    crate::executor::decode_json(&record.value)
}

async fn walk<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    effect: EffectId,
    root: Option<Digest>,
    used: &mut u64,
    maximum: u64,
) -> Result<Walk> {
    let mut path: Vec<Branch> = Vec::new();
    let mut next = root;
    while let Some(hash) = next {
        match load_node(client, authority, hash, used, maximum).await? {
            Node::Leaf {
                effect: selected,
                head,
            } => {
                if head.revision == 0
                    || path
                        .iter()
                        .any(|branch| right(selected, branch.bit) != right(effect, branch.bit))
                {
                    return Err(Error::Storage(
                        "effect leaf contradicts authenticated branch path".into(),
                    ));
                }
                return Ok(Walk {
                    path,
                    leaf: Some((hash, selected, head)),
                });
            }
            Node::Branch {
                bit,
                left,
                right: right_hash,
            } => {
                if bit >= 128 || path.last().is_some_and(|parent| parent.bit >= bit) {
                    return Err(Error::Storage(
                        "effect index branch depth is invalid".into(),
                    ));
                }
                next = Some(if right(effect, bit) { right_hash } else { left });
                path.push(Branch {
                    hash,
                    bit,
                    left,
                    right: right_hash,
                });
            }
        }
    }
    Ok(Walk { path, leaf: None })
}

async fn stage_node<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    node: Node,
) -> Result<Digest> {
    let bytes = crate::contract::canonical_json_bytes(&node)?;
    if bytes.len() as u64 > MAX_NODE_BYTES {
        return Err(Error::Invalid(
            "effect index node exceeds publication bound".into(),
        ));
    }
    let hash = *blake3::hash(&bytes).as_bytes();
    checkpoints::stage_immutable_record(client, node_path(authority, hash)?, Bytes::from(bytes))
        .await?;
    Ok(hash)
}

async fn update<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    effect: EffectId,
    head: Head,
    mut observed: Walk,
) -> Result<Digest> {
    match observed.leaf {
        Some((_, selected, _)) if selected == effect && head.position == 0 => {
            return Err(Error::Conflict(
                "effect identity has an original published head".into(),
            ));
        }
        Some((_, selected, previous)) if selected == effect => {
            if previous.position.checked_add(1) != Some(head.position)
                || previous.revision >= head.revision
            {
                return Err(Error::Storage(
                    "effect head update skips its original transition".into(),
                ));
            }
        }
        _ if head.position != 0 => {
            return Err(Error::Storage(
                "effect head update lacks an authenticated predecessor".into(),
            ));
        }
        _ => {}
    }
    let mut replacement = stage_node(client, authority, Node::Leaf { effect, head }).await?;
    if let Some((old_hash, old_effect, _)) = observed.leaf
        && old_effect != effect
    {
        let bit = (0_u8..128)
            .find(|bit| right(effect, *bit) != right(old_effect, *bit))
            .ok_or_else(|| Error::Storage("effect index key distinction is invalid".into()))?;
        let split = observed.path.partition_point(|branch| branch.bit < bit);
        let previous = observed
            .path
            .get(split)
            .map_or(old_hash, |branch| branch.hash);
        observed.path.truncate(split);
        let (left, right_hash) = if right(effect, bit) {
            (previous, replacement)
        } else {
            (replacement, previous)
        };
        replacement = stage_node(
            client,
            authority,
            Node::Branch {
                bit,
                left,
                right: right_hash,
            },
        )
        .await?;
    }
    for branch in observed.path.into_iter().rev() {
        let (left, right_hash) = if right(effect, branch.bit) {
            (branch.left, replacement)
        } else {
            (replacement, branch.right)
        };
        replacement = stage_node(
            client,
            authority,
            Node::Branch {
                bit: branch.bit,
                left,
                right: right_hash,
            },
        )
        .await?;
    }
    Ok(replacement)
}

pub(super) async fn publication<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    event: &Event,
    head: Option<(EffectId, Head)>,
    publication: &mut operations::IndexedPublication,
) -> Result<()> {
    verifier.verify_event(event)?;
    let previous_revision = event
        .revision
        .checked_sub(1)
        .ok_or_else(|| Error::Invalid("effect cut publication revision must be positive".into()))?;
    if publication.path.as_str() != authority.stream_path()?
        || publication.expected_tail != previous_revision
        || publication.operation_id != event.operation_id
        || publication.bytes.as_ref()
            != crate::wire_codec::encode_event(authority, event)?.as_slice()
    {
        return Err(Error::Storage(
            "effect cut publication differs from its admitted canonical event".into(),
        ));
    }
    let mut previous = root_at(
        client,
        authority,
        verifier,
        previous_revision,
        PUBLICATION_READ_BYTES,
    )
    .await?;
    if let Some((effect, head)) = head {
        if head.revision != event.revision
            || crate::core::event_effect_id(&event.payload) != Some(effect)
        {
            return Err(Error::Storage(
                "effect cut update differs from admitted event".into(),
            ));
        }
        let observed = walk(
            client,
            authority,
            effect,
            previous.hash,
            &mut previous.consumed,
            PUBLICATION_READ_BYTES,
        )
        .await?;
        previous.hash = Some(update(client, authority, effect, head, observed).await?);
    } else if crate::core::event_effect_id(&event.payload).is_some() {
        return Err(Error::Storage(
            "effect cut publication omits its effect update".into(),
        ));
    }
    let canonical_digest = *blake3::hash(&publication.bytes).as_bytes();
    let certificate = Certificate {
        authority: authority.clone(),
        canonical_path: authority.stream_path()?,
        revision: event.revision,
        canonical_digest,
        root: previous.hash,
        proof: verifier.effect_history_root_proof(
            event.revision,
            canonical_digest,
            previous.hash,
        )?,
    };
    let bytes = crate::contract::canonical_json_bytes(&certificate)?;
    if bytes.len() as u64 > MAX_CERTIFICATE_BYTES {
        return Err(Error::Invalid(
            "effect cut certificate exceeds publication bound".into(),
        ));
    }
    publication.add_derived_index(
        certificate_path(authority)?,
        previous_revision,
        Bytes::from(bytes),
    );
    Ok(())
}

pub(super) async fn select<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    effect: EffectId,
    revision: u64,
    maximum_bytes: u64,
) -> Result<Selection> {
    let mut root = root_at(client, authority, verifier, revision, maximum_bytes).await?;
    let observed = walk(
        client,
        authority,
        effect,
        root.hash,
        &mut root.consumed,
        maximum_bytes,
    )
    .await?;
    let head = observed
        .leaf
        .and_then(|(_, selected, head)| (selected == effect).then_some(head));
    if head.is_some_and(|head| head.revision > revision) {
        return Err(Error::Storage(
            "effect head exceeds authenticated original cut".into(),
        ));
    }
    Ok(Selection {
        head,
        consumed_bytes: root.consumed,
    })
}

#[cfg(test)]
mod tests;
