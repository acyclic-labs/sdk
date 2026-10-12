//! Complete effect-head observations at immutable canonical publication cuts.
use super::history::canonical_bytes_with_work as encode;
use operations::one_record as read;
use operations::publication_path as path;
use operations::sdk_record_retained_bytes as record_bytes;

use super::{checkpoints, history::HistoryIo, operations};
use crate::wire_codec::CodecQuota;
use crate::{
    EffectId, Error, Result,
    core::{Authority, AuthorityVerifier, Event},
};
use acyclic_stream::{StreamPath, StreamProvider};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
    revision: u64,
    canonical_digest: Digest,
    root: Option<Digest>,
    proof: Digest,
}

fn decode_certificate_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Certificate> {
    operations::decode_index(
        bytes,
        MAX_CERTIFICATE_BYTES,
        &[
            "Certificate",
            "revision",
            "canonical_digest",
            "root",
            "proof",
        ],
        |_, _| Ok(0),
        |_| (0, 0),
        work,
    )
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
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

fn decode_node_with_work(bytes: &[u8], work: &mut Option<&mut dyn HistoryIo>) -> Result<Node> {
    operations::decode_index(
        bytes,
        MAX_NODE_BYTES,
        &[
            "Node", "Leaf", "Branch", "Head", "kind", "leaf", "branch", "effect", "head",
            "revision", "position", "bit", "left", "right",
        ],
        |_, _| Ok(0),
        |_| (0, 0),
        work,
    )
}

fn certificate_path_with_work(
    authority: &Authority,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamPath, u64)> {
    path(
        authority,
        "harness/v2/effect-heads/",
        Some((&"cuts", 4)),
        work,
    )
}
fn node_path_with_work(
    authority: &Authority,
    hash: Digest,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamPath, u64)> {
    let hex = blake3::Hash::from_bytes(hash).to_hex();
    path(
        authority,
        "harness/v2/effect-heads/",
        Some((&format_args!("nodes/{hex}"), 70)),
        work,
    )
}
pub(super) fn charge(used: &mut u64, bytes: u64, maximum: u64) -> Result<()> {
    *used = used
        .checked_add(bytes)
        .filter(|total| *total <= maximum)
        .ok_or_else(|| Error::Invalid("effect proof exceeds byte allowance".into()))?;
    Ok(())
}
fn right(effect: EffectId, bit: u8) -> bool {
    effect.into_bytes()[usize::from(bit / 8)] & (0x80 >> (bit % 8)) != 0
}

pub(super) struct Tree<'a, P> {
    pub(super) client: &'a Arc<P>,
    pub(super) authority: &'a Authority,
}
impl<P: StreamProvider> Tree<'_, P> {
    pub(super) async fn record(
        &self,
        stream: (StreamPath, u64),
        from: u64,
        quota: &mut CodecQuota<'_, '_>,
    ) -> Result<Option<operations::ObservedRecord>> {
        let (stream, held) = stream;
        quota.adopt(held)?;
        let received = read(self.client, &stream, from, quota.work()).await;
        drop(stream);
        quota.relinquish(held)?;
        let record = received?;
        if let Some(record) = &record {
            let held = record_bytes(record, quota.work());
            quota.adopt(held)?;
        }
        Ok(record)
    }

    async fn root_at(
        &self,
        verifier: &AuthorityVerifier,
        revision: u64,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Option<Digest>, u64)> {
        let (client, authority) = (self.client, self.authority);
        verifier.verify_audience(authority, &mut None)?;
        if revision == 0 {
            return Ok((None, 0));
        }
        let mut quota = CodecQuota::from_retained(work, 0);
        let stream = certificate_path_with_work(authority, quota.work())?;
        let record = self
            .record(stream, revision - 1, &mut quota)
            .await?
            .ok_or_else(|| Error::Unsupported("complete effect cut is missing".into()))?;
        let mut used = 0;
        charge(&mut used, record.value.len() as u64, maximum)?;
        if record.value.len() as u64 > MAX_CERTIFICATE_BYTES {
            return Err(Error::Storage(
                "effect certificate exceeds byte bound".into(),
            ));
        }
        let certificate = decode_certificate_with_work(&record.value, quota.work())?;
        let source = path(authority, "harness/v2/", None, quota.work())?;
        if certificate.revision != revision
            || certificate.proof
                != verifier.effect_history_root_proof_with_work(
                    revision,
                    certificate.canonical_digest,
                    certificate.root,
                    quota.work(),
                )?
        {
            return Err(Error::Unauthorized("effect cut proof differs".into()));
        }
        let canonical = self
            .record(source, revision - 1, &mut quota)
            .await?
            .ok_or_else(|| Error::Storage("effect cut anchor is missing".into()))?;
        charge(&mut used, canonical.value.len() as u64, maximum)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(canonical.value.len() as u64)?;
        }
        if canonical.commit_id != record.commit_id
            || *blake3::hash(&canonical.value).as_bytes() != certificate.canonical_digest
        {
            return Err(Error::Storage("effect cut atomic anchor differs".into()));
        }
        Ok((certificate.root, used))
    }
    async fn load_node(
        &self,
        hash: Digest,
        used: &mut u64,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Node> {
        let (client, authority) = (self.client, self.authority);
        let mut quota = CodecQuota::from_retained(work, 0);
        let (stream, held) = node_path_with_work(authority, hash, quota.work())?;
        quota.adopt(held)?;
        let record = read(client, &stream, 0, quota.work())
            .await?
            .ok_or_else(|| Error::Storage("effect node is missing".into()))?;
        let held = record_bytes(&record, quota.work());
        quota.adopt(held)?;
        charge(used, record.value.len() as u64, maximum)?;
        if record.value.len() as u64 > MAX_NODE_BYTES
            || operations::tail_with_work(client, &stream, quota.work()).await? != 1
        {
            return Err(Error::Storage("effect node is not immutable".into()));
        }
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(record.value.len() as u64)?;
        }
        if *blake3::hash(&record.value).as_bytes() != hash {
            return Err(Error::Storage("effect node digest differs".into()));
        }
        decode_node_with_work(&record.value, quota.work())
    }
    async fn walk(
        &self,
        effect: EffectId,
        mut root: Option<Digest>,
        used: &mut u64,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Vec<(Digest, u8, Digest)>, Option<(Digest, EffectId, Head)>)> {
        let (client, authority) = (self.client, self.authority);
        let mut quota = CodecQuota::from_retained(work, 0);
        let mut stream = Vec::new();
        while let Some(hash) = root {
            match self.load_node(hash, used, maximum, quota.work()).await? {
                Node::Leaf {
                    effect: selected,
                    head,
                } => {
                    if head.revision == 0
                        || stream
                            .iter()
                            .any(|(_, bit, _)| right(selected, *bit) != right(effect, *bit))
                    {
                        return Err(Error::Storage("effect leaf contradicts its stream".into()));
                    }
                    quota.transfer();
                    return Ok((stream, Some((hash, selected, head))));
                }
                Node::Branch {
                    bit,
                    left,
                    right: other,
                } => {
                    if bit >= 128
                        || stream
                            .last()
                            .is_some_and(|(_, previous, _)| *previous >= bit)
                    {
                        return Err(Error::Storage("effect branch depth is invalid".into()));
                    }
                    let (next, sibling) = if right(effect, bit) {
                        (other, left)
                    } else {
                        (left, other)
                    };
                    if let Some(io) = quota.work().as_deref_mut() {
                        let old = stream.capacity();
                        super::history::ensure_slot(&mut stream, 128, io)?;
                        quota.adopt(
                            ((stream.capacity() - old)
                                * std::mem::size_of::<(Digest, u8, Digest)>())
                                as u64,
                        )?;
                    }
                    stream.push((hash, bit, sibling));
                    root = Some(next);
                }
            }
        }
        quota.transfer();
        Ok((stream, None))
    }
    async fn stage_node(
        &self,
        node: Node,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Digest> {
        let (client, authority) = (self.client, self.authority);
        let mut quota = CodecQuota::from_retained(work, 0);
        let bytes = encode(&node, MAX_NODE_BYTES, quota.work())?;
        quota.adopt(bytes.capacity() as u64)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(bytes.len() as u64)?;
        }
        let hash = *blake3::hash(&bytes).as_bytes();
        let stream = node_path_with_work(authority, hash, quota.work())?;
        quota.transfer();
        checkpoints::stage_immutable_vec_with_work(client, stream, Ok(bytes), quota.work()).await?;
        Ok(hash)
    }
    async fn update(
        &self,
        effect: EffectId,
        head: Head,
        observed: (Vec<(Digest, u8, Digest)>, Option<(Digest, EffectId, Head)>),
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Digest> {
        let (client, authority) = (self.client, self.authority);
        let held =
            observed.0.capacity() as u64 * std::mem::size_of::<(Digest, u8, Digest)>() as u64;
        let mut quota = CodecQuota::from_retained(work, held);
        let (mut stream, leaf) = observed;
        match leaf {
            Some((_, selected, _)) if selected == effect && head.position == 0 => {
                return Err(Error::Conflict(
                    "effect identity is already published".into(),
                ));
            }
            Some((_, selected, previous)) if selected == effect => {
                if previous.position.checked_add(1) != Some(head.position)
                    || previous.revision >= head.revision
                {
                    return Err(Error::Storage("effect update skips its predecessor".into()));
                }
            }
            _ if head.position != 0 => {
                return Err(Error::Storage("effect predecessor is missing".into()));
            }
            _ => {}
        }
        let mut replacement = self
            .stage_node(Node::Leaf { effect, head }, quota.work())
            .await?;
        if let Some((old_hash, old_effect, _)) = leaf
            && old_effect != effect
        {
            let bit = (0..128)
                .find(|bit| right(effect, *bit) != right(old_effect, *bit))
                .ok_or_else(|| Error::Storage("effect keys are indistinguishable".into()))?;
            let split = stream.partition_point(|(_, previous, _)| *previous < bit);
            let previous = stream.get(split).map_or(old_hash, |(hash, _, _)| *hash);
            stream.truncate(split);
            let (left, right_hash) = if right(effect, bit) {
                (previous, replacement)
            } else {
                (replacement, previous)
            };
            replacement = self
                .stage_node(
                    Node::Branch {
                        bit,
                        left,
                        right: right_hash,
                    },
                    quota.work(),
                )
                .await?;
        }
        // Keep the stream allocation alive until its quota is released.
        for (_, bit, sibling) in stream.iter().rev() {
            let (left, right_hash) = if right(effect, *bit) {
                (*sibling, replacement)
            } else {
                (replacement, *sibling)
            };
            replacement = self
                .stage_node(
                    Node::Branch {
                        bit: *bit,
                        left,
                        right: right_hash,
                    },
                    quota.work(),
                )
                .await?;
        }
        Ok(replacement)
    }
}
pub(super) async fn publication_with_work<P: StreamProvider>(
    client: &Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    event: &Event,
    head: Option<(EffectId, Head)>,
    publication: &mut operations::IndexedPublication,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let tree = Tree { client, authority };
    verifier.verify_event(event, &mut None)?;
    let previous = event
        .revision
        .checked_sub(1)
        .ok_or_else(|| Error::Invalid("effect revision must be positive".into()))?;
    let mut quota = CodecQuota::from_retained(work, 0);
    let (mut root, mut used) = tree
        .root_at(verifier, previous, PUBLICATION_READ_BYTES, quota.work())
        .await?;
    if let Some((effect, head)) = head {
        if head.revision != event.revision
            || crate::core::event_effect_id(&event.payload) != Some(effect)
        {
            return Err(Error::Storage("effect update differs from event".into()));
        }
        let observed = tree
            .walk(
                effect,
                root,
                &mut used,
                PUBLICATION_READ_BYTES,
                quota.work(),
            )
            .await?;
        root = Some(tree.update(effect, head, observed, quota.work()).await?);
    } else if crate::core::event_effect_id(&event.payload).is_some() {
        return Err(Error::Storage("effect update is omitted".into()));
    }
    if let Some(io) = quota.work().as_deref_mut() {
        io.hashed_bytes(publication.bytes.len() as u64)?;
    }
    let canonical_digest = *blake3::hash(&publication.bytes).as_bytes();
    let proof = verifier.effect_history_root_proof_with_work(
        event.revision,
        canonical_digest,
        root,
        quota.work(),
    )?;
    let certificate = Certificate {
        revision: event.revision,
        canonical_digest,
        root,
        proof,
    };
    let stream = certificate_path_with_work(authority, quota.work())?;
    let encoded = encode(&certificate, MAX_CERTIFICATE_BYTES, quota.work());
    publication.add_derived_index_with_work(stream, previous, encoded, quota.work())
}
pub(super) async fn select_with_work<P: StreamProvider>(
    client: &Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    effect: EffectId,
    revision: u64,
    maximum: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(Option<Head>, u64)> {
    let tree = Tree { client, authority };
    let mut quota = CodecQuota::from_retained(work, 0);
    let (root, mut used) = tree
        .root_at(verifier, revision, maximum, quota.work())
        .await?;
    let observed = tree
        .walk(effect, root, &mut used, maximum, quota.work())
        .await?;
    quota
        .adopt(observed.0.capacity() as u64 * std::mem::size_of::<(Digest, u8, Digest)>() as u64)?;
    let head = observed
        .1
        .and_then(|(_, selected, head)| (selected == effect).then_some(head));
    if head.is_some_and(|head| head.revision > revision) {
        return Err(Error::Storage(
            "effect head exceeds the original cut".into(),
        ));
    }
    Ok((head, used))
}
#[cfg(test)]
mod tests;
