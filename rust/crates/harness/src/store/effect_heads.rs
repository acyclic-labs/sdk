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
const HEADS: &str = "harness/v2/effect-heads/";
const MAX_NODE_BYTES: u64 = 1_024;
const MAX_CERTIFICATE_BYTES: u64 = 4_096;
const PUBLICATION_READ_BYTES: u64 = 4 * acyclic_stream::MAX_COMMAND_BYTES as u64;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Node {
    Leaf(EffectId, (u64, u64)),
    Branch(u8, Digest, Digest),
}

fn node_path_with_work(
    authority: &Authority,
    hash: Digest,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamPath, u64)> {
    let hex = blake3::Hash::from_bytes(hash).to_hex();
    path(
        authority,
        HEADS,
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
        let held = record
            .as_ref()
            .map_or(0, |record| record_bytes(record, quota.work()));
        quota.adopt(held)?;
        Ok(record)
    }

    pub(super) async fn root_at(
        &self,
        verifier: &AuthorityVerifier,
        revision: u64,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Option<Digest>, u64)> {
        let authority = self.authority;
        verifier.verify_audience(authority, &mut None)?;
        if revision == 0 {
            return Ok((None, 0));
        }
        let mut quota = CodecQuota::from_retained(work, 0);
        let stream = path(authority, HEADS, Some((&"cuts", 4)), quota.work())?;
        let record = self
            .record(stream, revision - 1, &mut quota)
            .await?
            .ok_or_else(|| Error::Unsupported("complete effect cut is missing".into()))?;
        let mut used = 0;
        charge(&mut used, record.value.len() as u64, maximum)?;
        let certificate: (Option<Digest>, Digest) = operations::decode_index(
            &record.value,
            MAX_CERTIFICATE_BYTES,
            &["tuple", "array"],
            |_, _| Ok(0),
            |_| (0, 0),
            quota.work(),
        )?;
        let source = path(authority, "harness/v2/", None, quota.work())?;
        let canonical = self
            .record(source, revision - 1, &mut quota)
            .await?
            .ok_or_else(|| Error::Storage("effect cut anchor is missing".into()))?;
        charge(&mut used, canonical.value.len() as u64, maximum)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(canonical.value.len() as u64)?;
        }
        if canonical.commit_id != record.commit_id {
            return Err(Error::Storage("effect cut atomic anchor differs".into()));
        }
        if certificate.1
            != verifier.effect_history_root_proof_with_work(
                revision,
                *blake3::hash(&canonical.value).as_bytes(),
                certificate.0,
                quota.work(),
            )?
        {
            return Err(Error::Unauthorized("effect cut proof differs".into()));
        }
        Ok((certificate.0, used))
    }
    async fn load_node(
        &self,
        hash: Digest,
        used: &mut u64,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Node> {
        let mut quota = CodecQuota::from_retained(work, 0);
        let stream = node_path_with_work(self.authority, hash, quota.work())?;
        quota.adopt(stream.1)?;
        let tail = operations::tail_with_work(self.client, &stream.0, quota.work()).await?;
        quota.transfer();
        let record = self
            .record(stream, 0, &mut quota)
            .await?
            .ok_or_else(|| Error::Storage("effect node is missing".into()))?;
        charge(used, record.value.len() as u64, maximum)?;
        if tail != 1 {
            return Err(Error::Storage("effect node is not immutable".into()));
        }
        let node = operations::decode_index(
            &record.value,
            MAX_NODE_BYTES,
            &[
                "Node", "Leaf", "Branch", "leaf", "branch", "array", "tuple", "u8", "u64",
                "EffectId",
            ],
            |_, _| Ok(0),
            |_| (0, 0),
            quota.work(),
        )?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(record.value.len() as u64)?;
        }
        if *blake3::hash(&record.value).as_bytes() != hash {
            return Err(Error::Storage("effect node digest differs".into()));
        }
        Ok(node)
    }
    pub(super) async fn walk(
        &self,
        effect: EffectId,
        mut root: Option<Digest>,
        used: &mut u64,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(
        Vec<(Digest, u8, Digest)>,
        Option<(Digest, EffectId, (u64, u64))>,
    )> {
        let mut quota = CodecQuota::from_retained(work, 0);
        let mut stream = Vec::new();
        while let Some(hash) = root {
            match self.load_node(hash, used, maximum, quota.work()).await? {
                Node::Leaf(selected, head) => {
                    if head.0 == 0
                        || stream
                            .iter()
                            .any(|(_, bit, _)| right(selected, *bit) != right(effect, *bit))
                    {
                        return Err(Error::Storage("effect leaf contradicts its stream".into()));
                    }
                    quota.transfer();
                    return Ok((stream, Some((hash, selected, head))));
                }
                Node::Branch(bit, left, other) => {
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
        head: (u64, u64),
        observed: (
            Vec<(Digest, u8, Digest)>,
            Option<(Digest, EffectId, (u64, u64))>,
        ),
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Digest> {
        let held =
            observed.0.capacity() as u64 * std::mem::size_of::<(Digest, u8, Digest)>() as u64;
        let mut quota = CodecQuota::from_retained(work, held);
        let (mut stream, leaf) = observed;
        let mut replacement = self
            .stage_node(Node::Leaf(effect, head), quota.work())
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
                .stage_node(Node::Branch(bit, left, right_hash), quota.work())
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
                .stage_node(Node::Branch(*bit, left, right_hash), quota.work())
                .await?;
        }
        Ok(replacement)
    }
}
pub(super) async fn publication<P: StreamProvider>(
    client: &Arc<P>,
    reducer: &crate::core::Reducer,
    event: &Event,
    publication: &mut operations::IndexedPublication,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let authority = reducer.authority();
    let verifier = reducer.event_verifier();
    let tree = Tree { client, authority };
    let previous = reducer.revision();
    let mut quota = CodecQuota::from_retained(work, 0);
    let (mut root, mut used) = tree
        .root_at(verifier, previous, PUBLICATION_READ_BYTES, quota.work())
        .await?;
    if let Some(effect) = crate::core::event_effect_id(&event.payload) {
        let observed = tree
            .walk(
                effect,
                root,
                &mut used,
                PUBLICATION_READ_BYTES,
                quota.work(),
            )
            .await?;
        quota.adopt(
            observed.0.capacity() as u64 * std::mem::size_of::<(Digest, u8, Digest)>() as u64,
        )?;
        let position = if matches!(
            event.payload,
            crate::core::EventPayload::EffectPlanned { .. }
        ) {
            if observed.1.is_some_and(|(_, id, _)| id == effect) {
                return Err(Error::Conflict(
                    "effect identity is already published".into(),
                ));
            }
            0
        } else {
            let prior = observed
                .1
                .filter(|(_, id, head)| *id == effect && head.0 <= previous)
                .ok_or_else(|| Error::Storage("effect predecessor is missing".into()))?
                .2;
            prior
                .1
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("effect position overflows".into()))?
        };
        quota.transfer();
        root = Some(
            tree.update(effect, (event.revision, position), observed, quota.work())
                .await?,
        );
        let stream = path(
            authority,
            "harness/v2/effect-transitions/",
            Some((&effect, 36)),
            quota.work(),
        )?;
        quota.adopt(stream.1)?;
        let bytes = publication.set_effect_position_with_work(position, quota.work())?;
        quota.transfer();
        publication.push_index(stream, position, bytes, 0, quota.work())?;
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
    let stream = path(authority, HEADS, Some((&"cuts", 4)), quota.work())?;
    let encoded = encode(&(root, proof), MAX_CERTIFICATE_BYTES, quota.work());
    publication.add_derived_index_with_work(stream, previous, encoded, quota.work())
}
#[cfg(test)]
mod tests;
