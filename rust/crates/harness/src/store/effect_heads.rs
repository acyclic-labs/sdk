//! Complete effect-head observations at immutable canonical publication cuts.

use super::{checkpoints, operations, history::HistoryIo};
use crate::{
    EffectId, Error, Result,
    core::{Authority, AuthorityVerifier, Event},
};
use acyclic_stream::{StreamClient, StreamPath, StreamProvider};
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

fn decode_certificate_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Certificate> {
    if work.is_some() {
        operations::validate_canonical_json_with_work(bytes, MAX_CERTIFICATE_BYTES, work)?;
    }
    let Some(observer) = work.as_deref_mut() else { return crate::executor::decode_json(bytes); };
    let mut reservation = 0;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["Certificate", "Authority", "AggregateKind", "authority", "canonical_path",
         "revision", "canonical_digest", "root", "proof", "kind", "id", "agent",
         "conversation", "session", "turn", "task"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut observed_failure = false;
    let native = operations::original_native_decode_mode(observer);
    let decoded = operations::decode_preclaimed_json_with_mode::<Certificate, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes: MAX_CERTIFICATE_BYTES,
            maximum_items: MAX_CERTIFICATE_BYTES,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        crate::wire::ErrorCode::Storage,
        native,
        crate::contract::JsonValueSchema::None,
        &mut |charge| {
            let charged = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } = charge {
                reservation = maximum_owned_bytes.checked_add(diagnostic_bytes)
                    .ok_or_else(|| Error::Invalid("effect certificate reservation overflows".into()))?;
                observer.retain_owned(reservation)
            } else { observer.codec_charge(charge) };
            if charged.is_err() { observed_failure = true; }
            charged
        },
    );
    let decoded = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal content is invalid: {message}")),
                other => other,
            }
        }
    });
    let decoded = operations::retain_original_decode_error(decoded, &mut reservation, observer);
    let certificate = match decoded {
        Ok(certificate) => certificate,
        Err(error) => {
            observer.release_owned(reservation)?;
            return Err(error);
        }
    };
    let owned = (certificate.authority.id.capacity() + certificate.canonical_path.capacity()) as u64;
    observer.release_owned(reservation - owned)?;
    if let Err(error) = observer.copy_bytes((certificate.authority.id.len() + certificate.canonical_path.len()) as u64) {
        drop(certificate);
        let _ = observer.release_owned(owned);
        return Err(error);
    }
    Ok(certificate)
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

fn decode_node_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Node> {
    if work.is_some() {
        operations::validate_canonical_json_with_work(bytes, MAX_NODE_BYTES, work)?;
    }
    let Some(observer) = work.as_deref_mut() else { return crate::executor::decode_json(bytes); };
    let mut items = 0u64;
    let mut reservation = 0;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["Node", "Leaf", "Branch", "Head", "kind", "leaf", "branch", "effect",
         "head", "revision", "position", "bit", "left", "right"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut observed_failure = false;
    let native = operations::original_native_decode_mode(observer);
    let decoded = operations::decode_preclaimed_json_with_mode::<Node, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes: MAX_NODE_BYTES,
            maximum_items: MAX_NODE_BYTES,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        crate::wire::ErrorCode::Storage,
        native,
        crate::contract::JsonValueSchema::None,
        &mut |charge| {
            let charged = match charge {
                crate::contract::JsonCodecCharge::Work { items: visited, .. } => {
                    items = items.checked_add(visited)
                        .ok_or_else(|| Error::Invalid("effect node item count overflows".into()))?;
                    observer.codec_charge(charge)
                }
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                    let slot = std::mem::size_of::<serde::__private229::de::Content<'static>>()
                        .max(std::mem::size_of::<(
                            serde::__private229::de::Content<'static>,
                            serde::__private229::de::Content<'static>,
                        )>()) as u64;
                    reservation = items.checked_mul(10).and_then(|items| items.checked_mul(slot))
                        .and_then(|slots| slots.checked_add(maximum_owned_bytes))
                        .and_then(|owned| owned.checked_add(diagnostic_bytes))
                        .ok_or_else(|| Error::Invalid("effect node schema reservation overflows".into()))?;
                    observer.retain_owned(reservation)
                }
                _ => observer.codec_charge(charge),
            };
            if charged.is_err() { observed_failure = true; }
            charged
        },
    );
    let decoded = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal content is invalid: {message}")),
                other => other,
            }
        }
    });
    let decoded = operations::retain_original_decode_error(decoded, &mut reservation, observer);
    observer.release_owned(reservation)?;
    decoded
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

fn certificate_path_with_work(
    authority: &Authority, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamPath, u64)> {
    operations::publication_path_with_work(
        authority, "harness/v2/effect-heads/", Some((&"cuts", 4)), work)
}


fn node_path_with_work(
    authority: &Authority, hash: Digest, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamPath, u64)> {
    let suffix = blake3::Hash::from_bytes(hash).to_hex();
    operations::publication_path_with_work(
        authority, "harness/v2/effect-heads/", Some((&format_args!("nodes/{suffix}"), 70)), work)
}

fn release_after_drop<T>(result: Result<T>, owned: u64, work: &mut Option<&mut dyn HistoryIo>) -> Result<T> {
    let release = match work.as_deref_mut() {
        Some(observer) => observer.release_owned(owned),
        None => Ok(()),
    };
    match result { Err(error) => Err(error), Ok(value) => release.map(|()| value) }
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
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Root> {
    verifier.verify_audience(authority)?;
    if revision == 0 {
        return Ok(Root {
            hash: None,
            consumed: 0,
        });
    }
    let (path, path_owned) = certificate_path_with_work(authority, work)?;
    let record = operations::one_record(client, &path, revision - 1, work).await;
    drop(path);
    let record = release_after_drop(record, path_owned, work)?
        .ok_or_else(|| Error::Unsupported(
            "effect history requires its original complete cut certificate".into()))?;
    let result = async {
    let mut used = 0;
    charge(&mut used, record.value.len() as u64, maximum_bytes)?;
    if record.value.len() as u64 > MAX_CERTIFICATE_BYTES {
        return Err(Error::Storage(
            "effect cut certificate exceeds its encoded bound".into(),
        ));
    }
    let certificate = decode_certificate_with_work(&record.value, work)?;
    let certificate_owned = (certificate.authority.id.capacity() + certificate.canonical_path.capacity()) as u64;
    let result = async {
    let (source_path, source_owned) = operations::publication_path_with_work(authority, "harness/v2/", None, work)?;
    let result = async {
    if &certificate.authority != authority
        || certificate.canonical_path != source_path.as_str()
        || certificate.revision != revision
        || certificate.proof
            != verifier.effect_history_root_proof_with_work(
                revision,
                certificate.canonical_digest,
                certificate.root,
                work,
            )?
    {
        return Err(Error::Unauthorized(
            "effect cut certificate proof differs".into(),
        ));
    }
    let canonical = operations::one_record(
        client,
        &source_path,
        revision - 1,
        work,
    )
    .await?
    .ok_or_else(|| Error::Storage("effect cut canonical anchor is missing".into()))?;
    let checked = (|| {
    charge(&mut used, canonical.value.len() as u64, maximum_bytes)?;
    if canonical.commit_id != record.commit_id || {
        if let Some(observer) = work.as_deref_mut() {
            observer.hashed_bytes(canonical.value.len() as u64)?;
        }
        *blake3::hash(&canonical.value).as_bytes() != certificate.canonical_digest
    }
    {
        return Err(Error::Storage(
            "effect cut differs from its original atomic publication".into(),
        ));
    }
    Ok(Root { hash: certificate.root, consumed: used })
    })();
    let canonical_bytes = operations::sdk_record_retained_bytes(&canonical, work);
    drop(canonical);
    let release = match work.as_deref_mut() {
        Some(observer) => observer.release_record(canonical_bytes),
        None => Ok(()),
    };
    match checked { Err(error) => Err(error), Ok(root) => release.map(|()| root) }
    }.await;
    drop(source_path);
    release_after_drop(result, source_owned, work)
    }.await;
    drop(certificate);
    let result = release_after_drop(result, certificate_owned, work);
    result
    }.await;
    let record_bytes = operations::sdk_record_retained_bytes(&record, work);
    drop(record);
    let release = match work.as_deref_mut() {
        Some(observer) => observer.release_record(record_bytes),
        None => Ok(()),
    };
    match result { Err(error) => Err(error), Ok(root) => release.map(|()| root) }
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
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Node> {
    let (path, path_owned) = node_path_with_work(authority, hash, work)?;
    let result = async {
    let record = operations::one_record(client, &path, 0, work)
        .await?
        .ok_or_else(|| Error::Storage("authenticated effect index node is missing".into()))?;
    let node = async {
    charge(used, record.value.len() as u64, maximum)?;
    if record.value.len() as u64 > MAX_NODE_BYTES || {
        operations::tail_with_work(client, &path, work).await? != 1
    } || {
        if let Some(observer) = work.as_deref_mut() {
            observer.hashed_bytes(record.value.len() as u64)?;
        }
        *blake3::hash(&record.value).as_bytes() != hash
    }
    {
        return Err(Error::Storage(
            "authenticated effect index node differs".into(),
        ));
    }
    decode_node_with_work(&record.value, work)
    }.await;
    let record_bytes = operations::sdk_record_retained_bytes(&record, work);
    drop(record);
    let release = match work.as_deref_mut() {
        Some(observer) => observer.release_record(record_bytes),
        None => Ok(()),
    };
    match node { Err(error) => Err(error), Ok(node) => release.map(|()| node) }
    }.await;
    drop(path);
    release_after_drop(result, path_owned, work)
}

async fn walk<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    effect: EffectId,
    root: Option<Digest>,
    used: &mut u64,
    maximum: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Walk> {
    let mut path: Vec<Branch> = Vec::new();
    let mut next = root;
    while let Some(hash) = next {
        match load_node(client, authority, hash, used, maximum, work).await? {
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
                if path.len() == path.capacity() && work.is_some() {
                    let old_capacity = path.capacity();
                    let target = (old_capacity.max(1) * 2).min(128);
                    let element_bytes = std::mem::size_of::<Branch>() as u64;
                    if let Some(observer) = work.as_deref_mut() {
                        observer.admit_allocation(target as u64 * element_bytes)?;
                    }
                    path.try_reserve_exact(target - path.len())
                        .map_err(|_| Error::Invalid("effect proof path allocation failed".into()))?;
                    if let Some(observer) = work.as_deref_mut() {
                        observer.retain_owned(path.capacity() as u64 * element_bytes)?;
                        observer.release_owned(old_capacity as u64 * element_bytes)?;
                    }
                }
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
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Digest> {
    let bytes = super::history::canonical_bytes_with_work(&node, MAX_NODE_BYTES, work)?;
    let prepared = (|| {
    if bytes.len() as u64 > MAX_NODE_BYTES {
        return Err(Error::Invalid(
            "effect index node exceeds publication bound".into(),
        ));
    }
    if let Some(observer) = work.as_deref_mut() {
        observer.hashed_bytes(bytes.len() as u64)?;
    }
    let hash = *blake3::hash(&bytes).as_bytes();
    Ok((hash, node_path_with_work(authority, hash, work)?))
    })();
    let (hash, path) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            let capacity = bytes.capacity() as u64;
            drop(bytes);
            return release_after_drop(Err(error), capacity, work);
        }
    };
    checkpoints::stage_immutable_vec_with_work(client, path, Ok(bytes), work).await?;
    Ok(hash)
}

async fn update<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    effect: EffectId,
    head: Head,
    mut observed: Walk,
    work: &mut Option<&mut dyn HistoryIo>,
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
    let mut replacement = stage_node(client, authority, Node::Leaf { effect, head }, work).await?;
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
            work,
        )
        .await?;
    }
    let path_bytes = observed.path.capacity() as u64 * std::mem::size_of::<Branch>() as u64;
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
            work,
        )
        .await?;
    }
    if let Some(observer) = work.as_deref_mut() {
        observer.release_owned(path_bytes)?;
    }
    Ok(replacement)
}

pub(super) async fn publication_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    event: &Event,
    head: Option<(EffectId, Head)>,
    publication: &mut operations::IndexedPublication,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    verifier.verify_event(event, &mut None)?;
    let previous_revision = event
        .revision
        .checked_sub(1)
        .ok_or_else(|| Error::Invalid("effect cut publication revision must be positive".into()))?;
    let ((encoded_authority, encoded_event), _) =
        crate::wire_codec::decode_event(&publication.bytes, work)?;
    let decoded_owned = crate::core::source_inventory(&encoded_event)?
        + encoded_authority.id.capacity() as u64;
    let admitted = (|| {
    let (canonical_path, canonical_owned) = operations::publication_path_with_work(authority, "harness/v2/", None, work)?;
    let canonical_matches = publication.path.as_str() == canonical_path.as_str();
    drop(canonical_path);
    release_after_drop(Ok(()), canonical_owned, work)?;
    if !canonical_matches
        || publication.expected_tail != previous_revision
        || publication.operation_id != event.operation_id
        || encoded_authority != *authority
        || encoded_event != *event
    {
        return Err(Error::Storage(
            "effect cut publication differs from its admitted canonical event".into(),
        ));
    }
    Ok(())
    })();
    drop(encoded_event);
    drop(encoded_authority);
    release_after_drop(admitted, decoded_owned, work)?;
    let mut previous = root_at(
        client,
        authority,
        verifier,
        previous_revision,
        PUBLICATION_READ_BYTES,
        work,
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
            work,
        )
        .await?;
        previous.hash = Some(update(client, authority, effect, head, observed, work).await?);
    } else if crate::core::event_effect_id(&event.payload).is_some() {
        return Err(Error::Storage(
            "effect cut publication omits its effect update".into(),
        ));
    }
    if let Some(observer) = work.as_deref_mut() {
        observer.hashed_bytes(publication.bytes.len() as u64)?;
    }
    let canonical_digest = *blake3::hash(&publication.bytes).as_bytes();
    let proof = verifier.effect_history_root_proof_with_work(event.revision, canonical_digest, previous.hash, work)?;
    let canonical_path = authority.stream_path_with_work(work)?;
    let path_owned = canonical_path.capacity() as u64;
    let cloned_authority = match work.as_deref_mut() {
        Some(observer) => observer.clone_authority(authority),
        None => Ok(authority.clone()),
    };
    let cloned_authority = match cloned_authority {
        Ok(authority) => authority,
        Err(error) => {
            drop(canonical_path);
            return release_after_drop(Err(error), path_owned, work);
        }
    };
    let certificate = Certificate {
        authority: cloned_authority,
        canonical_path,
        revision: event.revision,
        canonical_digest,
        root: previous.hash,
        proof,
    };
    let owned = (certificate.authority.id.capacity() + certificate.canonical_path.capacity()) as u64;
    let path = certificate_path_with_work(authority, work);
    let result = match path {
        Ok(path) => {
            let bytes = super::history::canonical_bytes_with_work(&certificate, MAX_CERTIFICATE_BYTES, work);
            publication.add_derived_index_with_work(path, previous_revision, bytes, work)
        }
        Err(error) => Err(error),
    };
    drop(certificate);
    release_after_drop(result, owned, work)
}


pub(super) async fn select_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    effect: EffectId,
    revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Selection> {
    let mut root = root_at(client, authority, verifier, revision, maximum_bytes, work).await?;
    let observed = walk(
        client,
        authority,
        effect,
        root.hash,
        &mut root.consumed,
        maximum_bytes,
        work,
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
    let owned = observed.path.capacity() as u64 * std::mem::size_of::<Branch>() as u64;
    drop(observed);
    if let Some(observer) = work.as_deref_mut() { observer.release_owned(owned)?; }
    Ok(Selection {
        head,
        consumed_bytes: root.consumed,
    })
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_decode_controls {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

    fn budget() -> WorkBudget {
        WorkBudget {
            allocation_operations: 1024,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 1024 * 1024,
            source_bytes_read: 1024 * 1024,
            items_examined: 1024 * 1024,
            bytes_copied: 1024 * 1024,
            ..WorkBudget::default()
        }
    }

    #[test]
    fn malformed_node_and_certificate_keep_original_serde_custody() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let mut io = super::super::history::native_work_io(&work).expect("original observer");
        let node = decode_node_with_work(br#"{"kind":"leaf","effect":false,"head":{"revision":1,"position":0}}"#, &mut Some(&mut io));
        let certificate = decode_certificate_with_work(br#"{"authority":false,"canonical_path":"harness/v2/task/x","revision":1,"canonical_digest":[],"root":null,"proof":[]}"#, &mut Some(&mut io));
        for error in [node.err().expect("invalid node"), certificate.err().expect("invalid certificate")] {
            match error {
                Error::NativeDecode { source, custody: Some(custody), .. } => {
                    assert!(custody.bytes() > 0);
                    assert!(work.shares_ledger(custody.work()));
                    assert!(source.is_data());
                }
                other => panic!("expected retained native serde cause: {other:?}"),
            }
        }
    }

    #[test]
    fn cancellation_preserves_original_callback_cause() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
        let mut io = super::super::history::native_work_io(&work).expect("original observer");
        cancellation.cancel();
        let error = decode_node_with_work(br#"{"kind":"branch","bit":0,"left":[],"right":[]}"#, &mut Some(&mut io)).err().expect("cancelled");
        assert!(matches!(error, Error::NativeWork { .. }));
    }

    #[test]
    fn original_quota_denies_node_before_serde_diagnostic_allocation() {
        let cancellation = CancellationToken::new();
        let mut limit = budget();
        limit.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes();
        let work = NativeMaterializationWork::new(limit, &cancellation).expect("original ledger");
        let mut io = super::super::history::native_work_io(&work).expect("original observer");
        let error = decode_node_with_work(br#"{"kind":"leaf","effect":false,"head":null}"#, &mut Some(&mut io)).err().expect("quota denial");
        assert!(matches!(error, Error::NativeWork { .. }));
    }
}
