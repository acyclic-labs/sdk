//! Versioned JSON envelopes for immutable merge and publication values.
//!
//! Maps with typed binary keys use ordered entry arrays, keeping the wire
//! representation valid and deterministic in every target language.

use crate::{
    ConflictKey, GenerationId, MergePlan, MergeResolution, MultiRootFence, MultiRootMergeCandidate,
    MultiRootMergePlan, MultiRootMergeRoot, MultiRootPublication, MultiRootPublicationPhase,
    OperationId, Publication, UnpublishedMergeCandidate, WorkspaceContextId, WorkspaceId,
    WorkspaceRootId,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

const WIRE_VERSION: u16 = 1;
const PUBLICATION_VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireEnvelope<T> {
    version: u16,
    kind: String,
    payload: T,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolutionEntry {
    key: ConflictKey,
    resolution: MergeResolution,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergeCandidatePayload {
    plan: MergePlan,
    resolutions: Vec<ResolutionEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MultiRootEntry {
    root_id: WorkspaceRootId,
    source_workspace_id: WorkspaceId,
    merge_workspace_id: Option<WorkspaceId>,
    source_generation: GenerationId,
    target_workspace_id: WorkspaceId,
    target_generation: GenerationId,
    base_generation: GenerationId,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    child_wins_bindings: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MultiRootPlanPayload {
    operation_id: OperationId,
    parent_context_id: WorkspaceContextId,
    child_context_id: WorkspaceContextId,
    roots: Vec<MultiRootEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RootResolutionsEntry {
    root_id: WorkspaceRootId,
    resolutions: Vec<ResolutionEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MultiRootCandidatePayload {
    plan: MultiRootPlanPayload,
    resolutions: Vec<RootResolutionsEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RootFenceEntry {
    root_id: WorkspaceRootId,
    fence: MultiRootFence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RootConflictEntry {
    root_id: WorkspaceRootId,
    plan: MergePlan,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RootGenerationEntry {
    root_id: WorkspaceRootId,
    generation: GenerationId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RootConflictKeysEntry {
    root_id: WorkspaceRootId,
    keys: Vec<ConflictKey>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MultiRootPublicationPayload {
    version: u32,
    revision: u64,
    candidate: MultiRootCandidatePayload,
    phase: MultiRootPublicationPhase,
    published_roots: Vec<WorkspaceRootId>,
    published_generations: Vec<RootGenerationEntry>,
    fences: Vec<RootFenceEntry>,
    conflicts: Vec<RootConflictEntry>,
    projected_roots: Vec<RootGenerationEntry>,
    declared_conflicts: Vec<RootConflictKeysEntry>,
    paused_root: Option<WorkspaceRootId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "kebab-case")]
enum PublicationPayload {
    Applied(MultiRootPublicationPayload),
    StaleBeforeCommit(WorkspaceRootId),
    Paused(MultiRootPublicationPayload),
    Conflicted(MultiRootPublicationPayload),
}

/// Invalid or unsupported compatibility wire data.
#[derive(Debug, Error)]
pub enum CompatibilityWireError {
    /// JSON syntax or a typed payload was malformed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// The envelope version or value kind is unsupported.
    #[error("unsupported compatibility wire envelope")]
    UnsupportedEnvelope,
    /// The value violates immutable candidate/publication invariants.
    #[error("compatibility wire value is inconsistent")]
    InvalidValue,
}

fn encode<T: Serialize>(kind: &str, payload: &T) -> Result<String, CompatibilityWireError> {
    Ok(serde_json::to_string(&WireEnvelope {
        version: WIRE_VERSION,
        kind: kind.to_owned(),
        payload,
    })?)
}

fn decode<T: DeserializeOwned>(kind: &str, json: &str) -> Result<T, CompatibilityWireError> {
    let envelope: WireEnvelope<T> = serde_json::from_str(json)?;
    if envelope.version != WIRE_VERSION || envelope.kind != kind {
        return Err(CompatibilityWireError::UnsupportedEnvelope);
    }
    Ok(envelope.payload)
}

fn resolution_entries(value: &BTreeMap<ConflictKey, MergeResolution>) -> Vec<ResolutionEntry> {
    value
        .iter()
        .map(|(key, resolution)| ResolutionEntry {
            key: key.clone(),
            resolution: resolution.clone(),
        })
        .collect()
}

fn resolutions_from_entries(
    entries: Vec<ResolutionEntry>,
) -> Result<BTreeMap<ConflictKey, MergeResolution>, CompatibilityWireError> {
    let length = entries.len();
    let value = entries
        .into_iter()
        .map(|entry| (entry.key, entry.resolution))
        .collect::<BTreeMap<_, _>>();
    (value.len() == length)
        .then_some(value)
        .ok_or(CompatibilityWireError::InvalidValue)
}

fn merge_candidate_payload(value: &UnpublishedMergeCandidate) -> MergeCandidatePayload {
    MergeCandidatePayload {
        plan: value.plan.clone(),
        resolutions: resolution_entries(&value.resolutions),
    }
}

fn merge_candidate_from_payload(
    value: MergeCandidatePayload,
) -> Result<UnpublishedMergeCandidate, CompatibilityWireError> {
    Ok(UnpublishedMergeCandidate {
        plan: value.plan,
        resolutions: resolutions_from_entries(value.resolutions)?,
    })
}

fn multi_root_plan_payload(value: &MultiRootMergePlan) -> MultiRootPlanPayload {
    MultiRootPlanPayload {
        operation_id: value.operation_id,
        parent_context_id: value.parent_context_id,
        child_context_id: value.child_context_id,
        roots: value
            .roots
            .iter()
            .map(|(root_id, root)| MultiRootEntry {
                root_id: *root_id,
                source_workspace_id: root.source_workspace_id,
                merge_workspace_id: root.merge_workspace_id,
                source_generation: root.source_generation,
                target_workspace_id: root.target_workspace_id,
                target_generation: root.target_generation,
                base_generation: root.base_generation,
                child_wins_bindings: root.child_wins_bindings.clone(),
            })
            .collect(),
    }
}

fn multi_root_plan_from_payload(
    value: MultiRootPlanPayload,
) -> Result<MultiRootMergePlan, CompatibilityWireError> {
    let length = value.roots.len();
    let roots = value
        .roots
        .into_iter()
        .map(|entry| {
            (
                entry.root_id,
                MultiRootMergeRoot {
                    source_workspace_id: entry.source_workspace_id,
                    merge_workspace_id: entry.merge_workspace_id,
                    source_generation: entry.source_generation,
                    target_workspace_id: entry.target_workspace_id,
                    target_generation: entry.target_generation,
                    base_generation: entry.base_generation,
                    child_wins_bindings: entry.child_wins_bindings,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    if roots.len() != length {
        return Err(CompatibilityWireError::InvalidValue);
    }
    Ok(MultiRootMergePlan {
        operation_id: value.operation_id,
        parent_context_id: value.parent_context_id,
        child_context_id: value.child_context_id,
        roots,
    })
}

fn multi_root_candidate_payload(value: &MultiRootMergeCandidate) -> MultiRootCandidatePayload {
    MultiRootCandidatePayload {
        plan: multi_root_plan_payload(&value.plan),
        resolutions: value
            .resolutions
            .iter()
            .map(|(root_id, resolutions)| RootResolutionsEntry {
                root_id: *root_id,
                resolutions: resolution_entries(resolutions),
            })
            .collect(),
    }
}

fn multi_root_candidate_from_payload(
    value: MultiRootCandidatePayload,
) -> Result<MultiRootMergeCandidate, CompatibilityWireError> {
    let plan = multi_root_plan_from_payload(value.plan)?;
    let length = value.resolutions.len();
    let mut resolutions = BTreeMap::new();
    for entry in value.resolutions {
        let root = resolutions_from_entries(entry.resolutions)?;
        if resolutions.insert(entry.root_id, root).is_some() {
            return Err(CompatibilityWireError::InvalidValue);
        }
    }
    if resolutions.len() != length {
        return Err(CompatibilityWireError::InvalidValue);
    }
    Ok(MultiRootMergeCandidate { plan, resolutions })
}

fn publication_payload(value: &MultiRootPublication) -> MultiRootPublicationPayload {
    MultiRootPublicationPayload {
        version: value.version,
        revision: value.revision,
        candidate: multi_root_candidate_payload(&value.candidate),
        phase: value.phase,
        published_roots: value.published_roots.iter().copied().collect(),
        published_generations: value
            .published_generations
            .iter()
            .map(|(root_id, generation)| RootGenerationEntry {
                root_id: *root_id,
                generation: *generation,
            })
            .collect(),
        fences: value
            .fences
            .iter()
            .map(|(root_id, fence)| RootFenceEntry {
                root_id: *root_id,
                fence: fence.clone(),
            })
            .collect(),
        conflicts: value
            .conflicts
            .iter()
            .map(|(root_id, plan)| RootConflictEntry {
                root_id: *root_id,
                plan: plan.clone(),
            })
            .collect(),
        projected_roots: value
            .projected_roots
            .iter()
            .map(|(root_id, generation)| RootGenerationEntry {
                root_id: *root_id,
                generation: *generation,
            })
            .collect(),
        declared_conflicts: value
            .declared_conflicts
            .iter()
            .map(|(root_id, keys)| RootConflictKeysEntry {
                root_id: *root_id,
                keys: keys.iter().cloned().collect(),
            })
            .collect(),
        paused_root: value.paused_root,
    }
}

fn publication_from_payload(
    value: MultiRootPublicationPayload,
) -> Result<MultiRootPublication, CompatibilityWireError> {
    let published_length = value.published_roots.len();
    let published_roots = value.published_roots.into_iter().collect::<BTreeSet<_>>();
    let published_generation_length = value.published_generations.len();
    let published_generations = value
        .published_generations
        .into_iter()
        .map(|entry| (entry.root_id, entry.generation))
        .collect::<BTreeMap<_, _>>();
    let fence_length = value.fences.len();
    let fences = value
        .fences
        .into_iter()
        .map(|entry| (entry.root_id, entry.fence))
        .collect::<BTreeMap<_, _>>();
    let conflict_length = value.conflicts.len();
    let conflicts = value
        .conflicts
        .into_iter()
        .map(|entry| (entry.root_id, entry.plan))
        .collect::<BTreeMap<_, _>>();
    let projected_length = value.projected_roots.len();
    let projected_roots = value
        .projected_roots
        .into_iter()
        .map(|entry| (entry.root_id, entry.generation))
        .collect::<BTreeMap<_, _>>();
    let declared_length = value.declared_conflicts.len();
    let declared_conflicts = value
        .declared_conflicts
        .into_iter()
        .map(|entry| {
            (
                entry.root_id,
                entry.keys.into_iter().collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if published_roots.len() != published_length
        || published_generations.len() != published_generation_length
        || fences.len() != fence_length
        || conflicts.len() != conflict_length
        || projected_roots.len() != projected_length
        || declared_conflicts.len() != declared_length
    {
        return Err(CompatibilityWireError::InvalidValue);
    }
    Ok(MultiRootPublication {
        version: value.version,
        revision: value.revision,
        candidate: multi_root_candidate_from_payload(value.candidate)?,
        phase: value.phase,
        published_roots,
        published_generations,
        fences,
        conflicts,
        projected_roots,
        declared_conflicts,
        paused_root: value.paused_root,
    })
}

fn valid_merge_candidate(value: &UnpublishedMergeCandidate) -> bool {
    !value.plan.truncated
        && value.plan.conflicts.len() == value.resolutions.len()
        && value
            .plan
            .conflicts
            .iter()
            .all(|conflict| value.resolutions.contains_key(&conflict.key))
}

fn valid_multi_root_plan(value: &MultiRootMergePlan) -> bool {
    !value.roots.is_empty()
        && value.parent_context_id != value.child_context_id
        && value
            .roots
            .values()
            .all(|root| root.source_workspace_id != root.target_workspace_id)
}

fn valid_multi_root_candidate(value: &MultiRootMergeCandidate) -> bool {
    valid_multi_root_plan(&value.plan) && value.plan.roots.keys().eq(value.resolutions.keys())
}

fn valid_publication(value: &MultiRootPublication) -> bool {
    value.version == PUBLICATION_VERSION
        && value.revision > 0
        && valid_multi_root_candidate(&value.candidate)
        && value
            .published_roots
            .is_subset(&value.candidate.plan.roots.keys().copied().collect())
        && value
            .published_generations
            .keys()
            .all(|root| value.published_roots.contains(root))
        && value
            .fences
            .keys()
            .all(|root| value.candidate.plan.roots.contains_key(root))
        && value
            .conflicts
            .keys()
            .all(|root| value.candidate.plan.roots.contains_key(root))
        && value
            .projected_roots
            .keys()
            .all(|root| value.candidate.plan.roots.contains_key(root))
        && value
            .declared_conflicts
            .keys()
            .all(|root| value.candidate.plan.roots.contains_key(root))
}

/// Encodes one immutable merge plan.
pub fn encode_merge_plan(value: &MergePlan) -> Result<String, CompatibilityWireError> {
    encode("merge-plan", value)
}

/// Decodes one immutable merge plan envelope.
pub fn decode_merge_plan(json: &str) -> Result<MergePlan, CompatibilityWireError> {
    decode("merge-plan", json)
}

/// Encodes one complete unpublished merge candidate.
pub fn encode_merge_candidate(
    value: &UnpublishedMergeCandidate,
) -> Result<String, CompatibilityWireError> {
    if !valid_merge_candidate(value) {
        return Err(CompatibilityWireError::InvalidValue);
    }
    encode("merge-candidate", &merge_candidate_payload(value))
}

/// Decodes and validates one complete unpublished merge candidate.
pub fn decode_merge_candidate(
    json: &str,
) -> Result<UnpublishedMergeCandidate, CompatibilityWireError> {
    let value = merge_candidate_from_payload(decode("merge-candidate", json)?)?;
    valid_merge_candidate(&value)
        .then_some(value)
        .ok_or(CompatibilityWireError::InvalidValue)
}

/// Encodes one immutable multi-root merge plan.
pub fn encode_multi_root_plan(
    value: &MultiRootMergePlan,
) -> Result<String, CompatibilityWireError> {
    if !valid_multi_root_plan(value) {
        return Err(CompatibilityWireError::InvalidValue);
    }
    encode("multi-root-plan", &multi_root_plan_payload(value))
}

/// Decodes and validates one immutable multi-root merge plan.
pub fn decode_multi_root_plan(json: &str) -> Result<MultiRootMergePlan, CompatibilityWireError> {
    let value = multi_root_plan_from_payload(decode("multi-root-plan", json)?)?;
    valid_multi_root_plan(&value)
        .then_some(value)
        .ok_or(CompatibilityWireError::InvalidValue)
}

/// Encodes one complete multi-root merge candidate.
pub fn encode_multi_root_candidate(
    value: &MultiRootMergeCandidate,
) -> Result<String, CompatibilityWireError> {
    if !valid_multi_root_candidate(value) {
        return Err(CompatibilityWireError::InvalidValue);
    }
    encode("multi-root-candidate", &multi_root_candidate_payload(value))
}

/// Decodes and validates one complete multi-root merge candidate.
pub fn decode_multi_root_candidate(
    json: &str,
) -> Result<MultiRootMergeCandidate, CompatibilityWireError> {
    let value = multi_root_candidate_from_payload(decode("multi-root-candidate", json)?)?;
    valid_multi_root_candidate(&value)
        .then_some(value)
        .ok_or(CompatibilityWireError::InvalidValue)
}

/// Encodes one publication result.
pub fn encode_publication(value: &Publication) -> Result<String, CompatibilityWireError> {
    let payload = match value {
        Publication::Applied(value) => PublicationPayload::Applied(publication_payload(value)),
        Publication::StaleBeforeCommit(root) => PublicationPayload::StaleBeforeCommit(*root),
        Publication::Paused(value) => PublicationPayload::Paused(publication_payload(value)),
        Publication::Conflicted(value) => {
            PublicationPayload::Conflicted(publication_payload(value))
        }
    };
    encode("publication", &payload)
}

/// Decodes and validates one publication result.
pub fn decode_publication(json: &str) -> Result<Publication, CompatibilityWireError> {
    let value = match decode("publication", json)? {
        PublicationPayload::Applied(value) => {
            Publication::Applied(publication_from_payload(value)?)
        }
        PublicationPayload::StaleBeforeCommit(root) => Publication::StaleBeforeCommit(root),
        PublicationPayload::Paused(value) => Publication::Paused(publication_from_payload(value)?),
        PublicationPayload::Conflicted(value) => {
            Publication::Conflicted(publication_from_payload(value)?)
        }
    };
    let valid = match &value {
        Publication::Applied(value)
        | Publication::Paused(value)
        | Publication::Conflicted(value) => valid_publication(value),
        Publication::StaleBeforeCommit(_) => true,
    };
    valid
        .then_some(value)
        .ok_or(CompatibilityWireError::InvalidValue)
}

/// Encodes a JSON merge-plan payload without duplicating its schema in a binding.
pub fn encode_merge_plan_payload(json: &str) -> Result<String, CompatibilityWireError> {
    encode_merge_plan(&serde_json::from_str(json)?)
}

/// Decodes a merge-plan envelope to its canonical JSON payload.
pub fn decode_merge_plan_payload(json: &str) -> Result<String, CompatibilityWireError> {
    Ok(serde_json::to_string(&decode_merge_plan(json)?)?)
}

/// Encodes a JSON merge-candidate payload using entry-array maps.
pub fn encode_merge_candidate_payload(json: &str) -> Result<String, CompatibilityWireError> {
    let value = merge_candidate_from_payload(serde_json::from_str(json)?)?;
    encode_merge_candidate(&value)
}

/// Decodes a merge-candidate envelope to its canonical JSON payload.
pub fn decode_merge_candidate_payload(json: &str) -> Result<String, CompatibilityWireError> {
    Ok(serde_json::to_string(&merge_candidate_payload(
        &decode_merge_candidate(json)?,
    ))?)
}

/// Encodes a JSON multi-root plan payload using root entry arrays.
pub fn encode_multi_root_plan_payload(json: &str) -> Result<String, CompatibilityWireError> {
    let value = multi_root_plan_from_payload(serde_json::from_str(json)?)?;
    encode_multi_root_plan(&value)
}

/// Decodes a multi-root plan envelope to its canonical JSON payload.
pub fn decode_multi_root_plan_payload(json: &str) -> Result<String, CompatibilityWireError> {
    Ok(serde_json::to_string(&multi_root_plan_payload(
        &decode_multi_root_plan(json)?,
    ))?)
}

/// Encodes a JSON multi-root candidate payload using entry-array maps.
pub fn encode_multi_root_candidate_payload(json: &str) -> Result<String, CompatibilityWireError> {
    let value = multi_root_candidate_from_payload(serde_json::from_str(json)?)?;
    encode_multi_root_candidate(&value)
}

/// Decodes a multi-root candidate envelope to its canonical JSON payload.
pub fn decode_multi_root_candidate_payload(json: &str) -> Result<String, CompatibilityWireError> {
    Ok(serde_json::to_string(&multi_root_candidate_payload(
        &decode_multi_root_candidate(json)?,
    ))?)
}

/// Encodes a JSON publication payload using entry-array maps.
pub fn encode_publication_payload(json: &str) -> Result<String, CompatibilityWireError> {
    let payload: PublicationPayload = serde_json::from_str(json)?;
    let value = match payload {
        PublicationPayload::Applied(value) => {
            Publication::Applied(publication_from_payload(value)?)
        }
        PublicationPayload::StaleBeforeCommit(root) => Publication::StaleBeforeCommit(root),
        PublicationPayload::Paused(value) => Publication::Paused(publication_from_payload(value)?),
        PublicationPayload::Conflicted(value) => {
            Publication::Conflicted(publication_from_payload(value)?)
        }
    };
    encode_publication(&value)
}

/// Decodes a publication envelope to its canonical JSON payload.
pub fn decode_publication_payload(json: &str) -> Result<String, CompatibilityWireError> {
    let value = decode_publication(json)?;
    let payload = match &value {
        Publication::Applied(value) => PublicationPayload::Applied(publication_payload(value)),
        Publication::StaleBeforeCommit(root) => PublicationPayload::StaleBeforeCommit(*root),
        Publication::Paused(value) => PublicationPayload::Paused(publication_payload(value)),
        Publication::Conflicted(value) => {
            PublicationPayload::Conflicted(publication_payload(value))
        }
    };
    Ok(serde_json::to_string(&payload)?)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::Digest;

    fn generation(byte: u8) -> GenerationId {
        GenerationId::new(Digest::from_bytes([byte; 32]))
    }

    fn root_plan() -> MultiRootMergePlan {
        MultiRootMergePlan {
            operation_id: OperationId::from_bytes([1; 16]),
            parent_context_id: WorkspaceContextId::from_bytes([2; 16]),
            child_context_id: WorkspaceContextId::from_bytes([3; 16]),
            roots: [(
                WorkspaceRootId::from_bytes([4; 16]),
                MultiRootMergeRoot {
                    source_workspace_id: WorkspaceId::from_bytes([5; 16]),
                    merge_workspace_id: None,
                    source_generation: generation(6),
                    target_workspace_id: WorkspaceId::from_bytes([7; 16]),
                    target_generation: generation(8),
                    base_generation: generation(9),
                    child_wins_bindings: BTreeSet::new(),
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn merge_plan_round_trips_and_envelope_is_forward_compatible() {
        let plan = MergePlan {
            base: generation(1),
            ours: generation(2),
            theirs: generation(3),
            conflicts: Vec::new(),
            truncated: false,
        };
        let encoded = encode_merge_plan(&plan).expect("encode");
        assert!(encoded.contains("\"payload\":"));
        assert!(!encoded.contains("\"value\":"));
        assert_eq!(decode_merge_plan(&encoded).expect("decode"), plan);
        let unsupported = encoded.replacen("\"version\":1", "\"version\":2", 1);
        assert!(matches!(
            decode_merge_plan(&unsupported),
            Err(CompatibilityWireError::UnsupportedEnvelope)
        ));
        let additive = encoded.replacen("{", "{\"futureField\":true,", 1);
        assert_eq!(decode_merge_plan(&additive).expect("additive fields"), plan);
    }

    #[test]
    fn multi_root_plan_uses_entry_arrays_and_round_trips() {
        let plan = root_plan();
        let encoded = encode_multi_root_plan(&plan).expect("encode");
        assert!(encoded.contains("\"roots\":[{"));
        assert_eq!(decode_multi_root_plan(&encoded).expect("decode"), plan);
    }

    #[test]
    fn publication_round_trip_preserves_exact_published_generations() {
        let plan = root_plan();
        let root_id = *plan.roots.keys().next().expect("root");
        let generation = generation(42);
        let publication = Publication::Applied(MultiRootPublication {
            version: PUBLICATION_VERSION,
            revision: 2,
            candidate: MultiRootMergeCandidate {
                resolutions: plan
                    .roots
                    .keys()
                    .map(|root| (*root, BTreeMap::new()))
                    .collect(),
                plan,
            },
            phase: MultiRootPublicationPhase::Applied,
            published_roots: BTreeSet::from([root_id]),
            published_generations: BTreeMap::from([(root_id, generation)]),
            fences: BTreeMap::new(),
            conflicts: BTreeMap::new(),
            projected_roots: BTreeMap::new(),
            declared_conflicts: BTreeMap::new(),
            paused_root: None,
        });
        let encoded = encode_publication(&publication).expect("encode");
        assert!(encoded.contains("publishedGenerations"));
        assert_eq!(decode_publication(&encoded).expect("decode"), publication);
    }

    #[test]
    fn multi_root_wire_rejects_empty_and_mismatched_candidates() {
        let mut plan = root_plan();
        plan.roots.clear();
        assert!(matches!(
            encode_multi_root_plan(&plan),
            Err(CompatibilityWireError::InvalidValue)
        ));
        let candidate = MultiRootMergeCandidate {
            plan: root_plan(),
            resolutions: BTreeMap::new(),
        };
        assert!(matches!(
            encode_multi_root_candidate(&candidate),
            Err(CompatibilityWireError::InvalidValue)
        ));
    }
}
