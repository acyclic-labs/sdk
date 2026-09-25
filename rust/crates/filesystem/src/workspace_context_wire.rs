//! One typed, bounded wire boundary for native and browser context adapters.

use crate::wire::filesystem::v2 as wire;
use crate::workspace_context::{is_canonical, record_is_valid};
use crate::{
    WorkspaceContext, WorkspaceContextId, WorkspaceContextRoot, WorkspaceContextState, WorkspaceId,
    WorkspaceRootId,
};
use prost::Message;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Maximum encoded context-control record accepted across either binding.
pub const MAXIMUM_CONTEXT_WIRE_BYTES: usize = 4 * 1024 * 1024;

/// A malformed or unrepresentable context-control record.
#[derive(Debug, Error)]
pub enum WorkspaceContextWireError {
    /// A record exceeds the bounded control-plane limit.
    #[error("workspace context record exceeds the wire limit")]
    TooLarge,
    /// The protobuf payload is malformed or noncanonical.
    #[error("invalid workspace context protobuf")]
    InvalidProtobuf,
    /// An identity is absent or has the wrong byte length.
    #[error("invalid workspace context identity")]
    InvalidIdentity,
    /// A path cannot be represented in the cross-platform UTF-8 contract.
    #[error("workspace context path is not UTF-8")]
    NonUtf8Path,
    /// A required state or record invariant is invalid.
    #[error("invalid workspace context record")]
    InvalidRecord,
}

fn id(bytes: &[u8]) -> Result<[u8; 16], WorkspaceContextWireError> {
    bytes
        .try_into()
        .map_err(|_| WorkspaceContextWireError::InvalidIdentity)
}

fn path(value: &Path) -> Result<String, WorkspaceContextWireError> {
    value
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or(WorkspaceContextWireError::NonUtf8Path)
}

fn decode<M: Message + Default>(bytes: &[u8]) -> Result<M, WorkspaceContextWireError> {
    if bytes.len() > MAXIMUM_CONTEXT_WIRE_BYTES {
        return Err(WorkspaceContextWireError::TooLarge);
    }
    let value = M::decode(bytes).map_err(|_| WorkspaceContextWireError::InvalidProtobuf)?;
    if value.encode_to_vec() != bytes {
        return Err(WorkspaceContextWireError::InvalidProtobuf);
    }
    Ok(value)
}

fn encode<M: Message>(value: M) -> Result<Vec<u8>, WorkspaceContextWireError> {
    let bytes = value.encode_to_vec();
    if bytes.len() > MAXIMUM_CONTEXT_WIRE_BYTES {
        return Err(WorkspaceContextWireError::TooLarge);
    }
    Ok(bytes)
}

fn root_to_wire(
    root: &WorkspaceContextRoot,
) -> Result<wire::WorkspaceContextRoot, WorkspaceContextWireError> {
    if root.workspace_name.is_empty()
        || !is_canonical(&root.source_path)
        || root
            .mount_path
            .as_deref()
            .is_some_and(|path| !is_canonical(path))
    {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    Ok(wire::WorkspaceContextRoot {
        root_id: root.root_id.into_bytes().to_vec(),
        source_path: path(&root.source_path)?,
        workspace_id: root.workspace_id.into_bytes().to_vec(),
        workspace_name: root.workspace_name.clone(),
        parent_workspace_id: root.parent_workspace_id.map(|id| id.into_bytes().to_vec()),
        mount_path: root.mount_path.as_deref().map(path).transpose()?,
    })
}

fn root_from_wire(
    root: wire::WorkspaceContextRoot,
) -> Result<WorkspaceContextRoot, WorkspaceContextWireError> {
    if root.workspace_name.is_empty() || root.source_path.is_empty() {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    let root = WorkspaceContextRoot {
        root_id: WorkspaceRootId::from_bytes(id(&root.root_id)?),
        source_path: PathBuf::from(root.source_path),
        workspace_id: WorkspaceId::from_bytes(id(&root.workspace_id)?),
        workspace_name: root.workspace_name,
        parent_workspace_id: root
            .parent_workspace_id
            .map(|bytes| id(&bytes).map(WorkspaceId::from_bytes))
            .transpose()?,
        mount_path: root.mount_path.map(PathBuf::from),
    };
    if !is_canonical(&root.source_path)
        || root
            .mount_path
            .as_deref()
            .is_some_and(|path| !is_canonical(path))
    {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    Ok(root)
}

/// Encodes ordered root bindings for registration.
pub fn encode_roots(roots: &[WorkspaceContextRoot]) -> Result<Vec<u8>, WorkspaceContextWireError> {
    if roots.is_empty() {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    let mut ordered: Vec<_> = roots.iter().collect();
    ordered.sort_by_key(|root| root.root_id);
    if ordered
        .windows(2)
        .any(|pair| pair[0].root_id == pair[1].root_id)
    {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    encode(wire::WorkspaceContextRoots {
        roots: ordered
            .into_iter()
            .map(root_to_wire)
            .collect::<Result<_, _>>()?,
    })
}

/// Decodes ordered root bindings for registration.
pub fn decode_roots(bytes: &[u8]) -> Result<Vec<WorkspaceContextRoot>, WorkspaceContextWireError> {
    let payload: wire::WorkspaceContextRoots = decode(bytes)?;
    let roots: Vec<_> = payload
        .roots
        .into_iter()
        .map(root_from_wire)
        .collect::<Result<_, _>>()?;
    if roots.is_empty()
        || roots
            .windows(2)
            .any(|pair| pair[0].root_id >= pair[1].root_id)
    {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    Ok(roots)
}

/// Encodes one root binding for adoption.
pub fn encode_root(root: &WorkspaceContextRoot) -> Result<Vec<u8>, WorkspaceContextWireError> {
    encode(root_to_wire(root)?)
}

/// Decodes one root binding for adoption.
pub fn decode_root(bytes: &[u8]) -> Result<WorkspaceContextRoot, WorkspaceContextWireError> {
    root_from_wire(decode(bytes)?)
}

/// Encodes a complete context snapshot without filesystem contents.
pub fn encode_context(context: &WorkspaceContext) -> Result<Vec<u8>, WorkspaceContextWireError> {
    if !record_is_valid(context, context.context_id) {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    let state = match context.state {
        WorkspaceContextState::Active => wire::WorkspaceContextState::Active,
        WorkspaceContextState::Frozen => wire::WorkspaceContextState::Frozen,
        WorkspaceContextState::Discarded => wire::WorkspaceContextState::Discarded,
    };
    encode(wire::WorkspaceContextSnapshot {
        version: context.version,
        revision: context.revision,
        context_id: context.context_id.into_bytes().to_vec(),
        parent_context_id: context.parent_context_id.map(|id| id.into_bytes().to_vec()),
        roots: context
            .roots
            .values()
            .map(root_to_wire)
            .collect::<Result<_, _>>()?,
        state: state as i32,
    })
}

/// Decodes a complete context snapshot.
pub fn decode_context(bytes: &[u8]) -> Result<WorkspaceContext, WorkspaceContextWireError> {
    let payload: wire::WorkspaceContextSnapshot = decode(bytes)?;
    if payload.version != 1 {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    let state = match wire::WorkspaceContextState::try_from(payload.state) {
        Ok(wire::WorkspaceContextState::Active) => WorkspaceContextState::Active,
        Ok(wire::WorkspaceContextState::Frozen) => WorkspaceContextState::Frozen,
        Ok(wire::WorkspaceContextState::Discarded) => WorkspaceContextState::Discarded,
        _ => return Err(WorkspaceContextWireError::InvalidRecord),
    };
    let mut roots = BTreeMap::new();
    let mut previous_root = None;
    for wire_root in payload.roots {
        let root = root_from_wire(wire_root)?;
        if previous_root.is_some_and(|previous| previous >= root.root_id) {
            return Err(WorkspaceContextWireError::InvalidRecord);
        }
        previous_root = Some(root.root_id);
        if roots.insert(root.root_id, root).is_some() {
            return Err(WorkspaceContextWireError::InvalidRecord);
        }
    }
    let context = WorkspaceContext {
        version: payload.version,
        revision: payload.revision,
        context_id: WorkspaceContextId::from_bytes(id(&payload.context_id)?),
        parent_context_id: payload
            .parent_context_id
            .map(|bytes| id(&bytes).map(WorkspaceContextId::from_bytes))
            .transpose()?,
        roots,
        state,
    };
    if !record_is_valid(&context, context.context_id) {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    Ok(context)
}

/// Encodes the IDs of a durably discarded subtree.
pub fn encode_discard(ids: &[WorkspaceContextId]) -> Result<Vec<u8>, WorkspaceContextWireError> {
    let unique: std::collections::BTreeSet<_> = ids.iter().copied().collect();
    if ids.is_empty() || unique.len() != ids.len() {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    encode(wire::WorkspaceContextDiscard {
        context_ids: ids.iter().map(|id| id.into_bytes().to_vec()).collect(),
    })
}

/// Decodes the IDs of a durably discarded subtree.
pub fn decode_discard(bytes: &[u8]) -> Result<Vec<WorkspaceContextId>, WorkspaceContextWireError> {
    let payload: wire::WorkspaceContextDiscard = decode(bytes)?;
    let ids: Vec<_> = payload
        .context_ids
        .iter()
        .map(|bytes| id(bytes).map(WorkspaceContextId::from_bytes))
        .collect::<Result<_, _>>()?;
    if ids.is_empty() {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    let unique: std::collections::BTreeSet<_> = ids.iter().copied().collect();
    if unique.len() != ids.len() {
        return Err(WorkspaceContextWireError::InvalidRecord);
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(byte: u8) -> Result<WorkspaceContextRoot, Box<dyn std::error::Error>> {
        Ok(WorkspaceContextRoot {
            root_id: WorkspaceRootId::from_bytes([byte; 16]),
            source_path: std::env::current_dir()?,
            workspace_id: WorkspaceId::from_bytes([byte + 1; 16]),
            workspace_name: "project".to_owned(),
            parent_workspace_id: None,
            mount_path: None,
        })
    }

    #[test]
    fn context_and_root_wire_round_trip_without_json_or_float_revisions()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = root(3)?;
        let context = WorkspaceContext {
            version: 1,
            revision: u64::MAX,
            context_id: WorkspaceContextId::from_bytes([5; 16]),
            parent_context_id: None,
            roots: BTreeMap::from([(root.root_id, root.clone())]),
            state: WorkspaceContextState::Frozen,
        };
        assert_eq!(decode_context(&encode_context(&context)?)?, context);
        assert_eq!(
            decode_roots(&encode_roots(std::slice::from_ref(&root))?)?,
            vec![root.clone()]
        );
        assert_eq!(decode_root(&encode_root(&root)?)?, root);
        Ok(())
    }

    #[test]
    fn duplicate_roots_and_unknown_state_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            encode_roots(&[]),
            Err(WorkspaceContextWireError::InvalidRecord)
        ));
        assert!(matches!(
            decode_roots(&[]),
            Err(WorkspaceContextWireError::InvalidRecord)
        ));
        assert!(matches!(
            encode_discard(&[]),
            Err(WorkspaceContextWireError::InvalidRecord)
        ));
        assert!(matches!(
            decode_discard(&[]),
            Err(WorkspaceContextWireError::InvalidRecord)
        ));
        let root = root(3)?;
        let duplicate = wire::WorkspaceContextRoots {
            roots: vec![root_to_wire(&root)?, root_to_wire(&root)?],
        };
        assert!(matches!(
            decode_roots(&duplicate.encode_to_vec()),
            Err(WorkspaceContextWireError::InvalidRecord)
        ));
        let invalid = wire::WorkspaceContextSnapshot {
            version: 1,
            revision: 0,
            context_id: vec![5; 16],
            parent_context_id: None,
            roots: vec![],
            state: wire::WorkspaceContextState::Unspecified as i32,
        };
        assert!(matches!(
            decode_context(&invalid.encode_to_vec()),
            Err(WorkspaceContextWireError::InvalidRecord)
        ));
        Ok(())
    }
}
