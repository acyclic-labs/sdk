//! Small shared helpers.

use super::*;

pub(crate) fn string(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("missing string field '{field}'"))
}

pub(crate) fn decode_fixed<const N: usize>(value: &str) -> Result<[u8; N], String> {
    let mut bytes = [0_u8; N];
    hex::decode_to_slice(value, &mut bytes).map_err(display)?;
    Ok(bytes)
}

pub(crate) fn short_hash(bytes: &[u8]) -> String {
    hex::encode(&blake3::hash(bytes).as_bytes()[..12])
}

pub(crate) fn compact_id(bytes: &[u8; 16]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub(crate) fn root_key(root_id: WorkspaceRootId) -> String {
    hex::encode(root_id.into_bytes())
}

pub(crate) fn repository_id(
    workspace_id: [u8; 16],
    repository_workspace_id: [u8; 16],
) -> acyclic_fs::WorkspaceId {
    acyclic_fs::WorkspaceId::from_bytes(if repository_workspace_id == [0; 16] {
        workspace_id
    } else {
        repository_workspace_id
    })
}

pub(crate) fn route_name(root_id: WorkspaceRootId) -> String {
    format!("r{}", compact_id(&root_id.into_bytes()))
}

pub(crate) fn derived_idempotency_key(
    fork_key: IdempotencyKey,
    root_id: WorkspaceRootId,
) -> IdempotencyKey {
    let mut input = Vec::with_capacity(32);
    input.extend_from_slice(&fork_key.into_bytes());
    input.extend_from_slice(&root_id.into_bytes());
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(digest.as_bytes().get(..16).unwrap_or(digest.as_bytes()));
    IdempotencyKey::from_bytes(bytes)
}

pub(crate) fn derived_cleanup_key(fork_key: [u8; 16], root_id: WorkspaceRootId) -> IdempotencyKey {
    let mut input = Vec::with_capacity(39);
    input.extend_from_slice(&fork_key);
    input.extend_from_slice(&root_id.into_bytes());
    input.extend_from_slice(b"discard");
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(digest.as_bytes().get(..16).unwrap_or(digest.as_bytes()));
    IdempotencyKey::from_bytes(bytes)
}

const ROOT_MATERIALIZATION_LEAF: &str = ".acyclic-m";

pub(crate) fn root_materialization_root(root: &Path) -> Result<PathBuf, String> {
    let digest = blake3::hash(root.as_os_str().to_string_lossy().as_bytes());
    Ok(root
        .parent()
        .ok_or_else(|| "root checkout has no same-filesystem staging parent".to_owned())?
        .join(ROOT_MATERIALIZATION_LEAF)
        .join(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest.as_bytes()[..16])))
}

pub(crate) fn root_materialization_directory(
    root: &Path,
    operation_id: OperationId,
) -> Result<PathBuf, String> {
    let parent = root.parent()
        .ok_or_else(|| "root checkout has no same-filesystem staging parent".to_owned())?;
    let digest = blake3::hash(root.as_os_str().to_string_lossy().as_bytes());
    let leaf = root_materialization_leaf(&digest, operation_id).map_err(display)?;
    let leaf = std::str::from_utf8(&leaf).map_err(display)?;
    Ok(parent.join(leaf))
}

fn root_materialization_leaf(
    digest: &blake3::Hash,
    operation_id: OperationId,
) -> Result<[u8; 56], base64::EncodeSliceError> {
    let mut leaf = [std::path::MAIN_SEPARATOR as u8; 56];
    leaf[..10].copy_from_slice(ROOT_MATERIALIZATION_LEAF.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_slice(&digest.as_bytes()[..16], &mut leaf[11..33])?;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_slice(operation_id.into_bytes(), &mut leaf[34..])?;
    Ok(leaf)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn root_materialization_directory_with_native_work(
    root: &Path,
    operation_id: OperationId,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(PathBuf, acyclic_fs::NativeAllocation)> {
    let parent = root.parent().ok_or(acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason: "root checkout has no same-filesystem staging parent",
    })?;
    let borrowed = root.as_os_str().to_str();
    // A tuple drops formatted bytes before their original allocation custody,
    // including every early hash or path-construction refusal.
    let lossy = match borrowed {
        Some(_) => None,
        None => Some(work.display(&root.display())?),
    };
    let text = match borrowed {
        Some(text) => text,
        None => &lossy.as_ref().ok_or(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Storage,
            reason: "root staging path has no admitted display image",
        })?.0,
    };
    let hashing = acyclic_fs::WorkCounters {
        bytes_hashed: text.len() as u64,
        ..acyclic_fs::WorkCounters::UNCHARGED
    };
    work.admit(hashing)?;
    let digest = blake3::hash(text.as_bytes());
    work.spent(hashing)?;
    drop(lossy);
    let encoding = acyclic_fs::WorkCounters {
        bytes_encoded: 44,
        bytes_copied: 56,
        ..acyclic_fs::WorkCounters::UNCHARGED
    };
    work.admit(encoding)?;
    let leaf = root_materialization_leaf(&digest, operation_id)
        .map_err(|_| acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Storage,
            reason: "root staging identifier encoding exceeded its fixed width",
        })?;
    work.spent(encoding)?;
    let leaf = std::str::from_utf8(&leaf).map_err(|_| acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Storage,
        reason: "root staging identifier encoding is not UTF-8",
    })?;
    Ok(work.join_path(parent, Path::new(leaf))?)
}

pub(crate) fn conflict_json(conflict: &MergeConflict) -> Value {
    match conflict {
        MergeConflict::File(file_id) => json!({
            "kind": "file-record",
            "fileId": hex::encode(file_id.into_bytes())
        }),
        MergeConflict::Binding { directory_id, name } => json!({
            "kind": "directory-binding",
            "directoryId": hex::encode(directory_id.into_bytes()),
            "nameEncoding": format!("{:?}", name.encoding()),
            "nameBytes": hex::encode(name.as_bytes())
        }),
    }
}

pub(crate) fn typed_conflict_json(conflict: &acyclic_fs::ConflictView) -> Value {
    json!({
        "key": conflict.key,
        "path": conflict.path,
        "kind": conflict.kind,
        "base": conflict.base,
        "ours": conflict.ours,
        "theirs": conflict.theirs,
    })
}

pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

pub(crate) fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}

pub(crate) fn public_tools(commandless: bool) -> Value {
    if !commandless {
        return json!([]);
    }
    json!([{
        "name": "acyclic",
        "description": "Run the Acyclic CLI dispatcher without shell parsing. Pass the same argv used after the `acyclic` executable; use `-C <path>` to select a workspace explicitly.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "argv": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Acyclic arguments, for example [\"git\", \"status\"] or [\"-C\", \"/workspace\", \"agents\"]."
                }
            },
            "required": ["argv"],
            "additionalProperties": false
        }
    }])
}
