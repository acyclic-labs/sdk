fn join_plan_id(
    source: &wire::GenerationRef,
    target: &wire::GenerationRef,
    base: &wire::GenerationRef,
    history: JoinHistory,
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-wire-join-plan-v2\0");
    for reference in [source, target, base] {
        if let Some(workspace) = &reference.workspace {
            hasher.update(&(workspace.workspace_id.len() as u64).to_le_bytes());
            hasher.update(&workspace.workspace_id);
            hasher.update(&(workspace.name.len() as u64).to_le_bytes());
            hasher.update(workspace.name.as_bytes());
        } else {
            hasher.update(&0_u64.to_le_bytes());
            hasher.update(&0_u64.to_le_bytes());
        }
        hasher.update(&(reference.generation_id.len() as u64).to_le_bytes());
        hasher.update(&reference.generation_id);
    }
    hasher.update(&[join_history_code(history)]);
    hasher.update(&maximum_generations.to_le_bytes());
    hasher.update(&maximum_changes.to_le_bytes());
    hasher.update(&maximum_conflicts.to_le_bytes());
    *hasher.finalize().as_bytes()
}

fn join_history(value: i32) -> Result<JoinHistory, Status> {
    let value = wire::JoinHistory::try_from(value)
        .map_err(|_| Status::invalid_argument("join history is invalid"))?;
    hosted_contract::join_history_from_wire(value)
        .ok_or_else(|| Status::invalid_argument("join history must be specified"))
}

const fn join_history_code(value: JoinHistory) -> u8 {
    hosted_contract::join_history(value) as u8
}

fn decode_object_id(bytes: &[u8]) -> Result<ObjectId, Status> {
    ObjectId::try_from(bytes).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn transfer_cursor(bytes: &[u8]) -> Result<u64, Status> {
    if bytes.is_empty() {
        return Ok(0);
    }
    let cursor: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("transfer cursor must contain 8 bytes"))?;
    Ok(u64::from_le_bytes(cursor))
}

fn mutation(
    status: wire::MutationStatus,
    generation: Option<wire::GenerationRef>,
    actual_head: Option<wire::GenerationRef>,
) -> wire::MutationResponse {
    wire::MutationResponse {
        status: status as i32,
        generation,
        actual_head,
        conflicts: Vec::new(),
        truncated: false,
    }
}

fn mutation_with_conflicts(
    status: wire::MutationStatus,
    conflicts: Vec<wire::Conflict>,
    truncated: bool,
) -> wire::MutationResponse {
    wire::MutationResponse {
        status: status as i32,
        generation: None,
        actual_head: None,
        conflicts,
        truncated,
    }
}

fn transaction_conflict(value: RebaseConflict) -> wire::Conflict {
    use wire::conflict::Region;
    let region = match value.region {
        DependencyRegion::FileRecord(file_id) => Region::FileRecord(wire::FileConflict {
            file_id: file_id.into_bytes().to_vec(),
        }),
        DependencyRegion::Metadata(file_id) => Region::Metadata(wire::FileConflict {
            file_id: file_id.into_bytes().to_vec(),
        }),
        DependencyRegion::FileLength(file_id) => Region::FileLength(wire::FileConflict {
            file_id: file_id.into_bytes().to_vec(),
        }),
        DependencyRegion::ContentRange {
            file_id,
            offset,
            length,
        } => Region::ContentRange(wire::ContentConflict {
            file_id: file_id.into_bytes().to_vec(),
            range: Some(wire::ByteRange { offset, length }),
        }),
        DependencyRegion::SparseSeek {
            file_id,
            offset,
            target,
        } => Region::SparseSeek(wire::SparseConflict {
            file_id: file_id.into_bytes().to_vec(),
            offset,
            target: hosted_contract::sparse_target(target) as i32,
        }),
        DependencyRegion::DirectoryName { directory_id, name } => {
            Region::DirectoryName(wire::DirectoryNameConflict {
                directory_id: directory_id.into_bytes().to_vec(),
                name: Some(logical_name(&name)),
            })
        }
        DependencyRegion::DirectoryRange {
            directory_id,
            after,
            maximum_entries,
        } => Region::DirectoryRange(wire::DirectoryRangeConflict {
            directory_id: directory_id.into_bytes().to_vec(),
            after: after.as_ref().map(logical_name),
            maximum_entries,
        }),
    };
    wire::Conflict {
        region: Some(region),
        r#use: hosted_contract::conflict_use(value.usage) as i32,
        expected_digest: match value.expected {
            DependencyState::Absent => Vec::new(),
            DependencyState::Present(digest) => digest.as_bytes().to_vec(),
        },
        actual_digest: match value.actual {
            DependencyState::Absent => Vec::new(),
            DependencyState::Present(digest) => digest.as_bytes().to_vec(),
        },
    }
}

fn merge_conflict(value: MergeConflict) -> wire::Conflict {
    let region = match value {
        MergeConflict::File(file_id) => wire::conflict::Region::FileRecord(wire::FileConflict {
            file_id: file_id.into_bytes().to_vec(),
        }),
        MergeConflict::Binding { directory_id, name } => {
            wire::conflict::Region::DirectoryName(wire::DirectoryNameConflict {
                directory_id: directory_id.into_bytes().to_vec(),
                name: Some(logical_name(&name)),
            })
        }
    };
    wire::Conflict {
        region: Some(region),
        r#use: wire::ConflictUse::Unspecified as i32,
        expected_digest: Vec::new(),
        actual_digest: Vec::new(),
    }
}

fn rebase_message<A, O>(value: WorkspaceRebase<A, O>) -> wire::RebaseResponse {
    match value {
        WorkspaceRebase::Rebased(generation) => wire::RebaseResponse {
            status: wire::RebaseStatus::Rebased as i32,
            generation: Some(generation_ref(&generation)),
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::AlreadyRebased(generation) => wire::RebaseResponse {
            status: wire::RebaseStatus::AlreadyRebased as i32,
            generation: Some(generation_ref(&generation)),
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::Current(generation) => wire::RebaseResponse {
            status: wire::RebaseStatus::Current as i32,
            generation: Some(generation_ref(&generation)),
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::Stale(generation) => wire::RebaseResponse {
            status: wire::RebaseStatus::Stale as i32,
            generation: Some(generation_ref(&generation)),
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::Conflicted {
            conflicts,
            truncated,
        } => wire::RebaseResponse {
            status: wire::RebaseStatus::Conflicted as i32,
            generation: None,
            conflicts: conflicts.into_iter().map(merge_conflict).collect(),
            truncated,
        },
        WorkspaceRebase::Fenced => wire::RebaseResponse {
            status: wire::RebaseStatus::Fenced as i32,
            generation: None,
            conflicts: Vec::new(),
            truncated: false,
        },
        WorkspaceRebase::IdempotencyConflict => wire::RebaseResponse {
            status: wire::RebaseStatus::IdempotencyConflict as i32,
            generation: None,
            conflicts: Vec::new(),
            truncated: false,
        },
    }
}
