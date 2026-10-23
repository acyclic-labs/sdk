fn stat_message(value: &crate::WorkspaceStat) -> wire::FileStat {
    wire::FileStat {
        file_id: value.file_id.into_bytes().to_vec(),
        kind: file_kind(value.kind),
        link_count: value.link_count,
        logical_bytes: Some(output_u64(value.logical_bytes)),
        metadata: Some(metadata_message(value.metadata)),
    }
}

fn metadata_message(value: WorkspaceMetadata) -> wire::Metadata {
    wire::Metadata {
        posix_mode: Some(output_u32(value.posix_mode)),
        posix_uid: Some(output_u32(value.posix_uid)),
        posix_gid: Some(output_u32(value.posix_gid)),
        posix_flags: Some(output_u64(value.posix_flags)),
        windows_attributes: Some(output_u32(value.windows_attributes)),
        created_ns: Some(output_i64(value.created_ns)),
        modified_ns: Some(output_i64(value.modified_ns)),
        accessed_ns: Some(output_i64(value.accessed_ns)),
        changed_ns: Some(output_i64(value.changed_ns)),
        has_named_attributes: value.has_named_attributes,
        has_acl: value.has_acl,
        has_security_descriptor: value.has_security_descriptor,
    }
}

fn output_u32(value: Option<u32>) -> wire::OptionalU32 {
    wire::OptionalU32 {
        value: Some(value.map_or(
            wire::optional_u32::Value::Unavailable(true),
            wire::optional_u32::Value::Present,
        )),
    }
}

fn output_u64(value: Option<u64>) -> wire::OptionalU64 {
    wire::OptionalU64 {
        value: Some(value.map_or(
            wire::optional_u64::Value::Unavailable(true),
            wire::optional_u64::Value::Present,
        )),
    }
}

fn output_i64(value: Option<i64>) -> wire::OptionalI64 {
    wire::OptionalI64 {
        value: Some(value.map_or(
            wire::optional_i64::Value::Unavailable(true),
            wire::optional_i64::Value::Present,
        )),
    }
}

fn file_kind(value: EngineFileKind) -> i32 {
    hosted_contract::file_kind(value) as i32
}

fn required<T>(value: Option<T>, name: &'static str) -> Result<T, Status> {
    value.ok_or_else(|| Status::invalid_argument(format!("{name} is required")))
}

fn status(error: &WorkspaceError) -> Status {
    match error {
        WorkspaceError::AccessDenied => Status::permission_denied(error.to_string()),
        WorkspaceError::NotFound => Status::not_found(error.to_string()),
        WorkspaceError::Name(_)
        | WorkspaceError::Path(_)
        | WorkspaceError::NamespacePath(_)
        | WorkspaceError::ReadLimitExceeded
        | WorkspaceError::InvalidMergeResolution
        | WorkspaceError::NotRegularFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::IsDirectory
        | WorkspaceError::EmptyContentSet
        | WorkspaceError::ContentLengthOverflow => Status::invalid_argument(error.to_string()),
        WorkspaceError::ForeignGeneration
        | WorkspaceError::StaleGeneration
        | WorkspaceError::StaleSource
        | WorkspaceError::StaleIdentity
        | WorkspaceError::IncompatibleWorkspace
        | WorkspaceError::ChangeSetContinuity
        | WorkspaceError::RetentionConflict
        | WorkspaceError::NoCommonAncestor
        | WorkspaceError::LineageLimit
        | WorkspaceError::JoinLimit
        | WorkspaceError::StaleTarget
        | WorkspaceError::ChangedPathLimit
        | WorkspaceError::DirectoryNotEmpty
        | WorkspaceError::NotFork => Status::failed_precondition(error.to_string()),
        WorkspaceError::Cancelled(_) => Status::cancelled(error.to_string()),
        WorkspaceError::Work(_) | WorkspaceError::WorkMerge { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        WorkspaceError::Engine(_) | WorkspaceError::Filesystem(_) => {
            Status::unavailable(error.to_string())
        }
    }
}
