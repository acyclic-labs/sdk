fn stat_message(value: &crate::WorkspaceStat) -> wire::FileStat {
    wire::FileStat {
        file_id: value.file_id.into_bytes().to_vec(),
        kind: file_kind(value.kind),
        link_count: value.link_count,
        logical_bytes: Some(output_u64(value.logical_bytes)),
        metadata: Some(metadata_message(value.metadata)),
    }
}

fn file_kind(value: EngineFileKind) -> i32 {
    hosted_contract::file_kind(value) as i32
}

fn required<T>(value: Option<T>, name: &'static str) -> Result<T, Status> {
    value.ok_or_else(|| Status::invalid_argument(format!("{name} is required")))
}
