fn file_record_change(change: &crate::FileRecordChange) -> wire::FileRecordChange {
    wire::FileRecordChange {
        file_id: change.file_id.into_bytes().to_vec(),
        before: change.before.map(file_record_snapshot),
        after: change.after.map(file_record_snapshot),
    }
}

fn file_record_snapshot(record: crate::kernel::FileRecord) -> wire::FileRecordSnapshot {
    let mut value = wire::FileRecordSnapshot {
        file_id: record.file_id.into_bytes().to_vec(),
        file_kind: file_kind(record.kind),
        link_count: record.link_count,
        metadata_object: record.metadata.to_bytes().to_vec(),
        payload_kind: String::new(),
        logical_bytes: None,
        payload_object: Vec::new(),
        inline_bytes: Vec::new(),
        device_major: None,
        device_minor: None,
    };
    match record.payload {
        FilePayload::InlineRegular(bytes) => {
            value.payload_kind = "inline-regular".to_owned();
            value.logical_bytes = Some(output_u64(Some(
                u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX),
            )));
            value.inline_bytes = bytes.as_bytes().to_vec();
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            value.payload_kind = "regular".to_owned();
            value.logical_bytes = Some(output_u64(Some(logical_bytes)));
            value.payload_object = extents.to_bytes().to_vec();
        }
        FilePayload::Directory { entries } => {
            value.payload_kind = "directory".to_owned();
            value.payload_object = entries.to_bytes().to_vec();
        }
        FilePayload::SymbolicLink {
            target_bytes,
            target,
        } => {
            value.payload_kind = "symbolic-link".to_owned();
            value.logical_bytes = Some(output_u64(Some(target_bytes)));
            value.payload_object = target.to_bytes().to_vec();
        }
        FilePayload::Empty => value.payload_kind = "empty".to_owned(),
        FilePayload::Device { major, minor } => {
            value.payload_kind = "device".to_owned();
            value.device_major = Some(output_u32(Some(major)));
            value.device_minor = Some(output_u32(Some(minor)));
        }
        FilePayload::ReparsePoint {
            payload_bytes,
            payload,
        } => {
            value.payload_kind = "reparse-point".to_owned();
            value.logical_bytes = Some(output_u64(Some(payload_bytes)));
            value.payload_object = payload.to_bytes().to_vec();
        }
    }
    value
}

fn directory_binding_change(
    change: &crate::DirectoryBindingChange,
) -> wire::DirectoryBindingChange {
    wire::DirectoryBindingChange {
        directory_id: change.directory_id.into_bytes().to_vec(),
        name: Some(logical_name(&change.name)),
        before: change.before.as_ref().map(tree_entry_snapshot),
        after: change.after.as_ref().map(tree_entry_snapshot),
    }
}

fn tree_entry_snapshot(entry: &TreeEntry) -> wire::TreeEntrySnapshot {
    wire::TreeEntrySnapshot {
        name: Some(logical_name(&entry.name)),
        file_id: entry.file_id.into_bytes().to_vec(),
        file_kind: file_kind(entry.kind),
    }
}

fn logical_name(name: &LogicalName) -> wire::LogicalName {
    let encoding = hosted_contract::name_encoding(name.encoding());
    wire::LogicalName {
        encoding: encoding as i32,
        bytes: name.as_bytes().to_vec(),
    }
}

fn input_logical_name(name: wire::LogicalName) -> Result<LogicalName, Status> {
    let value = wire::NameEncoding::try_from(name.encoding)
        .map_err(|_| Status::invalid_argument("name encoding is invalid"))?;
    let encoding = hosted_contract::name_encoding_from_wire(value)
        .ok_or_else(|| Status::invalid_argument("name encoding is required"))?;
    LogicalName::new(encoding, name.bytes, 255)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn work_counters(work: crate::WorkCounters) -> wire::WorkCounters {
    wire::WorkCounters {
        authority_records_read: work.authority_records_read,
        authority_records_appended: work.authority_records_appended,
        authority_bytes_read: work.authority_bytes_read,
        authority_bytes_written: work.authority_bytes_written,
        object_probes: work.object_probes,
        backend_read_operations: work.backend_read_operations,
        backend_write_operations: work.backend_write_operations,
        durability_operations: work.durability_operations,
        page_reads: work.page_reads,
        page_writes: work.page_writes,
        object_bytes_read: work.object_bytes_read,
        object_bytes_written: work.object_bytes_written,
        bytes_hashed: work.bytes_hashed,
        bytes_copied: work.bytes_copied,
        bytes_encoded: work.bytes_encoded,
        source_bytes_read: work.source_bytes_read,
        source_path_components: work.source_path_components,
        source_entries_visited: work.source_entries_visited,
        output_bytes: work.output_bytes,
        items_examined: work.items_examined,
        items_returned: work.items_returned,
        allocation_operations: work.allocation_operations,
        peak_allocation_bytes: work.peak_allocation_bytes,
        materializations: work.materializations,
    }
}
