//! PSEC 1.1 ABI slots from Microsoft's ProcessSecurityEnvironment.fbs. Only
//! the policy translation lives here; all authority and attenuation remain
//! in the shared grant core. The retained schema documents the native ABI.
use super::*;
use flatbuffers::FlatBufferBuilder;

pub(super) fn encode(grant: &Grant, access: ExecutionAccess) -> io::Result<Vec<u8>> {
    let mut builder = FlatBufferBuilder::new();
    let writable = access == ExecutionAccess::WorkspaceWrite;
    // PSEC applies the most specific listed path, and a path listed both ways
    // is read-only. The grant core lets every write root win, so a root at or
    // below one is listed once, as writable.
    let written = |path: &Path| -> io::Result<bool> {
        if !writable {
            return Ok(false);
        }
        let roots = Grant::roots(&grant.write, path, None)?;
        Ok(
            (grant.workspace_write && path.starts_with(&grant.workspace.path))
                || roots.exact.is_some()
                || roots.shortest.is_some(),
        )
    };
    let mut read = Vec::new();
    for root in std::iter::once(&grant.workspace)
        .chain(grant.write.entries())
        .chain(grant.read.entries())
    {
        if !written(&root.path)? {
            read.push(builder.create_string(&policy_path(&root.path)?));
        }
    }
    let read = builder.create_vector(&read);
    let mut write = Vec::new();
    if writable {
        for root in grant
            .workspace_write
            .then_some(&grant.workspace)
            .into_iter()
            .chain(grant.write.entries())
        {
            write.push(builder.create_string(&policy_path(&root.path)?));
        }
    }
    let write = builder.create_vector(&write);
    let network_enabled = grant.network == ExecutionNetwork::On;
    let capabilities = builder.create_string(if network_enabled {
        "internetClient,internetClientServer,privateNetworkClientServer,networkLoopback"
    } else {
        ""
    });
    let table = builder.start_table();
    builder.push_slot::<i8>(4, i8::from(network_enabled), 0);
    let egress = builder.end_table(table);
    let table = builder.start_table();
    builder.push_slot::<i8>(4, i8::from(network_enabled), 0);
    builder.push_slot::<i8>(6, i8::from(network_enabled), 0);
    let ingress = builder.end_table(table);
    let table = builder.start_table();
    builder.push_slot_always(6, egress);
    builder.push_slot_always(10, ingress);
    let network = builder.end_table(table);
    let table = builder.start_table();
    // SchemaVersion is an inline pair of little-endian u16s. A u32 writes
    // exactly those four bytes with stronger, valid alignment: major=minor=1.
    builder.push_slot_always::<u32>(4, 0x0001_0001);
    builder.push_slot_always(6, capabilities);
    builder.push_slot::<u64>(10, 0xff, 0); // all native Job UI restrictions
    builder.push_slot_always(12, write);
    builder.push_slot_always(14, read);
    builder.push_slot_always(18, network);
    let specification = builder.end_table(table);
    builder.finish(specification, Some("PSEC"));
    Ok(builder.finished_data().to_vec())
}

/// PSEC resolves policy paths in the Win32 namespace and rejects verbatim
/// spellings on some volumes (a ReFS Dev Drive reports `ERROR_INVALID_DRIVE`).
/// Bound roots are canonical verbatim paths, so the policy carries their drive
/// or UNC spelling. That spelling is admitted only when Win32 normalisation
/// maps it back to itself; a trailing dot or space can never widen a grant.
pub(super) fn policy_path(path: &Path) -> io::Result<String> {
    let plain = crate::paths::strip_verbatim(path);
    let text = plain
        .to_str()
        .ok_or_else(|| refused("PSEC path is not UTF-8"))?;
    if text.starts_with(r"\\?\") {
        return Err(unavailable(format!(
            "Windows sandbox policy needs a drive-letter or UNC path for {}; assign the volume a drive letter",
            path.display()
        )));
    }
    if std::path::absolute(&plain).map_err(refused)?.as_os_str() != plain.as_os_str() {
        return Err(refused(format!(
            "{} has no exact Win32 spelling for the Windows sandbox policy",
            path.display()
        )));
    }
    Ok(text.to_owned())
}
