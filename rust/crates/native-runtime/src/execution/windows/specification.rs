//! PSEC 1.1 ABI slots from Microsoft's ProcessSecurityEnvironment.fbs. Only
//! the policy translation lives here; all authority and attenuation remain
//! in the shared grant core. The retained schema documents the native ABI.
use super::*;
use flatbuffers::FlatBufferBuilder;

pub(super) fn encode(grant: &Grant, access: ExecutionAccess) -> io::Result<Vec<u8>> {
    let mut builder = FlatBufferBuilder::new();
    let mut read = Vec::new();
    for root in std::iter::once(&grant.workspace)
        .chain(grant.write.entries())
        .chain(grant.read.entries())
    {
        read.push(
            builder.create_string(
                root.path
                    .to_str()
                    .ok_or_else(|| refused("PSEC path is not UTF-8"))?,
            ),
        );
    }
    let read = builder.create_vector(&read);
    let mut write = Vec::new();
    if access == ExecutionAccess::WorkspaceWrite {
        for root in grant
            .workspace_write
            .then_some(&grant.workspace)
            .into_iter()
            .chain(grant.write.entries())
        {
            write.push(
                builder.create_string(
                    root.path
                        .to_str()
                        .ok_or_else(|| refused("PSEC path is not UTF-8"))?,
                ),
            );
        }
    }
    let write = builder.create_vector(&write);
    let capabilities = builder.create_string(
        "internetClient,internetClientServer,privateNetworkClientServer,networkLoopback",
    );
    let table = builder.start_table();
    builder.push_slot::<i8>(4, 1, 0); // FilterAction::allow
    let egress = builder.end_table(table);
    let table = builder.start_table();
    builder.push_slot::<i8>(4, 1, 0);
    builder.push_slot::<i8>(6, 1, 0);
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
