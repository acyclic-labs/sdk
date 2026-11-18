use acyclic_fs_wire::wire::filesystem::v2 as wire;

pub(crate) fn operation(value: u8) -> Option<wire::OperationOptions> {
    Some(wire::OperationOptions {
        idempotency_key: operation_bytes(value),
    })
}

pub(crate) fn operation_bytes(value: u8) -> Vec<u8> {
    let mut id = [0_u8; 16];
    id[15] = value;
    id.to_vec()
}

pub(crate) fn mutation(value: wire::mutation::Mutation) -> wire::Mutation {
    wire::Mutation {
        mutation: Some(value),
    }
}
