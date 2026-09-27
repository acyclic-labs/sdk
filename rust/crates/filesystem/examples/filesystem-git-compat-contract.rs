#![allow(missing_docs)]

use acyclic_fs::git_compat::{
    GIT_COMPAT_BYTE_WIRE_FIELDS, GIT_COMPAT_IDENTITY_WIRE_FIELDS, GIT_COMPAT_OPAQUE_WIRE_PATHS,
    GIT_COMPAT_OUTPUT_VARIANTS, GIT_COMPAT_PENDING_WIRE_FIELDS, GIT_COMPAT_PUBLIC_WIRE_ALIASES,
    GIT_COMPAT_TIMESTAMP_WIRE_FIELDS, GIT_COMPAT_TRANSITION_ID_BYTES, GIT_COMPAT_UUID_WIRE_PATHS,
};
use serde::Serialize;

#[derive(Serialize)]
struct IdentityField<'a> {
    key: &'a str,
    bytes: usize,
}

#[derive(Serialize)]
struct ByteField<'a> {
    key: &'a str,
    bytes: Option<usize>,
}

#[derive(Serialize)]
struct Contract<'a> {
    output_variants: &'a [&'a str],
    identity_fields: Vec<IdentityField<'a>>,
    byte_fields: Vec<ByteField<'a>>,
    timestamp_fields: &'a [&'a str],
    uuid_paths: &'a [&'a str],
    opaque_paths: &'a [&'a str],
    public_aliases: Vec<(&'a str, &'a str)>,
    pending_fields: &'a [&'a str],
    transition_identity_bytes: usize,
}

fn main() -> Result<(), serde_json::Error> {
    let identity_fields = GIT_COMPAT_IDENTITY_WIRE_FIELDS
        .iter()
        .map(|(key, bytes)| IdentityField { key, bytes: *bytes })
        .collect();
    let contract = Contract {
        output_variants: GIT_COMPAT_OUTPUT_VARIANTS,
        identity_fields,
        byte_fields: GIT_COMPAT_BYTE_WIRE_FIELDS
            .iter()
            .map(|(key, bytes)| ByteField { key, bytes: *bytes })
            .collect(),
        timestamp_fields: GIT_COMPAT_TIMESTAMP_WIRE_FIELDS,
        uuid_paths: GIT_COMPAT_UUID_WIRE_PATHS,
        opaque_paths: GIT_COMPAT_OPAQUE_WIRE_PATHS,
        public_aliases: GIT_COMPAT_PUBLIC_WIRE_ALIASES.to_vec(),
        pending_fields: GIT_COMPAT_PENDING_WIRE_FIELDS,
        transition_identity_bytes: GIT_COMPAT_TRANSITION_ID_BYTES,
    };
    println!("{}", serde_json::to_string(&contract)?);
    Ok(())
}
