#![allow(missing_docs)]

use acyclic_fs::git_compat::{
    GIT_COMPAT_ACTION_TYPESCRIPT_TYPES, GIT_COMPAT_ACTION_VARIANTS, GIT_COMPAT_BYTE_WIRE_FIELDS,
    GIT_COMPAT_COMMAND_TYPESCRIPT_TYPES, GIT_COMPAT_COMMAND_VARIANTS,
    GIT_COMPAT_DIRTY_STATE_TYPESCRIPT_VARIANTS, GIT_COMPAT_DIRTY_STATE_VARIANTS,
    GIT_COMPAT_DIRTY_STATE_WIRE, GIT_COMPAT_IDENTITY_WIRE_FIELDS,
    GIT_COMPAT_NESTED_TYPESCRIPT_TYPES, GIT_COMPAT_OPAQUE_WIRE_PATHS,
    GIT_COMPAT_OUTPUT_TYPESCRIPT_TYPES, GIT_COMPAT_OUTPUT_VARIANTS, GIT_COMPAT_PENDING_WIRE_FIELDS,
    GIT_COMPAT_PUBLIC_WIRE_ALIASES, GIT_COMPAT_RESET_MODE_TYPESCRIPT_VARIANTS,
    GIT_COMPAT_RESET_MODE_VARIANTS, GIT_COMPAT_RESET_MODE_WIRE, GIT_COMPAT_RESULT_TYPESCRIPT_TYPES,
    GIT_COMPAT_RESULT_VARIANTS, GIT_COMPAT_TIMESTAMP_WIRE_FIELDS, GIT_COMPAT_TRANSITION_ID_BYTES,
    GIT_COMPAT_UUID_WIRE_PATHS,
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
    command_variants: &'a [&'a str],
    command_types: &'a [(&'a str, &'a str)],
    output_variants: &'a [&'a str],
    output_types: &'a [(&'a str, &'a str)],
    action_variants: &'a [&'a str],
    action_types: &'a [(&'a str, &'a str)],
    result_variants: &'a [&'a str],
    result_types: &'a [(&'a str, &'a str)],
    nested_types: &'a [(&'a str, &'a str)],
    reset_mode_variants: &'a [&'a str],
    reset_mode_typescript_variants: &'a [&'a str],
    reset_mode_wire: &'a [(&'a str, &'a str)],
    dirty_state_variants: &'a [&'a str],
    dirty_state_typescript_variants: &'a [&'a str],
    dirty_state_wire: &'a [(&'a str, &'a str)],
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
        command_variants: GIT_COMPAT_COMMAND_VARIANTS,
        command_types: GIT_COMPAT_COMMAND_TYPESCRIPT_TYPES,
        output_variants: GIT_COMPAT_OUTPUT_VARIANTS,
        output_types: GIT_COMPAT_OUTPUT_TYPESCRIPT_TYPES,
        action_variants: GIT_COMPAT_ACTION_VARIANTS,
        action_types: GIT_COMPAT_ACTION_TYPESCRIPT_TYPES,
        result_variants: GIT_COMPAT_RESULT_VARIANTS,
        result_types: GIT_COMPAT_RESULT_TYPESCRIPT_TYPES,
        nested_types: GIT_COMPAT_NESTED_TYPESCRIPT_TYPES,
        reset_mode_variants: GIT_COMPAT_RESET_MODE_VARIANTS,
        reset_mode_typescript_variants: GIT_COMPAT_RESET_MODE_TYPESCRIPT_VARIANTS,
        reset_mode_wire: GIT_COMPAT_RESET_MODE_WIRE,
        dirty_state_variants: GIT_COMPAT_DIRTY_STATE_VARIANTS,
        dirty_state_typescript_variants: GIT_COMPAT_DIRTY_STATE_TYPESCRIPT_VARIANTS,
        dirty_state_wire: GIT_COMPAT_DIRTY_STATE_WIRE,
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
