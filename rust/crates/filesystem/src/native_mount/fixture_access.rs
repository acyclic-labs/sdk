//! Explicit shared authority for privileged native-mount unit fixtures.
use crate as acyclic_fs;
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/support/native_access.rs"
));
