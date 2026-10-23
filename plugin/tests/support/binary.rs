use std::path::PathBuf;

pub fn acyclic() -> PathBuf {
    std::env::var_os("ACYCLIC_TEST_BINARY").map_or_else(
        || PathBuf::from(env!("CARGO_BIN_EXE_acyclic")),
        PathBuf::from,
    )
}
