use std::path::PathBuf;

pub fn acyclic() -> PathBuf {
    acyclic_test_support::cargo_binary!("acyclic", std::env::var_os("ACYCLIC_TEST_BINARY"))
}
