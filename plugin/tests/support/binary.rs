use std::path::PathBuf;

pub fn acyclic() -> PathBuf {
    acyclic_test_support::cargo_binary!("acyclic", std::env::var_os("ACYCLIC_TEST_BINARY"))
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic(expected = "test binary path must be absolute")]
    fn relative_cli_paths_fail_before_starting_a_child() {
        acyclic_test_support::cargo_binary!("acyclic", Some("target/debug/acyclic".into()));
    }
}
