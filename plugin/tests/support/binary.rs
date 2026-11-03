use std::path::PathBuf;

pub fn acyclic() -> PathBuf {
    acyclic_test_support::cargo_binary!("acyclic", std::env::var_os("ACYCLIC_TEST_BINARY"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_cli_paths_survive_child_working_directory_changes() {
        let path = std::env::temp_dir().join("acyclic-test-cli");
        assert_eq!(
            acyclic_test_support::cargo_binary!("acyclic", Some(path.clone().into_os_string())),
            path
        );
    }

    #[test]
    #[should_panic(expected = "test binary path must be absolute")]
    fn relative_cli_paths_fail_before_starting_a_child() {
        acyclic_test_support::cargo_binary!("acyclic", Some("target/debug/acyclic".into()));
    }
}
