use std::path::PathBuf;

pub fn acyclic() -> PathBuf {
    absolute(std::env::var_os("ACYCLIC_TEST_BINARY").map_or_else(
        || PathBuf::from(env!("CARGO_BIN_EXE_acyclic")),
        PathBuf::from,
    ))
}

fn absolute(path: PathBuf) -> PathBuf {
    assert!(
        path.is_absolute(),
        "ACYCLIC_TEST_BINARY must be an absolute path: {}",
        path.display()
    );
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_cli_paths_survive_child_working_directory_changes() {
        let path = std::env::temp_dir().join("acyclic-test-cli");
        assert_eq!(absolute(path.clone()), path);
    }

    #[test]
    #[should_panic(expected = "ACYCLIC_TEST_BINARY must be an absolute path")]
    fn relative_cli_paths_fail_before_starting_a_child() {
        absolute(PathBuf::from("target/debug/acyclic"));
    }
}
