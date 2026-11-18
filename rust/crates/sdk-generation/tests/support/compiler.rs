pub use acyclic_test_support::fixture_compiler::{binary, configure, path};

pub fn with<T>(operation: impl FnOnce() -> T) -> T {
    let mut command = std::process::Command::new(binary("CARGO"));
    configure(&mut command);
    command.env("PATH", path());
    sdk_docs::with_command_environment(&command, operation)
}
