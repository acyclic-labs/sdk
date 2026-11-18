use std::path::PathBuf;
use std::process::Command;

/// Resolves a declared compiler executable and its runfiles alias.
pub fn binary(name: &str) -> PathBuf {
    std::env::var_os(format!("CARGO_BIN_EXE_{name}"))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
        })
        // Runfiles aliases do not own the compiler's companion data.
        .canonicalize()
        .unwrap()
}

/// Places declared compiler directories before the inherited search path.
pub fn path() -> std::ffi::OsString {
    std::env::join_paths(
        [binary("CARGO"), binary("rustup")]
            .into_iter()
            .map(|tool| tool.parent().unwrap().to_owned())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap()
}

/// Configures a child command to use the declared compiler tools.
pub fn configure(command: &mut Command) {
    let rustup = binary("rustup");
    command
        .env("CARGO_PROFILE_DEV_DEBUG", "0")
        .env("RUSTC", binary("rustc"))
        .env(
            "RUSTUP_HOME",
            rustup
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("rustup"),
        );
}
