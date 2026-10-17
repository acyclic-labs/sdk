use super::command;
use std::{path::PathBuf, process::Command, sync::OnceLock};

/// Keep the installed compiler available after isolating host configuration.
/// Calling the real executable avoids rustup interpreting the fixture HOME.
pub fn bind_native_compiler(host: &mut Command) {
    static DIRECTORY: OnceLock<PathBuf> = OnceLock::new();
    let directory = DIRECTORY.get_or_init(|| {
        let output = command("rustc")
            .args(["--print", "sysroot"])
            .output()
            .expect("resolve installed Rust toolchain");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        PathBuf::from(
            String::from_utf8(output.stdout)
                .expect("Rust sysroot is UTF-8")
                .trim(),
        )
        .join("bin")
    });
    let inherited = host
        .get_envs()
        .find_map(|(name, value)| {
            (name == "PATH")
                .then_some(value)
                .flatten()
                .map(std::ffi::OsStr::to_owned)
        })
        .or_else(|| std::env::var_os("PATH"))
        .expect("qualification PATH");
    let mut paths = vec![directory.clone()];
    paths.extend(std::env::split_paths(&inherited));
    host.env("PATH", std::env::join_paths(paths).expect("compiler PATH"));
}
