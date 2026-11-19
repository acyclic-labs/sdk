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
    #[cfg(windows)]
    LINKING.with(|(key, linker, libraries)| {
        command.env(key, linker).env("LIB", libraries);
    });
}

/// Selects the declared compiler's linker for a direct rustc invocation.
pub fn configure_rustc(command: &mut Command) {
    if let Some((arguments, libraries)) = rustc_linking() {
        command.env("LIB", libraries).args(arguments);
    }
}

/// Compiler arguments and `LIB` for the declared Windows fixture toolchain.
/// Ordinary Cargo tests inherit their developer toolchain configuration.
pub fn rustc_linking() -> Option<([std::ffi::OsString; 2], std::ffi::OsString)> {
    #[cfg(windows)]
    {
        std::env::var_os("CARGO_BIN_EXE_rustup")?;
        Some(LINKING.with(|(_, linker, libraries)| {
            let mut argument = std::ffi::OsString::from("linker=");
            argument.push(linker);
            (["-C".into(), argument], libraries.clone())
        }))
    }
    #[cfg(not(windows))]
    None
}

#[cfg(windows)]
thread_local! {
    static LINKING: (String, PathBuf, std::ffi::OsString) = resolve_linking();
}

#[cfg(windows)]
fn resolve_linking() -> (String, PathBuf, std::ffi::OsString) {
    let rustc = binary("rustc");
    let sysroot = rustc.parent().unwrap().parent().unwrap();
    let linkers = std::fs::read_dir(sysroot.join("lib/rustlib"))
        .unwrap()
        .map(|entry| entry.unwrap().path().join("bin/rust-lld.exe"))
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    let [linker] = linkers.as_slice() else {
        panic!("the pinned compiler must provide exactly one host linker");
    };
    let host = linker
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .file_name()
        .unwrap();
    let rustup = binary("rustup");
    let tools = rustup.parent().unwrap().parent().unwrap();
    let runfiles = tools.parent().unwrap().parent().unwrap();
    let paths = std::fs::read_to_string(tools.join("msvc-lib-paths")).unwrap();
    let libraries = std::env::join_paths(paths.lines().map(|path| runfiles.join(path))).unwrap();
    (
        format!(
            "CARGO_TARGET_{}_LINKER",
            host.to_str()
                .unwrap()
                .replace('-', "_")
                .to_ascii_uppercase()
        ),
        linker.clone(),
        libraries,
    )
}
