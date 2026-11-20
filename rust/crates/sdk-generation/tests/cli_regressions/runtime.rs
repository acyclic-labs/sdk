use super::cache as sync;
use super::*;

use acyclic_test_support::fixture_compiler as compiler;
pub(super) use acyclic_test_support::fixture_compiler::binary;

thread_local! {
    static GIT: PathBuf = resolve_git();
    static COMPILER: sync::Arc<PathBuf> = sync::Arc::new(binary("CARGO"));
    static CLI_PATH: std::ffi::OsString = GIT.with(|git| {
        std::env::join_paths(
            std::iter::once(git.parent().unwrap().to_owned())
                .chain(std::env::split_paths(&compiler::path())),
        )
        .unwrap()
    });
    #[cfg(windows)]
    static SCRATCH: PathBuf = {
        let executable = std::env::current_exe().unwrap();
        let directory = executable.ancestors().last().unwrap().join("tmp");
        // Keep compiler I/O on its own volume without inheriting a repository's
        // Cargo workspace. Each test thread owns a fresh, private directory.
        if directory.ancestors().any(|path| path.join("Cargo.toml").exists()) {
            std::env::temp_dir()
        } else {
            fs::create_dir_all(&directory).ok().and_then(|_| {
                tempfile::Builder::new().prefix("sdk-generation-cli-").tempdir_in(directory).ok()
            }).map(|directory| directory.keep()).unwrap_or_else(std::env::temp_dir)
        }
    };
}

pub(super) fn temp_dir() -> PathBuf {
    #[cfg(windows)]
    {
        SCRATCH.with(Clone::clone)
    }
    #[cfg(not(windows))]
    {
        std::env::temp_dir()
    }
}

fn resolve_git() -> PathBuf {
    let paths = std::env::var_os("PATH").unwrap();
    let selected = std::env::split_paths(&paths)
        .map(|directory| directory.join(format!("git{}", std::env::consts::EXE_SUFFIX)))
        .find(|path| path.is_file())
        .unwrap();
    if cfg!(windows)
        && selected.parent().and_then(Path::file_name) == Some(std::ffi::OsStr::new("cmd"))
    {
        let concrete = selected
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("mingw64/bin/git.exe");
        if concrete.is_file() {
            return concrete;
        }
    }
    selected
}

pub(super) fn git() -> PathBuf {
    GIT.with(Clone::clone)
}

pub(super) fn path() -> std::ffi::OsString {
    CLI_PATH.with(Clone::clone)
}

pub(super) fn cargo() -> sync::Arc<PathBuf> {
    COMPILER.with(sync::Arc::clone)
}

pub(super) fn configure(command: &mut Command) {
    // These controls inspect API/source identity and execute unoptimized code.
    command
        // Private fixtures do not inherit this checkout's static-CRT flags.
        // Resolver/lock configuration is still discovered by the real Cargo.
        .env(
            "CARGO_ENCODED_RUSTFLAGS",
            // Cargo supplies the compiler's DLL search path when running these
            // private examples. Avoid linking a separate static standard library
            // into every Windows fixture executable; assertions stay enabled.
            if cfg!(windows) {
                "-C\u{1f}prefer-dynamic"
            } else {
                ""
            },
        );
    compiler::configure(command);
    #[cfg(windows)]
    {
        command.env("TEMP", temp_dir()).env("TMP", temp_dir());
    }
}

struct Timing(std::time::Instant, String);
thread_local! {
    static TIMING: Timing = Timing(std::time::Instant::now(), std::thread::current().name().unwrap_or("unnamed").to_owned());
}
pub(super) fn time_control() {
    TIMING.with(|_| ());
}
pub(super) fn phase(name: &str) {
    TIMING.with(|timer| {
        eprintln!(
            "CONTROL_PHASE {} {name} {:.6}",
            timer.1,
            timer.0.elapsed().as_secs_f64()
        )
    });
}
impl Drop for Timing {
    fn drop(&mut self) {
        eprintln!(
            "CONTROL_TIME {} {:.6}",
            self.1,
            self.0.elapsed().as_secs_f64()
        );
    }
}
