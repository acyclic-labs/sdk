use super::*;
#[path = "../../../sdk-docs/src/sync.rs"]
mod sync;

thread_local! {
    static GIT: PathBuf = resolve_git();
    static COMPILER: sync::Arc<Toolchain> = resolve_cargo();
    #[cfg(windows)]
    static LINKER: (String, PathBuf) = resolve_linker();
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
    std::env::join_paths(
        [
            cargo().parent().unwrap().to_owned(),
            git().parent().unwrap().to_owned(),
        ]
        .into_iter()
        .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap()
}

// Leaf slot: initialization only runs explicit rustup probes, acquiring no
// fixture or SDK cache lock. Publish Cargo and its immutable fixture recordings
// together, and release this lock before any Cargo work or fixture locks.
struct Toolchain {
    cargo: sync::Arc<PathBuf>,
    probes: BTreeMap<Vec<std::ffi::OsString>, std::process::Output>,
}
static CARGO: sync::Mutex<Option<sync::Arc<Toolchain>>> = sync::Mutex::new(None);
// Independent leaf slots: constructors do not acquire another fixture slot.
static FIXTURES: [sync::Mutex<Option<sync::Arc<Fixture>>>; 3] =
    [const { sync::Mutex::new(None) }; 3];

pub(super) fn fixture(kind: u8) -> sync::Arc<Fixture> {
    sync::get_or_try_init(&FIXTURES[usize::from(kind)], || {
        let fixture = if kind == 2 {
            let fixture = feature_profile_source();
            init_git(&fixture.root, "feature profile fixture");
            fixture
        } else {
            build_fixture(kind == 1)
        };
        Ok::<_, std::convert::Infallible>(fixture)
    })
    .unwrap()
}

pub(super) fn cargo() -> sync::Arc<PathBuf> {
    COMPILER.with(|compiler| sync::Arc::clone(&compiler.cargo))
}

pub(super) fn rustc() -> PathBuf {
    cargo()
        .parent()
        .unwrap()
        .join(format!("rustc{}", std::env::consts::EXE_SUFFIX))
}

pub(super) fn configure(command: &mut Command) {
    // These controls inspect API/source identity and execute unoptimized code.
    // Debug symbol files and incremental compiler state are unused fixtures.
    command
        .env("CARGO_PROFILE_DEV_DEBUG", "0")
        .env("CARGO_PROFILE_DEV_INCREMENTAL", "false")
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
        )
        .env(
            "RUSTUP_TOOLCHAIN",
            cargo()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .file_name()
                .unwrap(),
        );
    #[cfg(windows)]
    {
        command.env("TEMP", temp_dir()).env("TMP", temp_dir());
        LINKER.with(|(key, linker)| {
            command.env(key, linker);
        });
    }
}

#[cfg(windows)]
fn resolve_linker() -> (String, PathBuf) {
    let cargo = cargo();
    let sysroot = cargo.parent().unwrap().parent().unwrap();
    let linkers = fs::read_dir(sysroot.join("lib/rustlib"))
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
    (
        format!(
            "CARGO_TARGET_{}_LINKER",
            host.to_str()
                .unwrap()
                .replace('-', "_")
                .to_ascii_uppercase()
        ),
        linker.clone(),
    )
}

fn resolve_cargo() -> sync::Arc<Toolchain> {
    sync::get_or_try_init(&CARGO, || {
        let commands = [
            vec!["which", "cargo", "--toolchain", "1.98.1"],
            vec!["which", "cargo", "--toolchain", "nightly-2026-07-01"],
            vec![
                "target",
                "list",
                "--installed",
                "--toolchain",
                "nightly-2026-07-01",
            ],
        ];
        let mut probes = BTreeMap::new();
        let mut cargo = None;
        for arguments in commands {
            let output = Command::new("rustup")
                .env("RUSTUP_AUTO_INSTALL", "0")
                .args(&arguments)
                .output()
                .unwrap();
            assert!(output.status.success(), "{}", output_message(&output));
            if cargo.is_none() {
                cargo = Some(sync::Arc::new(PathBuf::from(
                    String::from_utf8(output.stdout.clone()).unwrap().trim(),
                )));
            }
            probes.insert(
                arguments
                    .into_iter()
                    .map(std::ffi::OsString::from)
                    .collect(),
                output,
            );
        }
        Ok::<_, std::convert::Infallible>(Toolchain {
            cargo: cargo.unwrap(),
            probes,
        })
    })
    .unwrap()
}

// Real recorded provisioning outputs for this test binary's fixed, explicitly
// selected toolchains. Production selection and wrapper-freshness controls use
// the real executor. Only these exact explicit rustup commands are replayed.
pub(super) fn tool_output(command: &Command) -> Option<std::process::Output> {
    if command.get_program() != "rustup" {
        return None;
    }
    for (key, value) in command.get_envs() {
        if ["RUSTUP_HOME", "RUSTUP_DIST_SERVER", "RUSTUP_UPDATE_ROOT"]
            .iter()
            .any(|name| key.as_encoded_bytes().eq_ignore_ascii_case(name.as_bytes()))
            && value != std::env::var_os(key).as_deref()
        {
            return None;
        }
    }
    let arguments = command
        .get_args()
        .map(std::ffi::OsString::from)
        .collect::<Vec<_>>();
    COMPILER.with(|compiler| compiler.probes.get(&arguments).cloned())
}

pub(super) fn binary(name: &str) -> PathBuf {
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

struct Timing(std::time::Instant, String);
thread_local! {
    static TIMING: Timing = Timing(std::time::Instant::now(), std::thread::current().name().unwrap_or("unnamed").to_owned());
}
pub(super) fn time_control() {
    TIMING.with(|_| ());
    // Resolve this leaf before fixture initialization can acquire its own slot.
    let _ = cargo();
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
