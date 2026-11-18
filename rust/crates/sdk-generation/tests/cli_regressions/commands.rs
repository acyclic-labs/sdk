use super::*;

use std::{
    cell::{Cell, RefCell},
    ffi::{OsStr, OsString},
    process::Output,
    rc::Rc,
};

// Only immutable policy fixtures use this scope. Source mutations, wrapper
// freshness, compiler matrices and independent publisher processes stay real. Only the
// duplicate compiler invocation of an identical policy profile is replayed.
// Record stdout/stderr/status from maintained tools; validation is never replayed.
#[derive(Default, Clone)]
pub(super) struct Queries(Rc<State>);

type Replay = (Output, Vec<(PathBuf, Vec<u8>)>);

#[derive(Default)]
struct State {
    outputs: RefCell<BTreeMap<Key, Replay>>,
    replayed_compilers: Cell<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    program: OsString,
    arguments: Vec<OsString>,
    directory: Option<PathBuf>,
    environment: Vec<(OsString, Option<OsString>)>,
}

fn query(command: &Command) -> Option<Key> {
    let mut arguments = command.get_args();
    let first = arguments.next()?.to_str()?;
    let program = Path::new(command.get_program()).file_stem()?.to_str()?;
    let git = program == "git";
    let pure = match program {
        "git" => {
            matches!(first, "rev-parse" | "status" | "merge-base")
                || first == "--no-replace-objects"
                    && arguments.next().is_some_and(|arg| {
                        matches!(arg.to_str(), Some("rev-parse" | "log" | "ls-tree"))
                    })
        }
        "cargo" => matches!(first, "metadata" | "-vV" | "rustdoc"),
        "rustup" => matches!(first, "which" | "target"),
        "rustc" => matches!(first, "-vV" | "--print"),
        _ => false,
    };
    pure.then(|| Key {
        program: command.get_program().to_owned(),
        arguments: command.get_args().map(OsString::from).collect(),
        directory: git
            .then(|| command.get_current_dir().map(PathBuf::from))
            .flatten(),
        // These are the deliberately varied policy authorities. The production
        // discovery fingerprint still reads them; explicit --config arguments
        // remain in the key, so removing an owned lock cannot replay its output.
        environment: command
            .get_envs()
            .filter(|(key, _)| {
                !key.as_encoded_bytes().eq_ignore_ascii_case(b"CARGO_HOME")
                    && !key
                        .as_encoded_bytes()
                        .eq_ignore_ascii_case(b"CARGO_RESOLVER_LOCKFILE_PATH")
            })
            .map(|(key, value)| (key.to_owned(), value.map(OsString::from)))
            .collect(),
    })
}

fn argument_value<'a>(args: impl Iterator<Item = &'a OsStr>, name: &str) -> Option<&'a OsStr> {
    args.skip_while(|arg| *arg != name).nth(1)
}

fn command_env<'a>(command: &'a Command, name: &str) -> Option<&'a OsStr> {
    command
        .get_envs()
        .find(|(key, _)| *key == name)
        .and_then(|(_, value)| value)
}

fn rustdoc_directory(target_dir: &Path, target: Option<&OsStr>) -> PathBuf {
    target.map_or_else(
        || target_dir.join("doc"),
        |target| target_dir.join(target).join("doc"),
    )
}

fn optional_io<T>(result: std::io::Result<T>) -> std::io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

// Replay exactly the JSON that this real Cargo invocation produced. Different
// feature/target/owner arguments have different keys. Build scripts and examples
// in mutation controls never enter this scope.
fn compiler_outputs(command: &Command) -> std::io::Result<Vec<(PathBuf, Vec<u8>)>> {
    let mut args = command.get_args();
    if args.next().and_then(|arg| arg.to_str()) != Some("rustdoc") {
        return Ok(Vec::new());
    }
    let directory = Path::new(command_env(command, "CARGO_TARGET_DIR").unwrap());
    let target = argument_value(args, "--target").unwrap();
    let mut artifacts = Vec::new();
    for target in [None, Some(target)] {
        let Some(entries) = optional_io(fs::read_dir(rustdoc_directory(directory, target)))? else {
            continue;
        };
        for entry in entries {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                let bytes = fs::read(&path)?;
                artifacts.push((path, bytes));
            }
        }
    }
    assert!(
        !artifacts.is_empty(),
        "successful real compiler must produce its JSON"
    );
    Ok(artifacts)
}

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static DECLARED_RUSTDOC_HITS: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn expect_declared_profiles(expected: usize, run: impl FnOnce()) {
    let hits = DECLARED_RUSTDOC_HITS.get();
    run();
    if std::env::var_os("ACYCLIC_TEST_SOURCE_ROOT").is_some() {
        assert_eq!(DECLARED_RUSTDOC_HITS.get() - hits, expected);
    }
}

pub(super) fn execute<T>(operation: impl FnOnce() -> T) -> T {
    if ACTIVE.get() {
        return operation();
    }
    sdk_docs::with_command_executor(execute_command, operation)
}

fn execute_command(command: &mut Command) -> std::io::Result<Output> {
    if let Some(output) = declared_rustdoc(command)? {
        DECLARED_RUSTDOC_HITS.set(DECLARED_RUSTDOC_HITS.get() + 1);
        return Ok(output);
    }
    command.output()
}

#[derive(serde::Deserialize)]
struct FixtureMetadata {
    package: String,
    toolchain: String,
    manifest_path: PathBuf,
    owner_path: PathBuf,
    selected_lock: Option<PathBuf>,
    receipt_name: String,
    target_args: Vec<String>,
    entrypoint: PathBuf,
    proc_macro: bool,
    cargo_flags: Vec<String>,
    sources: Vec<FixtureSource>,
    output: PathBuf,
    artifacts: Option<PathBuf>,
}

#[derive(serde::Deserialize)]
struct FixtureSource {
    file: PathBuf,
    relative: PathBuf,
}

fn rustdoc_fixtures() -> std::io::Result<&'static [FixtureMetadata]> {
    static FIXTURES: std::sync::OnceLock<std::io::Result<Vec<FixtureMetadata>>> =
        std::sync::OnceLock::new();
    FIXTURES
        .get_or_init(|| {
            let index = acyclic_test_support::source_path(
                "rust/proofs/sdk-rustdoc-fixtures/feature_profiles.index.json",
            );
            match optional_io(fs::read(index))? {
                Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
                None => Ok(Vec::new()),
            }
        })
        .as_deref()
        .map_err(|error| std::io::Error::new(error.kind(), error.to_string()))
}

// The action's metadata names every source and Cargo flag in its key. Reuse
// its output only for byte-identical source and the same complete profile.
fn declared_rustdoc(command: &Command) -> std::io::Result<Option<Output>> {
    if command.get_args().next() != Some(OsStr::new("rustdoc")) {
        return Ok(None);
    }
    let value = |name: &str| argument_value(command.get_args(), name).and_then(OsStr::to_str);
    let Some(manifest) = value("--manifest-path") else {
        return Ok(None);
    };
    let manifest_path = Path::new(manifest);
    let Some(package) = value("--package") else {
        return Ok(None);
    };
    let Some(target) = value("--target") else {
        return Ok(None);
    };
    let env = |name: &str| command_env(command, name);
    let rustflags = if cfg!(windows) {
        "-C\u{1f}prefer-dynamic"
    } else {
        ""
    };
    if env("CARGO_ENCODED_RUSTFLAGS").and_then(OsStr::to_str) != Some(rustflags) {
        return Ok(None);
    }
    if env("CARGO_HOME")
        .map(Path::new)
        .is_some_and(|home| home.join("config").exists() || home.join("config.toml").exists())
    {
        return Ok(None);
    }
    let Some(selected_toolchain) = env("RUSTUP_TOOLCHAIN")
        .and_then(OsStr::to_str)
        .and_then(|selected| selected.strip_suffix(target))
        .and_then(|selected| selected.strip_suffix('-'))
    else {
        return Ok(None);
    };
    'fixture: for metadata in rustdoc_fixtures()? {
        if metadata.package != package {
            continue;
        }
        if metadata.toolchain != selected_toolchain {
            continue;
        }
        let declared_manifest = &metadata.manifest_path;
        if !manifest_path.ends_with(declared_manifest) {
            continue;
        }
        let mut prefix = [
            "rustdoc",
            "--locked",
            "--manifest-path",
            manifest,
            "--package",
            package,
        ]
        .into_iter()
        .chain(metadata.target_args.iter().map(String::as_str))
        .chain(["--target", target])
        .chain(metadata.cargo_flags.iter().map(String::as_str));
        let mut actual = command.get_args().map(OsStr::to_str);
        if !prefix.all(|expected| actual.next() == Some(Some(expected))) {
            continue;
        }
        let Some(root) = manifest_path
            .ancestors()
            .nth(declared_manifest.components().count())
        else {
            return Ok(None);
        };
        let config = if let Some(selected_lock) = &metadata.selected_lock {
            let Some(lock) = optional_io(root.join(selected_lock).canonicalize())? else {
                continue;
            };
            Some(format!(
                "resolver.lockfile-path={}",
                serde_json::to_string(lock.to_str().unwrap())?
            ))
        } else {
            None
        };
        let suffix = config
            .as_deref()
            .into_iter()
            .flat_map(|value| ["--config", value])
            .chain(["--", "-Z", "unstable-options", "--output-format", "json"]);
        if !actual.eq(suffix.map(Some)) {
            continue;
        }
        for ancestor in root.ancestors() {
            if ancestor.join(".cargo").try_exists()? {
                continue 'fixture;
            }
        }
        let sources = &metadata.sources;
        let artifacts = metadata.artifacts.as_deref();
        let mut source_paths = Vec::new();
        for source in sources {
            let relative = source.relative.as_path();
            let checkout_source = root.join(relative);
            let Some(actual) = optional_io(fs::read(&checkout_source))? else {
                continue 'fixture;
            };
            if actual != fs::read(acyclic_test_support::source_path(&source.file))? {
                continue 'fixture;
            }
            if relative
                .file_name()
                .is_some_and(|name| name == "Cargo.toml")
            {
                let build = relative.parent().unwrap().join("build.rs");
                let covered =
                    artifacts.is_some() && sources.iter().any(|source| source.relative == build);
                if !covered && root.join(&build).try_exists()? {
                    continue 'fixture;
                }
            }
            if relative.extension() == Some(OsStr::new("rs")) {
                source_paths.push((relative, checkout_source.to_string_lossy().into_owned()));
            }
        }
        let output_path = &metadata.output;
        let mut json: Value =
            serde_json::from_slice(&fs::read(acyclic_test_support::source_path(output_path))?)?;
        if json["target"]["triple"].as_str() != Some(target) {
            continue;
        }
        let target_dir = Path::new(env("CARGO_TARGET_DIR").unwrap());
        let mut referenced = std::collections::BTreeSet::new();
        let artifact_target = target_dir.join(output_path.file_stem().unwrap());
        let owner = &metadata.owner_path;
        let entrypoint = metadata.entrypoint.as_path();
        let owner_entrypoint = owner.join(entrypoint);
        rustdoc_paths::visit(&mut json, |kind, path| {
            if let Some(relative) = path.strip_prefix(rustdoc_paths::TARGET_PREFIX) {
                let relative = PathBuf::from(relative);
                *path = artifact_target
                    .join(&relative)
                    .to_string_lossy()
                    .into_owned();
                referenced.insert(relative);
            } else if matches!(kind, rustdoc_paths::Kind::Source) {
                let filename = Path::new(path);
                if let Some((_, replacement)) = source_paths.iter().find(|(source, _)| {
                    filename.ends_with(source)
                        || filename == entrypoint && **source == owner_entrypoint.as_path()
                }) {
                    path.clone_from(replacement);
                }
            }
        });
        if !referenced.is_empty() {
            let Some(artifacts) = artifacts else {
                return Err(std::io::Error::other(
                    "Rustdoc fixture references undeclared artifacts",
                ));
            };
            let artifacts = acyclic_test_support::source_path(artifacts);
            for relative in referenced {
                let destination = artifact_target.join(&relative);
                let source = artifacts.join(relative);
                match optional_io(fs::read(&destination))? {
                    Some(existing) if existing == fs::read(&source)? => {}
                    Some(_) => {
                        return Err(std::io::Error::other(format!(
                            "Rustdoc fixture artifacts disagree at {}",
                            destination.display()
                        )));
                    }
                    None => {
                        fs::create_dir_all(destination.parent().unwrap())?;
                        fs::copy(source, destination)?;
                    }
                }
            }
        }
        let target = (!metadata.proc_macro).then_some(OsStr::new(target));
        let output_dir = rustdoc_directory(target_dir, target);
        let output = output_dir.join(format!("{}.json", metadata.receipt_name));
        fs::create_dir_all(&output_dir)?;
        fs::write(output, serde_json::to_vec(&json)?)?;
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt;
        #[cfg(windows)]
        use std::os::windows::process::ExitStatusExt;
        return Ok(Some(Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }));
    }
    Ok(None)
}

impl Queries {
    pub(super) fn run<T>(&self, operation: impl FnOnce() -> T) -> T {
        struct Restore(bool);
        impl Drop for Restore {
            fn drop(&mut self) {
                ACTIVE.set(self.0);
            }
        }
        let _restore = Restore(ACTIVE.replace(true));
        let state = Rc::clone(&self.0);
        sdk_docs::with_command_executor(
            move |command| {
                let Some(key) = query(command) else {
                    return command.output();
                };
                if let Some((output, artifacts)) = state.outputs.borrow().get(&key) {
                    for (path, bytes) in artifacts {
                        if optional_io(fs::read(path))?.as_ref() != Some(bytes) {
                            fs::write(path, bytes)?;
                        }
                    }
                    if !artifacts.is_empty() {
                        state
                            .replayed_compilers
                            .set(state.replayed_compilers.get() + 1);
                    }
                    return Ok(output.clone());
                }
                let output = execute_command(command)?;
                let artifacts = if output.status.success() {
                    compiler_outputs(command)?
                } else {
                    Vec::new()
                };
                state
                    .outputs
                    .borrow_mut()
                    .insert(key, (output.clone(), artifacts));
                Ok(output)
            },
            operation,
        )
    }

    pub(super) fn compiler_counts(&self) -> (usize, usize) {
        (
            self.0
                .outputs
                .borrow()
                .values()
                .filter(|(_, artifacts)| !artifacts.is_empty())
                .count(),
            self.0.replayed_compilers.get(),
        )
    }
}

proptest::proptest! {
    #[test]
    fn policy_replay_keys_preserve_every_argument_and_nonpolicy_environment(
        argument in "[a-z-]{1,12}", value in "[a-z]{1,12}",
    ) {
        runtime::time_control();
        let mut command = Command::new("cargo");
        command.args(["metadata", "--locked", "--config", &argument]);
        let original = query(&command).unwrap();
        command.env("CARGO_HOME", &value);
        proptest::prop_assert_eq!(&query(&command).unwrap(), &original);
        command.env("RUSTUP_TOOLCHAIN", &value);
        proptest::prop_assert_ne!(&query(&command).unwrap(), &original);
        command.env_remove("RUSTUP_TOOLCHAIN");
        let mut missing = Command::new("cargo"); missing.args(["metadata", "--locked"]);
        // Negative control: discarding authority arguments aliases these two.
        proptest::prop_assert_ne!(query(&missing).unwrap(), original);
        let mut compiler = Command::new("cargo"); compiler.args(["rustdoc", "--locked"]);
        let compiler_key = query(&compiler).unwrap();
        compiler.args(["--features", &argument]);
        proptest::prop_assert_ne!(query(&compiler).unwrap(), compiler_key);
        let mut example = Command::new("cargo"); example.args(["run", "--example", "historical"]);
        proptest::prop_assert!(query(&example).is_none());
    }
}
