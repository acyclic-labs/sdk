use super::*;

use std::{
    cell::{Cell, RefCell},
    ffi::OsString,
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
    let arguments = command.get_args().map(OsString::from).collect::<Vec<_>>();
    let first = arguments.first()?.to_str()?;
    let program = Path::new(command.get_program()).file_stem()?.to_str()?;
    let git = program == "git";
    let pure = match program {
        "git" => {
            matches!(first, "rev-parse" | "status" | "merge-base")
                || first == "--no-replace-objects"
                    && arguments.get(1).is_some_and(|arg| {
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
        arguments,
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

// Replay exactly the JSON that this real Cargo invocation produced. Different
// feature/target/owner arguments have different keys. Build scripts and examples
// in mutation controls never enter this scope.
fn compiler_outputs(command: &Command) -> std::io::Result<Vec<(PathBuf, Vec<u8>)>> {
    let mut args = command.get_args();
    if args.next().and_then(|arg| arg.to_str()) != Some("rustdoc") {
        return Ok(Vec::new());
    }
    let directory = command
        .get_envs()
        .find(|(key, _)| *key == "CARGO_TARGET_DIR")
        .and_then(|(_, value)| value)
        .map(PathBuf::from)
        .unwrap();
    let target = args.skip_while(|arg| *arg != "--target").nth(1).unwrap();
    let mut artifacts = Vec::new();
    for directory in [directory.join("doc"), directory.join(target).join("doc")] {
        if !directory.is_dir() {
            continue;
        }
        for entry in fs::read_dir(directory)? {
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

fn rustdoc_fixtures() -> std::io::Result<&'static [Value]> {
    static FIXTURES: std::sync::OnceLock<std::io::Result<Vec<Value>>> = std::sync::OnceLock::new();
    FIXTURES
        .get_or_init(|| {
            let index = acyclic_test_support::source_path(
                "rust/proofs/sdk-rustdoc-fixtures/feature_profiles.index.json",
            );
            match fs::read(index) {
                Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
                Err(error) => Err(error),
            }
        })
        .as_deref()
        .map_err(|error| std::io::Error::new(error.kind(), error.to_string()))
}

// The action's metadata names every source and Cargo flag in its key. Reuse
// its output only for byte-identical source and the same complete profile.
fn declared_rustdoc(command: &Command) -> std::io::Result<Option<Output>> {
    if command.get_args().next() != Some(std::ffi::OsStr::new("rustdoc")) {
        return Ok(None);
    }
    let args = command.get_args().collect::<Vec<_>>();
    if !args.iter().any(|arg| *arg == "--lib" || *arg == "--bin") {
        return Ok(None);
    }
    let value = |name: &str| {
        args.windows(2)
            .find(|pair| pair[0] == name)
            .map(|pair| pair[1])
    };
    let Some(manifest_path) = value("--manifest-path").map(Path::new) else {
        return Ok(None);
    };
    let Some(manifest) = manifest_path.to_str() else {
        return Ok(None);
    };
    let Some(package) = value("--package").and_then(|arg| arg.to_str()) else {
        return Ok(None);
    };
    let Some(target) = value("--target").and_then(|arg| arg.to_str()) else {
        return Ok(None);
    };
    let env = |name: &str| {
        command
            .get_envs()
            .find(|(key, _)| *key == name)
            .and_then(|(_, value)| value)
    };
    let rustflags = if cfg!(windows) {
        "-C\u{1f}prefer-dynamic"
    } else {
        ""
    };
    if env("CARGO_ENCODED_RUSTFLAGS").and_then(std::ffi::OsStr::to_str) != Some(rustflags) {
        return Ok(None);
    }
    if env("CARGO_HOME")
        .map(Path::new)
        .is_some_and(|home| home.join("config").exists() || home.join("config.toml").exists())
    {
        return Ok(None);
    }
    let Some(selected_toolchain) = env("RUSTUP_TOOLCHAIN")
        .and_then(std::ffi::OsStr::to_str)
        .and_then(|selected| selected.strip_suffix(target))
        .and_then(|selected| selected.strip_suffix('-'))
    else {
        return Ok(None);
    };
    for metadata in rustdoc_fixtures()? {
        if metadata["package"].as_str() != Some(package) {
            continue;
        }
        if metadata["toolchain"].as_str().unwrap() != selected_toolchain {
            continue;
        }
        let declared_manifest = Path::new(metadata["manifest_path"].as_str().unwrap());
        let mut root = manifest_path;
        for _ in declared_manifest.components() {
            let Some(parent) = root.parent() else {
                return Ok(None);
            };
            root = parent;
        }
        if root.join(declared_manifest) != manifest_path
            || root
                .ancestors()
                .any(|ancestor| ancestor.join(".cargo").exists())
        {
            continue;
        }
        let mut expected_args = vec![
            "rustdoc",
            "--locked",
            "--manifest-path",
            manifest,
            "--package",
            package,
        ];
        expected_args.extend(
            metadata["target_args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|flag| flag.as_str().unwrap()),
        );
        expected_args.extend(["--target", target]);
        expected_args.extend(
            metadata["cargo_flags"]
                .as_array()
                .unwrap()
                .iter()
                .map(|flag| flag.as_str().unwrap()),
        );
        let selected_lock = metadata["selected_lock"].as_str().unwrap();
        let config = if selected_lock.is_empty() {
            None
        } else {
            let Ok(lock) = root.join(selected_lock).canonicalize() else {
                continue;
            };
            Some(format!(
                "resolver.lockfile-path={}",
                serde_json::to_string(lock.to_str().unwrap())?
            ))
        };
        if let Some(config) = &config {
            expected_args.extend(["--config", config.as_str()]);
        }
        expected_args.extend(["--", "-Z", "unstable-options", "--output-format", "json"]);
        if args
            .iter()
            .map(|arg| arg.to_str())
            .collect::<Option<Vec<_>>>()
            .as_deref()
            != Some(expected_args.as_slice())
        {
            continue;
        }
        let sources = metadata["sources"].as_array().unwrap();
        let artifacts = metadata["artifacts"].as_str().unwrap();
        let mut relative_sources = Vec::new();
        let mut matches = true;
        for source in sources {
            let relative = source["relative"].as_str().unwrap();
            let source = source["file"].as_str().unwrap();
            let expected = fs::read(acyclic_test_support::source_path(source))?;
            if fs::read(root.join(relative)).ok().as_deref() != Some(expected.as_slice()) {
                matches = false;
                break;
            }
            if Path::new(relative)
                .file_name()
                .is_some_and(|name| name == "Cargo.toml")
            {
                let build = Path::new(relative).parent().unwrap().join("build.rs");
                if root.join(&build).exists()
                    && (artifacts.is_empty()
                        || !sources.iter().any(|source| {
                            source["relative"].as_str().map(Path::new) == Some(build.as_path())
                        }))
                {
                    matches = false;
                    break;
                }
            }
            if relative.ends_with(".rs") {
                relative_sources.push(Path::new(relative));
            }
        }
        if !matches {
            continue;
        }
        let output_path = metadata["output"].as_str().unwrap();
        let mut json: Value =
            serde_json::from_slice(&fs::read(acyclic_test_support::source_path(output_path))?)?;
        if json["target"]["triple"].as_str() != Some(target) {
            continue;
        }
        let target_dir = Path::new(env("CARGO_TARGET_DIR").unwrap());
        let mut referenced = std::collections::BTreeSet::new();
        let artifact_target = target_dir.join(Path::new(output_path).file_stem().unwrap());
        let owner = metadata["owner_path"].as_str().unwrap();
        let entrypoint = Path::new(metadata["entrypoint"].as_str().unwrap());
        let owner_entrypoint = Path::new(owner).join(entrypoint);
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
                if let Some(source) = relative_sources.iter().find(|source| {
                    filename.ends_with(source)
                        || filename == entrypoint && **source == owner_entrypoint.as_path()
                }) {
                    *path = root.join(source).to_string_lossy().into_owned();
                }
            }
        });
        if !referenced.is_empty() {
            let artifacts = acyclic_test_support::source_path(artifacts);
            for relative in referenced {
                let destination = artifact_target.join(&relative);
                let source = artifacts.join(relative);
                if destination.exists() {
                    if fs::read(&destination)? != fs::read(&source)? {
                        return Err(std::io::Error::other(format!(
                            "Rustdoc fixture artifacts disagree at {}",
                            destination.display()
                        )));
                    }
                } else {
                    fs::create_dir_all(destination.parent().unwrap())?;
                    fs::copy(source, destination)?;
                }
            }
        }
        let output_dir = if metadata["proc_macro"].as_bool() == Some(true) {
            target_dir.join("doc")
        } else {
            target_dir.join(target).join("doc")
        };
        let output = output_dir.join(format!(
            "{}.json",
            metadata["receipt_name"].as_str().unwrap()
        ));
        fs::create_dir_all(output.parent().unwrap())?;
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
                        if !fs::read(path).is_ok_and(|existing| &existing == bytes) {
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
        proptest::prop_assert_eq!(query(&command).unwrap(), original.clone());
        command.env("RUSTUP_TOOLCHAIN", &value);
        proptest::prop_assert_ne!(query(&command).unwrap(), original.clone());
        command.env_remove("RUSTUP_TOOLCHAIN");
        let mut missing = Command::new("cargo"); missing.args(["metadata", "--locked"]);
        proptest::prop_assert_ne!(query(&missing).unwrap(), original);
        // Negative control: discarding authority arguments aliases these two.
        proptest::prop_assert_eq!(command.get_program(), missing.get_program());
        proptest::prop_assert_eq!(command.get_args().next(), missing.get_args().next());
        let mut compiler = Command::new("cargo"); compiler.args(["rustdoc", "--locked"]);
        let compiler_key = query(&compiler).unwrap();
        compiler.args(["--features", &argument]);
        proptest::prop_assert_ne!(query(&compiler).unwrap(), compiler_key);
        let mut example = Command::new("cargo"); example.args(["run", "--example", "historical"]);
        proptest::prop_assert!(query(&example).is_none());
    }
}
