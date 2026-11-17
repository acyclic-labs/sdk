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

#[derive(Default)]
struct State {
    outputs: RefCell<BTreeMap<Key, (Output, Vec<(PathBuf, Vec<u8>)>)>>,
    executed: Cell<usize>,
    replayed: Cell<usize>,
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
    let args = command.get_args().collect::<Vec<_>>();
    if args.first().and_then(|arg| arg.to_str()) != Some("rustdoc") {
        return Ok(Vec::new());
    }
    let directory = command
        .get_envs()
        .find(|(key, _)| *key == "CARGO_TARGET_DIR")
        .and_then(|(_, value)| value)
        .map(PathBuf::from)
        .unwrap();
    let target = args.windows(2).find(|pair| pair[0] == "--target").unwrap()[1];
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
                artifacts.push((path.clone(), fs::read(path)?));
            }
        }
    }
    assert!(
        !artifacts.is_empty(),
        "successful real compiler must produce its JSON"
    );
    Ok(artifacts)
}

thread_local! { static ACTIVE: Cell<bool> = const { Cell::new(false) }; }

pub(super) fn execute<T>(operation: impl FnOnce() -> T) -> T {
    if ACTIVE.get() {
        return operation();
    }
    sdk_docs::with_command_executor(execute_command, operation)
}

fn execute_command(command: &mut Command) -> std::io::Result<Output> {
    if let Some(output) = declared_rustdoc(command)? {
        return Ok(output);
    }
    command.output()
}

// The action's metadata names every source and Cargo flag in its key. Reuse
// its output only for byte-identical source and the same complete profile.
fn declared_rustdoc(command: &Command) -> std::io::Result<Option<Output>> {
    let args = command.get_args().collect::<Vec<_>>();
    let value = |name: &str| {
        args.windows(2)
            .find(|pair| pair[0] == name)
            .map(|pair| pair[1])
    };
    if args.first().and_then(|arg| arg.to_str()) != Some("rustdoc")
        || !args.iter().any(|arg| *arg == "--lib")
        || args.iter().any(|arg| *arg == "--config")
    {
        return Ok(None);
    }
    let Some(root) = value("--manifest-path")
        .map(Path::new)
        .and_then(Path::parent)
    else {
        return Ok(None);
    };
    let Some(package) = value("--package").and_then(|arg| arg.to_str()) else {
        return Ok(None);
    };
    let Some(target) = value("--target").and_then(|arg| arg.to_str()) else {
        return Ok(None);
    };
    let environment = command.get_envs().collect::<BTreeMap<_, _>>();
    if environment
        .get(std::ffi::OsStr::new("RUSTUP_TOOLCHAIN"))
        .copied()
        .flatten()
        .and_then(|v| v.to_str())
        != Some(format!("nightly-2026-07-01-{target}").as_str())
    {
        return Ok(None);
    }
    let rustflags = if cfg!(windows) {
        "-C\u{1f}prefer-dynamic"
    } else {
        ""
    };
    if environment
        .get(std::ffi::OsStr::new("CARGO_ENCODED_RUSTFLAGS"))
        .copied()
        .flatten()
        .and_then(|v| v.to_str())
        != Some(rustflags)
    {
        return Ok(None);
    }
    if root
        .ancestors()
        .any(|ancestor| ancestor.join(".cargo").exists())
    {
        return Ok(None);
    }
    if command.get_envs().any(|(key, value)| {
        key == "CARGO_HOME"
            && value.is_some_and(|home| {
                let home = Path::new(home);
                home.join("config").exists() || home.join("config.toml").exists()
            })
    }) {
        return Ok(None);
    }
    let directory = acyclic_test_support::source_path("rust/proofs/sdk-rustdoc-fixtures");
    let Ok(entries) = fs::read_dir(directory) else {
        return Ok(None);
    };
    for entry in entries {
        let entry = entry?;
        if !entry
            .file_name()
            .to_string_lossy()
            .ends_with(".fixture.json")
        {
            continue;
        }
        let metadata: Value = serde_json::from_slice(&fs::read(entry.path())?)?;
        if metadata["package"].as_str() != Some(package) {
            continue;
        }
        let declared = metadata["cargo_flags"].as_array().unwrap();
        let mut flags = Vec::new();
        if args.iter().any(|arg| *arg == "--no-default-features") {
            flags.push("--no-default-features");
        }
        if let Some(feature) = value("--features").and_then(|arg| arg.to_str()) {
            flags.extend(["--features", feature]);
        }
        if declared
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            != flags
        {
            continue;
        }
        let manifest = value("--manifest-path").unwrap().to_str().unwrap();
        let mut expected_args = vec![
            "rustdoc",
            "--locked",
            "--manifest-path",
            manifest,
            "--package",
            package,
            "--lib",
            "--target",
            target,
        ];
        expected_args.extend(flags);
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
        let fixture_root = metadata["fixture_root"].as_str().unwrap();
        let sources = metadata["sources"].as_array().unwrap();
        let mut relative_sources = Vec::new();
        let mut matches = true;
        for source in sources {
            let source = source.as_str().unwrap();
            let Some(relative) = source.strip_prefix(fixture_root) else {
                matches = false;
                break;
            };
            let expected = fs::read(acyclic_test_support::source_path(source))?;
            if fs::read(root.join(relative)).ok().as_deref() != Some(expected.as_slice()) {
                matches = false;
                break;
            }
            if relative.ends_with("/Cargo.toml")
                && root
                    .join(relative)
                    .parent()
                    .unwrap()
                    .join("build.rs")
                    .exists()
            {
                matches = false;
                break;
            }
            if relative.ends_with(".rs") {
                relative_sources.push(relative.to_owned());
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
        fn relocate(value: &mut Value, root: &Path, sources: &[String]) {
            match value {
                Value::Object(object) => {
                    if let Some(Value::Object(span)) = object.get_mut("span") {
                        if let Some(Value::String(filename)) = span.get_mut("filename") {
                            let normalized = filename.replace('\\', "/");
                            if let Some(source) = sources.iter().find(|source| {
                                normalized.ends_with(source.as_str())
                                    || normalized == "src/lib.rs" && source.ends_with("/src/lib.rs")
                            }) {
                                *filename = root.join(source).to_string_lossy().into_owned();
                            }
                        }
                    }
                    for child in object.values_mut() {
                        relocate(child, root, sources);
                    }
                }
                Value::Array(array) => {
                    for child in array {
                        relocate(child, root, sources);
                    }
                }
                _ => {}
            }
        }
        relocate(&mut json, root, &relative_sources);
        let target_dir = command
            .get_envs()
            .find(|(key, _)| *key == "CARGO_TARGET_DIR")
            .and_then(|(_, value)| value)
            .map(Path::new)
            .unwrap();
        let output = target_dir
            .join(target)
            .join("doc")
            .join(format!("{}.json", package.replace('-', "_")));
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
                        fs::write(path, bytes)?;
                    }
                    state.replayed.set(state.replayed.get() + 1);
                    if !artifacts.is_empty() {
                        state
                            .replayed_compilers
                            .set(state.replayed_compilers.get() + 1);
                    }
                    return Ok(output.clone());
                }
                let output = execute_command(command)?;
                state.executed.set(state.executed.get() + 1);
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

    pub(super) fn counts(&self) -> (usize, usize) {
        (self.0.executed.get(), self.0.replayed.get())
    }
}

impl Drop for State {
    fn drop(&mut self) {
        eprintln!(
            "POLICY_COMMANDS executed={} replayed={}",
            self.executed.get(),
            self.replayed.get()
        );
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
