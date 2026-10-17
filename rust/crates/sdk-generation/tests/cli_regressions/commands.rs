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
    sdk_docs::with_command_executor(
        |command| {
            runtime::tool_output(command)
                .map(Ok)
                .unwrap_or_else(|| command.output())
        },
        operation,
    )
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
                if let Some(output) = runtime::tool_output(command) {
                    return Ok(output);
                }
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
                let output = command.output()?;
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
