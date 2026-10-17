use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    path::PathBuf,
    process::{Command, Output},
};

type Overrides = Vec<(OsString, Option<OsString>)>;
thread_local! {
    // Calling-thread ownership; no process environment mutation or shared lock.
    static OVERRIDES: RefCell<Overrides> = const { RefCell::new(Vec::new()) };
    static DIRECTORY: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    #[cfg(any(test, feature = "test-support"))]
    static EXECUTOR: RefCell<Option<std::rc::Rc<dyn Fn(&mut Command) -> std::io::Result<Output>>>> = const { RefCell::new(None) };
}

pub(crate) fn overrides() -> Overrides {
    OVERRIDES.with_borrow(Clone::clone)
}

/// Scopes explicit child-command environment overrides without changing process state.
pub fn with_command_environment<T>(command: &Command, operation: impl FnOnce() -> T) -> T {
    struct Restore(Overrides, Option<PathBuf>);
    impl Drop for Restore {
        fn drop(&mut self) {
            OVERRIDES.set(std::mem::take(&mut self.0));
            DIRECTORY.set(self.1.take());
        }
    }
    let mut inherited = overrides();
    inherited.extend(
        command
            .get_envs()
            .map(|(key, value)| (key.to_owned(), value.map(OsStr::to_owned))),
    );
    let directory = command
        .get_current_dir()
        .map(|path| std::path::absolute(path).unwrap_or_else(|_| path.to_owned()))
        .or_else(command_directory);
    let _restore = Restore(OVERRIDES.replace(inherited), DIRECTORY.replace(directory));
    operation()
}

fn overlay(
    base: impl IntoIterator<Item = (OsString, OsString)>,
    changes: Overrides,
    windows: bool,
) -> BTreeMap<OsString, OsString> {
    let mut values = base.into_iter().collect::<BTreeMap<_, _>>();
    for (key, value) in changes {
        values.retain(|old, _| {
            !(old == &key
                || windows
                    && old
                        .as_encoded_bytes()
                        .eq_ignore_ascii_case(key.as_encoded_bytes()))
        });
        if let Some(value) = value {
            values.insert(key, value);
        }
    }
    values
}

/// Returns the effective child environment for configuration and authority checks.
pub fn command_environment() -> BTreeMap<OsString, OsString> {
    overlay(std::env::vars_os(), overrides(), cfg!(windows))
}

/// Constructs a standard child command with the calling operation's explicit environment.
pub fn command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    configure(&mut command);
    command
}

pub(crate) fn configure(command: &mut Command) {
    if command.get_current_dir().is_none() {
        if let Some(directory) = command_directory() {
            command.current_dir(directory);
        }
    }
    let explicit = command
        .get_envs()
        .map(|(key, value)| (key.to_owned(), value.map(OsStr::to_owned)))
        .collect::<Vec<_>>();
    for (key, value) in overrides().into_iter().chain(explicit) {
        match value {
            Some(value) => {
                command.env(key, value);
            }
            None => {
                command.env_remove(key);
            }
        }
    }
}

#[doc(hidden)]
pub fn command_directory() -> Option<PathBuf> {
    DIRECTORY.with_borrow(Clone::clone)
}

// Shared only by the unpublished documentation/generation crates. Production
// always executes std::process::Command; test scopes can substitute outputs.
#[doc(hidden)]
pub trait CommandOutput {
    fn run_output(&mut self) -> std::io::Result<Output>;
}

impl CommandOutput for Command {
    fn run_output(&mut self) -> std::io::Result<Output> {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(executor) = EXECUTOR.with_borrow(Clone::clone) {
            return executor(self);
        }
        self.output()
    }
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn with_command_executor<T>(
    executor: impl Fn(&mut Command) -> std::io::Result<Output> + 'static,
    operation: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<std::rc::Rc<dyn Fn(&mut Command) -> std::io::Result<Output>>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXECUTOR.set(self.0.take());
        }
    }
    let _restore = Restore(EXECUTOR.replace(Some(std::rc::Rc::new(executor))));
    operation()
}

#[doc(hidden)]
pub fn execute_metadata(
    command: &cargo_metadata::MetadataCommand,
) -> cargo_metadata::Result<cargo_metadata::Metadata> {
    let output = command.cargo_command().run_output()?;
    if !output.status.success() {
        return Err(cargo_metadata::Error::CargoMetadata {
            stderr: String::from_utf8(output.stderr)?,
        });
    }
    let text = std::str::from_utf8(&output.stdout)?;
    let json = text
        .lines()
        .find(|line| line.starts_with('{'))
        .ok_or(cargo_metadata::Error::NoJson)?;
    cargo_metadata::MetadataCommand::parse(json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn overrides_remove_aliases_and_preserve_unrelated_values(
            values in prop::collection::btree_map("[A-Z]{1,8}", ".{0,16}", 0..12),
            name in "[A-Z]{1,8}", value in prop::option::of(".{0,16}"), windows in any::<bool>(),
        ) {
            let base = values.iter().map(|(key,value)| (key.into(), value.into())).collect::<Vec<_>>();
            let changes = vec![(name.to_ascii_lowercase().into(), value.clone().map(Into::into))];
            let actual = overlay(base.clone(), changes.clone(), windows);
            for (key, previous) in values {
                if !windows || key != name { prop_assert_eq!(actual.get(OsStr::new(&key)), Some(&OsString::from(previous))); }
            }
            prop_assert_eq!(actual.get(OsStr::new(&name.to_ascii_lowercase())).cloned(), value.as_ref().map(OsString::from));
            if windows { prop_assert!(!actual.contains_key(OsStr::new(&name))); }
            prop_assert_eq!(overlay(actual.clone(), changes, windows), actual);
        }

        #[test]
        fn nested_scopes_configure_commands_without_mutating_the_process(a in "[a-z]{1,16}", b in "[a-z]{1,16}") {
            let environment = std::env::var_os("CARGO_HOME");
            let before = overrides();
            let mut outer = Command::new("cargo"); outer.env("CARGO_HOME", &a);
            with_command_environment(&outer, || {
                let mut inner = Command::new("cargo"); inner.env("CARGO_HOME", &b).env_remove("CARGO_RESOLVER_LOCKFILE_PATH");
                with_command_environment(&inner, || {
                    let child = command("cargo");
                    let home = child.get_envs().find(|(key,_)| *key == "CARGO_HOME").unwrap().1.unwrap();
                    prop_assert_eq!(home, OsStr::new(&b));
                    prop_assert_eq!(command_environment().get(OsStr::new("CARGO_HOME")).cloned(), Some(OsString::from(&b)));
                    prop_assert!(!command_environment().contains_key(OsStr::new("CARGO_RESOLVER_LOCKFILE_PATH")));
                    let mut explicit = command("cargo");
                    explicit.env("CARGO_HOME", "explicit").env("RUSTC", "pinned");
                    crate::rustdoc_profiles::CargoExecutionContext::default().configure(&mut explicit).unwrap();
                    prop_assert_eq!(explicit.get_envs().find(|(key,_)| *key == "CARGO_HOME").unwrap().1, Some(OsStr::new("explicit")));
                    prop_assert_eq!(explicit.get_envs().find(|(key,_)| *key == "RUSTC").unwrap().1, Some(OsStr::new("pinned")));
                    Ok(())
                })?;
                prop_assert_eq!(command_environment().get(OsStr::new("CARGO_HOME")).cloned(), Some(OsString::from(&a)));
                Ok(())
            })?;
            prop_assert_eq!(overrides(), before);
            prop_assert_eq!(std::env::var_os("CARGO_HOME"), environment);
        }
    }

    fn success(stdout: Vec<u8>) -> Output {
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt;
        #[cfg(windows)]
        use std::os::windows::process::ExitStatusExt;
        Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        }
    }

    proptest! {
        #[test]
        fn working_directory_scopes_are_explicit_and_never_change_process_state(a in "[a-z]{1,12}", b in "[a-z]{1,12}") {
            let before = std::env::current_dir().unwrap();
            let previous = command_directory();
            let mut outer = Command::new("cargo"); outer.current_dir(before.join(&a));
            with_command_environment(&outer, || {
                prop_assert_eq!(command("cargo").get_current_dir().map(PathBuf::from), Some(before.join(&a)));
                let mut inner = Command::new("cargo"); inner.current_dir(before.join(&b));
                with_command_environment(&inner, || {
                    let mut explicit = command("cargo"); explicit.current_dir(&before);
                    configure(&mut explicit);
                    prop_assert_eq!(explicit.get_current_dir(), Some(before.as_path()));
                    prop_assert_eq!(command("cargo").get_current_dir().map(PathBuf::from), Some(before.join(&b)));
                    Ok(())
                })?;
                prop_assert_eq!(command_directory(), Some(before.join(&a)));
                Ok(())
            })?;
            prop_assert_eq!(command_directory(), previous);
            prop_assert_eq!(std::env::current_dir().unwrap(), before);
        }

        #[test]
        fn metadata_command_outputs_round_trip_through_the_maintained_parser(
            values in prop::collection::btree_map("[a-z]{1,12}", ".{0,16}", 0..12),
        ) {
            let value = serde_json::json!({"packages":[], "workspace_members":[],
                "workspace_default_members":[], "resolve":null, "target_directory":"/checkout/target",
                "workspace_root":"/checkout", "metadata":values, "version":1});
            let bytes = format!("cargo diagnostic\n{}\n", serde_json::to_string(&value).unwrap()).into_bytes();
            let result = with_command_executor(move |_| Ok(success(bytes.clone())), || {
                execute_metadata(&cargo_metadata::MetadataCommand::new())
            }).unwrap();
            prop_assert_eq!(&result.workspace_metadata, &value["metadata"]);
            let serialized = serde_json::to_string(&result).unwrap();
            let again = cargo_metadata::MetadataCommand::parse(&serialized).unwrap();
            prop_assert_eq!(serde_json::to_value(result).unwrap(), serde_json::to_value(again).unwrap());
        }
    }

    #[test]
    fn command_executor_scopes_restore_after_errors_and_unwinds() {
        with_command_executor(
            |_| Ok(success(b"outer".to_vec())),
            || {
                assert_eq!(command("unused").run_output().unwrap().stdout, b"outer");
                assert!(std::panic::catch_unwind(|| {
                    with_command_executor(
                        |_| Err(std::io::ErrorKind::NotFound.into()),
                        || {
                            assert_eq!(
                                command("unused").run_output().unwrap_err().kind(),
                                std::io::ErrorKind::NotFound
                            );
                            panic!("executor unwind");
                        },
                    );
                })
                .is_err());
                assert_eq!(command("unused").run_output().unwrap().stdout, b"outer");
            },
        );
        assert_eq!(
            command("sdk-test-executable-that-does-not-exist")
                .run_output()
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        with_command_executor(
            |_| Ok(success(b"not metadata".to_vec())),
            || {
                assert!(execute_metadata(&cargo_metadata::MetadataCommand::new()).is_err());
            },
        );
    }

    #[test]
    fn unwinding_restores_the_environment() {
        let before = overrides();
        assert!(std::panic::catch_unwind(|| {
            let mut child = Command::new("cargo");
            child.env("CARGO_HOME", "inner");
            with_command_environment(&child, || panic!("scope unwind"))
        })
        .is_err());
        assert_eq!(overrides(), before);
    }
}
