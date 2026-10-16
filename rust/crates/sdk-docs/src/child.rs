use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    process::Command,
};

type Overrides = Vec<(OsString, Option<OsString>)>;
thread_local! {
    // Calling-thread ownership; no process environment mutation or shared lock.
    static OVERRIDES: RefCell<Overrides> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn overrides() -> Overrides {
    OVERRIDES.with_borrow(Clone::clone)
}

/// Scopes explicit child-command environment overrides without changing process state.
pub fn with_command_environment<T>(command: &Command, operation: impl FnOnce() -> T) -> T {
    struct Restore(Overrides);
    impl Drop for Restore {
        fn drop(&mut self) {
            OVERRIDES.set(std::mem::take(&mut self.0));
        }
    }
    let mut inherited = overrides();
    inherited.extend(
        command
            .get_envs()
            .map(|(key, value)| (key.to_owned(), value.map(OsStr::to_owned))),
    );
    let _restore = Restore(OVERRIDES.replace(inherited));
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
