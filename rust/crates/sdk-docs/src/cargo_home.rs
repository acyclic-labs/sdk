use std::{
    cell::RefCell,
    path::{Path, PathBuf},
};
thread_local! {
    // Only the calling thread owns this child-command context.
    static HOME: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}

pub(crate) fn current() -> Option<PathBuf> {
    HOME.with_borrow(Clone::clone)
}

/// Scopes Cargo metadata/profile child commands to an explicit Cargo home.
/// The process environment is unchanged; nesting and unwinding restore the caller.
pub fn with_cargo_home<T>(home: &Path, run: impl FnOnce() -> T) -> T {
    struct Restore(Option<PathBuf>);
    impl Drop for Restore {
        fn drop(&mut self) {
            HOME.set(self.0.take());
        }
    }
    let _restore = Restore(HOME.replace(Some(home.to_owned())));
    run()
}

#[cfg(test)]
mod tests {
    use super::*;
    proptest::proptest! {
        #[test]
        fn nested_cargo_homes_apply_only_to_their_child_commands(a in "[a-z]{1,16}", b in "[a-z]{1,16}") {
            let original = current();
            let environment = std::env::var_os("CARGO_HOME");
            with_cargo_home(Path::new(&a), || {
                proptest::prop_assert_eq!(current(), Some(PathBuf::from(&a)));
                with_cargo_home(Path::new(&b), || {
                    let mut command = std::process::Command::new("cargo");
                    crate::rustdoc_profiles::CargoExecutionContext::default().configure(&mut command).unwrap();
                    let home = command.get_envs().find(|(key,_)| *key == "CARGO_HOME").unwrap().1.unwrap();
                    proptest::prop_assert_eq!(home, Path::new(&b).as_os_str());
                    Ok(())
                })?;
                proptest::prop_assert_eq!(current(), Some(PathBuf::from(&a)));
                Ok(())
            })?;
            proptest::prop_assert_eq!(current(), original);
            proptest::prop_assert_eq!(std::env::var_os("CARGO_HOME"), environment);
        }
    }

    #[test]
    fn unwinding_restores_the_cargo_home() {
        let before = current();
        let failed = std::panic::catch_unwind(|| {
            with_cargo_home(Path::new("inner"), || panic!("scope unwind"))
        });
        assert!(failed.is_err());
        assert_eq!(current(), before);
    }
}
