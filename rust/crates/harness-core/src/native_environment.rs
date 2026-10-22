//! One process-environment seam with explicit fixture discovery policy.
use std::ffi::OsString;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
use std::path::{Path, PathBuf};

/// Platform facts supplied by a native composition or an isolated fixture.
#[derive(Clone, Debug)]
pub enum PlatformPolicy {
    /// Platform of the host executing the bound Bash tool.
    Native,
    /// Explicit facts, without process discovery.
    Fixture {
        /// Explicit tool platform.
        os: String,
        /// Explicit tool shell.
        shell: String,
    },
}

impl PlatformPolicy {
    /// Resolves platform facts through the native environment seam.
    pub fn facts(&self) -> (&str, &str) {
        match self {
            Self::Native => (std::env::consts::OS, "bash"),
            Self::Fixture { os, shell } => (os, shell),
        }
    }
}

enum Read<'a> {
    Variable(&'a str),
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    Home,
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    State(Option<&'a Path>),
}

fn user_location(name: &str) -> bool {
    [
        "HOME",
        "USERPROFILE",
        "LOCALAPPDATA",
        "APPDATA",
        "CODEX_HOME",
        "HOMEDRIVE",
        "HOMEPATH",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
    ]
    .iter()
    .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

/// Reads a fixture control without allowing user-directory discovery.
pub fn variable_with_user_locations(name: &str, discover_user: bool) -> Option<OsString> {
    if !discover_user && user_location(name) {
        None
    } else {
        resolve(Read::Variable(name))
    }
}

/// Resolves one process variable through the native environment seam.
pub fn variable(name: &str) -> Option<OsString> {
    variable_with_user_locations(name, true)
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
/// Resolves the native home directory for an ambient composition.
pub fn home() -> Option<PathBuf> {
    resolve(Read::Home).map(PathBuf::from)
}
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
/// Resolves an explicitly injected state root or the native data directory.
pub fn state(injected: Option<&Path>) -> Option<PathBuf> {
    resolve(Read::State(injected)).map(PathBuf::from)
}

#[expect(
    clippy::disallowed_methods,
    reason = "the only native state/environment resolver; fixture callers select discovery policy explicitly"
)]
fn resolve(read: Read<'_>) -> Option<OsString> {
    match read {
        Read::Variable(name) => std::env::var_os(name),
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        Read::Home => dirs::home_dir().map(PathBuf::into_os_string),
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        Read::State(None) => dirs::data_local_dir().map(PathBuf::into_os_string),
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        Read::State(Some(directory)) => Some(directory.as_os_str().to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn location_names_stay_isolated_under_arbitrary_casing(mask in any::<u16>()) {
            for name in ["HOME", "USERPROFILE", "LOCALAPPDATA", "APPDATA", "CODEX_HOME"] {
                let cased: String = name.chars().enumerate().map(|(index, letter)| {
                    if mask & (1 << index) != 0 { letter.to_ascii_lowercase() } else { letter }
                }).collect();
                prop_assert!(variable_with_user_locations(&cased, false).is_none());
                prop_assert!(crate::test_support::environment(&cased).is_none());
            }
        }
    }
    #[test]
    fn native_user_locations_are_unreachable_from_unit_fixtures() {
        for name in [
            "HOME",
            "USERPROFILE",
            "APPDATA",
            "LOCALAPPDATA",
            "CODEX_HOME",
            "home",
            "XDG_CONFIG_HOME",
        ] {
            assert!(variable_with_user_locations(name, false).is_none());
        }
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        {
            let directory = tempfile::tempdir().unwrap();
            assert_eq!(
                state(Some(directory.path())).as_deref(),
                Some(directory.path())
            );
        }
    }
}

#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
#[path = "native_environment/checkout.rs"]
mod checkout;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use checkout::{CheckoutFacts, checkout};
