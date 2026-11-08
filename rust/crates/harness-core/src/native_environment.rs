//! One process-environment seam with explicit fixture discovery policy.
use std::ffi::OsString;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
use std::path::{Path, PathBuf};

/// Platform facts supplied by a native composition or an isolated fixture.
#[derive(Clone, Debug)]
pub enum PlatformPolicy {
    /// Platform of the host executing the bound shell tool.
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
            Self::Native => (
                std::env::consts::OS,
                if cfg!(windows) { "PowerShell" } else { "bash" },
            ),
            Self::Fixture { os, shell } => (os, shell),
        }
    }
}

#[derive(Clone, Copy)]
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

/// Snapshots the native owner's closed allowlist for locating ordinary tools.
///
/// An admitted shell may locate the owner's installed tools without receiving
/// its home, configuration, loader hooks or credentials. Call once when binding
/// the executor, then pass this explicit map through preparation and env_clear.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub fn tool_environment() -> std::io::Result<std::collections::BTreeMap<String, String>> {
    [
        "PATH",
        #[cfg(windows)]
        "PATHEXT",
        #[cfg(windows)]
        "SystemRoot",
    ]
    .into_iter()
    .filter_map(|name| variable(name).map(|value| (name, value)))
    .map(|(name, value)| {
        value.into_string().map(|value| (name.into(), value)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("approved tool setting {name} is not UTF-8; recovery: supply a UTF-8 tool location before opening the executor"),
            )
        })
    })
    .collect()
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
        Read::State(None) => {
            // Known Folder lookup does not honor an explicitly isolated Windows
            // profile. All native owners must use the caller's local-data root.
            #[cfg(windows)]
            if let Some(directory) =
                std::env::var_os("LOCALAPPDATA").filter(|value| !value.is_empty())
            {
                return Some(directory);
            }
            dirs::data_local_dir().map(PathBuf::into_os_string)
        }
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        Read::State(Some(directory)) => Some(directory.as_os_str().to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[cfg(all(windows, feature = "filesystem-local"))]
    #[test]
    fn native_state_honors_literal_local_app_data() {
        const EXPECTED: &str = "ACYCLIC_STATE_LOCATION_TEST";
        if let Some(expected) = variable(EXPECTED) {
            assert_eq!(state(None), Some(PathBuf::from(&expected)));
            let injected = PathBuf::from(expected).join("explicit");
            assert_eq!(state(Some(&injected)), Some(injected));
            return;
        }
        let fixture = tempfile::tempdir().unwrap();
        let local = fixture.path().join("local & % data-λ");
        std::fs::create_dir(&local).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_environment::tests::native_state_honors_literal_local_app_data",
                "--test-threads=1",
            ])
            .env("LOCALAPPDATA", &local)
            .env(EXPECTED, &local)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    }
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
