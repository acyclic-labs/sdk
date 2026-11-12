//! One process-environment seam with explicit fixture discovery policy.
use std::ffi::OsString;
#[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))]
    Home,
    #[cfg(not(target_arch = "wasm32"))]
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
/// its host home, configuration, loader hooks or credentials. Call once when binding
/// the executor, then pass this explicit map through preparation and `env_clear`.
/// Resolve the selected shell only on Unix, where Bash needs the SHELL setting.
#[cfg(not(target_arch = "wasm32"))]
pub fn tool_environment<'a>(
    _shell: impl FnOnce() -> std::io::Result<&'a Path>,
) -> std::io::Result<std::collections::BTreeMap<String, String>> {
    let environment: std::collections::BTreeMap<String, String> = [
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
    .collect::<std::io::Result<_>>()?;
    #[cfg(unix)]
    let environment = {
        let mut environment = environment;
        // This fact is already selected by the host. Without it Bash asks NSS
        // for a login shell, which can contact host IPC outside the grant.
        let shell = _shell()?.to_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "approved shell path is not UTF-8",
            )
        })?;
        environment.insert("SHELL".into(), shell.into());
        environment
    };
    Ok(environment)
}

#[cfg(not(target_arch = "wasm32"))]
/// Resolves an absolute native home directory for an ambient composition.
/// Returns `None` when the platform supplies no absolute home.
pub fn home() -> Option<PathBuf> {
    resolve(Read::Home).map(PathBuf::from)
}
#[cfg(not(target_arch = "wasm32"))]
/// Resolves an injected root, an absolute data-directory override, or the native default.
/// The environment override is `LOCALAPPDATA` on Windows and `XDG_DATA_HOME`
/// elsewhere. Empty or relative environment values use an absolute platform
/// default, or return `None` if no absolute default is available.
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
        #[cfg(not(target_arch = "wasm32"))]
        Read::Home => dirs::home_dir()
            .filter(|directory| directory.is_absolute())
            .map(PathBuf::into_os_string),
        #[cfg(not(target_arch = "wasm32"))]
        Read::State(None) => {
            // Native directory APIs can ignore process-level overrides. Resolve
            // the caller's explicit root before consulting the platform default.
            let variable = if cfg!(windows) {
                "LOCALAPPDATA"
            } else {
                "XDG_DATA_HOME"
            };
            if let Some(directory) =
                std::env::var_os(variable).filter(|value| Path::new(value).is_absolute())
            {
                return Some(directory);
            }
            dirs::data_local_dir()
                .filter(|directory| directory.is_absolute())
                .map(PathBuf::into_os_string)
        }
        #[cfg(not(target_arch = "wasm32"))]
        Read::State(Some(directory)) => Some(directory.as_os_str().to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_state_honors_literal_data_override() {
        const EXPECTED: &str = "ACYCLIC_STATE_LOCATION_TEST";
        const NATIVE_DEFAULT: &str = "native-default";
        if let Some(expected) = variable(EXPECTED) {
            let expected = if expected == NATIVE_DEFAULT {
                dirs::data_local_dir().filter(|directory| directory.is_absolute())
            } else {
                (!expected.is_empty()).then(|| PathBuf::from(&expected))
            };
            #[cfg(unix)]
            if expected.is_none() {
                assert!(home().is_none(), "relative HOME reached user settings");
            }
            assert_eq!(state(None), expected);
            let injected = PathBuf::from("explicit");
            assert_eq!(state(Some(&injected)), Some(injected));
            return;
        }
        let fixture = tempfile::tempdir().unwrap();
        let local = fixture.path().join("local & % data-λ");
        std::fs::create_dir(&local).unwrap();
        let variable = if cfg!(windows) {
            "LOCALAPPDATA"
        } else {
            "XDG_DATA_HOME"
        };
        let empty = std::ffi::OsStr::new("");
        for (directory, invalid_home) in [
            (local.as_os_str(), false),
            (empty, false),
            (std::ffi::OsStr::new("relative-data"), false),
            #[cfg(unix)]
            (std::ffi::OsStr::new("relative-data"), true),
        ] {
            let expected = if invalid_home {
                empty
            } else if Path::new(directory).is_absolute() {
                directory
            } else {
                std::ffi::OsStr::new(NATIVE_DEFAULT)
            };
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "native_environment::tests::native_state_honors_literal_data_override",
                    "--test-threads=1",
                ])
                .env(variable, directory)
                .env(EXPECTED, expected);
            if invalid_home {
                command.env("HOME", "relative-home");
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        }
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
        #[cfg(not(target_arch = "wasm32"))]
        {
            let directory = tempfile::tempdir().unwrap();
            assert_eq!(
                state(Some(directory.path())).as_deref(),
                Some(directory.path())
            );
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[path = "native_environment/checkout.rs"]
mod checkout;
#[cfg(not(target_arch = "wasm32"))]
pub use checkout::{CheckoutFacts, checkout};
