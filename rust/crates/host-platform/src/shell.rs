//! One shell language and invocation contract shared by every host.
/// Platform shell data shared by registration, execution and client routing.
pub struct ShellSpec {
    /// Model-visible tool and approval-policy name.
    pub name: &'static str,
    /// Model guidance for the host's command language.
    pub description: &'static str,
    /// Native executables in preference order; resolved once at host startup.
    pub executables: &'static [&'static str],
    /// Non-interactive arguments preceding the source text.
    pub arguments: &'static [&'static str],
    /// Fixed bootstrap reading UTF-8 source from stdin; otherwise pass source as an argument.
    pub stdin_bootstrap: Option<&'static str>,
}

impl ShellSpec {
    /// Moves original UTF-8 source into its platform's one command channel.
    /// The iterator omits the executable so callers can prepend their already
    /// resolved selection without allocating another argument vector.
    pub fn prepare(&self, source: String) -> (impl Iterator<Item = String> + '_, Option<Vec<u8>>) {
        let (source, stdin) = match self.stdin_bootstrap {
            Some(bootstrap) => (bootstrap.to_owned(), Some(source.into_bytes())),
            None => (source, None),
        };
        (
            self.arguments
                .iter()
                .map(|value| (*value).to_owned())
                .chain([source]),
            stdin,
        )
    }
}

/// Windows commands must use the native filesystem namespace.
#[cfg(windows)]
pub const PLATFORM: ShellSpec = POWERSHELL;

/// PowerShell contract, including when a different client OS selects a Machine.
pub const POWERSHELL: ShellSpec = ShellSpec {
    name: "shell",
    description: "Run a command in the platform shell (bash on Linux/macOS; PowerShell 7, or Windows PowerShell 5.1, on Windows) in the calling task's composed workspace view, using PowerShell 7 (pwsh) when available, otherwise Windows PowerShell 5.1. Use PowerShell syntax and Windows paths. Runs with -NoProfile -NonInteractive; writes outside the task view are not captured. No OS sandbox in release 1",
    executables: &["pwsh.exe", "powershell.exe"],
    arguments: POWERSHELL_ARGUMENTS,
    stdin_bootstrap: Some(
        "[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false); [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); $OutputEncoding = [Console]::OutputEncoding; & ([ScriptBlock]::Create([Console]::In.ReadToEnd()))",
    ),
};

/// Unix hosts retain the bash command language.
#[cfg(not(windows))]
pub const PLATFORM: ShellSpec = BASH;

/// Bash contract for a Linux or macOS Machine, regardless of the client OS.
pub const BASH: ShellSpec = ShellSpec {
    name: "shell",
    description: "Run a command in the platform shell (bash on Linux/macOS; PowerShell 7, or Windows PowerShell 5.1, on Windows) in the calling task's composed workspace view. Use bash syntax and Unix paths. No OS sandbox in release 1; writes outside the task view are not captured",
    executables: &["bash"],
    arguments: BASH_ARGUMENTS,
    stdin_bootstrap: None,
};

const POWERSHELL_ARGUMENTS: &[&str] = &["-NoProfile", "-NonInteractive", "-Command"];
const BASH_ARGUMENTS: &[&str] = &["--noprofile", "--norc", "-c"];

/// Explicit hook language selection uses the same invocation table as tools.
#[must_use]
pub fn arguments(selected: &str) -> Option<&'static [&'static str]> {
    match selected {
        "powershell" | "pwsh" => Some(POWERSHELL_ARGUMENTS),
        "bash" => Some(BASH_ARGUMENTS),
        "sh" => Some(&["-c"]),
        "cmd" => Some(&["/d", "/s", "/c"]),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn source_moves_into_one_exact_command_channel(source in any::<String>()) {
            // Arbitrary Unicode, quoting and newlines remain original source,
            // never escaped or split into shell arguments by another adapter.
            for spec in [&BASH, &POWERSHELL] {
                let owned = source.clone();
                let original = owned.as_ptr();
                let (arguments, stdin) = spec.prepare(owned);
                let arguments: Vec<_> = arguments.collect();
                prop_assert_eq!(arguments.len(), spec.arguments.len() + 1);
                prop_assert_eq!(&arguments[..spec.arguments.len()], spec.arguments);
                let script = arguments.last().unwrap();
                let actual = match spec.stdin_bootstrap {
                    Some(bootstrap) => {
                        prop_assert_eq!(script, bootstrap);
                        stdin.as_deref().unwrap()
                    }
                    None => {
                        prop_assert!(stdin.is_none());
                        script.as_bytes()
                    }
                };
                prop_assert_eq!(actual, source.as_bytes());
                prop_assert_eq!(actual.as_ptr(), original);
                // A second source copy fails this same physical-buffer oracle.
                if !actual.is_empty() {
                    let copied = actual.to_vec();
                    prop_assert_ne!(copied.as_ptr(), original);
                }
            }
        }
    }
}
