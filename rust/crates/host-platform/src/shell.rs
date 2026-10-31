//! One shell language and invocation contract shared by every host.
/// Platform shell data shared by registration, execution and client routing.
pub struct ShellSpec {
    language: Language,
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

enum Language {
    Bash,
    PowerShell,
}

impl ShellSpec {
    /// Embeds pinned source and positional arguments as separate shell literals.
    /// This runs in the original shell process: bash receives arguments through
    /// `set --`, while PowerShell passes them to the compiled script block.
    /// The source is interpreted once after literal decoding; argument contents
    /// are never interpreted as source. Skills read their JSON input from `$1`
    /// in bash or `$args[0]` in PowerShell.
    pub fn prepare_script(&self, source: &str, arguments: &[String]) -> String {
        let (prefix, suffix, escape) = match self.language {
            Language::Bash => ("set --", "; eval -- ", "'\\''"),
            Language::PowerShell => ("& ([ScriptBlock]::Create(", "))", "''"),
        };
        let literal_length = |value: &str| {
            value.len()
                + 2
                + value.bytes().filter(|byte| *byte == b'\'').count() * (escape.len() - 1)
        };
        let mut command = String::with_capacity(
            prefix.len()
                + suffix.len()
                + literal_length(source)
                + arguments
                    .iter()
                    .map(|value| 1 + literal_length(value))
                    .sum::<usize>(),
        );
        let quote = |command: &mut String, value: &str| {
            command.push('\'');
            for (index, part) in value.split('\'').enumerate() {
                if index > 0 {
                    command.push_str(escape);
                }
                command.push_str(part);
            }
            command.push('\'');
        };
        command.push_str(prefix);
        if matches!(self.language, Language::PowerShell) {
            quote(&mut command, source);
            command.push_str(suffix);
        }
        for argument in arguments {
            command.push(' ');
            quote(&mut command, argument);
        }
        if matches!(self.language, Language::Bash) {
            command.push_str(suffix);
            quote(&mut command, source);
        }
        command
    }

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
    language: Language::PowerShell,
    name: "shell",
    description: "Run PowerShell in your workspace, using PowerShell 7 when available, otherwise Windows PowerShell 5.1. Use PowerShell syntax and Windows paths; profiles are disabled. timeout_ms defaults to 300000 and cannot exceed it. Returns the exit code and output tail, about 30000 characters; overflow is saved in .volumes/scratch. Writes outside the workspace are not captured",
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
    language: Language::Bash,
    name: "shell",
    description: "Run bash in your workspace, without profile or rc files. Use bash syntax and Unix paths. timeout_ms defaults to 300000 and cannot exceed it. Returns the exit code and output tail, about 30000 characters; overflow is saved in .volumes/scratch. Writes outside the workspace are not captured",
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

    #[test]
    fn script_arguments_remain_data_in_the_native_shell() {
        use std::io::Write;
        use std::process::{Command, Stdio};

        let input = "{\"text\":\"' ; $() ` $(throw 'injected') \\\" \\n 🦀\"}";
        #[cfg(windows)]
        let source =
            "# A pinned source containing ' and ` and Unicode 🦀\n[Console]::Write($args[0])";
        #[cfg(not(windows))]
        let source = "# A pinned source containing ' and ` and Unicode 🦀\nprintf '%s' \"$1\"";
        let command = PLATFORM.prepare_script(source, &[input.into()]);
        let (arguments, stdin) = PLATFORM.prepare(command);
        let arguments: Vec<_> = arguments.collect();
        let mut child = PLATFORM
            .executables
            .iter()
            .find_map(|executable| {
                match Command::new(executable)
                    .args(&arguments)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                {
                    Ok(child) => Some(child),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => panic!("native shell start failed: {error}; recovery: restore the platform shell executable"),
                }
            })
            .expect("platform shell is absent; recovery: install the platform's native shell");
        if let Some(stdin) = stdin {
            child.stdin.take().unwrap().write_all(&stdin).unwrap();
        }
        // The shell's own terminal event is the producer. No readiness loop,
        // temporary script path or second process participates in the check.
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "{:?}", output.stderr);
        assert_eq!(output.stdout, input.as_bytes());
        assert!(output.stderr.is_empty());
        // Interpreting this argument as source changes stdout or fails the
        // process, so the same exact-byte oracle rejects that mutation.
    }

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
