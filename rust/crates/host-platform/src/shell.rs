//! One shell language and invocation contract shared by every host.
macro_rules! powershell_utf8 {
    () => {
        "[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false); [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); $OutputEncoding = [Console]::OutputEncoding; "
    };
}
/// Platform shell data shared by registration, execution and client routing.
pub struct ShellSpec {
    /// Model-visible tool and approval-policy name.
    pub name: &'static str,
    /// Description of this target's shell language and result contract.
    pub description: &'static str,
    /// Command language the roster lists for targets using this contract.
    pub language: &'static str,
    /// Whether commands use the Windows filesystem namespace.
    pub windows_paths: bool,
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
    language: "powershell",
    windows_paths: true,
    description: "Run PowerShell in your workspace, using PowerShell 7 when available, otherwise Windows PowerShell 5.1. Use PowerShell syntax and Windows paths; profiles are disabled. Returns the exit code and output tail, about 30000 characters; overflow is saved in volume:///output/. Writes outside the workspace are not captured",
    executables: &["pwsh.exe", "powershell.exe"],
    arguments: POWERSHELL_ARGUMENTS,
    stdin_bootstrap: Some(concat!(
        powershell_utf8!(),
        "& ([ScriptBlock]::Create([Console]::In.ReadToEnd()))"
    )),
};

/// Unix hosts retain the bash command language.
#[cfg(not(windows))]
pub const PLATFORM: ShellSpec = BASH;

/// Bash contract for a Linux or macOS Machine, regardless of the client OS.
pub const BASH: ShellSpec = ShellSpec {
    name: "shell",
    language: "bash",
    windows_paths: false,
    description: "Run bash in your workspace, without profile or rc files. Use bash syntax and Unix paths. Returns the exit code and output tail, about 30000 characters; overflow is saved in volume:///output/. Writes outside the workspace are not captured",
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

/// Source passed as an argument while stdin carries separate UTF-8 data.
/// PowerShell establishes its console encoding before parsing the original
/// script block, preserving parameter blocks, quotes and newlines. Other
/// supported languages borrow the source. `admit` reserves the exact buffer
/// before any copy. `None` means an unsupported language or buffer-size overflow.
pub fn source_argument<'a, E>(
    selected: &str,
    source: &'a str,
    admit: impl FnOnce(usize) -> Result<(), E>,
) -> Result<Option<std::borrow::Cow<'a, str>>, E> {
    if arguments(selected).is_none() {
        return Ok(None);
    }
    if !matches!(selected, "powershell" | "pwsh") {
        return Ok(Some(std::borrow::Cow::Borrowed(source)));
    }
    const PREFIX: &str = concat!(powershell_utf8!(), "& ([ScriptBlock]::Create('");
    const SUFFIX: &str = "'))";
    let extra = source
        .chars()
        .filter(|character| powershell_single_quote(*character))
        .try_fold(0usize, |bytes, character| {
            bytes.checked_add(character.len_utf8())
        });
    let Some(bytes) = extra
        .and_then(|extra| source.len().checked_add(extra))
        .and_then(|bytes| bytes.checked_add(PREFIX.len()))
        .and_then(|bytes| bytes.checked_add(SUFFIX.len()))
    else {
        return Ok(None);
    };
    admit(bytes)?;
    let mut encoded = String::with_capacity(bytes);
    encoded.push_str(PREFIX);
    for part in source.split_inclusive(powershell_single_quote) {
        encoded.push_str(part);
        if let Some(quote) = part
            .chars()
            .next_back()
            .filter(|character| powershell_single_quote(*character))
        {
            encoded.push(quote);
        }
    }
    encoded.push_str(SUFFIX);
    Ok(Some(std::borrow::Cow::Owned(encoded)))
}

fn powershell_single_quote(character: char) -> bool {
    matches!(character, '\'' | '\u{2018}'..='\u{201b}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[cfg(windows)]
    #[test]
    fn powershell_decodes_json_data_after_a_legacy_console_encoding() -> std::io::Result<()> {
        use std::{
            io::Write,
            process::{Command, Stdio},
        };
        let source = "param(); $punctuation = \"‘’‚‛\"; $data = [Console]::In.ReadToEnd() | ConvertFrom-Json; if ($data.text -cne 'quoted '' value 🦀') { exit 9 }; [Console]::Out.Write($data.text + $punctuation)";
        for wrapped in [false, true] {
            let rendered = source_argument("powershell", source, |_| Ok::<_, std::io::Error>(()))?
                .expect("supported source");
            let rendered = if wrapped {
                rendered.as_ref()
            } else {
                rendered
                    .strip_prefix(powershell_utf8!())
                    .expect("encoding prefix")
            };
            // A legacy host is a supported initial state. The control fails
            // even when the test runner itself happens to use UTF-8.
            let command =
                format!("[Console]::InputEncoding = [Text.Encoding]::GetEncoding(437); {rendered}");
            let mut child = Command::new("powershell.exe")
                .args(POWERSHELL.arguments)
                .arg(command)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            child
                .stdin
                .take()
                .expect("piped input")
                .write_all("{\"text\":\"quoted ' value 🦀\"}".as_bytes())?;
            let output = child.wait_with_output()?;
            assert_eq!(output.status.success(), wrapped, "{output:?}");
            if wrapped {
                assert_eq!(output.stdout, "quoted ' value 🦀‘’‚‛".as_bytes());
            } else {
                assert_eq!(output.status.code(), Some(9), "{output:?}");
            }
        }
        Ok(())
    }

    proptest! {
        #[test]
        fn source_moves_into_one_exact_command_channel(source in any::<String>()) {
            let source = format!("{source}‘’‚‛");
            // Arbitrary Unicode, quoting and newlines remain original source,
            // never escaped or split into shell arguments by another adapter.
            for spec in [&BASH, &POWERSHELL] {
                let owned = source.clone();
                let original = owned.as_ptr();
                let (arguments, stdin) = spec.prepare(owned);
                let arguments: Vec<_> = arguments.collect();
                prop_assert_eq!(arguments.len(), spec.arguments.len() + 1);
                prop_assert!(arguments.iter().map(String::as_str)
                    .take(spec.arguments.len()).eq(spec.arguments.iter().copied()));
                let script = arguments.last().unwrap();
                let actual = if let Some(bootstrap) = spec.stdin_bootstrap {
                    prop_assert_eq!(script, bootstrap);
                    stdin.as_deref().unwrap()
                } else {
                    prop_assert!(stdin.is_none());
                    script.as_bytes()
                };
                prop_assert_eq!(actual, source.as_bytes());
                prop_assert_eq!(actual.as_ptr(), original);
                // A second source copy fails this same physical-buffer oracle.
                if !actual.is_empty() {
                    let copied = actual.to_vec();
                    prop_assert_ne!(copied.as_ptr(), original);
                }
            }
            let mut admitted = None;
            let rendered = source_argument("powershell", &source, |bytes| {
                admitted = Some(bytes);
                Ok::<_, ()>(())
            }).unwrap().unwrap();
            prop_assert_eq!(admitted, Some(rendered.len()));
            let encoded = rendered.strip_prefix(concat!(powershell_utf8!(), "& ([ScriptBlock]::Create('")).unwrap()
                .strip_suffix("'))").unwrap();
            let decoded = encoded.replace("''", "'")
                .replace("‘‘", "‘").replace("’’", "’")
                .replace("‚‚", "‚").replace("‛‛", "‛");
            prop_assert_eq!(decoded, source.as_str());
            prop_assert_eq!(source_argument("powershell", &source, |_| Err("refused")), Err("refused"));
            let borrowed = source_argument("bash", &source, |_| Err::<(), _>("unexpected copy"))
                .unwrap().unwrap();
            prop_assert_eq!(borrowed.as_ptr(), source.as_ptr());
        }
    }
}
