use std::process::Command;

/// Selects the native command shell shared by hooks and workspace process tools.
/// The caller supplies the workspace, environment, streams and process owner.
#[must_use]
pub fn shell_command(command: &str, selected: Option<&str>) -> Command {
    let selected = selected.unwrap_or(if cfg!(windows) { "powershell" } else { "sh" });
    let mut shell = Command::new(selected);
    if matches!(selected, "powershell" | "pwsh") {
        shell.args(["-NoProfile", "-NonInteractive", "-Command", command]);
    } else if selected == "cmd" {
        shell.args(["/d", "/s", "/c"]);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            shell.raw_arg(format!("\"{command}\""));
        }
        #[cfg(not(windows))]
        shell.arg(command);
    } else {
        shell.args(["-c", command]);
    }
    shell
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_selection_preserves_the_script_as_one_argument() {
        for selected in [
            None,
            Some("sh"),
            Some("bash"),
            Some("powershell"),
            Some("pwsh"),
        ] {
            let command = shell_command("echo 'a b'; exit 3", selected);
            let program = selected.unwrap_or(if cfg!(windows) { "powershell" } else { "sh" });
            assert_eq!(command.get_program(), program);
            let args = command.get_args().collect::<Vec<_>>();
            let expected = if matches!(program, "powershell" | "pwsh") {
                vec![
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    "echo 'a b'; exit 3",
                ]
            } else {
                vec!["-c", "echo 'a b'; exit 3"]
            };
            assert_eq!(args, expected);
        }
    }
}
