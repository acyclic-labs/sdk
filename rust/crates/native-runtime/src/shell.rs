use acyclic_executable_search::{ExecutableSearch, platform_shell};
use acyclic_host_platform::shell::PLATFORM;
use std::io;

/// Creates a resolved shell command from the sole platform language table.
/// The caller supplies workspace, streams, environment and the process owner.
pub fn shell_command(command: &str, selected: Option<&str>) -> io::Result<crate::ProcessCommand> {
    let cwd = std::env::current_dir()?;
    let search = ExecutableSearch::from_environment(&cwd, |key| std::env::var_os(key));
    let (program, arguments) = if let Some(selected) = selected {
        let arguments =
            acyclic_host_platform::shell::arguments(selected).ok_or(io::ErrorKind::InvalidInput)?;
        (search.resolve_native(selected)?, arguments)
    } else {
        (platform_shell()?.to_owned(), PLATFORM.arguments)
    };
    let mut shell = crate::ProcessCommand::new(program);
    shell.args(arguments);
    let command = acyclic_host_platform::shell::source_argument(
        selected.unwrap_or(PLATFORM.language),
        command,
        |_| Ok::<_, io::Error>(()),
    )?
    .ok_or(io::ErrorKind::InvalidInput)?;
    if selected == Some("cmd") {
        #[cfg(windows)]
        shell.raw_arg(format!("\"{command}\""));
        #[cfg(not(windows))]
        shell.arg(command.as_ref());
    } else {
        shell.arg(command.as_ref());
    }
    Ok(shell)
}
