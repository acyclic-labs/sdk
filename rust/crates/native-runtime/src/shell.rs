use crate::ExecutableSearch;
use acyclic_host_platform::shell::PLATFORM;
use std::{
    io,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

static EXECUTABLE: OnceLock<Result<PathBuf, (io::ErrorKind, String)>> = OnceLock::new();

/// The native host's shell is selected once; later PATH changes cannot retarget it.
pub fn platform_shell() -> io::Result<&'static Path> {
    match EXECUTABLE.get_or_init(|| {
        (|| {
            let cwd = std::env::current_dir()?;
            let search = ExecutableSearch::from_environment(&cwd, |key| std::env::var_os(key));
            select(|name| search.resolve_native(name).ok())
        })()
        .map_err(|error: io::Error| (error.kind(), error.to_string()))
    }) {
        Ok(path) => Ok(path),
        Err((kind, message)) => Err(io::Error::new(*kind, message.clone())),
    }
}

fn select(mut lookup: impl FnMut(&str) -> Option<PathBuf>) -> io::Result<PathBuf> {
    PLATFORM
        .executables
        .iter()
        .find_map(|name| lookup(name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "native platform shell is unavailable",
            )
        })
}

/// Creates a resolved shell command from the sole platform language table.
/// The caller supplies workspace, streams, environment and the process owner.
pub fn shell_command(command: &str, selected: Option<&str>) -> io::Result<Command> {
    let cwd = std::env::current_dir()?;
    let search = ExecutableSearch::from_environment(&cwd, |key| std::env::var_os(key));
    let (program, arguments) = if let Some(selected) = selected {
        let arguments =
            acyclic_host_platform::shell::arguments(selected).ok_or(io::ErrorKind::InvalidInput)?;
        (search.resolve_native(selected)?, arguments)
    } else {
        (platform_shell()?.to_owned(), PLATFORM.arguments)
    };
    let mut shell = Command::new(program);
    shell.args(arguments);
    if selected == Some("cmd") {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            shell.raw_arg(format!("\"{command}\""));
        }
        #[cfg(not(windows))]
        shell.arg(command);
    } else {
        shell.arg(command);
    }
    Ok(shell)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_uses_the_platform_table_and_preserves_preference_order() {
        for chosen in 0..=PLATFORM.executables.len() {
            let mut calls = Vec::new();
            let result = select(|name| {
                calls.push(name.to_owned());
                (PLATFORM.executables.get(chosen).copied() == Some(name)).then(|| name.into())
            });
            assert_eq!(
                result.ok(),
                PLATFORM
                    .executables
                    .get(chosen)
                    .map(|name| PathBuf::from(*name))
            );
            assert_eq!(
                calls,
                PLATFORM
                    .executables
                    .iter()
                    .take(chosen + 1)
                    .copied()
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn default_shell_is_pinned_and_preserves_source_as_one_argument() {
        let command = shell_command("echo 'a b'; exit 3", None).unwrap();
        assert_eq!(command.get_program(), platform_shell().unwrap());
        let args: Vec<_> = command.get_args().collect();
        let mut expected: Vec<_> = PLATFORM
            .arguments
            .iter()
            .map(std::ffi::OsStr::new)
            .collect();
        expected.push(std::ffi::OsStr::new("echo 'a b'; exit 3"));
        assert_eq!(args, expected);
        assert!(shell_command("echo x", Some("unknown")).is_err());
    }
}
