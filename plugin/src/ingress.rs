//! Process argument admission shared by the executable and its fuzz target.
use std::ffi::OsString;

/// The original argv for the agent parser, or decoded native-host arguments.
pub enum Input {
    /// Retains the program name and OS-native arguments for clap.
    Agent(Vec<OsString>),
    /// Native-host commands accept UTF-8 and omit the program name.
    Native(Vec<String>),
}

#[derive(Debug)]
/// A native-host argument could not be decoded without changing its bytes.
pub struct InvalidNativeArgument;

impl std::fmt::Display for InvalidNativeArgument {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("step=decode native command arguments: expected UTF-8; recovery: run acyclic --help with UTF-8 arguments")
    }
}

impl std::error::Error for InvalidNativeArgument {}

/// Routes one owned argv without opening a workspace or copying its payloads.
pub fn parse(arguments: Vec<OsString>) -> Result<Input, InvalidNativeArgument> {
    let first = arguments.get(1).and_then(|argument| argument.to_str());
    if matches!(
        first,
        Some("run" | "serve" | "attach" | "exec" | "sessions" | "history")
    ) || first.is_some_and(|argument| {
        argument.starts_with('-')
            && !matches!(argument, "--help" | "-h" | "--version" | "-V" | "--task")
    }) {
        return Ok(Input::Agent(arguments));
    }
    arguments
        .into_iter()
        .skip(1)
        .map(|argument| argument.into_string().map_err(|_| InvalidNativeArgument))
        .collect::<Result<_, _>>()
        .map(Input::Native)
}
