//! Admission shared by the executable and its byte-input fuzz target.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Invocation {
    pub(super) executable: OsString,
    pub(super) arguments: Vec<OsString>,
    pub(super) inherit_input: bool,
    pub(super) control: Option<PathBuf>,
    pub(super) custody: Option<PathBuf>,
    pub(super) lease: Option<PathBuf>,
    pub(super) completion: Option<PathBuf>,
    pub(super) invocation: Option<String>,
}

impl Invocation {
    pub(super) fn parse(arguments: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
        let mut arguments = arguments.into_iter();
        let mut inherit_input = false;
        let mut control = None;
        let mut custody = None;
        let mut lease = None;
        let mut completion = None;
        let mut invocation = None;
        while let Some(argument) = arguments.next() {
            if argument == "--inherit-input" && !inherit_input {
                inherit_input = true;
            } else if argument == "--invocation" && invocation.is_none() {
                let value = arguments.next().ok_or_else(invalid_arguments)?;
                let value = value.into_string().map_err(|_| invalid_arguments())?;
                if value.is_empty()
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
                {
                    return Err(invalid_arguments());
                }
                invocation = Some(value);
            } else if argument == "--custody"
                || argument == "--completion"
                || argument == "--lease"
                || argument == "--control"
            {
                let path = arguments.next().ok_or_else(invalid_arguments)?;
                if !Path::new(&path).is_absolute() {
                    return Err(invalid_arguments());
                }
                let destination = if argument == "--custody" {
                    &mut custody
                } else if argument == "--lease" {
                    &mut lease
                } else if argument == "--control" {
                    &mut control
                } else {
                    &mut completion
                };
                if destination.replace(PathBuf::from(path)).is_some() {
                    return Err(invalid_arguments());
                }
            } else if argument == "--" || Path::new(&argument).is_absolute() {
                let executable = if argument == "--" {
                    arguments
                        .next()
                        .filter(|value| !value.is_empty())
                        .ok_or_else(invalid_arguments)?
                } else {
                    argument
                };
                if (custody.is_some() && lease.is_some())
                    || (invocation.is_some() && completion.is_none())
                {
                    return Err(invalid_arguments());
                }
                return Ok(Self {
                    executable,
                    arguments: arguments.collect(),
                    inherit_input,
                    control,
                    custody,
                    lease,
                    completion,
                    invocation,
                });
            } else {
                return Err(invalid_arguments());
            }
        }
        Err(invalid_arguments())
    }
}

pub(super) fn invalid_arguments() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "step=process-host-arguments; recovery=acyclic-process-host [--lease ABSOLUTE_ROOT | --custody ABSOLUTE_DIRECTORY] [--completion ABSOLUTE_FILE [--invocation OPAQUE_ID]] [--control ABSOLUTE_ENDPOINT] [--inherit-input] -- EXECUTABLE ARGS",
    )
}
