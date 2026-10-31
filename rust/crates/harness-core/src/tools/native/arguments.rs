use super::*;
use crate::{
    NativeCustody,
    tool::{ToolInvocation, files::LineSelection},
};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Capability {
    Read,
    Write,
    Edit,
    Shell,
    Glob,
    Search,
}

impl Capability {
    pub(super) fn parse(name: &str) -> Result<Self> {
        match name {
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "edit" => Ok(Self::Edit),
            "shell" => Ok(Self::Shell),
            "glob" => Ok(Self::Glob),
            "search" => Ok(Self::Search),
            _ => Err(Error::Invalid("unknown native profile capability".into())),
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum Arguments<'a> {
    Read {
        path: &'a str,
        lines: Option<LineSelection>,
    },
    Write {
        path: &'a str,
        content: &'a str,
    },
    Edit {
        path: &'a str,
        old_text: &'a str,
        new_text: &'a str,
        replace_all: bool,
    },
    Shell {
        command: &'a Value,
        cwd: Option<&'a str>,
        timeout_ms: Option<u64>,
    },
    Patch {
        patch: &'a str,
    },
}

pub(super) struct Adapted {
    pub invocation: ToolInvocation,
    _arguments: NativeCustody,
    _identity: NativeCustody,
}

pub(super) fn text<'a>(arguments: &'a Value, key: &str) -> Result<&'a str> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Invalid(format!("{key} must be a string")))
}
fn number(arguments: &Value, key: &str) -> Result<Option<u64>> {
    arguments
        .get(key)
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| Error::Invalid(format!("{key} must be a nonnegative integer")))
        })
        .transpose()
}
fn optional_text<'a>(arguments: &'a Value, key: &str) -> Result<Option<&'a str>> {
    arguments
        .get(key)
        .filter(|value| !value.is_null())
        .map(|_| text(arguments, key))
        .transpose()
}
fn boolean(arguments: &Value, key: &str) -> Result<bool> {
    arguments
        .get(key)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| Error::Invalid(format!("{key} must be a boolean")))
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

impl adapter::Adapter {
    pub(super) fn adapt(
        &self,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<Adapted> {
        let fields = &invocation.arguments;
        let cc = self.profile == NativeToolProfile::Anthropic;
        let args = match self.capability {
            Capability::Read => {
                if fields.get("pages").is_some() {
                    return Err(Error::Unsupported("native PDF page selection is unavailable; recovery: bind the canonical media selector".into()));
                }
                let (offset, limit) = if cc {
                    let start = number(fields, "offset")?.unwrap_or(1);
                    (
                        start
                            .checked_sub(1)
                            .ok_or_else(|| Error::Invalid("offset must be at least 1".into()))?,
                        number(fields, "limit")?,
                    )
                } else {
                    let start = number(fields, "start_line")?.unwrap_or(1);
                    let offset = start
                        .checked_sub(1)
                        .ok_or_else(|| Error::Invalid("start_line must be at least 1".into()))?;
                    let limit = number(fields, "end_line")?
                        .map(|end| {
                            end.checked_sub(offset)
                                .filter(|limit| *limit > 0)
                                .ok_or_else(|| {
                                    Error::Invalid("end_line precedes start_line".into())
                                })
                        })
                        .transpose()?;
                    (offset, limit)
                };
                Arguments::Read {
                    path: text(fields, "file_path")?,
                    lines: Some(LineSelection { offset, limit }),
                }
            }
            Capability::Write => Arguments::Write {
                path: text(fields, "file_path")?,
                content: text(fields, "content")?,
            },
            Capability::Edit if self.profile == NativeToolProfile::Openai => Arguments::Patch {
                patch: text(fields, "input")?,
            },
            Capability::Edit => Arguments::Edit {
                path: text(fields, "file_path")?,
                old_text: text(fields, "old_string")?,
                new_text: text(fields, "new_string")?,
                replace_all: boolean(fields, if cc { "replace_all" } else { "allow_multiple" })?,
            },
            Capability::Shell => {
                if boolean(
                    fields,
                    if cc {
                        "run_in_background"
                    } else {
                        "is_background"
                    },
                )? || fields.get("delay_ms").is_some()
                {
                    return Err(Error::Unsupported("native process yield is unavailable; recovery: bind the original retained process child".into()));
                }
                if cc && boolean(fields, "dangerouslyDisableSandbox")? {
                    return Err(Error::Unauthorized("native process policy cannot be changed by tool arguments; recovery: obtain host policy authorization".into()));
                }
                Arguments::Shell {
                    command: fields
                        .get("command")
                        .ok_or_else(|| Error::Invalid("command is required".into()))?,
                    cwd: if cc {
                        None
                    } else {
                        optional_text(
                            fields,
                            if self.profile == NativeToolProfile::Openai {
                                "workdir"
                            } else {
                                "dir_path"
                            },
                        )?
                    },
                    timeout_ms: number(
                        fields,
                        match self.profile {
                            NativeToolProfile::Google => "timeout_ms",
                            _ => "timeout",
                        },
                    )?,
                }
            }
            Capability::Glob | Capability::Search => return self.query(invocation, work),
        };
        self.canonical(invocation, &args, work)
    }

    pub(super) fn canonical(
        &self,
        invocation: &ToolInvocation,
        arguments: &impl Serialize,
        work: crate::NativeWork<'_>,
    ) -> Result<Adapted> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owned_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = owned_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io: Option<&crate::executor::PreparationIo> = None;
        let (arguments, argument_custody) = crate::tool::value(arguments, u64::MAX, work)?;
        let call_id = crate::executor::stock_string_copy(&invocation.call_id, io)?;
        let name = crate::executor::stock_string_copy(&self.original.definition.name, io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let identity = owned_io
            .map(|io| io.into_native_custody((call_id.capacity() + name.capacity()) as u64))
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let identity = None;
        Ok(Adapted {
            invocation: ToolInvocation {
                operation_id: invocation.operation_id,
                call_id,
                name,
                arguments,
            },
            _arguments: argument_custody,
            _identity: identity,
        })
    }

    fn query(&self, invocation: &ToolInvocation, work: crate::NativeWork<'_>) -> Result<Adapted> {
        let fields = &invocation.arguments;
        let cc = self.profile == NativeToolProfile::Anthropic;
        let mut custody = crate::tool::edit::allocation(work, 0)?;
        let mut argv: Vec<&str> = Vec::new();
        crate::tool::edit::reserve_vec(&mut custody, &mut argv, 24)?;
        argv.push("rg");
        let glob = self.capability == Capability::Glob;
        let pattern = text(fields, "pattern")?;
        if glob {
            argv.push("--files");
            argv.push(
                if fields
                    .get("case_sensitive")
                    .and_then(Value::as_bool)
                    .unwrap_or(cc)
                {
                    "--glob"
                } else {
                    "--iglob"
                },
            );
            argv.push(pattern);
            if fields.get("respect_git_ignore").and_then(Value::as_bool) == Some(false) {
                argv.push("--no-ignore-vcs");
            }
        } else {
            match fields
                .get("output_mode")
                .and_then(Value::as_str)
                .unwrap_or(if cc { "files_with_matches" } else { "content" })
            {
                "files_with_matches" => argv.push("--files-with-matches"),
                "count" => argv.push("--count"),
                _ => argv.push("--line-number"),
            }
            if fields.get("names_only").and_then(Value::as_bool) == Some(true) {
                argv.push("--files-with-matches");
            }
            if fields.get("-i").and_then(Value::as_bool) == Some(true)
                || fields.get("case_sensitive").and_then(Value::as_bool) == Some(false)
            {
                argv.push("--ignore-case");
            }
            if fields.get("multiline").and_then(Value::as_bool) == Some(true) {
                argv.push("--multiline");
                argv.push("--multiline-dotall");
            }
            if fields.get("fixed_strings").and_then(Value::as_bool) == Some(true) {
                argv.push("--fixed-strings");
            }
            if let Some(include) =
                optional_text(fields, if cc { "glob" } else { "include_pattern" })?
            {
                argv.push("--glob");
                argv.push(include);
            }
            if let Some(kind) = optional_text(fields, "type")? {
                argv.push("--type");
                argv.push(kind);
            }
            argv.push("--regexp");
            argv.push(pattern);
        }
        argv.push("--");
        argv.push(optional_text(fields, if cc { "path" } else { "dir_path" })?.unwrap_or("."));
        #[derive(Serialize)]
        struct Query<'a> {
            command: &'a [&'a str],
        }
        self.canonical(invocation, &Query { command: &argv }, work)
    }
}
