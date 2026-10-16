use super::*;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct CommandHook {
    #[serde(rename = "type")]
    kind: String,
    command: String,
    #[serde(default)]
    args: Option<Vec<String>>,
    #[serde(default = "default_timeout")]
    timeout: u64,
    #[serde(default)]
    shell: Option<String>,
    #[serde(default, rename = "async")]
    asynchronous: bool,
    #[serde(default, rename = "onFailure")]
    on_failure: OnFailure,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnFailure {
    #[default]
    Continue,
    Block,
}

fn default_timeout() -> u64 {
    60
}

impl CommandHook {
    fn normalize(self) -> Result<HookSpec> {
        if self.kind != "command" || self.asynchronous {
            return Err(Error::Unsupported(
                "only command hooks are supported".into(),
            ));
        }
        let run = if let Some(argv) = self.args {
            HookRun::Executable {
                executable: self.command,
                argv,
            }
        } else {
            HookRun::Shell {
                command: self.command,
                shell: self.shell,
            }
        };
        Ok(HookSpec {
            run,
            timeout_ms: self
                .timeout
                .checked_mul(1_000)
                .ok_or_else(|| Error::Invalid("hook timeout overflows milliseconds".into()))?,
            failure: match self.on_failure {
                OnFailure::Continue => HookFailure::Report,
                OnFailure::Block => HookFailure::Block,
            },
        })
    }
}

#[derive(Deserialize)]
struct Group {
    #[serde(default)]
    matcher: String,
    hooks: Vec<CommandHook>,
}

#[derive(Deserialize)]
pub(crate) struct Document {
    #[serde(default)]
    pub(crate) hooks: BTreeMap<String, Value>,
}

impl Hooks {
    pub(crate) fn add_document(&mut self, document: Document) -> Result<()> {
        let mut rules = Vec::new();
        for (name, groups) in document.hooks {
            let on = match name.as_str() {
                "SessionStart" => HookEvent::SessionStart,
                "PreToolUse" => HookEvent::PreToolUse,
                "PostToolUse" => HookEvent::PostToolUse,
                "Stop" => HookEvent::Stop,
                _ => continue,
            };
            let groups: Vec<Group> = serde_json::from_value(groups)
                .map_err(|error| Error::Invalid(format!("hook configuration: {error}")))?;
            for group in groups {
                rules.push(HookRule {
                    on,
                    matcher: group.matcher,
                    hooks: group
                        .hooks
                        .into_iter()
                        .map(CommandHook::normalize)
                        .collect::<Result<_>>()?,
                });
            }
        }
        self.add_rules(rules)
    }
}
