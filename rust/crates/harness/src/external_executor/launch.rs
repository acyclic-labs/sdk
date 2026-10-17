//! A native executor declaration retained as an ordinary admitted input file.
use super::{ChildSession, ExternalAction, ExternalHost, ExternalRequest, normalize};
use crate::{Error, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

// Workflow command kinds are component labels; the existing process effect URI
// is not a valid command label. Both dispatch and detection use this one tag.
pub(crate) const PROCESS_COMMAND: &str = "acyclic.native_process.v1";

// The retained file contains one already-bounded request plus its fixed parent
// envelope. It is not a transport frame and must not reuse the record bound.
const BOUNDS: crate::JsonBounds = crate::JsonBounds {
    maximum_bytes: (acyclic_stream::MAX_RECORD_BYTES
        + br#"{"parent":"00000000-0000-0000-0000-000000000000","request":}"#.len())
        as u64,
    maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
    maximum_depth: 256,
};

pub(crate) fn encode<T: Serialize>(launch: &Launch<T>) -> Result<Vec<u8>> {
    crate::json_bytes_bounded(launch, BOUNDS, None, &mut |_| Ok(()))
}

/// Native arguments are literal argv entries, never shell interpolation.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Argument {
    Text(String),
    SessionFile,
    SessionId,
    Prompt,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Executor {
    pub session: ChildSession,
    pub executable: PathBuf,
    pub arguments: Vec<Argument>,
    pub session_bytes: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<super::native_context::source::Selection>,
    pub timeout_ms: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Spawn {
    pub id: String,
    pub prompt: String,
    pub executor: Executor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Context {
    Scratch,
    NativeCut,
}

pub(crate) struct Prepared {
    pub executor: crate::conversation::FileRef,
    pub context: Option<crate::conversation::FileRef>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Launch<T = ExternalRequest> {
    pub parent: TaskId,
    pub request: T,
}

pub(crate) fn requested(request: &ExternalRequest) -> bool {
    matches!(&request.action, ExternalAction::RunTool {name, arguments, ..}
        if name == "acyclic_spawn" &&
            ["id", "prompt", "executor"].iter().any(|key| arguments.get(key).is_some()))
}

impl Spawn {
    pub(crate) fn from_request(request: &ExternalRequest) -> Result<Self> {
        let ExternalAction::RunTool { arguments, .. } = &request.action else {
            return Err(invalid());
        };
        if !requested(request) {
            return Err(invalid());
        }
        let spawn: Self = Self::deserialize(arguments).map_err(|_| invalid())?;
        if !super::request::valid_text(&spawn.id)
            || !super::request::valid_text(&spawn.prompt)
            || !spawn.executor.session.valid()
            || !spawn.executor.executable.is_absolute()
            || spawn
                .executor
                .executable
                .as_os_str()
                .as_encoded_bytes()
                .contains(&0)
            || spawn
                .executor
                .executable
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
            || spawn.executor.timeout_ms == 0
            || spawn
                .executor
                .arguments
                .iter()
                .any(|argument| matches!(argument, Argument::Text(text) if text.contains('\0')))
            || !spawn
                .executor
                .arguments
                .iter()
                .any(|argument| matches!(argument, Argument::Prompt))
        {
            return Err(invalid());
        }
        if spawn.executor.session.host == ExternalHost::Pi {
            super::native_context::fresh(
                spawn.executor.session_bytes.as_bytes(),
                &spawn.executor.session.session_id,
            )?;
            if spawn.executor.session.caller.is_some() {
                return Err(invalid());
            }
        }
        if spawn.executor.context.is_some() != matches!(spawn.context, Some(Context::NativeCut))
            || (spawn.executor.context.is_some() && request.host != ExternalHost::Pi)
        {
            return Err(invalid());
        }
        Ok(spawn)
    }

    pub(crate) fn fork(&self, source: &ExternalRequest) -> ExternalRequest {
        let action = ExternalAction::Fork {
            child_id: self.id.clone(),
            kind: None,
            child_session: self.executor.session.clone(),
        };
        ExternalRequest {
            operation_id: normalize::operation(
                source.host,
                &source.session_id,
                &source.caller,
                &action,
            ),
            host: source.host,
            session_id: source.session_id.clone(),
            turn_id: source.turn_id.clone(),
            caller: source.caller.clone(),
            action,
        }
    }

    pub(crate) fn arguments(&self, session: &Path) -> Result<Vec<String>> {
        self.executor
            .arguments
            .iter()
            .map(|argument| {
                Ok(match argument {
                    Argument::Text(text) => text.clone(),
                    Argument::SessionFile => session.to_str().ok_or_else(invalid)?.into(),
                    Argument::SessionId => self.executor.session.session_id.clone(),
                    Argument::Prompt => self.prompt.clone(),
                })
            })
            .collect()
    }

    pub(crate) fn attachment(
        &self,
        source: &ExternalRequest,
        workspace: &str,
    ) -> Result<ExternalRequest> {
        self.fork(source).child_attachment(workspace)
    }

    pub(crate) fn seed(&self, root: &Path) -> Result<Vec<u8>> {
        if self.executor.session.host != ExternalHost::Pi {
            return Ok(self.executor.session_bytes.as_bytes().to_vec());
        }
        super::native_context::seed(self.executor.session_bytes.as_bytes(), root)
    }
}

impl Launch {
    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, Spawn)> {
        let launch: Self = crate::json_from_slice_bounded(
            std::marker::PhantomData,
            bytes,
            BOUNDS,
            bytes.len() as u64,
            None,
            None,
            &mut |_| Ok(()),
        )
        .map_err(|_| invalid())?;
        if !launch.request.valid() {
            return Err(invalid());
        }
        let spawn = Spawn::from_request(&launch.request)?;
        Ok((launch, spawn))
    }
}

pub(crate) fn process_operation(child: TaskId) -> crate::OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.native-process-task/v1\0");
    hash.update(&child.into_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    crate::OperationId::from_bytes(id)
}

pub(crate) fn process_directory(mount: &Path, session: TaskId) -> PathBuf {
    mount
        .join("native-processes")
        .join(TaskId::from_bytes(process_operation(session).into_bytes()).to_string())
}

fn invalid() -> Error {
    Error::Invalid("invalid native executor declaration".into())
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn fuzz(bytes: &[u8]) {
    if let Ok((launch, spawn)) = Launch::decode(bytes) {
        if let Ok(encoded) = encode(&launch) {
            assert_eq!(Launch::decode(&encoded).unwrap(), (launch, spawn));
        }
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
