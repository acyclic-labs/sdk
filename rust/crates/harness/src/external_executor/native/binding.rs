//! Provider selection is explicit when a native process changes working views.
use super::*;

/// Provider binding read from the user's configuration volume.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookBinding {
    /// Retains the original workspace's per-user SDK provider owner.
    Local {
        /// Absolute original workspace, independent of the native process cwd.
        workspace: PathBuf,
        /// Restricts a launched executor to its already admitted task. Omit for
        /// workspace clients that serve results for several native sessions.
        task: Option<crate::TaskId>,
    },
    /// Uses only the caller-supplied platform Streams credentials and paths.
    Platform {
        /// HTTPS or loopback HTTP, validated by the SDK provider.
        endpoint: String,
        /// Credential file, outside task requests and hook output.
        bearer_file: PathBuf,
        /// Caller-writable request-file volume.
        #[serde(
            serialize_with = "serialize_path",
            deserialize_with = "deserialize_path"
        )]
        requests: StreamPath,
        /// Actor-writable, caller-readable result-file volume.
        #[serde(
            serialize_with = "serialize_path",
            deserialize_with = "deserialize_path"
        )]
        results: StreamPath,
        /// Actor/task selected by the platform binding.
        task: Component,
    },
}

impl HookBinding {
    /// Decodes bounded configuration without opening credentials or transport.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let binding: Self = volume::decode(bytes).map_err(storage)?;
        match &binding {
            Self::Local { workspace, .. }
                if !workspace.is_absolute()
                    || workspace.as_os_str().as_encoded_bytes().contains(&0) =>
            {
                Err(Error::Invalid(
                    "local binding requires an absolute workspace".into(),
                ))
            }
            Self::Platform {
                requests, results, ..
            } if requests == results => Err(Error::Invalid(
                "hook requests and results require separate owner paths".into(),
            )),
            _ => Ok(binding),
        }
    }
}

#[cfg(any(test, all(feature = "filesystem-local", feature = "native-execution")))]
pub(super) fn require_task(
    task: crate::TaskId,
    bound: Option<crate::TaskId>,
) -> Result<crate::TaskId> {
    if bound.is_some_and(|bound| bound != task) {
        return Err(Error::Unauthorized(
            "native session differs from its admitted task binding".into(),
        ));
    }
    Ok(task)
}

pub(super) fn configured() -> Result<Option<HookBinding>> {
    std::env::var_os("ACYCLIC_HARNESS_BINDING")
        .map(|path| {
            HookBinding::from_bytes(&read_bounded(
                std::fs::File::open(path).map_err(storage)?,
                acyclic_stream::MAX_RECORD_BYTES,
            )?)
        })
        .transpose()
}

#[cfg(any(test, all(feature = "filesystem-local", feature = "native-execution")))]
pub(super) fn select_workspace(
    request: &mut super::super::ExternalRequest,
    workspace: &Path,
) -> Result<()> {
    if let super::super::ExternalAction::Attach {
        workspace: selected,
    } = &mut request.action
    {
        let workspace = workspace
            .to_str()
            .ok_or_else(|| Error::Invalid("native workspace is not UTF-8".into()))?;
        if selected != workspace {
            selected.clear();
            selected.push_str(workspace);
        }
    }
    Ok(())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

fn serialize_path<S: serde::Serializer>(
    path: &StreamPath,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(path.as_str())
}

fn deserialize_path<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<StreamPath, D::Error> {
    StreamPath::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}
