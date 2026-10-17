//! One immutable, fenced parent/label/native-child provenance file.
use super::rejection::{Checked, RequestRejection};
use super::{
    ChildSession, ExternalAction, ExternalRequest, normalize, publication::storage, volume,
};
use crate::{
    Error, Result, TaskId,
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskVolume},
};
use acyclic_stream::{CommitCondition, StreamProvider};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    parent: TaskId,
    label: String,
    child: ChildSession,
}

impl Binding {
    fn requested(parent: TaskId, request: &ExternalRequest) -> Result<Self> {
        let (label, child) = selection(request)?;
        Ok(Self {
            parent,
            label: label.into(),
            child: child.clone(),
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let binding: Self = volume::decode(bytes).map_err(storage)?;
        if !super::request::valid_text(&binding.label) || !binding.child.valid() {
            return Err(Error::Invalid(
                "invalid retained native child binding".into(),
            ));
        }
        Ok(binding)
    }

    fn require(&self, parent: TaskId, request: &ExternalRequest) -> Result<Checked<()>> {
        let (label, child) = selection(request)?;
        if self.parent != parent || self.label != label || &self.child != child {
            return Ok(Err(RequestRejection::ChildBinding));
        }
        Ok(Ok(()))
    }
}

fn selection(request: &ExternalRequest) -> Result<(&str, &ChildSession)> {
    match &request.action {
        ExternalAction::Fork {
            child_id,
            child_session,
            ..
        }
        | ExternalAction::Join {
            child_id,
            child_session,
        } => Ok((child_id, child_session)),
        _ => Err(Error::Invalid(
            "request does not select a native child".into(),
        )),
    }
}

fn key(parent: TaskId, request: &ExternalRequest, label: &str) -> Result<FileKey> {
    let operation =
        normalize::child_start_operation(request.host, &request.session_id, &request.caller, label);
    Ok(FileKey::Task {
        task: Component::new(parent.to_string()).map_err(storage)?,
        file: TaskFile::Context(
            Component::new(format!("native-child-{operation}")).map_err(storage)?,
        ),
    })
}

/// Called by fork before admission, using that command's original owner fence.
pub(super) async fn bind<P: StreamProvider + ?Sized>(
    results: &TaskVolume<P>,
    parent: TaskId,
    request: &ExternalRequest,
    condition: CommitCondition,
    deadline: Option<u64>,
) -> Result<Checked<()>> {
    let binding = Binding::requested(parent, request)?;
    if !matches!(request.action, ExternalAction::Fork { .. }) {
        return Err(Error::Invalid(
            "only a fork may create a native child binding".into(),
        ));
    }
    let file = FileValue::new(
        key(parent, request, &binding.label)?,
        volume::encode(&binding).map_err(storage)?,
    );
    match super::append::publish(results, &file, Some(condition), deadline).await {
        Ok(()) => Ok(Ok(())),
        Err(crate::task_volume::Error::ImmutableConflict(_)) => {
            match require(results, parent, request).await? {
                Err(rejected) => Ok(Err(rejected)),
                Ok(()) => Err(Error::Conflict(
                    "native child binding metadata changed".into(),
                )),
            }
        }
        Err(error) => Err(storage(error)),
    }
}

/// Stop resolves only the binding committed by the parent's fenced fork.
pub(super) async fn require<P: StreamProvider + ?Sized>(
    results: &TaskVolume<P>,
    parent: TaskId,
    request: &ExternalRequest,
) -> Result<Checked<()>> {
    let (label, _) = selection(request)?;
    let snapshot = results
        .snapshot(&Query::File(key(parent, request, label)?), 1)
        .await
        .map_err(storage)?;
    let Some(file) = snapshot.files.first() else {
        return Ok(Err(RequestRejection::UnboundChildLabel));
    };
    Binding::decode(&file.bytes)?.require(parent, request)
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn fuzz(bytes: &[u8]) {
    if let Ok(binding) = Binding::decode(bytes) {
        let encoded = volume::encode(&binding).unwrap();
        assert_eq!(Binding::decode(&encoded).unwrap(), binding);
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
