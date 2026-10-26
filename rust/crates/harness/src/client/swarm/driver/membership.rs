//! Active membership follows the parent-owned settled child status files.
use super::*;
use crate::task_volume::{ChangeStream, ChildFile, ChildStatus, FileKey};
use futures::StreamExt as _;

pub(super) async fn active<P: StreamProvider + ?Sized>(
    parent: &TaskVolume<P>,
    root: &StreamPath,
    agent: &StreamPath,
) -> Result<bool> {
    let prefix = format!("{root}/");
    let reference = agent
        .as_str()
        .strip_prefix(&prefix)
        .ok_or_else(|| Error::Invalid("status agent lies outside swarm".into()))?;
    let key = FileKey::Child {
        reference: crate::task_volume::Component::new(reference)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        file: ChildFile::Status,
    };
    let snapshot = parent
        .snapshot(&crate::task_volume::Query::File(key), 1)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    snapshot.files.first().map_or(Ok(false), |file| {
        ChildStatus::parse(&file.bytes)
            .map(|status| status.active())
            .map_err(|error| Error::Invalid(error.to_string()))
    })
}

pub(super) struct Membership {
    root: StreamPath,
    initial: std::collections::BTreeSet<StreamPath>,
    changes: ChangeStream,
}
impl Membership {
    pub async fn observe<P: StreamProvider + ?Sized>(
        parent: &TaskVolume<P>,
        session: &acyclic_stream::session::Client,
        root: StreamPath,
        maximum: usize,
    ) -> Result<Self> {
        let snapshot = parent
            .snapshot(&crate::task_volume::Query::ActiveAgents, maximum)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        // Live membership shares the same bounded session as discovery/status.
        // A direct provider follow can be ready for the entire accepted tail and
        // enqueue unbounded lazy registrations before the transport is driven.
        let records = session
            .follow_checked(
                parent.path().clone(),
                snapshot.cursor.position,
                acyclic_stream::CommitId::from_bytes(snapshot.cursor.commit),
                acyclic_stream::FollowFilter::default(),
            )
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let changes = crate::task_volume::record_changes(
            crate::task_volume::Query::ActiveAgents,
            snapshot.cursor,
            records,
        );
        Ok(Self {
            root: root.clone(),
            initial: snapshot
                .files
                .into_iter()
                .map(|file| {
                    let FileKey::Child {
                        reference,
                        file: ChildFile::Status,
                    } = file.key
                    else {
                        return Err(Error::Invalid("non-status membership snapshot".into()));
                    };
                    StreamPath::new(format!("{root}/{}", reference.as_str()))
                        .map_err(|e| Error::Invalid(e.to_string()))
                })
                .collect::<Result<_>>()?,
            changes,
        })
    }
    /// Admits a snapshot member early when its status arrives first.
    pub fn discovered(&mut self, agent: &StreamPath) -> bool {
        self.initial.remove(agent)
    }
    /// Admits one snapshot member per call before any live change.
    pub async fn next(&mut self) -> Result<Option<(StreamPath, bool)>> {
        if let Some(agent) = self.initial.pop_first() {
            return Ok(Some((agent, true)));
        }
        let value = |file: crate::task_volume::FileValue| -> Result<_> {
            let status =
                ChildStatus::parse(&file.bytes).map_err(|e| Error::Invalid(e.to_string()))?;
            Ok((file.key, status.active()))
        };
        let (key, active) = {
            let change = self
                .changes
                .next()
                .await
                .ok_or_else(|| Error::Storage("membership follow ended; reobserve".into()))?
                .map_err(|e| Error::Storage(e.to_string()))?;
            match (change.file, change.removed.as_slice()) {
                (Some(file), []) => value(file)?,
                (None, [key]) => (key.clone(), false),
                (None, []) => return Ok(None),
                _ => return Err(Error::Invalid("invalid atomic membership change".into())),
            }
        };
        let FileKey::Child {
            reference,
            file: ChildFile::Status,
        } = key
        else {
            return Err(Error::Invalid(
                "membership received a non-status file".into(),
            ));
        };
        let agent = StreamPath::new(format!("{}/{}", self.root, reference.as_str()))
            .map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(Some((agent, active)))
    }
}
