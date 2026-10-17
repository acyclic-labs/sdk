//! Active membership follows the parent-owned settled child status files.
use super::*;
use crate::task_volume::{ChangeStream, ChildFile, ChildStatus, FileKey};
use futures::StreamExt as _;

pub(super) struct Membership {
    root: StreamPath,
    initial: std::vec::IntoIter<crate::task_volume::FileValue>,
    changes: ChangeStream,
}
impl Membership {
    pub async fn observe<P: StreamProvider + ?Sized>(
        parent: &TaskVolume<P>,
        root: StreamPath,
        maximum: usize,
    ) -> Result<Self> {
        let observation = parent
            .observe(crate::task_volume::Query::ActiveAgents, None, maximum)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let snapshot = observation
            .snapshot
            .ok_or_else(|| Error::Conflict("active membership snapshot missing".into()))?;
        Ok(Self {
            root,
            initial: snapshot.files.into_iter(),
            changes: observation.changes,
        })
    }
    pub async fn next(&mut self) -> Result<Option<(StreamPath, bool)>> {
        let file = if let Some(file) = self.initial.next() {
            Some(file)
        } else {
            self.changes
                .next()
                .await
                .ok_or_else(|| Error::Storage("membership follow ended; reobserve".into()))?
                .map_err(|e| Error::Storage(e.to_string()))?
                .file
        };
        let Some(file) = file else {
            return Ok(None);
        };
        let FileKey::Child {
            reference,
            file: ChildFile::Status,
        } = file.key
        else {
            return Err(Error::Invalid(
                "membership received a non-status file".into(),
            ));
        };
        let status = ChildStatus::parse(&file.bytes).map_err(|e| Error::Invalid(e.to_string()))?;
        let agent = StreamPath::new(format!("{}/{}", self.root, reference.as_str()))
            .map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(Some((agent, status.active())))
    }
}
