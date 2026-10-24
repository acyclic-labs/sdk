//! Ordinary task-owned approvals share the private request/decision file ledger.
use super::*;
use crate::task_volume::{Component, FileApproval, FileKey, FileValue, Query};
use futures::StreamExt as _;
impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn open_file_approval(
        &self,
        id: InteractionId,
        request: &Interaction,
    ) -> Result<()> {
        let files = self
            .task_files
            .as_ref()
            .ok_or_else(|| Error::Unsupported("approval files are not bound".into()))?;
        let (owner, _) = self.owner.as_ref().ok_or_else(|| {
            Error::Unauthorized("approval requires an executing task owner".into())
        })?;
        let request = FileApproval {
            task: owner.task_id(),
            id,
            request: request.clone(),
        };
        request.binding()?;
        let bytes =
            serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() as u64 > self.maximum_payload_bytes {
            return Err(Error::Invalid("approval request exceeds file bound".into()));
        }
        let key = request.key()?;
        loop {
            owner.verify(false, &mut None).await?;
            let snapshot = files.snapshot(&Query::File(key.clone()), 1).await?;
            if let Some(prior) = snapshot.files.first() {
                return if prior.bytes.as_ref() == bytes.as_slice() {
                    Ok(())
                } else {
                    Err(Error::Conflict("approval request identity reused".into()))
                };
            }
            match files
                .inner
                .append(
                    snapshot.generation,
                    &FileValue::new(key.clone(), bytes.clone()),
                )
                .await
            {
                Ok(_) => {
                    owner.verify(false, &mut None).await?;
                    return Ok(());
                }
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
    pub(super) async fn file_approval_outcome(
        &self,
        id: InteractionId,
    ) -> Result<Option<InteractionOutcome>> {
        let files = self
            .task_files
            .as_ref()
            .ok_or_else(|| Error::Unsupported("approval files are not bound".into()))?;
        let (owner, _) = self.owner.as_ref().ok_or_else(|| {
            Error::Unauthorized("approval requires an executing task owner".into())
        })?;
        let query = Query::Approval {
            task: Component::new(owner.task_id().to_string())?,
            request: Component::new(id.to_string())?,
        };
        let mut observed = files.observe(query, None, 2).await?;
        let snapshot = observed
            .snapshot
            .take()
            .ok_or_else(|| Error::Conflict("approval snapshot missing".into()))?;
        let key = FileKey::Task {
            task: Component::new(owner.task_id().to_string())?,
            file: crate::task_volume::TaskFile::ApprovalRequest(Component::new(id.to_string())?),
        };
        let request: FileApproval = serde_json::from_slice(
            &snapshot
                .files
                .iter()
                .find(|file| file.key == key)
                .ok_or_else(|| Error::Conflict("approval request missing".into()))?
                .bytes,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        if request.task != owner.task_id() || request.id != id {
            return Err(Error::Unauthorized(
                "approval request belongs to another task".into(),
            ));
        }
        let outcome = |approved| {
            if approved {
                InteractionOutcome::Approved {}
            } else {
                InteractionOutcome::Declined {}
            }
        };
        for file in &snapshot.files {
            if let Some(approved) = request.decision(&file.key, &file.bytes)? {
                return Ok(Some(outcome(approved)));
            }
        }
        // Snapshot and follow are one atomic cut: decisions arriving during the
        // read are retained, and suspended turns consume no timers or retries.
        while let Some(change) = observed.changes.next().await {
            owner.verify(false, &mut None).await?;
            let change = change?;
            for file in change.file.iter().chain(&change.files) {
                if let Some(approved) = request.decision(&file.key, &file.bytes)? {
                    return Ok(Some(outcome(approved)));
                }
            }
        }
        Err(Error::Conflict("approval follow ended".into()))
    }
}
