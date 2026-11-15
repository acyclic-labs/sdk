//! One exact conditional publication for filesystem companion codecs.
use super::*;
use crate::{FilesystemCommit, FilesystemCommitOutcome, PublicationHold, StreamStateStoreError};
use acyclic_stream::{CommitCondition, CommitEffect, CommitRequest};

impl<P: AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    /// Publishes against the original authorization observation. An uncertain
    /// reply is reconciled only at its predicted position; unresolved custody
    /// remains attached to the error, never rebuilt at a newer head.
    pub(crate) async fn publish_companion(
        &self,
        observation: &AuthorizedObservation,
        dependencies: &[AuthorizedObservation],
        record: state_record::StateRecord,
        hold: PublicationHold,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, StreamStateStoreError> {
        let (tag, root, payload) = record.fields()?;
        if root.is_some() {
            return Err(StreamStateStoreError::Incompatible);
        }
        let (path, sequence) = match observation.condition() {
            CommitCondition::Absent { path } => (path.clone(), 0),
            CommitCondition::Head { path, expected, .. } => (path.clone(), *expected),
            _ => return Err(StreamStateStoreError::Incompatible),
        };
        let domain = b"acyclic-fs-companion-publication-v1\0";
        let hashing = crate::WorkCounters {
            bytes_hashed: (domain.len() + 1) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(hashing)?;
        let mut hash = blake3::Hasher::new();
        hash.update(domain);
        hash.update(&[tag]);
        work.charge(crate::WorkCounters {
            bytes_hashed: (domain.len() + 1) as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        for observation in std::iter::once(observation).chain(dependencies) {
            let (path, sequence, predecessor) = match observation.condition() {
                CommitCondition::Absent { path } => (path, 0, None),
                CommitCondition::Head {
                    path,
                    expected,
                    commit_id,
                } => (path, *expected, Some(commit_id)),
                _ => return Err(StreamStateStoreError::Incompatible),
            };
            let hashing = crate::WorkCounters {
                bytes_hashed: (8 + path.as_str().len() + 8 + 1 + predecessor.map_or(0, |_| 32))
                    as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            work.admit(hashing)?;
            hash.update(&(path.as_str().len() as u64).to_le_bytes());
            hash.update(path.as_str().as_bytes());
            hash.update(&sequence.to_le_bytes());
            hash.update(&[u8::from(predecessor.is_some())]);
            if let Some(predecessor) = predecessor {
                hash.update(predecessor.as_bytes());
            }
            work.charge(hashing)?;
        }
        let hashing = crate::WorkCounters {
            bytes_hashed: payload.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(hashing)?;
        hash.update(payload);
        work.charge(hashing)?;
        let key = acyclic_stream::IdempotencyKey::new(work.bytes(hash.finalize().as_bytes())?)?;
        let mut effects = work.vec(1)?;
        effects.push(CommitEffect::hard(Vec::new(), Vec::new()));
        let mut commit = FilesystemCommit::new(CommitRequest {
            idempotency_key: key.clone(),
            effects,
            deadline_unix_millis: None,
        });
        self.contribute_record(&mut commit, observation, &record, hold, work)?;
        self.seal_observed(&mut commit, dependencies, None, work)?;
        match self.submit(commit, work).await? {
            FilesystemCommitOutcome::Committed(_) => Ok(true),
            FilesystemCommitOutcome::Rejected(_) => Ok(false),
            FilesystemCommitOutcome::Unknown { commit, error } => {
                match self
                    .reconcile_record(path, sequence, &key, &record, work)
                    .await
                {
                    Ok(_) => {
                        drop(commit);
                        Ok(true)
                    }
                    Err(_) => Err(StreamStateStoreError::Unknown {
                        commit: Box::new(move || commit),
                        error,
                    }),
                }
            }
        }
    }
}
