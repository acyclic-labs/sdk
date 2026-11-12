//! Coupled-head publication through the same provider-backed conformance seam.
use super::*;
use crate::{FilesystemCommit, FilesystemCommitOutcome, PublishSpec, StreamAuthorityStore};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitEffect, CommitMutation, CommitRequest, StreamPath,
    StreamProviderExt as _,
};

/// Prove two staged workspace heads and a caller record share one decision,
/// including rejection when the second workspace changes after preparation.
pub async fn verify<P, O>(fs: &Fs<StreamAuthorityStore<P>, O>) -> Result<(), Failure>
where
    P: crate::distributed::AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    let cancel = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let mut workspaces = Vec::new();
    let mut expected = Vec::new();
    let mut staged = Vec::new();
    for name in ["conformance-publish-first", "conformance-publish-second"] {
        let workspace = value(fs.create_workspace(name, budget, &cancel).await)?;
        published(
            workspace
                .write("/file", Bytes::from_static(b"before"))
                .await?,
        )?;
        expected.push(value(workspace.head(budget, &cancel).await)?);
        let mut transaction = value(
            workspace
                .begin_transaction(IdempotencyKey::new(), budget, &cancel)
                .await,
        )?;
        transaction.write_text("/file", "after").await?;
        staged.push(
            transaction
                .checkout
                .checkpoint(budget, &cancel)
                .await
                .map_err(|failure| failure.error)?
                .value,
        );
        workspaces.push(workspace);
    }
    let store = crate::StreamGitCompatStore::new(StreamAuthorityStore::new(fs.stream_provider()));
    let states = expected
        .iter()
        .map(|before| {
            let mut state = crate::GitCompatState::new("pending-merge", before.workspace_id());
            state.revision = 1;
            state
        })
        .collect::<Vec<_>>();
    let state_changes = expected
        .iter()
        .zip(&states)
        .map(|(before, state)| crate::GitCompatCas {
            workspace: before.workspace_id(),
            expected: 0,
            replacement: Some(state),
        })
        .collect::<Vec<_>>();
    let marker = StreamPath::new("filesystem-conformance/coupled-cut")?;
    for concurrent in [true, false] {
        let specs = expected
            .iter()
            .zip(&staged)
            .enumerate()
            .map(|(index, (expected, generation))| PublishSpec {
                expected,
                generation: *generation,
                operation: crate::OperationId::from_bytes([if index == 0 { 73 } else { 74 }; 16]),
                permit: crate::PublicationPermit::Unrestricted,
            })
            .collect::<Vec<_>>();
        let mut commit = FilesystemCommit::new(CommitRequest {
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: marker.clone(),
                }],
                vec![CommitMutation::Append {
                    path: marker.clone(),
                    records: vec![AppendRecord::from(acyclic_stream::OwnedBytes::from_static(
                        b"coupled-cut",
                    ))],
                }],
            )],
            idempotency_key: acyclic_stream::IdempotencyKey::new(if concurrent {
                b"coupled-conflict".to_vec()
            } else {
                b"coupled-success".to_vec()
            })?,
        });
        value(
            fs.prepare_publish_many(&specs, &state_changes, &mut commit, budget, &cancel)
                .await,
        )?;
        drop(specs);
        for (workspace, original) in workspaces.iter().zip(&expected) {
            if value(workspace.head(budget, &cancel).await)?.id() != original.id() {
                return Err(Failure::Invariant("preparation published a workspace"));
            }
        }
        if concurrent {
            let second = workspaces
                .get(1)
                .ok_or(Failure::Invariant("missing second workspace"))?;
            published(
                second
                    .write("/file", Bytes::from_static(b"user edit"))
                    .await?,
            )?;
            let slot = expected
                .get_mut(1)
                .ok_or(Failure::Invariant("missing second expected head"))?;
            *slot = value(second.head(budget, &cancel).await)?;
        }
        let outcome = fs
            .submit_commit(commit, budget, &cancel)
            .await
            .map_err(|failure| failure.error)?
            .value;
        if concurrent {
            if !matches!(outcome, FilesystemCommitOutcome::Rejected(_)) {
                return Err(Failure::Invariant(
                    "one stale head did not reject the coupled cut",
                ));
            }
            if !matches!(
                fs.stream_provider().tail(marker.clone()).await,
                Err(acyclic_stream::StreamError::NotFound)
            ) {
                return Err(Failure::Invariant(
                    "rejected publication appended its caller record",
                ));
            }
        } else {
            if !matches!(outcome, FilesystemCommitOutcome::Committed(_)) {
                return Err(Failure::Invariant("uncontended coupled cut did not commit"));
            }
            if fs.stream_provider().tail(marker.clone()).await? != 1 {
                return Err(Failure::Invariant(
                    "coupled cut did not append exactly one caller record",
                ));
            }
        }
        for (before, state) in expected.iter().zip(&states) {
            let observed = crate::GitCompatStore::load(&store, before.workspace_id())
                .await
                .map_err(|error| WorkspaceError::GitCompat(Box::new(error)))?;
            if observed.as_ref() != (!concurrent).then_some(state) {
                return Err(Failure::Invariant(
                    "merge state exposed a partial coupled cut",
                ));
            }
        }
        for ((workspace, original), generation) in workspaces.iter().zip(&expected).zip(&staged) {
            let wanted = if concurrent {
                original.id()
            } else {
                *generation
            };
            if value(workspace.head(budget, &cancel).await)?.id() != wanted {
                return Err(Failure::Invariant(
                    "workspace heads exposed a partial coupled cut",
                ));
            }
        }
    }
    Ok(())
}
