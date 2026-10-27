//! One staging path for measured and unmetered content publication.
use super::*;
use crate::{core::reserve_copy_vector, store::history::HistoryIo, wire_codec::CodecQuota};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(super) async fn stage_publication_batch(
        transaction: &mut acyclic_fs::Transaction<A, O>,
        mutations: &[WorkspaceMutation],
        io: Option<&PreparationIo>,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<bool> {
        crate::model::charge_items(io, mutations.len() as u64)?;
        if mutations
            .iter()
            .any(|mutation| matches!(mutation, WorkspaceMutation::Remove { .. }))
        {
            return Ok(false);
        }
        let directory_count = mutations
            .iter()
            .filter(|mutation| matches!(mutation, WorkspaceMutation::CreateDirectory { .. }))
            .count();
        crate::model::charge_items(io, mutations.len() as u64)?;
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer.as_mut().map(|value| value as &mut dyn HistoryIo);
        // Keep the quota alive across the SDK call. Every exit, including an
        // allocation failure or cancellation, retires these borrowed descriptors.
        let mut quota = CodecQuota::new(&mut work, 0)?;
        let mut directories = reserve_copy_vector(directory_count, &mut quota)?;
        let mut files = reserve_copy_vector(mutations.len() - directory_count, &mut quota)?;
        for mutation in mutations {
            crate::model::charge_items(io, 1)?;
            let copied = if matches!(mutation, WorkspaceMutation::CreateDirectory { .. }) {
                std::mem::size_of::<&str>()
            } else {
                std::mem::size_of::<(&str, acyclic_stream::OwnedBytes)>()
            } as u64;
            if let Some(work) = quota.work().as_deref_mut() {
                work.admit_copy(copied)?;
                work.copy_bytes(copied)?;
            }
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => {
                    directories.push(path.as_str());
                }
                WorkspaceMutation::PutFile { path, bytes } => {
                    files.push((path.as_str(), bytes.clone()));
                }
                WorkspaceMutation::Remove { .. } => {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "publication batch contains a removal",
                    ));
                }
            }
        }
        let result = if let Some(io) = io {
            io.sdk(
                transaction
                    .write_batch(&directories, &files, io, cancellation)
                    .await,
            )
        } else {
            transaction
                .write_batch(
                    &directories,
                    &files,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await
                .map_err(|failure| map_error(failure.error))
                .map(|_| ())
        };
        drop((files, directories));
        drop(quota);
        result.map(|()| true)
    }
}

#[cfg(test)]
#[path = "publication_batch_tests.rs"]
mod tests;
