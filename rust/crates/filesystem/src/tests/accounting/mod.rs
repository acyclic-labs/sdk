use crate::accounting::{
    AccountingCursor, AccountingError, AccountingOperationClass, AccountingPage,
    AccountingTransition, accounting_page,
};
use crate::distributed::StreamAuthorityStore;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Head, OperationId, ProposedCommit, Sequence,
};
use crate::kernel::{RetentionKind, retention_authority_id, volume_authority_id};
use crate::storage::{AuthorityStoreError, ObjectId, ObjectKind, ReplayLimit};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthorityWork, CancellationToken, Fs, IdempotencyKey,
    MemoryFs, MemoryObjectBackend, WorkBudget,
};
use std::error::Error;
use std::sync::Arc;

const BUDGET: WorkBudget = crate::performance::work_budget(4096, 16 * 1024 * 1024);
const B: WorkBudget = WorkBudget::UNBOUNDED;

fn fixture() -> (
    MemoryFs,
    StreamAuthorityStore<acyclic_stream::MemoryStream>,
    MemoryObjectBackend,
) {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let objects = acyclic_objects::MemoryObjects::memory();
    let objects = Arc::new(objects);
    (
        Fs::from_memory_providers(stream.clone(), objects.clone()),
        StreamAuthorityStore::new(stream),
        crate::LogicalObjectStore::new(objects),
    )
}

fn root(id: crate::GenerationId) -> ObjectId {
    id.object_id()
}

async fn records(
    authority: &impl AsyncAuthorityStore,
    id: AuthorityId,
) -> Result<Vec<DurableCommit>, AuthorityStoreError> {
    authority
        .replay(
            id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 256,
                payload_bytes: 1024 * 1024,
            },
            &mut AuthorityWork::new(BUDGET, &CancellationToken::new()),
        )
        .await
        .map(|(records, _)| records)
}

async fn page(
    authority: &impl AsyncAuthorityStore,
    objects: &impl AsyncObjectStore,
    id: AuthorityId,
    cursor: AccountingCursor,
    maximum: u32,
) -> Result<AccountingPage, AccountingError> {
    accounting_page(
        authority,
        objects,
        id,
        cursor,
        maximum,
        BUDGET,
        &CancellationToken::new(),
    )
    .await
}

fn cursor(record: &DurableCommit) -> Result<AccountingCursor, Box<dyn Error>> {
    Ok(AccountingCursor {
        sequence: record.sequence,
        digest: record.digest,
        settled_at_micros: record.settled_at_micros.ok_or("settlement time missing")?,
    })
}

mod archive;
mod pages;
mod validation;
