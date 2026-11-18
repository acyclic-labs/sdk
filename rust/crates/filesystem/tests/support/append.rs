use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, StagedContent, Transaction};
use std::error::Error;

pub(crate) async fn stage<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    transaction: &Transaction<A, O>,
    bytes: &[u8],
) -> Result<StagedContent, Box<dyn Error>> {
    let mut source = bytes;
    Ok(unbounded!(
        transaction,
        stage_content(&mut source, (bytes.len() as u64).max(1))
    )?)
}
