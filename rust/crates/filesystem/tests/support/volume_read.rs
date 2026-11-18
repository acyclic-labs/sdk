use acyclic_fs::{ByteRange, CancellationToken};
use acyclic_fs_format::identity::FileId;
use acyclic_stream::OwnedBytes;

use super::B;
use super::fixtures::{TestCheckout, TestResult};

pub(super) async fn read(
    checkout: &mut TestCheckout,
    id: FileId,
    offset: u64,
    length: u64,
    cancel: &CancellationToken,
) -> TestResult<OwnedBytes> {
    Ok(checkout
        .read_file_range(id, ByteRange { offset, length }, B, cancel)
        .await?
        .value
        .bytes)
}
