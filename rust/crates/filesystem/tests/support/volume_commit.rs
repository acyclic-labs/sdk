use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Checkout, FsError, PublicationPermit,
};
use acyclic_fs_format::identity::OperationId;

use super::fixtures::TestResult;
use super::{B, allow};

pub(super) async fn commit<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    checkout: &mut Checkout<A, O>,
    cancel: &CancellationToken,
) -> TestResult {
    checkout
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            cancel,
            allow::<FsError>,
        )
        .await?;
    Ok(())
}
