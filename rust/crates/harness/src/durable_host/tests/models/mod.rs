use super::*;

#[tokio::test]
async fn model_claims_use_pinned_ceiling_and_current_owner() -> Result<()> {
    model_claim_host(Arc::new(MemoryStream::default()))
        .await
        .map(|_| ())
}

mod fixture;
pub(super) use fixture::model_claim_host;
