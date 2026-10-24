use super::*;
use crate::{
    durable_host::tests::fixture::Fixture,
    test_support::{ScriptedSearch, ScriptedSearchProvider},
};
use acyclic_stream::{MemoryStream, StreamPath};

async fn once(query: String, foreign: bool) -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let fixture = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        serde_json::json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let owner = fixture.running(1).await?;
    let volume = TaskVolume::open(
        stream,
        StreamPath::new("dispatch").map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .await
    .map_err(storage)?
    .reopen_erased()
    .await
    .map_err(storage)?;
    let other = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("dispatch").map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .await
    .map_err(storage)?
    .reopen_erased()
    .await
    .map_err(storage)?;
    let input = SearchRequest {
        query,
        max_results: 5,
    };
    let provider = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: input.clone(),
        response: Some(SearchResponse { results: vec![] }),
        error: None,
    }])?);
    let operation = crate::OperationId::from_bytes([17; 16]);
    let forbidden_resolution = std::cell::Cell::new(false);
    if foreign {
        assert!(
            dispatch(
                &other,
                &owner,
                operation,
                Some(&input),
                || {
                    forbidden_resolution.set(true);
                    Err(Error::Invalid("foreign provider reached search".into()))
                },
                None
            )
            .await
            .is_err()
        );
        assert_eq!(provider.remaining(), 1);
        assert!(!forbidden_resolution.get());
    }
    let bytes = dispatch(
        &volume,
        &owner,
        operation,
        Some(&input),
        || Ok(Some(provider.clone() as Arc<dyn SearchProvider>)),
        None,
    )
    .await?
    .ok_or_else(|| Error::Invalid("missing search outcome".into()))?;
    assert_eq!(
        serde_json::from_slice::<SearchOutcome>(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        SearchOutcome::Succeeded {
            response: SearchResponse { results: vec![] }
        }
    );
    assert_eq!(provider.remaining(), 0);
    let replay = dispatch(
        &volume,
        &owner,
        operation,
        None,
        || {
            forbidden_resolution.set(true);
            Err(Error::Invalid("completed search was redispatched".into()))
        },
        None,
    )
    .await?
    .ok_or_else(|| Error::Invalid("missing replay outcome".into()))?;
    assert_eq!(bytes, replay);
    let changed = SearchRequest {
        query: format!("{} changed", input.query),
        max_results: 5,
    };
    assert!(
        dispatch(
            &volume,
            &owner,
            operation,
            Some(&changed),
            || {
                forbidden_resolution.set(true);
                Err(Error::Invalid("changed request reached search".into()))
            },
            None
        )
        .await
        .is_err()
    );
    assert!(!forbidden_resolution.get());
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn tool_dispatch_preserves_request_identity_and_never_redispatches(query in "[a-zA-Z0-9 ]{1,40}", foreign in proptest::bool::ANY) {
        let query = format!("query {query}");
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        runtime.block_on(once(query,foreign)).map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    }
}
