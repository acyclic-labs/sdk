use super::*;
use crate::test_support::{ScriptedSearch, ScriptedSearchProvider};
use std::sync::Arc;

#[test]
fn request_file_contracts() -> Result<()> {
    for (query, limit, valid) in [
        ("test".into(), 1, true),
        ("test".into(), 20, true),
        ("".into(), 1, false),
        (" \n\t".into(), 1, false),
        ("test".into(), 0, false),
        ("test".into(), 21, false),
        ("a".repeat(4097), 1, false),
        ("東".repeat(1400), 1, true),
        ("東".repeat(4096), 1, true),
        ("東".repeat(4097), 1, false),
        ("🦀".repeat(4096), 1, true),
        ("🦀".repeat(4097), 1, false),
    ] {
        assert_eq!(
            SearchRequest {
                query,
                max_results: limit
            }
            .validate()
            .is_ok(),
            valid
        );
    }
    for invalid in [
        serde_json::json!({"query":"test", "key":"must-not-accept"}),
        serde_json::json!({"query":"test", "max_results":-1}),
    ] {
        assert!(serde_json::from_value::<SearchRequest>(invalid).is_err());
    }
    Ok(())
}

#[test]
fn unicode_validation_matches_the_advertised_schema() -> Result<()> {
    let schema = crate::tool::schema::input::<SearchRequest>()?;
    let validator =
        jsonschema::validator_for(&schema).map_err(|e| Error::Invalid(e.to_string()))?;
    for (query, valid) in [
        ("東".repeat(1366), true),
        ("東".repeat(4096), true),
        ("東".repeat(4097), false),
        ("🦀".repeat(4096), true),
        ("🦀".repeat(4097), false),
    ] {
        let input = serde_json::json!({"query":query});
        assert_eq!(validator.is_valid(&input), valid);
        let request: SearchRequest =
            serde_json::from_value(input).map_err(|e| Error::Invalid(e.to_string()))?;
        assert_eq!(request.validate().is_ok(), valid);
    }
    Ok(())
}

#[tokio::test]
async fn scripted_search_checks_requests_and_exhaustion() -> Result<()> {
    let request = SearchRequest {
        query: "test".into(),
        max_results: 5,
    };
    let response = SearchResponse { results: vec![] };
    let search = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: request.clone(),
        response: Some(response.clone()),
        error: None,
    }])?);
    let wrong = SearchRequest {
        query: "wrong".into(),
        max_results: 5,
    };
    assert!(matches!(
        search.search(&wrong).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(search.remaining(), 1);
    assert_eq!(search.search(&request).await?, response);
    assert_eq!(search.remaining(), 0);
    assert!(search.search(&request).await.is_err());
    assert!(
        ScriptedSearchProvider::new([ScriptedSearch {
            request,
            response: Some(response),
            error: Some("ambiguous".into()),
        }])
        .is_err()
    );
    Ok(())
}
