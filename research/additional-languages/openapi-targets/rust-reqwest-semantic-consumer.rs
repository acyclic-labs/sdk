use acyclic_workers_http::{
    apis::{configuration::Configuration, default_api::invoke_deployment, Error},
    models::AcyclicWorkersV1InvokeDeploymentRequest,
};

#[tokio::main]
async fn main() {
    let port = std::env::var("PORT").unwrap_or_else(|_| "18765".to_owned());
    let expect_error = std::env::var("EXPECT_ERROR").is_ok();
    let mut configuration = Configuration::new();
    configuration.base_path = format!("http://127.0.0.1:{port}");
    let mut request = AcyclicWorkersV1InvokeDeploymentRequest::new();
    request.alias = Some("prod".to_owned());
    request.method = Some("POST".to_owned());
    request.url = Some("/hello".to_owned());
    request.body = Some(vec![1, 2, 3]);
    assert_eq!(serde_json::to_value(&request).unwrap()["body"], "AQID");

    match invoke_deployment(&configuration, "prod", request).await {
        Ok(response) if expect_error => panic!("expected generated Rust API error, got {response:?}"),
        Ok(response) => {
            assert_eq!(response.status, Some(200));
            assert_eq!(response.body, Some(vec![b'o', b'k']));
            assert_eq!(response.resolved_sha256, Some(vec![1, 2, 3]));
            if port == "18767" {
                assert_eq!(response.resolved_revision, Some(Some("18446744073709551615".to_owned())));
            }
            println!("rust-reqwest status={:?} revision={:?}", response.status, response.resolved_revision);
        }
        Err(Error::ResponseError(content)) if expect_error => {
            assert_eq!(content.status.as_u16(), 409);
            assert!(content.entity.is_some());
            println!("rust-reqwest error={} body={}", content.status, content.content);
        }
        Err(error) => panic!("generated Rust API request failed unexpectedly: {error}"),
    }
}