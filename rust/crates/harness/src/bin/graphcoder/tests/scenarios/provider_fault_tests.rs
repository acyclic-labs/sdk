use super::*;
use acyclic_harness_providers::{HttpRequest, HttpTransport, ReqwestTransport};
use futures::StreamExt as _;
use proptest::prelude::*;

fn request(protocol: &str) -> Value {
    if protocol == "responses" {
        json!({"model":"parent","stream":true,"input":"HTTP_FAULT_PROBE"})
    } else {
        json!({"model":"parent","stream":true,"max_tokens":64,
            "messages":[{"role":"user","content":"HTTP_FAULT_PROBE"}]})
    }
}

proptest! {
    #[test]
    fn status_fault_response_retains_status_and_retry_after(
        code in 400u16..600, retry_after in prop::option::of(any::<u32>()),
    ) {
        let matches = |response: &Response<Body>| {
            response.status().as_u16() == code
                && response.headers().get("retry-after").map(|value| value.to_str().unwrap().to_owned())
                    == retry_after.map(|seconds| seconds.to_string())
        };
        let mut response = fault_response(Fault::Status {code, retry_after}, 13);
        prop_assert!(matches(&response));
        // Restoring the old unconditional HTTP 200 response must fail the
        // same observer, even when its body still contains an error marker.
        *response.status_mut() = axum::http::StatusCode::OK;
        prop_assert!(!matches(&response));
    }
}

#[tokio::test]
async fn reset_and_malformed_replies_have_distinct_transport_outcomes() -> TestResult {
    let prefix = vec!["data: ".to_owned()];
    let reset = fault_response(Fault::Reset { prefix }, 2);
    assert!(axum::body::to_bytes(reset.into_body(), 4096).await.is_err());
    let malformed = "data: {broken JSON}\n\n";
    let response = fault_response(
        Fault::Malformed {
            body: malformed.into(),
        },
        2,
    );
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 4096)
            .await?
            .as_ref(),
        malformed.as_bytes(),
    );
    // Replacing a reset with a clean EOF is not an equivalent injection.
    let clean_eof = fault_response(
        Fault::Malformed {
            body: "data: ".into(),
        },
        2,
    );
    assert!(
        axum::body::to_bytes(clean_eof.into_body(), 4096)
            .await
            .is_ok()
    );
    Ok(())
}

#[tokio::test]
async fn a_real_http_fault_consumes_an_attempt_but_preserves_the_model_turn() -> TestResult {
    let workspace = tempfile::tempdir()?;
    for protocol in ["responses", "anthropic"] {
        let fault = Fault::Status {
            code: 429,
            retry_after: Some(0),
        };
        let case = json!({"turns":["after retry"],"faults":[fault],
            "expect":{"request_count":2}});
        let provider = start(&case, protocol, workspace.path()).await?;
        let transport = ReqwestTransport::new()?;
        let request = request(protocol);
        let mut bodies = Vec::new();
        for status in [429, 200] {
            let mut response = transport
                .send(HttpRequest {
                    url: format!(
                        "{}/{}",
                        provider.url,
                        if protocol == "responses" {
                            "responses"
                        } else {
                            "messages"
                        }
                    ),
                    headers: BTreeMap::new(),
                    body: serde_json::to_vec(&request)?.into(),
                })
                .await?;
            assert_eq!(response.status, status);
            let mut body = Vec::new();
            while let Some(bytes) = response.body.next().await {
                body.extend_from_slice(&bytes?);
            }
            bodies.push(String::from_utf8(body)?);
        }
        let recording = provider.finish().await?;
        assert_eq!(recording.attempts["parent"], 2);
        assert_eq!(recording.requests["parent"].len(), 2);
        assert_eq!(recording.remaining["parent"], 0);
        assert_eq!(recording.injected, vec![fault]);
        assert_eq!(recording.remaining_faults, 0);
        assert_eq!(bodies[0], "SCRIPTED_HTTP_ERROR_BODY");
        assert!(bodies[1].contains("after retry"));
        super::super::assertions::recording(&case, protocol, &recording)?;
        let mut lost = recording;
        lost.injected.clear();
        let error = super::super::assertions::recording(&case, protocol, &lost).unwrap_err();
        assert!(error.to_string().contains("faults were skipped"));
    }
    Ok(())
}

#[tokio::test]
async fn reset_is_observable_over_http_and_a_clean_eof_fails_the_same_oracle() -> TestResult {
    let workspace = tempfile::tempdir()?;
    for protocol in ["responses", "anthropic"] {
        for reset in [true, false] {
            let fault = if reset {
                Fault::Reset {
                    prefix: vec!["data: ".into()],
                }
            } else {
                // The old clean-EOF injection must not satisfy the reset oracle.
                Fault::Malformed {
                    body: "data: ".into(),
                }
            };
            let case = json!({"turns":[],"faults":[fault],"expect":{"request_count":1}});
            let provider = start(&case, protocol, workspace.path()).await?;
            let transport = ReqwestTransport::new()?;
            let outcome = async {
                let mut response = transport
                    .send(HttpRequest {
                        url: format!(
                            "{}/{}",
                            provider.url,
                            if protocol == "responses" {
                                "responses"
                            } else {
                                "messages"
                            }
                        ),
                        headers: BTreeMap::new(),
                        body: serde_json::to_vec(&request(protocol))?.into(),
                    })
                    .await?;
                let mut bytes = Vec::new();
                while let Some(chunk) = response.body.next().await {
                    bytes.extend_from_slice(&chunk?);
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(bytes)
            }
            .await;
            assert_eq!(outcome.is_err(), reset, "{protocol}: {outcome:?}");
            if !reset {
                assert_eq!(outcome?, b"data: ");
            }
            let recording = provider.finish().await?;
            assert_eq!(recording.attempts["parent"], 1);
            assert_eq!(recording.requests["parent"].len(), 1);
            assert_eq!(recording.remaining["parent"], 0);
            super::super::assertions::recording(&case, protocol, &recording)?;
        }
    }
    Ok(())
}
