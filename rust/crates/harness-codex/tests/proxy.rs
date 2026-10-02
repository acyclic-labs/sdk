//! A2 acceptance: the metered Responses proxy. Remove each `#[ignore]` as the
//! behaviour lands; `verify.sh status` lists what is still pending.

#![cfg(unix)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test assertions"
)]

mod support;

use acyclic_harness_codex::{
    Upstream,
    proxy::{ProxyStop, ResponsesProxy},
};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use support::{FakeUpstream, METERED, RecordingMeter, Reply, message_turn, sse};

const PENDING: &str = "A2: the proxy is not built yet";

fn upstream(fake: &FakeUpstream) -> Upstream {
    Upstream {
        base_url: format!("{}/v1", fake.url),
        api_key: "real-upstream-key".into(),
        extra_body: json!({"reasoning": {"effort": "low"}, "service_tier": "flex"}),
    }
}

/// What Codex sends, trimmed to what the proxy may care about.
fn codex_request() -> Value {
    json!({
        "model": "gpt-5.5",
        "stream": true,
        "store": false,
        "reasoning": {"summary": "auto"},
        "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "tools": []
    })
}

async fn post(proxy: &ResponsesProxy, body: &Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/responses", proxy.base_url()))
        .bearer_auth(proxy.client_key())
        .json(body)
        .send()
        .await
        .expect("proxy reachable")
}

#[tokio::test]
async fn only_the_turns_codex_can_spend_the_key() {
    let fake = FakeUpstream::message("hello").await;
    let proxy = ResponsesProxy::start(upstream(&fake), Arc::new(RecordingMeter::default()), 8)
        .await
        .expect("proxy starts");
    for key in [None, Some("guessed")] {
        let mut request = reqwest::Client::new()
            .post(format!("{}/responses", proxy.base_url()))
            .json(&codex_request());
        if let Some(key) = key {
            request = request.bearer_auth(key);
        }
        let response = request.send().await.expect("proxy reachable");
        assert_eq!(response.status(), 401, "key {key:?}");
    }
    assert!(fake.requests().is_empty(), "nothing reached the upstream");
    assert_eq!(proxy.steps(), 0);
}

#[tokio::test]
async fn forwards_with_the_real_key_and_merges_extra_body() {
    let fake = FakeUpstream::message("hello").await;
    let meter = Arc::new(RecordingMeter::default());
    let proxy = ResponsesProxy::start(upstream(&fake), meter.clone(), 8)
        .await
        .expect(PENDING);
    assert!(proxy.base_url().starts_with("http://127.0.0.1:") && proxy.base_url().ends_with("/v1"));

    let response = post(&proxy, &codex_request()).await;
    assert_eq!(response.status(), 200);
    let text = response.text().await.expect("body");
    assert!(
        text.contains("response.completed"),
        "the stream reaches the client whole"
    );

    let seen = fake.requests();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].path, "/v1/responses");
    assert_eq!(
        seen[0].headers["authorization"], "Bearer real-upstream-key",
        "never the dummy key"
    );
    assert_eq!(
        seen[0].body["service_tier"], "flex",
        "extra_body adds fields"
    );
    assert_eq!(
        seen[0].body["reasoning"],
        json!({"summary": "auto", "effort": "low"}),
        "objects merge deeply"
    );
    assert_eq!(seen[0].body["model"], "gpt-5.5");
    assert_eq!(
        meter.recorded(),
        vec![METERED],
        "usage read from response.completed"
    );
    assert_eq!(proxy.steps(), 1);
}

#[tokio::test]
async fn sse_frames_are_passed_through_as_they_arrive() {
    let fake = FakeUpstream::start(|n, _| Reply::Sse {
        events: message_turn(n, "slow"),
        gap: Duration::from_millis(400),
    })
    .await;
    let proxy = ResponsesProxy::start(upstream(&fake), Arc::new(RecordingMeter::default()), 8)
        .await
        .expect(PENDING);
    let started = Instant::now();
    let mut response = post(&proxy, &codex_request()).await;
    let first = response
        .chunk()
        .await
        .expect("chunk")
        .expect("a first frame");
    assert!(String::from_utf8_lossy(&first).contains("response.created"));
    assert!(
        started.elapsed() < Duration::from_millis(350),
        "the first frame was not held back"
    );
}

#[tokio::test]
async fn a_stopping_meter_refuses_the_next_call_the_way_codex_stops_on() {
    let fake = FakeUpstream::message("hello").await;
    let proxy = ResponsesProxy::start(
        upstream(&fake),
        Arc::new(RecordingMeter::stopping_after(1)),
        8,
    )
    .await
    .expect(PENDING);
    assert_eq!(post(&proxy, &codex_request()).await.status(), 200);

    let refused = post(&proxy, &codex_request()).await;
    // 0.155.1 retries a 402 six times; a 429 typed insufficient_quota ends the
    // turn after one request (fixtures/codex-0.155.1/request-counts.json).
    assert_eq!(refused.status(), 429);
    let body: Value = refused.json().await.expect("JSON error");
    assert_eq!(body["error"]["type"], "insufficient_quota");
    assert_eq!(
        fake.requests().len(),
        1,
        "a refused call never reaches upstream"
    );
    assert!(
        matches!(proxy.stopped(), Some(ProxyStop::Budget(reason)) if reason.contains("test budget"))
    );
}

#[tokio::test]
async fn the_step_cap_refuses_the_call_after_the_last_step() {
    let fake = FakeUpstream::message("hello").await;
    let proxy = ResponsesProxy::start(upstream(&fake), Arc::new(RecordingMeter::default()), 2)
        .await
        .expect(PENDING);
    for _ in 0..2 {
        assert_eq!(post(&proxy, &codex_request()).await.status(), 200);
    }
    assert_eq!(post(&proxy, &codex_request()).await.status(), 429);
    assert_eq!(proxy.steps(), 2);
    assert_eq!(proxy.stopped(), Some(ProxyStop::StepLimit(2)));
}

#[tokio::test]
async fn other_paths_are_a_visible_404() {
    let fake = FakeUpstream::message("hello").await;
    let proxy = ResponsesProxy::start(upstream(&fake), Arc::new(RecordingMeter::default()), 8)
        .await
        .expect(PENDING);
    let models = reqwest::get(format!("{}/models", proxy.base_url()))
        .await
        .expect("reachable");
    assert_eq!(models.status(), 404);
    let body: Value = models.json().await.expect("JSON error body");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("/v1/models"))
    );
    assert!(fake.requests().is_empty());
}

#[tokio::test]
async fn upstream_errors_pass_through_unchanged() {
    for code in [400_u16, 401, 500] {
        let fake = FakeUpstream::start(move |_, _| {
            Reply::Status(
                code,
                json!({"error": {"message": format!("upstream {code}"), "type": "x"}}),
            )
        })
        .await;
        let meter = Arc::new(RecordingMeter::default());
        let proxy = ResponsesProxy::start(upstream(&fake), meter.clone(), 8)
            .await
            .expect(PENDING);
        let response = post(&proxy, &codex_request()).await;
        assert_eq!(response.status(), code);
        let body: Value = response.json().await.expect("JSON");
        assert_eq!(body["error"]["message"], format!("upstream {code}"));
        assert!(meter.recorded().is_empty(), "failed calls cost nothing");
    }
}

#[tokio::test]
async fn a_failed_sse_response_is_not_metered_as_usage() {
    let fake = FakeUpstream::start(|n, _| {
        sse(vec![
            json!({"type": "response.created", "response": {"id": format!("resp_{n}")}}),
            json!({"type": "response.failed", "response": {"id": format!("resp_{n}"), "status": "failed",
                   "error": {"code": "server_error", "message": "boom"}}}),
        ])
    })
    .await;
    let meter = Arc::new(RecordingMeter::default());
    let proxy = ResponsesProxy::start(upstream(&fake), meter.clone(), 8)
        .await
        .expect(PENDING);
    let text = post(&proxy, &codex_request())
        .await
        .text()
        .await
        .expect("body");
    assert!(text.contains("response.failed"));
    assert!(meter.recorded().is_empty());
}
