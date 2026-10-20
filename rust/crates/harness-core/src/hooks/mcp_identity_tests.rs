//! Canonical consumer assertions and their mutation controls, without provider dependencies.
use futures::FutureExt as _;
use super::SUBSTITUTE_HOOK_NAME;
async fn canonical_consumers() {
    let call = crate::model::ToolCall::new(
        "call".into(),
        "mcp.docs.search".into(),
        serde_json::json!({}),
    );
    crate::test_support::assert_canonical_tool_consumers(&call)
        .await
        .unwrap();
}
#[tokio::test]
async fn canonical_hooks_and_approvals() {
    canonical_consumers().await;
}

async fn detects_substitution(
    flag: &'static std::thread::LocalKey<std::cell::Cell<bool>>,
    message: &str,
) {
    flag.with(|flag| flag.set(true));
    let result = std::panic::AssertUnwindSafe(canonical_consumers())
        .catch_unwind()
        .await;
    flag.with(|flag| flag.set(false));
    let panic = result.expect_err("consumer mutation must fail the same identity oracle");
    let text = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(text.contains(message), "{text}");
}

#[tokio::test]
async fn negative_control_detects_hook_identity_substitution() {
    detects_substitution(&SUBSTITUTE_HOOK_NAME, "canonical hook identity").await;
}

#[tokio::test]
async fn negative_control_detects_approval_identity_substitution() {
    detects_substitution(
        &crate::runtime::SUBSTITUTE_APPROVAL_NAME,
        "canonical approval identity",
    )
    .await;
}
