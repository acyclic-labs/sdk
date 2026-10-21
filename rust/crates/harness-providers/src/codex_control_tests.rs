use super::*;
use acyclic_harness_core::tool::ToolDefinition;
use proptest::prelude::*;

#[tokio::test]
async fn explicit_no_tools_and_serial_calls_survive_codex_dispatch() -> Result<()> {
    let (_root, login, server, _) = fixture(vec![(200, RESPONSE.into())])?;
    let provider = Registry::with_transport(Credentials::explicit(BTreeMap::new()), server.clone())
        .with_codex_login(login)
        .resolve_with_options(
            "openai/gpt-5",
            json!({"tool_choice":"none", "parallel_tool_calls":false}),
        )?;
    let (request, dispatch) = crate::tests::request(&provider)?;
    provider
        .generate(request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let body = crate::json(&requests[0].body)?;
    assert_eq!(body["tool_choice"], "none");
    assert_eq!(body["parallel_tool_calls"], false);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Every explicit control survives complete Codex projection; names still match
    // the declaration after aliasing. OAR-TOOL-01, OAR-TOOL-06, N-05.
    #[test]
    fn configured_codex_controls_preserve_choice_and_parallel_policy(
        name in ".{1,96}",
        parallel in any::<bool>(),
    ) {
        for choice in [
            json!("none"), json!("auto"), json!("required"),
            json!({"type":"function", "name":name}),
            json!({"type":"allowed_tools", "mode":"auto", "tools":[{"type":"function", "name":name}]}),
        ] {
            let server = Arc::new(Server {
                replies: Mutex::new(VecDeque::new()),
                requests: Mutex::new(Vec::new()),
            });
            let provider = Registry::with_transport(Credentials::explicit(BTreeMap::new()), server)
                .with_codex_login(CodexLogin::from_path("never-read-auth.json".into()))
                .resolve_with_options("openai/gpt-5", json!({
                    "tool_choice":choice, "parallel_tool_calls":parallel,
                })).unwrap();
            let (prepared, _) = crate::tests::request(&provider).unwrap();
            let mut request = prepared.request().clone();
            request.tools.push(ToolDefinition {
                name: name.clone(), revision: "1".into(), description: "Read".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                projection_schema: json!({"type":"object"}),
            });
            let body = crate::projection::body(&provider, &request).unwrap();
            prop_assert_eq!(&body["parallel_tool_calls"], &json!(parallel));
            let projected = &body["tool_choice"];
            match choice.as_str() {
                Some(choice) => prop_assert_eq!(projected, choice),
                None => {
                    prop_assert_eq!(&projected["type"], &choice["type"]);
                    if choice["type"] == "function" {
                        prop_assert_eq!(&projected["name"], &body["tools"][0]["name"]);
                    } else {
                        prop_assert_eq!(&projected["mode"], &choice["mode"]);
                        prop_assert_eq!(&projected["tools"][0]["name"], &body["tools"][0]["name"]);
                    }
                }
            }
        }
    }
}
