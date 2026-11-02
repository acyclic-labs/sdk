//! The shared conformance cases for several before-hooks on one request.
use super::*;
use serde_json::json;
use std::cell::RefCell;

/// A plugin before-tool hook named `instance`, optionally selecting requests.
fn plugin(instance: &str, matcher: Option<RequestMatch>) -> HookSpec {
    HookSpec {
        run: HookRun::Plugin {
            instance: instance.into(),
            sh: Some("exit 0".into()),
            ps1: None,
            config: "{}".into(),
            matcher,
        },
        timeout_ms: u64::MAX,
        failure: HookFailure::Block,
    }
}

fn instance(hook: &HookSpec) -> &str {
    match &hook.run {
        HookRun::Plugin { instance, .. } => instance,
        _ => unreachable!("only plugin hooks here"),
    }
}

/// Runs `hooks` (highest authority first) on `command`; `decide` answers for
/// a hook about the command it sees. Returns the outcome and every run.
async fn judge(
    hooks: Vec<HookSpec>,
    command: &str,
    decide: impl Fn(&str, &str) -> Value,
) -> Result<(HookOutput, Vec<(String, String)>)> {
    let mut configured = Hooks::default();
    configured.add_rules([HookRule {
        on: HookEvent::PreToolUse,
        matcher: String::new(),
        hooks,
    }])?;
    let original = json!({"tool_name":"shell","tool_input":{"command":command}});
    let runs = RefCell::new(Vec::new());
    let output = configured
        .run_commands(
            HookEvent::PreToolUse,
            &original,
            &crate::tool::ToolRegistry::default(),
            65_536,
            |hook, _, rewritten| {
                let input = rewritten.unwrap_or(&original["tool_input"]);
                let command = input["command"].as_str().unwrap_or_default().to_owned();
                let name = instance(hook).to_owned();
                let decision = decide(&name, &command);
                runs.borrow_mut().push((name, command));
                std::future::ready(Ok((Some(0), decision.to_string().into_bytes(), vec![])))
            },
        )
        .await?;
    Ok((output, runs.into_inner()))
}

fn allow() -> Value {
    json!({"allow": true})
}

fn rewrite(command: &str) -> Value {
    json!({"allow": true, "input": {"command": command}})
}

#[tokio::test]
async fn without_a_rewrite_the_strictest_answer_wins() -> Result<()> {
    let (output, runs) = judge(
        vec![plugin("ask", None), plugin("allow", None)],
        "git push",
        |name, _| match name {
            "ask" => json!({"ask": "push?", "to": "person"}),
            _ => allow(),
        },
    )
    .await?;
    assert_eq!(output.approval.as_deref(), Some("push?"));
    assert!(!output.blocks());
    assert_eq!(output.updated_input, None);
    assert_eq!(runs.len(), 2, "one parallel round");
    Ok(())
}

#[tokio::test]
async fn two_rewriters_apply_in_authority_order_and_a_denier_sees_the_final_input()
-> Result<()> {
    let (output, runs) = judge(
        vec![plugin("a", None), plugin("b", None), plugin("deny-c", None)],
        "x",
        |name, command| match (name, command) {
            ("a", "x") => rewrite("xa"),
            ("b", command) if !command.ends_with('b') => rewrite(&format!("{command}b")),
            ("deny-c", "xab") => json!({"allow": false, "reason": "no xab"}),
            _ => allow(),
        },
    )
    .await?;
    assert_eq!(output.blocked.as_deref(), Some("no xab"));
    assert_eq!(output.updated_input, None);
    // b saw a's rewrite: rewrites compose in order instead of racing.
    assert!(runs.contains(&("b".into(), "xa".into())));
    Ok(())
}

#[tokio::test]
async fn an_ask_is_about_the_rewritten_request() -> Result<()> {
    let (output, runs) = judge(
        vec![plugin("rewriter", None), plugin("asker", None)],
        "rm -rf build",
        |name, command| match (name, command) {
            ("rewriter", "rm -rf build") => rewrite("rm -rf ./build"),
            ("asker", command) => json!({"ask": format!("run {command}?"), "to": "person"}),
            _ => allow(),
        },
    )
    .await?;
    assert_eq!(output.approval.as_deref(), Some("run rm -rf ./build?"));
    assert_eq!(output.updated_input, Some(json!({"command": "rm -rf ./build"})));
    // The rewriter re-checks the final input and allows it unchanged.
    assert!(runs.contains(&("rewriter".into(), "rm -rf ./build".into())));
    Ok(())
}

#[tokio::test]
async fn a_rewrite_during_the_final_check_denies() -> Result<()> {
    // Each run appends, so the final input never satisfies this hook.
    let (output, _) = judge(vec![plugin("restless", None)], "x", |_, command| {
        rewrite(&format!("{command}!"))
    })
    .await?;
    assert!(output.blocks());
    assert_eq!(output.updated_input, None);
    Ok(())
}

#[tokio::test]
async fn every_instance_of_one_plugin_must_allow() -> Result<()> {
    // A scope and a template both enabling one plugin with other settings.
    let (output, _) = judge(
        vec![plugin("scope/no-force-push", None), plugin("template/no-force-push", None)],
        "git push --force",
        |name, _| match name {
            "template/no-force-push" => json!({"allow": false, "reason": "force-push"}),
            _ => allow(),
        },
    )
    .await?;
    assert_eq!(output.blocked.as_deref(), Some("force-push"));
    Ok(())
}

#[tokio::test]
async fn a_matcher_skips_unrelated_requests_without_running() -> Result<()> {
    let pushes = RequestMatch {
        action: vec!["exec".into()],
        command: Some(r"\bgit\b.*\bpush\b".into()),
        path: None,
    };
    let deny = |_: &str, _: &str| json!({"allow": false, "reason": "matched"});
    let (output, runs) = judge(vec![plugin("push", Some(pushes.clone()))], "ls", deny).await?;
    assert!(runs.is_empty() && !output.blocks());
    let (output, runs) = judge(vec![plugin("push", Some(pushes))], "git push", deny).await?;
    assert_eq!(runs.len(), 1);
    assert_eq!(output.blocked.as_deref(), Some("matched"));
    Ok(())
}

#[test]
fn a_plugin_decision_is_exactly_one_of_its_forms() {
    let decide = |code, stdout: &str| {
        let mut output = HookOutput::default();
        let mut budget = budget::Budget::new(65_536).unwrap();
        budget.apply(
            &mut output,
            HookEvent::PreToolUse,
            &plugin("p", None),
            Ok((Some(code), stdout.as_bytes().to_vec(), b"denied by stderr".to_vec())),
        );
        budget.finish(&mut output);
        output
    };
    assert_eq!(decide(0, ""), HookOutput::default());
    assert_eq!(decide(0, r#"{"allow":true}"#), HookOutput::default());
    assert_eq!(decide(2, "").blocked.as_deref(), Some("denied by stderr"));
    assert_eq!(
        decide(0, r#"{"allow":false,"reason":"no"}"#).blocked.as_deref(),
        Some("no")
    );
    assert_eq!(
        decide(0, r#"{"ask":"ok?","to":"parent"}"#).approval.as_deref(),
        Some("ok?")
    );
    for malformed in [
        r#"{"allow":false}"#,
        r#"{"ask":"ok?"}"#,
        r#"{"ask":"ok?","to":"everyone"}"#,
        r#"{"allow":true,"input":[1]}"#,
        r#"{"allow":true,"to":"person"}"#,
        "not json",
    ] {
        assert!(decide(0, malformed).blocks(), "{malformed}");
    }
    // Any other exit denies.
    assert!(decide(1, "").blocks());
}
