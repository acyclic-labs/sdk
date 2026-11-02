//! The shared conformance cases for several before-hooks on one request.
use super::model::{judge, plugin};
use super::*;
use serde_json::json;

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
async fn two_rewriters_apply_in_authority_order_and_a_denier_sees_the_final_input() -> Result<()> {
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
    assert_eq!(
        output.updated_input,
        Some(json!({"command": "rm -rf ./build"}))
    );
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
        vec![
            plugin("scope/no-force-push", None),
            plugin("template/no-force-push", None),
        ],
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
            Ok((
                Some(code),
                stdout.as_bytes().to_vec(),
                b"denied by stderr".to_vec(),
            )),
        );
        budget.finish(&mut output);
        output
    };
    assert_eq!(decide(0, ""), HookOutput::default());
    assert_eq!(decide(0, r#"{"allow":true}"#), HookOutput::default());
    assert_eq!(decide(2, "").blocked.as_deref(), Some("denied by stderr"));
    assert_eq!(
        decide(0, r#"{"allow":false,"reason":"no"}"#)
            .blocked
            .as_deref(),
        Some("no")
    );
    assert_eq!(
        decide(0, r#"{"ask":"ok?","to":"person"}"#)
            .approval
            .as_deref(),
        Some("ok?")
    );
    for malformed in [
        r#"{"allow":false}"#,
        r#"{"ask":"ok?"}"#,
        r#"{"ask":"ok?","to":"everyone"}"#,
        r#"{"ask":"ok?","to":"parent"}"#,
        r#"{"ask":"ok?","to":"model"}"#,
        r#"{"ask":"ok?","to":"command"}"#,
        r#"{"allow":true,"input":[1]}"#,
        r#"{"allow":true,"to":"person"}"#,
        "not json",
    ] {
        assert!(decide(0, malformed).blocks(), "{malformed}");
    }
    // Any other exit denies.
    assert!(decide(1, "").blocks());
}

#[tokio::test]
async fn a_rewrite_admits_newly_matching_guards() -> Result<()> {
    let matcher = RequestMatch {
        action: vec!["exec".into()],
        command: Some("git push".into()),
        path: None,
    };
    let (output, runs) = judge(
        vec![plugin("rewrite", None), plugin("guard", Some(matcher))],
        "ls",
        |name, command| match (name, command) {
            ("rewrite", "ls") => rewrite("git push --force"),
            ("guard", _) => json!({"allow": false, "reason": "force push denied"}),
            _ => allow(),
        },
    )
    .await?;
    assert_eq!(output.blocked.as_deref(), Some("force push denied"));
    assert!(runs.contains(&("guard".into(), "git push --force".into())));
    Ok(())
}

#[test]
fn final_input_and_context_share_one_output_reservation() -> Result<()> {
    let hook = Arc::new(plugin("guard", None));
    let input = json!({"command": "x".repeat(1024)});
    let result = fold(
        [(&hook, Ok((Some(0), b"{\"allow\":true}".to_vec(), vec![])))],
        HookEvent::PreToolUse,
        Some(512),
        Some(input),
    );
    assert!(matches!(
        result,
        Err(Error::Process(crate::ProcessError::OutputLimit { .. }))
    ));
    Ok(())
}

#[test]
fn report_only_context_overflow_cannot_discard_checked_arguments_and_allow() -> Result<()> {
    let hook = Arc::new(HookSpec {
        run: HookRun::Shell {
            command: "exit 0".into(),
            shell: None,
        },
        timeout_ms: None,
        failure: HookFailure::Report,
    });
    let context = json!({"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": "x".repeat(1024)}});
    let output = fold(
        [(
            &hook,
            Ok((Some(0), context.to_string().into_bytes(), vec![])),
        )],
        HookEvent::PreToolUse,
        Some(512),
        Some(json!({"command": "checked"})),
    )?;
    assert!(output.blocks());
    assert!(output.updated_input.is_none());
    Ok(())
}
