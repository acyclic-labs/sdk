use super::*;
use futures::future::BoxFuture;

/// An effective tree held in memory.
struct Files(BTreeMap<&'static str, &'static str>);

impl Tree for Files {
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async move {
            Ok(self
                .0
                .get(path)
                .map(|text| acyclic_stream::OwnedBytes::from_static(text.as_bytes())))
        })
    }
}

async fn enable(manifest: &'static str, files: &[(&'static str, &'static str)]) -> Result<Enabled> {
    let tree = Files(files.iter().copied().collect());
    let layers = [Layer {
        name: "home",
        generation: "1",
        bytes: Some(manifest.as_bytes()),
    }];
    load(&tree, &layers, "root").await
}

const GUARD: &str = r#"export default { name: "guard", description: "Deny force pushes.",
  defaults: { force: false }, matcher: { tool: { action: ["exec"], command: "push" } } };"#;

#[tokio::test]
async fn an_enabled_shell_hook_becomes_a_pinned_before_tool_rule()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let enabled = enable(
        r#"export default (base) => ({ ...base, guard: { enabled: true }, idle: { enabled: false } });"#,
        &[
            ("plugins/guard/plugin.ts", GUARD),
            ("plugins/guard/hooks/before-tool.sh", "exit 2"),
            ("plugins/guard/hooks/before-tool.ps1", "exit 2"),
        ],
    )
    .await?;
    assert_eq!(enabled.names, ["guard"]);
    let rules: Vec<HookRule> = serde_json::from_slice(&serde_json::to_vec(&enabled.rules)?)?;
    let [rule] = rules.as_slice() else {
        panic!("one rule: {enabled:?}");
    };
    assert_eq!(rule.on, HookEvent::PreToolUse);
    let [hook] = rule.hooks.as_slice() else {
        panic!("one hook");
    };
    assert_eq!(hook.failure, HookFailure::Block);
    assert_eq!(
        hook.run,
        HookRun::Plugin {
            instance: "root:plugins.ts#guard:hooks/before-tool".into(),
            sh: Some("exit 2".into()),
            ps1: Some("exit 2".into()),
            config: r#"{"force":false}"#.into(),
            matcher: Some(RequestMatch {
                action: vec!["exec".into()],
                command: Some("push".into()),
                path: None,
            }),
        }
    );
    Ok(())
}

#[tokio::test]
async fn settings_in_plugins_ts_replace_the_defaults()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let enabled = enable(
        r#"export default { guard: { enabled: true, config: { force: true } } };"#,
        &[
            ("plugins/guard/plugin.ts", GUARD),
            ("plugins/guard/hooks/before-tool.sh", "exit 0"),
        ],
    )
    .await?;
    let rules: Vec<HookRule> = serde_json::from_slice(&serde_json::to_vec(&enabled.rules)?)?;
    let HookRun::Plugin { config, .. } = &rules[0].hooks[0].run else {
        panic!("a plugin hook");
    };
    assert_eq!(config, r#"{"force":true}"#);
    Ok(())
}

#[tokio::test]
async fn an_unrunnable_or_unsatisfied_plugin_makes_plugins_ts_invalid() {
    let on = r#"export default { p: { enabled: true } };"#;
    let manifest = r#"export default { name: "p", description: "d" };"#;
    for (files, reason) in [
        (vec![], "does not exist"),
        (
            vec![
                ("plugins/p/plugin.ts", manifest),
                ("plugins/p/hooks/before-tool.sh", "exit 0"),
                ("plugins/p/hooks/before-tool.ps1", " "),
            ],
            "hook command",
        ),
        (
            vec![(
                "plugins/p/plugin.ts",
                r#"export default { name: "p", description: "d",
                  after: { usage: { path: "live/agents/self/usage.jsonl", from: "now" } } };"#,
            )],
            "after-hooks",
        ),
        (
            vec![
                (
                    "plugins/p/plugin.ts",
                    r#"export default { name: "p", description: "d", matcher: { tool: { command: "[" } } };"#,
                ),
                ("plugins/p/hooks/before-tool.sh", "exit 0"),
            ],
            "plugin hook matcher",
        ),
        (
            vec![
                ("plugins/p/plugin.ts", manifest),
                ("plugins/p/hooks/before-tool.sh", "  "),
            ],
            "hook command",
        ),
        (
            vec![
                ("plugins/p/plugin.ts", manifest),
                ("plugins/p/hooks/before-tool.sh", "exit 0\0"),
            ],
            "hook command",
        ),
        (
            vec![(
                "plugins/p/plugin.ts",
                r#"export default { name: "q", description: "d" };"#,
            )],
            "names",
        ),
        (
            vec![(
                "plugins/p/plugin.ts",
                r#"export default { name: "p", description: "d", requires: ["r"] };"#,
            )],
            "requires r",
        ),
        (
            vec![
                ("plugins/p/plugin.ts", manifest),
                ("plugins/p/hooks/before-tool.ts", ""),
            ],
            "TypeScript hooks",
        ),
        (
            vec![
                ("plugins/p/plugin.ts", manifest),
                ("plugins/p/hooks/before-spawn.sh", ""),
            ],
            "does not judge spawn",
        ),
        (
            vec![
                (
                    "plugins/p/plugin.ts",
                    r#"export default { name: "p", description: "d",
                      after: { usage: { path: "live/agents/self/usage.jsonl", from: "now" } } };"#,
                ),
                ("plugins/p/hooks/after-usage.sh", ""),
            ],
            "after-hooks",
        ),
    ] {
        let error = enable(on, &files).await.expect_err(reason).to_string();
        assert!(error.contains(reason), "{reason}: {error}");
    }
}

#[tokio::test]
async fn native_plugin_admission_retains_manifest_and_after_hook_ownership() -> Result<()> {
    struct Metered<'a> {
        files: Files,
        io: &'a crate::filesystem::PreparationIo,
    }
    impl Tree for Metered<'_> {
        fn preparation_io(&self) -> Option<&crate::filesystem::PreparationIo> {
            Some(self.io)
        }
        fn read<'a>(
            &'a self,
            path: &'a str,
        ) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
            self.files.read(path)
        }
    }
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 16 * 1024 * 1024),
        &cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let tree = Metered {
        io: &io,
        files: Files(BTreeMap::from([
            (
                "plugins/helper/plugin.ts",
                "export default {name:'helper', description:'d'.repeat(65536), after:{done:{path:'live/agents/self/status.json',from:'now'}}};",
            ),
            (
                "plugins/helper/hooks/after-done.ts",
                "export default [{notice:{text:'n'.repeat(65536),wake:true}}];",
            ),
        ])),
    };
    fn layers(bytes: &[u8]) -> [Layer<'_>; 1] {
        [Layer {
            name: "home",
            generation: "one",
            bytes: Some(bytes),
        }]
    }
    let empty = load(&tree, &layers(b"export default {};"), "owner").await?;
    assert!(empty.names.is_empty());
    drop(empty);
    let admitted = load_decision(
        &tree,
        &layers(b"export default {helper:{enabled:true}};"),
        "owner",
    )
    .await?
    .into_result()?;
    assert_eq!(admitted.descriptions[0].len(), 65536);
    let after = admitted.after[0].clone();
    assert_eq!(after.settlement().next().unwrap().len(), 65536);
    // Replace the evaluator's single cache entry. Admission owns its data independently.
    config::evaluate(
        "other.ts",
        b"export default 0;",
        &Files(BTreeMap::new()),
        &Value::Null,
        Surface::Data,
    )
    .await?;
    assert!(work.live_allocation_bytes() >= baseline + 131072);
    drop(admitted);
    assert!(work.live_allocation_bytes() >= baseline + 65536);
    drop(after);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
