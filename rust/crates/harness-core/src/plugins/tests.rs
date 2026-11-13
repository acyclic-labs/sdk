use super::*;
use crate::hooks::HookRule;
use futures::future::BoxFuture;
use std::collections::BTreeMap;

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
            ("plugins/helper/hooks/before-tool.sh", "exit 0"),
            ("plugins/helper/hooks/before-tool.ps1", "exit 0"),
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
    let rules = admitted.rules.clone();
    assert!(!rules.is_empty());
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
    assert!(
        work.live_allocation_bytes() > baseline,
        "shared executable rules retain their admission"
    );
    drop(rules);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

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

#[tokio::test]
async fn native_after_hook_refusals_retain_the_original_decision() -> Result<()> {
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 16 * 1024 * 1024),
        &cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let layers = [Layer {
        name: "home",
        generation: "one",
        bytes: Some(b"export default {helper:{enabled:true}};"),
    }];
    let mut original = None;
    for source in [
        "throw new Error('original refusal');",
        "throw new Error('changed refusal');",
    ] {
        let tree = Metered {
            io: &io,
            files: Files(BTreeMap::from([
                (
                    "plugins/helper/plugin.ts",
                    "export default {name:'helper', description:'helper', after:{done:{path:'live/agents/self/status.json',from:'now'}}};",
                ),
                ("plugins/helper/hooks/after-done.ts", source),
            ])),
        };
        let first = load_decision(&tree, &layers, "owner").await?;
        let identity = first.identity();
        assert!(matches!(
            first.into_result(),
            Err(Error::NativeDiagnostic {
                code: crate::wire::ErrorCode::Invalid,
                ..
            })
        ));
        let second = load_decision(&tree, &layers, "owner").await?;
        assert_eq!(second.identity(), identity);
        assert!(matches!(
            second.into_result(),
            Err(Error::NativeDiagnostic { .. })
        ));
        if let Some(original) = original {
            assert_ne!(
                original, identity,
                "changed source must change refusal evidence"
            );
        }
        original = Some(identity);
    }
    Ok(())
}

#[tokio::test]
async fn native_hook_script_copy_refuses_on_the_original_allowance() -> Result<()> {
    struct Script(acyclic_stream::OwnedBytes);
    impl Tree for Script {
        fn read<'a>(
            &'a self,
            _: &'a str,
        ) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
            Box::pin(std::future::ready(Ok(Some(self.0.clone()))))
        }
    }
    let tree = Script(acyclic_stream::OwnedBytes::from(vec![b'x'; 65536]));
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                + 4096,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    assert!(matches!(
        read_hook(
            &tree,
            None,
            "plugins/helper/hooks/before-tool.sh",
            Some(&io)
        )
        .await,
        Err(LoadFailure::Operational(Error::NativeWork { .. }))
    ));
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert_eq!(tree.0.len(), 65536, "refusal preserves the original source");
    Ok(())
}

#[tokio::test]
async fn native_rule_metadata_refuses_before_installation() -> Result<()> {
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let mut hooks = Hooks::default();
    let hook = || HookSpec {
        run: HookRun::Shell {
            command: "echo ready".into(),
            shell: None,
        },
        timeout_ms: None,
        failure: HookFailure::Block,
    };
    let refused = hooks
        .add_rule_with_io(
            HookEvent::PreToolUse,
            String::new(),
            std::iter::once(hook()),
            Some(&io),
        )
        .expect_err("rule metadata must be admitted before allocation");
    assert!(matches!(refused, Error::NativeWork { .. }));
    assert!(hooks.is_empty(), "failed admission must install no rule");
    hooks.add_rule_with_io(
        HookEvent::PreToolUse,
        String::new(),
        std::iter::once(hook()),
        None,
    )?;
    assert!(!hooks.is_empty());
    Ok(())
}

#[tokio::test]
async fn native_plugin_retains_only_settings_used_by_installed_rules() -> Result<()> {
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: 16 * 1024 * 1024,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let baseline = work.live_allocation_bytes();
    for has_hook in [false, true] {
        let mut previous = None;
        for (selector, manifest, payload) in [
            (
                "export default {helper:{enabled:true,config:{payload:'p'.repeat(0),kind:'settings'}}};",
                "export default {name:'helper',description:'helper',defaults:{unused:'d'.repeat(0)}};",
                0,
            ),
            (
                "export default {helper:{enabled:true,config:{payload:'p'.repeat(65536),kind:'settings'}}};",
                "export default {name:'helper',description:'helper',defaults:{unused:'d'.repeat(65536)}};",
                65536,
            ),
        ] {
            let mut files = BTreeMap::from([("plugins/helper/plugin.ts", manifest)]);
            if has_hook {
                files.insert("plugins/helper/hooks/before-tool.sh", "exit 0");
                files.insert("plugins/helper/hooks/before-tool.ps1", "exit 0");
            }
            let tree = Metered {
                files: Files(files),
                io: &io,
            };
            let layers = [Layer {
                name: "home",
                generation: "one",
                bytes: Some(selector.as_bytes()),
            }];
            let admitted = load(&tree, &layers, "owner").await?;
            config::evaluate(
                "retirement-probe.ts",
                b"export default null;",
                &Files(BTreeMap::new()),
                &Value::Null,
                Surface::Data,
            )
            .await?;
            let retained = work.live_allocation_bytes();
            if let Some(previous) = previous {
                assert_eq!(
                    retained - previous,
                    if has_hook { payload } else { 0 },
                    "only the installed rule's settings copy may retain its payload credit"
                );
            }
            previous = Some(retained);
            drop(admitted);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}
