use super::*;
use crate::hooks::{HookEvent, HookFailure, HookRule, HookSpec, Hooks};
use crate::tool::ToolRegistry;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

struct Files {
    files: BTreeMap<String, String>,
    reads: Mutex<Vec<String>>,
}

impl Files {
    fn program(root: &str) -> Self {
        Self {
            files: [
                (format!("{root}/program.ts"),
                    "import {description} from './description.ts'; export default {version: 1, name: 'count', description, inputSchema: {type: 'object', properties: {}, additionalProperties: false}, entry: {sh: 'main.sh', ps1: 'main.ps1'}};".into()),
                (format!("{root}/description.ts"), "export const description = 'Count entries';".into()),
                (format!("{root}/main.sh"), "printf done > receipt".into()),
                (format!("{root}/main.ps1"), "[System.IO.File]::WriteAllText((Join-Path (Get-Location) 'receipt'), 'done')".into()),
                ("tools/unselected/program.ts".into(), "throw new Error('must remain lazy')".into()),
            ].into_iter().collect(),
            reads: Mutex::new(Vec::new()),
        }
    }
}

impl Tree for Files {
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async move {
            self.reads.lock().unwrap().push(path.to_owned());
            Ok(self
                .files
                .get(path)
                .map(|text| acyclic_stream::OwnedBytes::from(text.as_bytes().to_vec())))
        })
    }
}

#[tokio::test]
async fn program_revision_pins_imports_and_entries_without_reading_siblings() -> Result<()> {
    let mut tree = Files::program("tools/count");
    let original = load_program(&tree, "tools/count").await?;
    assert!(
        tree.reads
            .lock()
            .unwrap()
            .iter()
            .all(|path| path.starts_with("tools/count/"))
    );
    for (path, replacement) in [
        (
            "tools/count/description.ts",
            "export const description = 'New description';",
        ),
        ("tools/count/main.sh", "printf changed > receipt"),
        ("tools/count/main.ps1", "Write-Output changed"),
    ] {
        let previous = tree.files.insert(path.into(), replacement.into()).unwrap();
        let changed = load_program(&tree, "tools/count").await?;
        assert_ne!(
            original.definition.revision, changed.definition.revision,
            "{path}"
        );
        tree.files.insert(path.into(), previous);
    }
    assert_eq!(
        original.definition,
        load_program(&tree, "tools/count").await?.definition
    );
    Ok(())
}

#[tokio::test]
async fn authored_arguments_reach_the_canonical_shell_only_as_json_stdin() -> Result<()> {
    struct Metered<'a> {
        files: &'a Files,
        io: &'a crate::filesystem::PreparationIo,
    }
    impl Tree for Metered<'_> {
        fn preparation_io(&self) -> Option<&crate::filesystem::PreparationIo> {
            Some(self.io)
        }
        fn read<'a>(
            &'a self,
            path: &'a str,
        ) -> futures::future::BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
            self.files.read(path)
        }
    }
    let files = Files::program("tools/count");
    let cancel = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancel)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let tree = Metered {
        files: &files,
        io: &io,
    };
    let registry = crate::tools::defaults()?;
    let shell = Arc::new(
        registry
            .get(crate::tools::shell::PLATFORM.name, None, None)?
            .unwrap()
            .clone(),
    );
    let program = load_program(&tree, "tools/count").await?;
    let retained = program.custody.as_ref().unwrap().bytes();
    let tool = program.bind(shell)?;
    let arguments = serde_json::json!({"text": "$(touch injected); ' \" \\ \n 🦀", "nested": [null, {"__proto__": "data"}]});
    let call = crate::tool::ToolInvocation {
        operation_id: crate::OperationId::new(),
        call_id: "authored-call".into(),
        name: "count".into(),
        arguments: arguments.clone(),
    };
    let canonical = tool.executor.canonical(&call, None)?.unwrap();
    let input: crate::tools::ShellInput = serde_json::from_value(canonical.invocation.arguments)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    input.validate()?;
    let (_, argv, stdin) = input.command.prepare(&crate::tools::shell::PLATFORM)?;
    assert!(
        !argv
            .iter()
            .any(|argument| argument.contains("touch injected"))
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stdin.unwrap())
            .map_err(|error| Error::Invalid(error.to_string()))?,
        arguments
    );
    assert_eq!(canonical.invocation.operation_id, call.operation_id);
    assert_eq!(canonical.invocation.call_id, call.call_id);
    let before_drop = work.live_allocation_bytes();
    drop(tool);
    assert!(
        before_drop - work.live_allocation_bytes() >= retained,
        "dropping the last bound tool retires all transferred program ownership"
    );
    Ok(())
}

#[tokio::test]
async fn tool_and_hook_programs_use_the_existing_runner_and_owner_check() -> Result<()> {
    for root in ["tools/count", "hooks/count"] {
        let tree = Files::program(root);
        let loaded = load_program(&tree, root).await?;
        let mut hooks = Hooks::default();
        hooks.add_rules(vec![HookRule {
            on: HookEvent::Stop,
            matcher: String::new(),
            hooks: vec![HookSpec {
                run: loaded.run,
                timeout_ms: None,
                failure: HookFailure::Block,
            }],
        }])?;
        let workspace = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let fields = serde_json::json!({});
        let registry = ToolRegistry::default();
        let refused = hooks
            .run_checked(
                HookEvent::Stop,
                (workspace.path(), workspace.path()),
                (&fields).into(),
                &registry,
                None,
                (
                    Arc::new(|| {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "program denied",
                        ))
                    }),
                    None,
                    None,
                ),
            )
            .await?;
        assert!(!refused.diagnostics.is_empty());
        assert!(!workspace.path().join("receipt").exists());
        hooks
            .run(
                HookEvent::Stop,
                workspace.path(),
                workspace.path(),
                &fields,
                &registry,
            )
            .await?;
        assert_eq!(
            std::fs::read(workspace.path().join("receipt"))
                .map_err(|error| Error::Storage(error.to_string()))?,
            b"done"
        );
    }
    Ok(())
}

#[tokio::test]
async fn invalid_programs_never_resolve_an_escaping_or_foreign_entry() -> Result<()> {
    for replacement in [
        "{version: 2, name: 'count', description: '', inputSchema: {}, entry: {sh: 'main.sh', ps1: 'main.ps1'}}",
        "{version: 1, name: 'other', description: '', inputSchema: {}, entry: {sh: 'main.sh', ps1: 'main.ps1'}}",
        "{version: 1, name: 'count', description: '', inputSchema: {}, entry: {sh: '../outside.sh', ps1: '../outside.ps1'}}",
        "{version: 1, name: 'count', description: '', inputSchema: {}, entry: {sh: '/outside.sh', ps1: '/outside.ps1'}}",
        "{version: 1, name: 'count', description: '', inputSchema: {}, entry: {sh: 'main.ts', ps1: 'main.ts'}}",
        "{version: 1, name: 'count', description: '', inputSchema: {}, entry: {}}",
        "{version: 1, name: 'count', description: '', inputSchema: {}, entry: {sh: 'missing.sh', ps1: 'missing.ps1'}}",
    ] {
        let mut tree = Files::program("tools/count");
        tree.files.insert(
            "tools/count/program.ts".into(),
            format!("export default {replacement};"),
        );
        assert!(
            load_program(&tree, "tools/count").await.is_err(),
            "{replacement}"
        );
        assert!(
            tree.reads
                .lock()
                .unwrap()
                .iter()
                .all(|path| !path.contains("..") && !path.starts_with('/'))
        );
    }
    for directory in ["tools/../count", "tools/count/extra", "other/count"] {
        let tree = Files::program("tools/count");
        assert!(load_program(&tree, directory).await.is_err());
        assert!(tree.reads.lock().unwrap().is_empty());
    }
    Ok(())
}
