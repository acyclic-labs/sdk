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
        let mut tree = Self {
            files: [
                (format!("{root}/program.ts"),
                    "import {description} from './description.ts'; export default {version: 1, name: 'count', description, inputSchema: {}, entry: {sh: 'main.sh', ps1: 'main.ps1'}};".into()),
                (format!("{root}/description.ts"), "export const description = 'Count entries';".into()),
                (format!("{root}/main.sh"), "printf done > receipt".into()),
                (format!("{root}/main.ps1"), "[System.IO.File]::WriteAllText((Join-Path (Get-Location) 'receipt'), 'done')".into()),
                ("tools/unselected/program.ts".into(), "throw new Error('must remain lazy')".into()),
            ].into_iter().collect(),
            reads: Mutex::new(Vec::new()),
        };
        if root.starts_with("hooks/") {
            let manifest = tree.files.get_mut(&format!("{root}/program.ts")).unwrap();
            *manifest = manifest.replace(
                "version: 1,",
                "version: 1, on: {kind: 'lifecycle', event: 'Stop'},",
            );
        }
        tree
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

struct EntryHashCut<'a> {
    tree: Metered<'a>,
    before: &'a std::sync::atomic::AtomicU64,
}

impl Tree for EntryHashCut<'_> {
    fn preparation_io(&self) -> Option<&crate::filesystem::PreparationIo> {
        Some(self.tree.io)
    }

    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        if path.ends_with("/main.ps1") {
            self.before.store(
                self.tree
                    .io
                    .original_native_work()
                    .unwrap()
                    .receipt()
                    .bytes_hashed,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        self.tree.read(path)
    }
}

#[tokio::test]
async fn repeated_program_loads_keep_each_receivers_allocation_and_cancellation() -> Result<()> {
    let mut files = Files::program("tools/count");
    files.files.insert(
        "tools/count/description.ts".into(),
        "export const description = 'a'.repeat(65536);".into(),
    );
    let cancel = acyclic_fs::CancellationToken::new();
    let make_work = |bytes, cancel: &acyclic_fs::CancellationToken| {
        acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: bytes,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            },
            cancel,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
    };
    let producer = make_work(16 * 1024 * 1024, &cancel)?;
    let producer_io = crate::filesystem::PreparationIo::from_native_work(&producer)?;
    let first = load_program(
        &Metered {
            files: &files,
            io: &producer_io,
        },
        "tools/count",
    )
    .await?;
    let tight = make_work(
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 4096,
        &cancel,
    )?;
    let tight_io = crate::filesystem::PreparationIo::from_native_work(&tight)?;
    let baseline = tight.live_allocation_bytes();
    let error = load_program(
        &Metered {
            files: &files,
            io: &tight_io,
        },
        "tools/count",
    )
    .await
    .expect_err("cached configuration cannot fund the receiving manifest copy");
    assert!(matches!(
        error,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            ..
        }
    ));
    drop(error);
    assert_eq!(tight.live_allocation_bytes(), baseline);
    let receiver = make_work(16 * 1024 * 1024, &cancel)?;
    let receiver_io = crate::filesystem::PreparationIo::from_native_work(&receiver)?;
    let entry_hash_cut = std::sync::atomic::AtomicU64::new(u64::MAX);
    let second = load_program(
        &EntryHashCut {
            tree: Metered {
                files: &files,
                io: &receiver_io,
            },
            before: &entry_hash_cut,
        },
        "tools/count",
    )
    .await?;
    let before_entry = entry_hash_cut.load(std::sync::atomic::Ordering::SeqCst);
    assert_ne!(
        before_entry,
        u64::MAX,
        "the original native entry was not read"
    );
    assert!(
        receiver.receipt().bytes_hashed - before_entry
            >= files.files["tools/count/main.ps1"].len() as u64,
        "entry hashing escaped the receiving operation"
    );
    let limited = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: 16 * 1024 * 1024,
            bytes_hashed: before_entry,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let limited_io = crate::filesystem::PreparationIo::from_native_work(&limited)?;
    let limited_cut = std::sync::atomic::AtomicU64::new(u64::MAX);
    let error = load_program(
        &EntryHashCut {
            tree: Metered {
                files: &files,
                io: &limited_io,
            },
            before: &limited_cut,
        },
        "tools/count",
    )
    .await
    .expect_err("a cached manifest cannot fund hashing its native entries");
    assert_ne!(
        limited_cut.load(std::sync::atomic::Ordering::SeqCst),
        u64::MAX,
        "hash allowance refused before the native entry boundary"
    );
    assert!(matches!(
        error,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "bytes_hashed",
                    ..
                }
            ),
            ..
        }
    ));
    assert_eq!(first.definition, second.definition);
    let retained = second.custody.as_ref().unwrap().bytes();
    assert!(retained >= 65536);
    let before_drop = receiver.live_allocation_bytes();
    drop(second);
    assert!(before_drop - receiver.live_allocation_bytes() >= retained);
    let stopped = acyclic_fs::CancellationToken::new();
    let stopped_work = make_work(16 * 1024 * 1024, &stopped)?;
    let stopped_io = crate::filesystem::PreparationIo::from_native_work(&stopped_work)?;
    stopped.cancel();
    let error = load_program(
        &Metered {
            files: &files,
            io: &stopped_io,
        },
        "tools/count",
    )
    .await
    .expect_err("a previously evaluated program must respect receiver cancellation");
    assert_eq!(error.code(), crate::wire::ErrorCode::Cancelled);
    Ok(())
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
async fn hook_manifests_pin_source_selectors_and_refuse_invalid_or_cross_kind_sources() -> Result<()>
{
    let path = "hooks/count/program.ts";
    let mut tree = Files::program("hooks/count");
    let lifecycle = tree.files[path].clone();
    tree.files.insert(
        path.into(),
        lifecycle.replace(
            "{kind: 'lifecycle', event: 'Stop'}",
            "{kind: 'file', volume: 'volume:///project', prefix: 'src', from: 'now'}",
        ),
    );
    let file = load_program(&tree, "hooks/count").await?;
    let shell = Arc::new(
        crate::tools::defaults()?
            .get("shell", None, None)?
            .unwrap()
            .clone(),
    );
    assert!(matches!(file.bind(shell), Err(Error::Invalid(_))));
    tree.files.insert(
        path.into(),
        lifecycle.replace(
            "{kind: 'lifecycle', event: 'Stop'}",
            "{kind: 'stream', path: 'attention', from: 'start'}",
        ),
    );
    let stream = load_program(&tree, "hooks/count").await?;
    assert_ne!(
        stream.definition.revision,
        load_program(&Files::program("hooks/count"), "hooks/count")
            .await?
            .definition
            .revision
    );
    for replacement in [
        "{kind: 'lifecycle', event: 'FileChanged'}",
        "{kind: 'lifecycle', event: 'StreamRecord'}",
        "{kind: 'file', volume: 'volume:///project', prefix: '../src', from: 'now'}",
        "{kind: 'file', volume: ' ', prefix: 'src', from: 'now'}",
        "{kind: 'stream', path: '', from: 'start'}",
    ] {
        tree.files.insert(
            path.into(),
            lifecycle.replace("{kind: 'lifecycle', event: 'Stop'}", replacement),
        );
        assert!(
            load_program(&tree, "hooks/count").await.is_err(),
            "{replacement}"
        );
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: 16 * 1024 * 1024,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            },
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let error = load_program(
            &Metered {
                files: &tree,
                io: &io,
            },
            "hooks/count",
        )
        .await
        .expect_err("bounded source validation must retain its original refusal boundary");
        assert!(
            matches!(
                error,
                Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    ..
                }
            ),
            "{replacement}: {error:?}"
        );
    }
    let mut tool = Files::program("tools/count");
    let manifest = tool.files.get_mut("tools/count/program.ts").unwrap();
    *manifest = manifest.replace(
        "version: 1,",
        "version: 1, on: {kind: 'lifecycle', event: 'Stop'},",
    );
    assert!(matches!(
        load_program(&tool, "tools/count").await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn authored_arguments_reach_the_canonical_shell_only_as_json_stdin() -> Result<()> {
    let files = Files::program("tools/count");
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
    let tree = Metered {
        files: &files,
        io: &io,
    };
    let registry = crate::tools::defaults()?;
    let shell = Arc::clone(
        registry
            .get_shared(crate::tools::shell::PLATFORM.name, None, None)?
            .unwrap(),
    );
    let mut native = registry.clone();
    crate::tools::native::bind(&mut native, crate::model::NativeToolProfile::Openai)?;
    let declaration = Arc::clone(native.get_shared("shell", None, None)?.unwrap());
    let program = load_program(&tree, "tools/count").await?;
    let retained = program.custody.as_ref().unwrap().bytes();
    let tool = program.bind(declaration)?;
    assert!(
        Arc::ptr_eq(tool.executor.capability().unwrap(), &shell),
        "authored binding must retain the original capability, not copy its metadata"
    );
    for arguments in [
        serde_json::json!({"text": "$(touch injected); ' \" \\ \n 🦀", "nested": [null, {"__proto__": "data"}]}),
        serde_json::json!([null, "$(touch injected)", {"text": "🦀"}]),
        serde_json::json!("$(touch injected); ' \" \\ \n 🦀"),
        serde_json::json!(17),
        serde_json::json!(true),
        serde_json::Value::Null,
    ] {
        crate::executor::with_history_io(Some(&io), |history| {
            crate::executor::check_json_schema_value(
                &tool.definition.input_schema,
                &arguments,
                "tool input",
                history,
            )
        })??;
        let call = crate::tool::ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "authored-call".into(),
            name: "count".into(),
            arguments: arguments.clone(),
        };
        let canonical = tool.executor.canonical(&call, Some(&work))?.unwrap();
        let input: crate::tools::ShellInput =
            serde_json::from_value(canonical.invocation.arguments)
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
    }
    let before_drop = work.live_allocation_bytes();
    let retained_tool = Arc::clone(&tool);
    drop(tool);
    assert_eq!(
        work.live_allocation_bytes(),
        before_drop,
        "a retained program binding still owns its original storage"
    );
    drop(retained_tool);
    assert!(
        before_drop - work.live_allocation_bytes() >= retained,
        "dropping the last bound tool retires all transferred program ownership"
    );
    let baseline = work.live_allocation_bytes();
    let program = load_program(&tree, "tools/count").await?;
    let occupied = work.claim(
        work.remaining()?.peak_allocation_bytes,
        1,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let error = program
        .bind(Arc::clone(&shell))
        .err()
        .expect("binding must admit its retained template before allocating");
    assert!(matches!(
        error,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            ..
        }
    ));
    drop(error);
    drop(occupied);
    assert_eq!(work.live_allocation_bytes(), baseline);
    let program = load_program(&tree, "tools/count").await?;
    cancel.cancel();
    let error = program
        .bind(shell)
        .err()
        .expect("binding retains the loader's original cancellation");
    assert_eq!(error.code(), crate::wire::ErrorCode::Cancelled);
    drop(error);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn hook_bindings_retain_source_and_executable_credit_through_canonical_admission()
-> Result<()> {
    let registry = crate::tools::defaults()?;
    let shell = Arc::new(registry.get("shell", None, None)?.unwrap().clone());
    let wrong = Arc::new(registry.get("read", None, None)?.unwrap().clone());
    for source in [
        "{kind: 'file', volume: 'volume:///project', prefix: 'src', from: 'now'}",
        "{kind: 'stream', path: 'events/activity', from: 'start'}",
    ] {
        let mut files = Files::program("hooks/count");
        let manifest = files.files.get_mut("hooks/count/program.ts").unwrap();
        *manifest = manifest.replace("{kind: 'lifecycle', event: 'Stop'}", source);
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        let tree = Metered {
            files: &files,
            io: &io,
        };
        let program = load_program(&tree, "hooks/count").await?;
        let retained = program.custody.as_ref().unwrap().bytes();
        let before = work.live_allocation_bytes();
        assert!(matches!(
            program.bind_hook(wrong.clone()),
            Err(Error::Invalid(_))
        ));
        assert!(before - work.live_allocation_bytes() >= retained);

        let program = load_program(&tree, "hooks/count").await?;
        let retained = program.custody.as_ref().unwrap().bytes();
        let hook = program.bind_hook(shell.clone())?;
        assert!(
            hook.selector
                ._custody
                .as_ref()
                .unwrap()
                .work()
                .shares_ledger(&work)
        );
        let call = crate::tool::ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "event".into(),
            name: hook.binding().definition.name.clone(),
            arguments: serde_json::json!({"text": "$(touch injected); 🦀"}),
        };
        let canonical = hook
            .binding()
            .executor
            .canonical(&call, Some(&work))?
            .unwrap();
        assert_eq!(canonical.invocation.name, "shell");
        let input: crate::tools::ShellInput =
            serde_json::from_value(canonical.invocation.arguments)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        let (_, argv, stdin) = input.command.prepare(&crate::tools::shell::PLATFORM)?;
        assert!(
            !argv
                .iter()
                .any(|argument| argument.contains("touch injected"))
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&stdin.unwrap())
                .map_err(|error| Error::Invalid(error.to_string()))?,
            call.arguments
        );
        let binding = Arc::clone(hook.binding());
        let selector_credit = hook.selector._custody.as_ref().unwrap().bytes();
        let before = work.live_allocation_bytes();
        drop(hook);
        assert_eq!(work.live_allocation_bytes(), before - selector_credit);
        let retained_call = binding.executor.canonical(&call, Some(&work))?.unwrap();
        assert_eq!(retained_call.invocation.name, "shell");
        drop(retained_call);
        drop(binding);
        assert!(before - work.live_allocation_bytes() >= retained);
    }
    Ok(())
}

#[tokio::test]
async fn tool_and_hook_programs_use_the_existing_runner_and_owner_check() -> Result<()> {
    use acyclic_native_runtime::{
        CaptureHandler, CaptureRequest, CaptureSchedule, LaunchFailure, OwnedCaptureInput,
        ProcessCommand, ProcessInput, ProcessOutput, ProcessStdio,
    };

    struct Owner(bool);
    impl CaptureHandler for Owner {
        fn check(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn observe(&mut self, _: ProcessOutput<'_>) -> std::io::Result<ProcessInput> {
            Ok(ProcessInput::default())
        }
        fn retired(&mut self, completed: bool) {
            self.0 = completed;
        }
    }

    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let expected = "script ' 🦀‘’‚‛";
    let fields = serde_json::json!({
        "text": "$(touch injected); ' \" \\ \n 🦀‘’‚‛",
        "nested": [null, true, {"value": 17}],
    });
    for root in ["tools/count", "hooks/count"] {
        let mut tree = Files::program(root);
        tree.files.insert(
            format!("{root}/main.sh"),
            "cat > input.json\nprintf '%s' \"script ' 🦀‘’‚‛\" > receipt".into(),
        );
        tree.files.insert(format!("{root}/main.ps1"), r#"param()
$data = [Console]::In.ReadToEnd()
$encoding = [Text.UTF8Encoding]::new($false)
[IO.File]::WriteAllText((Join-Path (Get-Location) 'input.json'), $data, $encoding)
[IO.File]::WriteAllText((Join-Path (Get-Location) 'receipt'), "script ' 🦀‘’‚‛", $encoding)
[IO.File]::WriteAllText((Join-Path (Get-Location) 'version'), [string]$PSVersionTable.PSVersion.Major, $encoding)
"#.into());
        let loaded = load_program(&tree, root).await?;
        let workspace = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let registry = crate::tools::defaults()?;
        if root.starts_with("tools/") {
            let shell = Arc::clone(registry.get_shared("shell", None, None)?.unwrap());
            let tool = load_program(&tree, root).await?.bind(shell)?;
            let invocation = crate::tool::ToolInvocation {
                operation_id: crate::OperationId::new(),
                call_id: "authored-input".into(),
                name: "count".into(),
                arguments: fields.clone(),
            };
            let canonical = tool.executor.canonical(&invocation, None)?.unwrap();
            let input: crate::tools::ShellInput =
                serde_json::from_value(canonical.invocation.arguments)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            let (program, mut argv, stdin) =
                input.command.prepare(&crate::tools::shell::PLATFORM)?;
            let program = program.expect("authored source uses executable argv");
            let program = if cfg!(windows) {
                // PowerShell 5 is supported even when the host normally selects
                // PowerShell 7. Start from a legacy console page deterministically.
                let source = argv.pop().unwrap();
                argv.push(format!(
                    "[Console]::InputEncoding = [Text.Encoding]::GetEncoding(437); {source}"
                ));
                "powershell.exe".to_owned()
            } else {
                program
            };
            let mut command = ProcessCommand::new(program);
            command
                .args(argv)
                .current_dir(workspace.path())
                .stdin(ProcessStdio::piped())
                .stdout(ProcessStdio::piped())
                .stderr(ProcessStdio::piped());
            let capture = CaptureRequest {
                command,
                custody: None,
                execution_grant: None,
                input: Some(OwnedCaptureInput::Literal(
                    stdin.expect("arguments are separate JSON stdin"),
                )),
                schedule: CaptureSchedule::Unbounded,
                maximum_output_bytes: None,
            }
            .capture(Owner(false))
            .await
            .map_err(|failure| match failure {
                LaunchFailure::Refused { failure, .. } => Error::process_io(failure.error),
                LaunchFailure::Control { error, .. }
                | LaunchFailure::Indeterminate { error, .. } => Error::process_io(error),
            })?;
            let outcome = capture.await.map_err(Error::process_io)?;
            assert!(
                outcome.handler.0,
                "authored process did not retire its original capture"
            );
            let output = outcome
                .result
                .map_err(|failure| Error::process_io(failure.error))?;
            assert!(
                output.status.success(),
                "authored source failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                std::fs::read(workspace.path().join("receipt")).map_err(Error::process_io)?,
                expected.as_bytes()
            );
            let received: serde_json::Value = serde_json::from_slice(
                &std::fs::read(workspace.path().join("input.json")).map_err(Error::process_io)?,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(
                received, fields,
                "native authored input changed encoding or argument meaning"
            );
            if cfg!(windows) {
                assert_eq!(
                    std::fs::read(workspace.path().join("version")).map_err(Error::process_io)?,
                    b"5"
                );
                std::fs::remove_file(workspace.path().join("version"))
                    .map_err(Error::process_io)?;
            }
            std::fs::remove_file(workspace.path().join("receipt")).map_err(Error::process_io)?;
            std::fs::remove_file(workspace.path().join("input.json")).map_err(Error::process_io)?;
        }
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
        assert!(!workspace.path().join("input.json").exists());
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
            std::fs::read(workspace.path().join("receipt")).map_err(Error::process_io)?,
            expected.as_bytes()
        );
        let received: serde_json::Value = serde_json::from_slice(
            &std::fs::read(workspace.path().join("input.json")).map_err(Error::process_io)?,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(received["text"], fields["text"]);
        assert_eq!(received["nested"], fields["nested"]);
        assert!(!workspace.path().join("injected").exists());
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
