//! Data-driven composer cases against the production filesystem reader.

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, Result,
    client::{CommandCatalog, CommandResolution, CommandRoot, CommandScope, resolve_file_mentions},
    context::{ContextDiscoveryLimits, ContextRoot},
    conversation::{
        ContentGrant, ContentResidencyVerifier, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentVerifier, FilesystemHost},
    resources::ProviderRef,
};
use serde_json::{Value, json};
use std::sync::Arc;

struct Fixture {
    host: Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    root: ContextRoot,
    writer: ContentGrant,
    reader: Arc<dyn ContentResidencyVerifier>,
}

impl Fixture {
    async fn new(files: &[(&str, &str)]) -> Result<Self> {
        let provider = ProviderRef::new(
            "commands".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            provider,
            "sources".into(),
            VolumeClass::SessionShared,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let issuer = AuthorityIssuer::new(
            "commands",
            [9; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "test".into(),
            },
        );
        let scope = issuer
            .issue(
                "owner",
                &Capabilities::new([
                    volume.capability(VolumeOperation::Read, &mut None)?,
                    volume.capability(VolumeOperation::Write, &mut None)?,
                    volume.directory_read_capability("", &mut None)?,
                ]),
                Some(agent),
                None,
                &mut None,
            )
            .expect("scope proof");
        host.create_volume(&volume).await?;
        let writer = ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier"),
            &scope,
            &volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        let reader = FilesystemContentVerifier::new(
            host.clone(),
            &issuer.verifier(&mut None).expect("verifier"),
            &scope,
            32_768,
            None,
        )?;
        let fixture = Self {
            host,
            root: ContextRoot {
                volume,
                directory: String::new(),
            },
            writer,
            reader,
        };
        for (path, text) in files {
            fixture.put(path, text, path).await?;
        }
        Ok(fixture)
    }

    async fn put(&self, path: &str, text: &str, key: &str) -> Result<()> {
        self.host
            .put_content(
                &self.root.volume,
                &self.writer,
                path,
                text.as_bytes(),
                "text/plain",
                "source.md",
                1_000_000,
                &IdempotencyKey::new(key)?,
                None,
                None,
            )
            .await?;
        Ok(())
    }

    async fn catalog(
        &self,
        roots: &[(&str, CommandScope)],
        skills: &[&str],
    ) -> Result<CommandCatalog> {
        let roots: Vec<_> = roots
            .iter()
            .map(|(directory, scope)| CommandRoot {
                scope: *scope,
                root: ContextRoot {
                    directory: (*directory).into(),
                    ..self.root.clone()
                },
            })
            .collect();
        let skills: Vec<_> = skills
            .iter()
            .map(|directory| ContextRoot {
                directory: (*directory).into(),
                ..self.root.clone()
            })
            .collect();
        CommandCatalog::capture(
            self.reader.as_ref(),
            &roots,
            &skills,
            ContextDiscoveryLimits::default(),
        )
        .await
    }
}

#[tokio::test]
async fn builtin_cases_resolve_from_the_same_catalog_as_completion() -> Result<()> {
    let fixture = Fixture::new(&[]).await?;
    let catalog = CommandCatalog::builtins()?;
    let cases = [
        (
            "/compact",
            json!({"kind":"submit","request":{"kind":"compact","instructions":""}}),
        ),
        (
            " /compact keep decisions\nand tests",
            json!({"kind":"submit","request":{"kind":"compact","instructions":"keep decisions\nand tests"}}),
        ),
        ("/model", json!({"kind":"select","query":"model"})),
        (
            "/model provider/model",
            json!({"kind":"submit","request":{"kind":"model","model":"provider/model"}}),
        ),
        ("/diff", json!({"kind":"select","query":"diff"})),
        ("/plan", json!({"kind":"select","query":"plan"})),
        ("/goal", json!({"kind":"select","query":"goal"})),
        ("/mcp", json!({"kind":"select","query":"mcp"})),
        (
            "hello /model",
            json!({"kind":"prompt","text":"hello /model","source":null,"mentions":[]}),
        ),
        (
            "",
            json!({"kind":"prompt","text":"","source":null,"mentions":[]}),
        ),
    ];
    for (input, expected) in cases {
        let resolved = catalog
            .resolve(input, fixture.reader.as_ref(), 1024)
            .await?;
        assert_eq!(
            serde_json::to_value(resolved).expect("JSON"),
            expected,
            "{input}"
        );
    }
    for input in [
        "/unknown",
        "/",
        "/model a b",
        "/model a\0b",
        "/approval",
        "/approval always",
        "/diff extra",
        "/plan change",
        "/goal done",
        "/mcp extra",
    ] {
        assert!(
            catalog
                .resolve(input, fixture.reader.as_ref(), 1024)
                .await
                .is_err(),
            "{input}"
        );
    }
    let names: Vec<_> = catalog.commands().map(|spec| spec.name.as_str()).collect();
    assert_eq!(names, ["compact", "diff", "goal", "mcp", "model", "plan"]);
    for (prefix, expected) in [
        ("/m", vec!["mcp", "model"]),
        ("pla", vec!["plan"]),
        ("z", vec![]),
    ] {
        assert_eq!(
            catalog
                .complete(prefix)
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>(),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn imported_prompt_and_skill_cases_keep_the_selected_revision() -> Result<()> {
    use CommandScope::{Project, User};
    let fixture = Fixture::new(&[
        ("home/.claude/commands/review.md", "User $ARGUMENTS"),
        ("project/.claude/commands/review.md", "---\ndescription: Review changes\nallowed-tools: [read]\n---\nProject $ARGUMENTS"),
        ("project/.codex/prompts/test.md", "Run tests"),
        ("project/.claude/commands/git/status.md", "Inspect status"),
        ("skills/audit/SKILL.md", "---\nname: audit\ndescription: Audit code and @missing.txt\nallowed-tools: [read]\n---\nInspect carefully"),
        ("project/readme.txt", "before"),
    ]).await?;
    // Reverse declaration order intentionally: precedence is data, not arrival order.
    let catalog = fixture
        .catalog(&[("project", Project), ("home", User)], &["skills"])
        .await?;
    let descriptors = serde_json::to_string(&catalog).expect("catalog JSON");
    assert!(descriptors.contains("Review changes"));
    assert!(descriptors.contains("Audit code"));
    assert!(!descriptors.contains("Inspect carefully"));
    fixture
        .put(
            "project/.claude/commands/review.md",
            "changed",
            "edit-command",
        )
        .await?;
    fixture
        .put("skills/audit/SKILL.md", "broken", "edit-skill")
        .await?;
    for (input, expected) in [
        ("/review auth", "Project auth"),
        ("/test suite", "Run tests\nsuite"),
        ("/git:status", "Inspect status"),
        ("/audit files", "Inspect carefully\nfiles"),
    ] {
        let CommandResolution::Prompt {
            text,
            source: Some(source),
            ..
        } = catalog
            .resolve(input, fixture.reader.as_ref(), 1024)
            .await?
        else {
            panic!("expected pinned prompt: {input}");
        };
        assert_eq!(text, expected, "{input}");
        assert!(source.path.ends_with(".md"));
        if input == "/audit files" {
            let workspace = ContextRoot {
                directory: "project".into(),
                ..fixture.root.clone()
            };
            for (input, count) in [("/audit", 0), ("/audit @readme.txt", 1)] {
                let CommandResolution::Prompt { text, mentions, .. } = catalog
                    .resolve_input(
                        input,
                        &workspace,
                        &source.generation,
                        fixture.reader.as_ref(),
                        1024,
                    )
                    .await?
                else {
                    panic!("skill composer prompt");
                };
                assert!(!text.contains("description:"));
                assert_eq!(mentions.len(), count, "{input}");
                if let Some(mention) = mentions.first() {
                    assert_eq!(mention.file.path(), "project/readme.txt");
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_catalog_cases_are_atomic_errors() -> Result<()> {
    use CommandScope::Project;
    let cases: &[&[(&str, &str)]] = &[
        &[(".claude/commands/model.md", "shadow builtin")],
        &[
            (".claude/commands/a.md", "one"),
            (".codex/prompts/a.md", "two"),
        ],
        &[(".claude/commands/a.md", "---\ndescription: missing close\n")],
        &[(
            ".claude/commands/a.md",
            "---\ndescription: one\ndescription: two\n---\nbody",
        )],
        &[(".claude/commands/bad name.md", "body")],
        &[(".claude/commands/a.md", "---\ndescription: ''\n---\nbody")],
    ];
    for files in cases {
        let fixture = Fixture::new(files).await?;
        assert!(
            fixture.catalog(&[("", Project)], &[]).await.is_err(),
            "{files:?}"
        );
    }
    let fixture = Fixture::new(&[
        (".claude/commands/audit.md", "command"),
        (
            "skills/a/SKILL.md",
            "---\nname: audit\ndescription: Audit\n---\nbody",
        ),
    ])
    .await?;
    assert!(
        fixture
            .catalog(&[("", Project)], &["skills"])
            .await
            .is_err()
    );
    let fixture = Fixture::new(&[]).await?;
    assert_eq!(
        fixture
            .catalog(&[("", Project)], &[])
            .await?
            .commands()
            .count(),
        6
    );
    Ok(())
}

#[tokio::test]
async fn mention_cases_resolve_exact_files_at_the_command_revision() -> Result<()> {
    let fixture = Fixture::new(&[
        ("home/.claude/commands/read-home.md", "Read $ARGUMENTS"),
        ("project/.claude/commands/read.md", "Read $ARGUMENTS"),
        ("project/a.txt", "before"),
        ("project/space name.txt", "spaces"),
        ("project/λ.txt", "unicode"),
    ])
    .await?;
    let catalog = fixture
        .catalog(
            &[
                ("project", CommandScope::Project),
                ("home", CommandScope::User),
            ],
            &[],
        )
        .await?;
    let CommandResolution::Prompt {
        text,
        source: Some(source),
        ..
    } = catalog
        .resolve("/read @a.txt", fixture.reader.as_ref(), 1024)
        .await?
    else {
        panic!("prompt");
    };
    fixture.put("project/a.txt", "after", "edit-file").await?;
    let mentions = resolve_file_mentions(
        &text,
        &source.root,
        &source.generation,
        fixture.reader.as_ref(),
        1024,
    )
    .await?;
    assert_eq!(mentions.len(), 1);
    assert_eq!(mentions[0].file.path(), "project/a.txt");
    assert_eq!(mentions[0].file.descriptor().byte_length(), 6);
    let CommandResolution::Prompt {
        mentions,
        source: Some(home_source),
        ..
    } = catalog
        .resolve_input(
            "/read-home @a.txt",
            &source.root,
            &source.generation,
            fixture.reader.as_ref(),
            1024,
        )
        .await?
    else {
        panic!("expanded composer prompt");
    };
    assert!(home_source.path.starts_with("home/"));
    assert_eq!(mentions[0].file.path(), "project/a.txt");
    assert_eq!(mentions[0].file.descriptor().byte_length(), 6);
    for (text, expected) in [
        ("@a.txt @a.txt", vec!["project/a.txt", "project/a.txt"]),
        ("Read @\"space name.txt\"", vec!["project/space name.txt"]),
        ("λ @λ.txt", vec!["project/λ.txt"]),
        ("mail@example.com \\@a.txt", vec![]),
    ] {
        let mentions = resolve_file_mentions(
            text,
            &source.root,
            &source.generation,
            fixture.reader.as_ref(),
            1024,
        )
        .await?;
        assert_eq!(
            mentions
                .iter()
                .map(|mention| mention.file.path())
                .collect::<Vec<_>>(),
            expected,
            "{text}"
        );
        for mention in mentions {
            assert!(text[mention.start..mention.end].starts_with('@'));
        }
    }
    for text in [
        "@../a.txt",
        "@/etc/passwd",
        "@.git/config",
        "@missing",
        "@\"unclosed",
        "@",
        "@\"a.txt\"suffix",
    ] {
        assert!(
            resolve_file_mentions(
                text,
                &source.root,
                &source.generation,
                fixture.reader.as_ref(),
                1024
            )
            .await
            .is_err(),
            "{text}"
        );
    }
    Ok(())
}

struct DeniedReader;
impl ContentResidencyVerifier for DeniedReader {}

#[tokio::test]
async fn builtins_are_pure_and_declared_reads_require_authority() -> Result<()> {
    let catalog = CommandCatalog::builtins()?;
    assert!(catalog.resolve("/model", &DeniedReader, 1024).await.is_ok());
    let fixture = Fixture::new(&[(".claude/commands/read.md", "body")]).await?;
    let roots = [CommandRoot {
        scope: CommandScope::Project,
        root: fixture.root.clone(),
    }];
    assert!(
        CommandCatalog::capture(
            &DeniedReader,
            &roots,
            &[],
            ContextDiscoveryLimits::default()
        )
        .await
        .is_err()
    );
    let catalog = fixture.catalog(&[("", CommandScope::Project)], &[]).await?;
    assert!(catalog.resolve("/read", &DeniedReader, 1024).await.is_err());
    Ok(())
}

#[tokio::test]
async fn caller_bounds_apply_to_discovery_bodies_and_expansion() -> Result<()> {
    let fixture = Fixture::new(&[
        (
            ".claude/commands/repeat.md",
            "$ARGUMENTS$ARGUMENTS$ARGUMENTS",
        ),
        (
            "skills/audit/SKILL.md",
            "---\nname: audit\ndescription: Audit\n---\nbody",
        ),
    ])
    .await?;
    let roots = [CommandRoot {
        scope: CommandScope::Project,
        root: fixture.root.clone(),
    }];
    for limits in [
        ContextDiscoveryLimits {
            entries: 1,
            ..ContextDiscoveryLimits::default()
        },
        ContextDiscoveryLimits {
            directories: 1,
            ..ContextDiscoveryLimits::default()
        },
        ContextDiscoveryLimits {
            entries: 0,
            ..ContextDiscoveryLimits::default()
        },
    ] {
        assert!(
            CommandCatalog::capture(fixture.reader.as_ref(), &roots, &[], limits)
                .await
                .is_err()
        );
    }
    let catalog = fixture.catalog(&[("", CommandScope::Project)], &[]).await?;
    for (input, bound) in [
        ("/repeat", 0),
        ("/repeat", 6),
        ("/repeat abcdefghijklmnop", 30),
        ("/repeat", 10),
    ] {
        assert!(
            catalog
                .resolve(input, fixture.reader.as_ref(), bound)
                .await
                .is_err(),
            "{input} {bound}"
        );
    }
    let expected: Value = json!({"kind":"prompt","text":"xyzxyzxyz"});
    let actual = serde_json::to_value(
        catalog
            .resolve("/repeat xyz", fixture.reader.as_ref(), 64)
            .await?,
    )
    .expect("JSON");
    assert_eq!(actual["text"], expected["text"]);
    let skill_roots = [ContextRoot {
        directory: "skills".into(),
        ..fixture.root.clone()
    }];
    assert!(
        CommandCatalog::capture(
            fixture.reader.as_ref(),
            &roots,
            &skill_roots,
            ContextDiscoveryLimits {
                directories: 4,
                ..ContextDiscoveryLimits::default()
            }
        )
        .await
        .is_err()
    );
    let large = "x".repeat(40_000);
    fixture
        .put(".claude/commands/large.md", &large, "large-command")
        .await?;
    let catalog = fixture.catalog(&[("", CommandScope::Project)], &[]).await?;
    assert!(
        catalog
            .resolve("/large", fixture.reader.as_ref(), 1024)
            .await
            .is_err()
    );
    let fixture =
        Fixture::new(&[(".claude/commands/a.md", "---\ndescription: A\n---\nbody")]).await?;
    let roots = [CommandRoot {
        scope: CommandScope::Project,
        root: fixture.root.clone(),
    }];
    for header_bytes in [1, 3, 4, 12] {
        assert!(
            CommandCatalog::capture(
                fixture.reader.as_ref(),
                &roots,
                &[],
                ContextDiscoveryLimits {
                    header_bytes,
                    ..ContextDiscoveryLimits::default()
                }
            )
            .await
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn mention_bytes_are_cumulative_and_duplicate_paths_read_once() -> Result<()> {
    let body = "x".repeat(100);
    let fixture = Fixture::new(&[
        (".claude/commands/read.md", "body"),
        ("a.txt", &body),
        ("b.txt", &body),
        ("empty.txt", ""),
    ])
    .await?;
    let catalog = fixture.catalog(&[("", CommandScope::Project)], &[]).await?;
    let CommandResolution::Prompt {
        source: Some(source),
        ..
    } = catalog
        .resolve("/read", fixture.reader.as_ref(), 256)
        .await?
    else {
        panic!("pinned source");
    };
    for (input, maximum_bytes, succeeds) in [
        ("@a.txt @a.txt", 150, true),
        ("@a.txt @b.txt", 150, false),
        ("@a.txt @empty.txt", 100, true),
        ("@a.txt @empty.txt @a.txt", 100, true),
        ("@empty.txt @a.txt", 100, true),
        ("@a.txt @b.txt", 100, false),
    ] {
        assert_eq!(
            resolve_file_mentions(
                input,
                &source.root,
                &source.generation,
                fixture.reader.as_ref(),
                maximum_bytes
            )
            .await
            .is_ok(),
            succeeds
        );
    }
    Ok(())
}
