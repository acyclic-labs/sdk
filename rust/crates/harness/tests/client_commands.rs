//! Data-driven composer cases against the production filesystem reader.

use acyclic_harness::Error;
use acyclic_harness::{
    Result,
    client::{CommandCatalog, CommandResolution, CommandRoot, CommandScope, resolve_file_mentions},
    context::{ContextDiscoveryLimits, ContextRoot},
    conversation::ContentResidencyVerifier,
};
use serde_json::{Value, json};

#[cfg(not(target_arch = "wasm32"))]
fn volume_error(error: acyclic_harness::task_volume::Error) -> acyclic_harness::Error {
    acyclic_harness::Error::Storage(error.to_string())
}

#[cfg(all(not(target_arch = "wasm32"), feature = "terminal"))]
mod client_command_driver;
mod client_command_files;
#[cfg(not(target_arch = "wasm32"))]
mod client_command_owners;
#[cfg(not(target_arch = "wasm32"))]
mod client_command_parity;
#[cfg(not(target_arch = "wasm32"))]
mod client_command_parser;
#[cfg(not(target_arch = "wasm32"))]
mod client_command_session;
#[cfg(not(target_arch = "wasm32"))]
mod client_command_views;
#[cfg(not(target_arch = "wasm32"))]
#[path = "../src/client/commands/name_index.rs"]
mod command_name_index;
#[cfg(not(target_arch = "wasm32"))]
#[path = "../../harness-fuzz/command_oracle.rs"]
mod command_oracle;
#[cfg(not(target_arch = "wasm32"))]
#[path = "../src/client/commands/parse.rs"]
mod command_parse;
#[cfg(not(target_arch = "wasm32"))]
#[path = "../src/client/commands/syntax.rs"]
mod command_syntax;
#[cfg(not(target_arch = "wasm32"))]
#[path = "../src/context/discovery/frontmatter.rs"]
mod frontmatter;

#[path = "support/client_files.rs"]
mod client_files;
use client_files::Fixture;

#[path = "client_mention_attachments.rs"]
mod attachments;
#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "filesystem-local",
    feature = "test-support"
))]
#[path = "support/client_local.rs"]
mod client_local;
#[cfg(not(target_arch = "wasm32"))]
#[path = "client_mention_provider.rs"]
mod mention_provider;

impl Fixture {
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

#[tokio::test]
async fn resolved_mentions_attach_to_the_stock_model_input_at_the_pinned_revision() -> Result<()> {
    use acyclic_harness::{
        filesystem::ModelTaskCommand,
        model::{FileProjectionPolicy, ModelContent, ModelContentPart},
    };
    let fixture = Fixture::new(&[
        (".claude/commands/read.md", "Read $ARGUMENTS"),
        ("a.txt", "before"),
        ("b.txt", "bee"),
        ("space name.txt", "spaces"),
    ])
    .await?;
    let catalog = fixture.catalog(&[("", CommandScope::Project)], &[]).await?;
    let CommandResolution::Prompt {
        source: Some(source),
        ..
    } = catalog
        .resolve("/read", fixture.reader.as_ref(), 1024)
        .await?
    else {
        panic!("pinned command");
    };
    fixture.put("a.txt", "after", "later-edit").await?;
    for (input, text, expected) in [
        ("hello", "hello", vec![]),
        ("@a.txt", "@a.txt", vec![("a.txt", "before")]),
        (
            "@b.txt @a.txt @b.txt",
            "@b.txt @a.txt @b.txt",
            vec![("b.txt", "bee"), ("a.txt", "before")],
        ),
        (
            "/read @\"space name.txt\"",
            "Read @\"space name.txt\"",
            vec![("space name.txt", "spaces")],
        ),
        (
            "mail@example.com \\@a.txt",
            "mail@example.com \\@a.txt",
            vec![],
        ),
    ] {
        let resolved = catalog
            .resolve_input(
                input,
                &source.root,
                &source.generation,
                fixture.reader.as_ref(),
                1024,
            )
            .await?;
        let CommandResolution::Prompt {
            text: prepared_text,
            ..
        } = &resolved
        else {
            panic!("prompt");
        };
        let allocation = prepared_text.as_ptr();
        let content = resolved.into_model_input()?;
        let (prepared_text, files) = match &content {
            ModelContent::Text(text) => (text, &[][..]),
            ModelContent::Parts(parts) => {
                let Some(ModelContentPart::Text { text }) = parts.first() else {
                    panic!("prompt text first");
                };
                (text, &parts[1..])
            }
            _ => panic!("complete prompt input"),
        };
        assert_eq!(prepared_text, text);
        assert_eq!(prepared_text.as_ptr(), allocation, "prompt text must move");
        assert_eq!(files.len(), expected.len());
        for (part, (path, body)) in files.iter().zip(expected) {
            let ModelContentPart::File { file, policy } = part else {
                panic!("file attachment");
            };
            assert_eq!(file.path(), path);
            assert_eq!(policy, &FileProjectionPolicy::BoundedFull);
            assert_eq!(
                fixture.reader.read(file, 1024, None)?.await?.as_ref(),
                body.as_bytes()
            );
        }
        // Submit uses the existing command schema, retaining immutable refs rather
        // than copying file bodies into a second prompt format.
        let command = ModelTaskCommand {
            context_manifest: source.read(fixture.reader.as_ref(), 1024).await?.0,
            summary_source: None,
            input: content,
            selected_context: None,
            max_steps: 1,
            max_output_tokens: None,
            mailbox_operation: None,
            selected_output_volume: None,
        };
        let encoded = serde_json::to_vec(&command).unwrap();
        let submitted: ModelTaskCommand = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(submitted, command);
        submitted.input.validate_user_input(&mut None)?;
        submitted
            .input
            .validate_limits(Default::default(), &mut None)?;
    }
    for input in ["/diff", "/compact now", "/model provider/model", ""] {
        assert!(
            catalog
                .resolve_input(
                    input,
                    &source.root,
                    &source.generation,
                    fixture.reader.as_ref(),
                    1024
                )
                .await?
                .into_model_input()
                .is_err(),
            "{input}"
        );
    }
    Ok(())
}
