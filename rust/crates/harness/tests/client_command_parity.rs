use super::*;
use acyclic_harness::client::{
    CommandAction, CommandArguments, CommandPaletteState, CommandRequest, CommandTaskScope,
};
use proptest::prelude::*;

#[test]
fn unicode_theme_resolution_matches_completion() -> Result<()> {
    let catalog = CommandCatalog::builtins()?.with_themes(["Éclair Light".into(), "ΟΣ".into()])?;
    assert_eq!(
        catalog.complete_input("/theme écl")[0].replacement,
        "/theme Éclair Light"
    );
    assert_eq!(
        futures::executor::block_on(catalog.resolve("/theme ος", &DeniedReader, 1024))?,
        CommandResolution::Submit {
            request: CommandRequest::Theme {
                name: "ΟΣ".into()
            }
        }
    );
    assert_eq!(
        futures::executor::block_on(catalog.resolve("/theme éclair light", &DeniedReader, 1024))?,
        CommandResolution::Submit {
            request: CommandRequest::Theme {
                name: "Éclair Light".into()
            }
        }
    );
    Ok(())
}

#[test]
fn catalog_aliases_dispatch_without_model_text_or_reads() -> Result<()> {
    let catalog = CommandCatalog::builtins()?;
    assert_eq!(catalog.commands().count(), 31);
    for spec in catalog.commands() {
        let argument = match spec.arguments {
            CommandArguments::RequiredText => " echo @missing.txt",
            _ => "",
        };
        let canonical = futures::executor::block_on(catalog.resolve(
            &format!("/{}{argument}", spec.name),
            &DeniedReader,
            1024,
        ))?;
        assert!(!matches!(canonical, CommandResolution::Prompt { .. }));
        for alias in &spec.aliases {
            let resolved = futures::executor::block_on(catalog.resolve(
                &format!("/{alias}{argument}"),
                &DeniedReader,
                1024,
            ))?;
            assert_eq!(resolved, canonical, "{alias}");
        }
    }
    for name in [
        "approval",
        "killserver",
        "kill",
        "shutdown",
        "open",
        "browser",
        "unknown",
    ] {
        let error =
            futures::executor::block_on(catalog.resolve(&format!("/{name}"), &DeniedReader, 1024))
                .expect_err("unknown slash cannot become chat");
        assert!(error.to_string().contains("Unknown command"));
    }
    Ok(())
}

#[test]
fn subtree_controls_and_destructive_confirmation_are_distinct() -> Result<()> {
    use CommandResolution::{Confirm, Submit};
    let catalog = CommandCatalog::builtins()?;
    for (name, request) in [
        (
            "stop",
            CommandRequest::Pause {
                scope: CommandTaskScope::Subtree,
            },
        ),
        (
            "stopall",
            CommandRequest::Pause {
                scope: CommandTaskScope::All,
            },
        ),
        (
            "resume",
            CommandRequest::Resume {
                scope: CommandTaskScope::Subtree,
            },
        ),
        (
            "resumeall",
            CommandRequest::Resume {
                scope: CommandTaskScope::All,
            },
        ),
    ] {
        assert_eq!(
            futures::executor::block_on(catalog.resolve(&format!("/{name}"), &DeniedReader, 1024))?,
            Submit { request }
        );
    }
    for (name, request) in [
        ("delete", CommandRequest::Delete),
        ("clearall", CommandRequest::ClearAll),
    ] {
        assert_eq!(
            futures::executor::block_on(catalog.resolve(&format!("/{name}"), &DeniedReader, 1024))?,
            Confirm { request }
        );
    }
    Ok(())
}

#[test]
fn completions_and_palette_share_the_catalog_and_theme_data() -> Result<()> {
    let catalog =
        CommandCatalog::builtins()?.with_themes(["Solarized Dark".into(), "Dracula".into()])?;
    let names: Vec<_> = catalog
        .complete("")
        .map(|spec| spec.name.as_str())
        .collect();
    assert_eq!(&names[..4], &["delegate", "fetch", "search", "shell"]);
    for (input, replacement, label) in [
        ("/sh", "/shell ", "/shell [tool]"),
        ("/curl", "/fetch ", "/fetch [tool]"),
        ("/them", "/theme ", "/theme"),
        ("/theme sol", "/theme Solarized Dark", "Solarized Dark"),
    ] {
        assert!(
            catalog
                .complete_input(input)
                .iter()
                .any(|item| item.replacement == replacement && item.label == label),
            "{input}"
        );
    }
    for input in [
        "shell",
        "ordinary text",
        "/shell argument",
        "/model argument",
        "/unknown argument",
    ] {
        assert!(catalog.complete_input(input).is_empty(), "{input}");
    }
    for (filter, name) in [
        ("curl", "fetch"),
        ("transcript", "find"),
        ("RESUMEALL", "resumeall"),
    ] {
        let state = CommandPaletteState {
            filter: filter.into(),
            selected: 0,
        };
        let palette = catalog.palette(&state);
        assert_eq!(palette.selected().expect("match").name, name);
    }
    assert!(
        CommandCatalog::builtins()?
            .with_themes(["A".into(), "a".into()])
            .is_err()
    );
    for bad in ["", " a", "a ", "a\n", "\0"] {
        assert!(
            CommandCatalog::builtins()?
                .with_themes([bad.into()])
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn aliases_are_reserved_against_prompts_and_skills() -> Result<()> {
    for name in ["q", "help", "curl", "ra", "next"] {
        let path = format!(".claude/commands/{name}.md");
        let fixture = Fixture::new(&[(&path, "override")]).await?;
        assert!(
            fixture
                .catalog(&[("", CommandScope::Project)], &[])
                .await
                .is_err(),
            "{name}"
        );
        let body = format!("---\nname: {name}\ndescription: Override\n---\nbody");
        let fixture = Fixture::new(&[("skills/a/SKILL.md", &body)]).await?;
        assert!(fixture.catalog(&[], &["skills"]).await.is_err(), "{name}");
    }
    Ok(())
}

#[test]
fn syntax_oracle_detects_classification_and_bound_mutants() {
    use proptest::test_runner::{Config, TestRunner};
    for mutation in ["slash", "bound"] {
        let mut runner = TestRunner::new(Config {
            cases: 32,
            failure_persistence: None,
            ..Config::default()
        });
        let result = runner.run(&"[a-zA-Z0-9 λ]{1,80}", |text| {
            let input = format!("/{text}");
            let maximum = if mutation == "bound" {
                input.len() as u64 - 1
            } else {
                1024
            };
            // Deliberate bugs: forward slash commands as chat, or bypass the
            // caller's byte allowance. The shared fuzz oracle must reject both.
            let broken = if mutation == "slash" {
                Ok(None)
            } else {
                command_syntax::split(&input, u64::MAX)
            };
            prop_assert!(command_oracle::accepts(&input, maximum, &broken));
            Ok(())
        });
        assert!(result.is_err(), "oracle missed {mutation} mutation");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn unicode_theme_completion_and_submission_agree(chars in proptest::collection::vec(any::<char>().prop_filter("theme text", |c| !c.is_control()), 0..48)) {
        let theme = format!("É{} X", chars.into_iter().collect::<String>());
        let catalog = CommandCatalog::builtins().unwrap().with_themes([theme.clone()]).unwrap();
        let input = format!("/theme {}", theme.to_lowercase());
        prop_assert_eq!(&catalog.complete_input(&input)[0].replacement, &format!("/theme {theme}"));
        prop_assert_eq!(futures::executor::block_on(catalog.resolve(&input, &DeniedReader, 1024)).unwrap(),
            CommandResolution::Submit { request: CommandRequest::Theme { name: theme } });
    }

    #[test]
    fn case_does_not_change_command_arguments(index in 0_usize..30, text in "[a-zA-Z0-9 @:/_-]{1,80}") {
        let catalog = CommandCatalog::builtins().unwrap();
        let spec = catalog.commands().nth(index).unwrap();
        for text in [text.as_str(), " "] {
            let arguments = match spec.arguments {
                CommandArguments::None => String::new(),
                CommandArguments::Word => " MixedCase-Model".into(),
                _ => format!(" {text}"),
            };
            let resolve = |name: &str| futures::executor::block_on(catalog.resolve(
                &format!("/{name}{arguments}"), &DeniedReader, 1024,
            )).map_err(|error| error.to_string().replacen(&format!("/{name}"), "/<command>", 1));
            let expected = resolve(&spec.name);
            for name in std::iter::once(&spec.name).chain(&spec.aliases) {
                let upper = name.to_ascii_uppercase();
                prop_assert_eq!(resolve(&upper), expected.clone());
                prop_assert_eq!(catalog.complete(&upper).map(|s| &s.name).collect::<Vec<_>>(), catalog.complete(name).map(|s| &s.name).collect::<Vec<_>>());
                prop_assert_eq!(catalog.complete_input(&format!("/{upper}")), catalog.complete_input(&format!("/{name}")));
            }
        }
    }

    #[test]
    fn parser_matches_independent_oracle(input in ".{0,128}", maximum in 0_u64..256) {
        prop_assert!(command_oracle::accepts(&input, maximum, &command_syntax::split(&input, maximum)));
    }

    // Canonical formatting round-trips every accepted slash split, including
    // Unicode whitespace and empty command names/arguments.
    #[test]
    fn syntax_normalization_round_trips(
        input in proptest::collection::vec(any::<char>(), 0..128).prop_map(|c| c.into_iter().collect::<String>()),
    ) {
        let input = format!("/{input}");
        if let Some((name, arguments)) = command_syntax::split(&input, 1024).expect("bounded input") {
            let normalized = format!("/{name} {arguments}");
            prop_assert_eq!(command_syntax::split(&normalized, 1024).expect("normalized"), Some((name, arguments)));
        }
    }

    // Classification never forwards slash syntax to the model. Every successful
    // classification preserves its typed intent through the public JSON codec.
    #[test]
    fn arbitrary_input_preserves_chat_or_a_nonprompt_intent(
        input in proptest::collection::vec(any::<char>(), 0..96).prop_map(|c| c.into_iter().collect::<String>()),
        slash in any::<bool>(), maximum in 0_u64..512,
    ) {
        let catalog = CommandCatalog::builtins().expect("catalog");
        let input = if slash { format!("/{input}") } else { input };
        if let Ok(result) = futures::executor::block_on(catalog.resolve(&input, &DeniedReader, maximum)) {
            prop_assert!(maximum > 0 && input.len() as u64 <= maximum);
            if input.trim_start().starts_with('/') {
                prop_assert!(!matches!(&result, CommandResolution::Prompt { .. }), "slash syntax must remain a command");
            } else {
                prop_assert!(matches!(&result, CommandResolution::Prompt { text, .. } if text == &input), "chat text must be preserved");
            }
            let encoded = serde_json::to_vec(&result).expect("encode");
            prop_assert_eq!(serde_json::from_slice::<CommandResolution>(&encoded).expect("decode"), result);
        }
    }

    // Each palette movement wraps exactly once; empty matches stay unselected.
    #[test]
    fn palette_navigation_obeys_modular_selection(
        filter in "[a-z]{0,5}", moves in proptest::collection::vec(any::<bool>(), 0..256),
    ) {
        let catalog = CommandCatalog::builtins().expect("catalog");
        let mut state = CommandPaletteState { filter, selected: 0 };
        let count = catalog.palette(&state).entries().len();
        let mut expected = 0;
        for forward in moves {
            if forward {
                state = catalog.palette(&state).next();
                if count > 0 { expected = (expected + 1) % count; }
            } else {
                state = catalog.palette(&state).previous();
                if count > 0 { expected = (expected + count - 1) % count; }
            }
            let bytes = serde_json::to_vec(&state).expect("palette file");
            prop_assert_eq!(serde_json::from_slice::<CommandPaletteState>(&bytes).expect("query state"), state.clone());
            let palette = catalog.palette(&state);
            prop_assert_eq!(palette.selected().map(|s| s.name.as_str()), palette.entries().get(expected).map(|s| s.name.as_str()));
        }
    }

    // Direct tool arguments remain one JSON string. Empty text and NUL are
    // rejected, including after trimming arbitrary Unicode whitespace.
    #[test]
    fn tool_arguments_are_structured_or_rejected(
        index in 0_usize..4,
        arguments in proptest::collection::vec(prop_oneof![any::<char>(), Just('\0'), Just(' '), Just('\n')], 0..80)
            .prop_map(|chars| chars.into_iter().collect::<String>()),
    ) {
        let catalog = CommandCatalog::builtins().expect("catalog");
        let (name, key) = [("shell", "command"), ("delegate", "task"), ("search", "query"), ("fetch", "url")][index];
        let input = format!("/{name} {arguments}");
        let result = futures::executor::block_on(catalog.resolve(&input, &DeniedReader, 1024));
        let arguments = arguments.trim();
        if arguments.is_empty() || arguments.contains('\0') {
            prop_assert!(result.is_err());
        } else {
            let CommandResolution::Tool { input, .. } = result.expect("tool arguments") else {
                return Err(TestCaseError::fail("direct tool became another command kind"));
            };
            prop_assert_eq!(input, serde_json::json!({key: arguments}));
        }
    }

    // Every name and alias resolves to identical structured arguments, rather
    // than expanding shell, delegation, search or fetch text as a model prompt.
    #[test]
    fn aliases_preserve_the_structured_request(index in 0_usize..30, text in "[a-zA-Z0-9 @:/_-]{1,80}") {
        let catalog = CommandCatalog::builtins().expect("catalog");
        let spec = catalog.commands().nth(index).expect("entry");
        let arguments = match spec.arguments {
            CommandArguments::None => String::new(),
            CommandArguments::Word => " model-name".into(),
            _ => format!(" task {text}"),
        };
        let canonical = futures::executor::block_on(catalog.resolve(&format!("/{}{arguments}", spec.name), &DeniedReader, 1024)).expect("canonical");
        for alias in &spec.aliases {
            let actual = futures::executor::block_on(catalog.resolve(&format!("/{alias}{arguments}"), &DeniedReader, 1024)).expect("alias");
            prop_assert_eq!(actual, canonical.clone());
        }
        if matches!(spec.action, CommandAction::Tool { .. }) { prop_assert!(matches!(canonical, CommandResolution::Tool { .. }), "direct tool must bypass the model"); }
    }
}
