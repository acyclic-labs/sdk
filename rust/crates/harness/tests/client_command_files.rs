use super::*;
use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandFileSnapshot, CommandPaletteState, CommandQuery},
    context::PinnedContextPath,
};
use proptest::prelude::*;
use std::collections::BTreeMap;

fn snapshot() -> CommandFileSnapshot {
    BTreeMap::from([
        (
            CommandFileRoot::Config,
            BTreeMap::from([
                (
                    "settings.json".into(),
                    json!({"diff_mode":"auto","diff_modes":["auto","unified","split"],"retained":"settings"}),
                ),
                ("theme.json".into(), json!({"retained":"theme"})),
            ]),
        ),
        (
            CommandFileRoot::Task,
            BTreeMap::from([(
                "settings.json".into(),
                json!({"agent_mode":"code","agent_modes":["code","plan"],"retained":"task"}),
            )]),
        ),
        (
            CommandFileRoot::Session,
            BTreeMap::from([
                (
                    "layout.json".into(),
                    json!({"retained":"layout","selected_task":"00000000-0000-0000-0000-000000000005"}),
                ),
                (
                    "attention.json".into(),
                    json!({"tasks":["00000000-0000-0000-0000-000000000003","00000000-0000-0000-0000-000000000004"]}),
                ),
            ]),
        ),
    ])
}
async fn source(fixture: &Fixture, path: &str) -> Result<PinnedContextPath> {
    let page = fixture
        .reader
        .list_private_directory(&fixture.root.volume, "", "", None, None, 128, None, None)?
        .await?;
    Ok(PinnedContextPath {
        root: fixture.root.clone(),
        generation: page.generation,
        path: path.into(),
    })
}

#[tokio::test]
async fn catalog_reads_the_file_at_the_query_cut() -> Result<()> {
    let defaults = include_str!("../src/client/commands.json");
    let fixture = Fixture::new(&[("config/commands.json", defaults)]).await?;
    let pinned = source(&fixture, "config/commands.json").await?;
    let mut changed: Value = serde_json::from_str(defaults).expect("definitions");
    changed[0]["description"] = json!("Changed through the config volume");
    fixture
        .put("config/commands.json", &changed.to_string(), "edit-catalog")
        .await?;
    let before = CommandCatalog::from_file(&pinned, fixture.reader.as_ref(), 32_768).await?;
    let after = CommandCatalog::from_file(
        &source(&fixture, "config/commands.json").await?,
        fixture.reader.as_ref(),
        32_768,
    )
    .await?;
    assert_ne!(
        before
            .commands()
            .find(|spec| spec.name == "compact")
            .unwrap()
            .description,
        after
            .commands()
            .find(|spec| spec.name == "compact")
            .unwrap()
            .description
    );
    assert!(
        CommandCatalog::from_file(&pinned, &DeniedReader, 32_768)
            .await
            .is_err()
    );
    assert!(
        CommandCatalog::from_file(&pinned, fixture.reader.as_ref(), 1)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn each_command_dispatches_through_ordinary_volume_files() -> Result<()> {
    let fixture = Fixture::new(&[]).await?;
    let catalog = CommandCatalog::builtins()?;
    for spec in catalog.commands() {
        let arguments = if matches!(
            spec.action,
            acyclic_harness::client::CommandAction::Tool { .. }
        ) {
            " echo @missing"
        } else {
            ""
        };
        let resolution = catalog
            .resolve(&format!("/{}{arguments}", spec.name), &DeniedReader, 1024)
            .await?;
        let changes = resolution.file_changes(&snapshot(), OperationId::new())?;
        assert_eq!(changes.len(), 1, "{}", spec.name);
        for change in changes {
            let prefix = match change.root {
                CommandFileRoot::Config => "config",
                CommandFileRoot::Session => "session",
                CommandFileRoot::Task => "task",
            };
            let path = format!("{prefix}/{}", change.path);
            assert_eq!(change.create_only, change.path.starts_with("requests/"));
            if change.create_only {
                assert!(change.path.starts_with("requests/"));
            }
            fixture
                .put(&path, &change.contents.to_string(), &spec.name)
                .await?;
            let (_, bytes) = source(&fixture, &path)
                .await?
                .read(fixture.reader.as_ref(), 32_768)
                .await?;
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).expect("published JSON"),
                change.contents
            );
            if matches!(spec.name.as_str(), "delete" | "clearall") {
                assert_eq!(change.root, CommandFileRoot::Session);
                assert!(change.contents.get("confirmation").is_some());
            }
        }
    }
    Ok(())
}

#[test]
fn direct_requests_do_not_consult_unrelated_broken_config_files() -> Result<()> {
    let catalog = CommandCatalog::builtins()?;
    let broken = BTreeMap::from([
        (
            CommandFileRoot::Session,
            BTreeMap::from([("layout.json".into(), json!(42))]),
        ),
        (
            CommandFileRoot::Config,
            BTreeMap::from([("settings.json".into(), Value::Null)]),
        ),
    ]);
    for input in ["/shell echo hi", "/delegate task", "/compact"] {
        let resolution = futures::executor::block_on(catalog.resolve(input, &DeniedReader, 1024))?;
        let changes = resolution.file_changes(&broken, OperationId::new())?;
        assert_eq!(changes[0].root, CommandFileRoot::Task);
    }
    Ok(())
}

#[test]
fn catalog_files_cannot_disable_delete_confirmation_or_add_approval_prompts() {
    for (kind, confirmation, accepted) in [
        ("delete", false, false),
        ("delete", true, true),
        ("clear_all", false, false),
        ("clear_all", true, true),
        ("new", true, false),
        ("new", false, true),
    ] {
        let bytes = serde_json::to_vec(&json!([{"name":"action","description":"Action","arguments":"none","action":{"kind":"request","request":{"kind":kind},"confirmation":confirmation}}])).unwrap();
        assert_eq!(CommandCatalog::from_bytes(&bytes, 1024).is_ok(), accepted);
    }
}

#[test]
fn task_settings_and_session_controls_have_distinct_roots() -> Result<()> {
    let catalog = CommandCatalog::builtins()?;
    for (input, root) in [
        ("/model provider/model", CommandFileRoot::Task),
        ("/mode", CommandFileRoot::Task),
        ("/quit", CommandFileRoot::Session),
        ("/tree", CommandFileRoot::Session),
        ("/reasoning", CommandFileRoot::Config),
    ] {
        let resolution = futures::executor::block_on(catalog.resolve(input, &DeniedReader, 1024))?;
        let changes = resolution.file_changes(&snapshot(), OperationId::new())?;
        assert_eq!(changes[0].root, root, "{input}");
        assert!(!changes[0].create_only);
    }
    for (input, root) in [
        ("/stop", CommandFileRoot::Task),
        ("/resume", CommandFileRoot::Task),
        ("/stopall", CommandFileRoot::Session),
        ("/resumeall", CommandFileRoot::Session),
    ] {
        let resolution = futures::executor::block_on(catalog.resolve(input, &DeniedReader, 1024))?;
        let changes = resolution.file_changes(&snapshot(), OperationId::new())?;
        assert_eq!(changes[0].root, root);
        assert!(changes[0].create_only);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Navigation after a deletion request must not retarget the persisted
    // confirmation. The operation and task are bound before another query cut.
    #[test]
    fn confirmation_keeps_its_target_across_navigation(
        selected in 1_u128..u128::MAX, next in 1_u128..u128::MAX,
    ) {
        let selected = uuid::Uuid::from_u128(selected).to_string();
        let next = uuid::Uuid::from_u128(next).to_string();
        let mut state = snapshot();
        let session = state.get_mut(&CommandFileRoot::Session).unwrap();
        session.get_mut("layout.json").unwrap()["selected_task"] = json!(selected);
        session.insert("attention.json".into(), json!({"tasks":[next]}));
        let operation = OperationId::new();
        let confirm = CommandResolution::Confirm { request: acyclic_harness::client::CommandRequest::Delete };
        let pending = confirm.file_changes(&state, operation).unwrap();
        prop_assert_eq!(&pending[0].contents["confirmation"]["task"], &json!(selected));
        prop_assert_eq!(&pending[0].contents["confirmation"]["operation"], &json!(operation));
        state.get_mut(&CommandFileRoot::Session).unwrap().insert("layout.json".into(), pending[0].contents.clone());
        let navigation = CommandResolution::Select { query: CommandQuery::Attention }.file_changes(&state, OperationId::new()).unwrap();
        prop_assert_eq!(&navigation[0].contents["selected_task"], &json!(next));
        prop_assert_eq!(&navigation[0].contents["confirmation"], &pending[0].contents["confirmation"]);
    }

    // Arbitrary catalog descriptions cannot bypass destructive confirmation or
    // turn ordinary requests into approval prompts.
    #[test]
    fn confirmation_is_a_catalog_invariant(kind in 0_usize..3, confirmation in any::<bool>(), description in "[a-z]{1,80}") {
        let kind = ["new", "delete", "clear_all"][kind];
        let bytes = serde_json::to_vec(&json!([{"name":"action","description":description,"arguments":"none","action":{"kind":"request","request":{"kind":kind},"confirmation":confirmation}}])).unwrap();
        prop_assert_eq!(CommandCatalog::from_bytes(&bytes, 2048).is_ok(), confirmation == (kind != "new"));
    }

    // Cycling every configured choice returns to the original file value,
    // never invents a mode and preserves unrelated settings.
    #[test]
    fn configured_cycles_close(choices in proptest::collection::btree_set("[a-z]{1,12}", 1..8), selected in any::<usize>(), extra in ".{0,40}") {
        let choices: Vec<_> = choices.into_iter().collect();
        let selected = selected % choices.len();
        let initial = choices[selected].clone();
        let mut snapshot = BTreeMap::from([(CommandFileRoot::Task, BTreeMap::from([("settings.json".into(), json!({"agent_modes":choices,"agent_mode":initial,"extra":extra}))]))]);
        let resolution = CommandResolution::Submit { request: acyclic_harness::client::CommandRequest::CycleMode };
        for _ in 0..choices.len() {
            let changes = resolution.file_changes(&snapshot, OperationId::new()).unwrap();
            prop_assert_eq!(&changes[0].contents["extra"], &snapshot[&CommandFileRoot::Task]["settings.json"]["extra"]);
            prop_assert!(choices.iter().any(|choice| changes[0].contents["agent_mode"] == json!(choice)));
            snapshot.get_mut(&CommandFileRoot::Task).unwrap().insert("settings.json".into(), changes[0].contents.clone());
        }
        prop_assert_eq!(&snapshot[&CommandFileRoot::Task]["settings.json"]["agent_mode"], &json!(initial));
    }

    // Accepted definition files round-trip through the same validated loader;
    // canonicalization preserves every command and alias.
    #[test]
    fn catalog_file_round_trips(description in "[a-zA-Z0-9 λ]{1,80}") {
        let mut data: Value = serde_json::from_str(include_str!("../src/client/commands.json")).unwrap();
        data[0]["description"] = json!(description);
        let bytes = serde_json::to_vec(&data).unwrap();
        let result = CommandCatalog::from_bytes(&bytes, 32_768);
        prop_assert_eq!(result.is_ok(), !description.trim().is_empty());
        if let Ok(catalog) = result {
            let specs: Vec<_> = catalog.commands().collect();
            let encoded = serde_json::to_vec(&specs).unwrap();
            let decoded = CommandCatalog::from_bytes(&encoded, 32_768).unwrap();
            prop_assert_eq!(decoded.commands().collect::<Vec<_>>(), specs);
        }
    }

    // File-derived toggles preserve unrelated keys and two published toggles
    // restore the observed boolean. No hidden in-memory state is required.
    #[test]
    fn file_toggles_preserve_other_fields(initial in any::<bool>(), extra in ".{0,80}", index in 0_usize..4) {
        let (query, file, field) = [
            (CommandQuery::Reasoning, "settings.json", "reasoning"),
            (CommandQuery::Details, "settings.json", "tool_details"),
            (CommandQuery::Tree, "layout.json", "tree"),
            (CommandQuery::Terminal, "layout.json", "terminal"),
        ][index];
        let root = if file == "settings.json" { CommandFileRoot::Config } else { CommandFileRoot::Session };
        let mut state = BTreeMap::from([(root, BTreeMap::from([(file.into(), json!({field:initial,"extra":extra}))]))]);
        let resolution = CommandResolution::Select { query };
        for expected in [!initial, initial] {
            let changes = resolution.file_changes(&state, OperationId::new()).unwrap();
            prop_assert_eq!(changes.len(), 1);
            prop_assert_eq!(changes[0].path.as_str(), file);
            prop_assert_eq!(&changes[0].contents["extra"], &state[&root][file]["extra"]);
            prop_assert_eq!(&changes[0].contents[field], &json!(expected));
            state.get_mut(&root).unwrap().insert(file.into(), changes[0].contents.clone());
        }
    }

    // A palette-file snapshot can contain any usize selection: projection and
    // both navigation directions stay within the live match set and round-trip.
    #[test]
    fn palette_file_selection_is_bounded(selected in any::<u64>(), filter in "[a-z]{0,5}") {
        let catalog = CommandCatalog::builtins().unwrap();
        let state = CommandPaletteState { filter, selected };
        let encoded = serde_json::to_vec(&state).unwrap();
        let decoded: CommandPaletteState = serde_json::from_slice(&encoded).unwrap();
        let palette = catalog.palette(&decoded);
        for next in [palette.next(), palette.previous()] {
            if palette.entries().is_empty() { prop_assert_eq!(next.selected, 0); }
            else { prop_assert!(next.selected < palette.entries().len() as u64); }
        }
        prop_assert_eq!(state, decoded);
    }
}
